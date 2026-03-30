/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.spannergeo.functions;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2RegionCoverer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Cloud Run function backing the Spanner Remote UDF {@code geo.s2_covering_rect_v4}.
 *
 * <p>This is the v4 variant of the rectangle covering function, designed for the
 * range-scan schema where each POI stores a single leaf-level S2 Cell ID (level 30).
 * Unlike {@link S2CoveringRectFunction} which filters covering cells to levels
 * 12/14/16 for the v3 token index, this function returns cells at whatever levels
 * the {@link S2RegionCoverer} chooses. The SQL query handles the conversion from
 * covering cells to leaf-cell ranges using bitwise arithmetic.
 *
 * <p>Wire protocol (Spanner Remote UDF batch format):
 * <pre>
 * Request:  {"requestId": "...", "calls": [[minLat, minLng, maxLat, maxLng], ...]}
 * Response: {"replies": [["cellId1", "cellId2", ...], ...]}
 * </pre>
 *
 * <p>Cell IDs are returned as JSON strings (not numbers) because S2 cell IDs are
 * unsigned 64-bit values that exceed JSON's safe integer limit (2^53). Spanner
 * handles the string-to-INT64 parsing automatically.
 */
public class S2CoveringRectV4Function implements HttpFunction {

    private static final Gson GSON = new Gson();

    /**
     * Coverer level range. Unlike the v3 function which constrains to 12-16,
     * we allow a wider range so the coverer can pick optimal levels for the
     * search region size. The SQL query handles any level via range scans.
     */
    private static final int MIN_LEVEL = 12;
    private static final int MAX_LEVEL = 20;

    /**
     * Maximum number of cells in the covering. We use 20 (the default) since
     * no cells are filtered out — every cell the coverer produces is returned.
     */
    private static final int MAX_CELLS = 20;

    @Override
    public void service(HttpRequest request, HttpResponse response) throws IOException {
        response.setContentType("application/json");
        BufferedWriter writer = response.getWriter();

        try {
            BufferedReader reader = request.getReader();
            JsonObject requestBody = JsonParser.parseReader(reader).getAsJsonObject();
            JsonArray calls = requestBody.getAsJsonArray("calls");

            JsonArray replies = new JsonArray();
            for (JsonElement callElement : calls) {
                JsonArray callArgs = callElement.getAsJsonArray();
                double minLat = callArgs.get(0).getAsDouble();
                double minLng = callArgs.get(1).getAsDouble();
                double maxLat = callArgs.get(2).getAsDouble();
                double maxLng = callArgs.get(3).getAsDouble();

                // Validate inputs
                if (minLat < -90 || minLat > 90) {
                    writeError(writer, "minLat must be between -90 and 90, got: " + minLat);
                    return;
                }
                if (maxLat < -90 || maxLat > 90) {
                    writeError(writer, "maxLat must be between -90 and 90, got: " + maxLat);
                    return;
                }
                if (minLng < -180 || minLng > 180) {
                    writeError(writer, "minLng must be between -180 and 180, got: " + minLng);
                    return;
                }
                if (maxLng < -180 || maxLng > 180) {
                    writeError(writer, "maxLng must be between -180 and 180, got: " + maxLng);
                    return;
                }
                if (minLat > maxLat) {
                    writeError(writer, "minLat must be <= maxLat, got: " + minLat + " > " + maxLat);
                    return;
                }
                if (minLng > maxLng) {
                    writeError(writer, "minLng must be <= maxLng, got: " + minLng + " > " + maxLng);
                    return;
                }

                JsonArray cellIdArray = computeCovering(minLat, minLng, maxLat, maxLng);
                replies.add(cellIdArray);
            }

            JsonObject responseBody = new JsonObject();
            responseBody.add("replies", replies);
            writer.write(GSON.toJson(responseBody));

        } catch (Exception e) {
            writeError(writer, "Failed to compute S2 covering for rect: " + e.getMessage());
        }
    }

    /**
     * Compute S2 covering cell IDs for a rectangular region. Unlike the v3 function,
     * no level filtering or promotion is applied — cells are returned at whatever
     * levels the coverer selects.
     */
    static JsonArray computeCovering(double minLat, double minLng, double maxLat, double maxLng) {
        S2LatLngRect rect = new S2LatLngRect(
                S2LatLng.fromDegrees(minLat, minLng),
                S2LatLng.fromDegrees(maxLat, maxLng));

        S2RegionCoverer coverer = S2RegionCoverer.builder()
                .setMinLevel(MIN_LEVEL)
                .setMaxLevel(MAX_LEVEL)
                .setMaxCells(MAX_CELLS)
                .build();

        S2CellUnion covering = coverer.getCovering(rect);

        // Return all cells directly — no filtering needed for v4.
        JsonArray cellIdArray = new JsonArray();
        for (S2CellId cellId : covering) {
            cellIdArray.add(String.valueOf(cellId.id()));
        }
        return cellIdArray;
    }

    /** Write a JSON error response. */
    private static void writeError(BufferedWriter writer, String message) throws IOException {
        JsonObject error = new JsonObject();
        error.addProperty("errorMessage", message);
        writer.write(GSON.toJson(error));
    }
}
