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
import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLng;
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
 * Cloud Run function backing the Spanner Remote UDF {@code geo.s2_covering_v4}.
 *
 * <p>This is the v4 variant of the covering function, designed for the range-scan
 * schema where each POI stores a single leaf-level S2 Cell ID (level 30). Unlike
 * {@link S2CoveringFunction} which filters covering cells to levels 12/14/16 for
 * the v3 token index, this function returns cells at whatever levels the
 * {@link S2RegionCoverer} chooses. The SQL query handles the conversion from
 * covering cells to leaf-cell ranges using bitwise arithmetic.
 *
 * <p>This produces tighter coverings with fewer false positives because the
 * coverer is not constrained to three specific levels.
 *
 * <p>Wire protocol (Spanner Remote UDF batch format):
 * <pre>
 * Request:  {"requestId": "...", "calls": [[centerLat, centerLng, radiusMeters], ...]}
 * Response: {"replies": [["cellId1", "cellId2", ...], ...]}
 * </pre>
 *
 * <p>Cell IDs are returned as JSON strings (not numbers) because S2 cell IDs are
 * unsigned 64-bit values that exceed JSON's safe integer limit (2^53). Spanner
 * handles the string-to-INT64 parsing automatically.
 */
public class S2CoveringV4Function implements HttpFunction {

    private static final Gson GSON = new Gson();

    /** Earth's mean radius in meters, used to convert a distance to an S1Angle. */
    private static final double EARTH_RADIUS_METERS = 6_371_000.0;

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
                double centerLat = callArgs.get(0).getAsDouble();
                double centerLng = callArgs.get(1).getAsDouble();
                double radiusMeters = callArgs.get(2).getAsDouble();

                // Validate inputs
                if (centerLat < -90 || centerLat > 90) {
                    writeError(writer, "centerLat must be between -90 and 90, got: " + centerLat);
                    return;
                }
                if (centerLng < -180 || centerLng > 180) {
                    writeError(writer, "centerLng must be between -180 and 180, got: " + centerLng);
                    return;
                }
                if (radiusMeters <= 0) {
                    writeError(writer, "radiusMeters must be positive, got: " + radiusMeters);
                    return;
                }

                JsonArray cellIdArray = computeCovering(centerLat, centerLng, radiusMeters);
                replies.add(cellIdArray);
            }

            JsonObject responseBody = new JsonObject();
            responseBody.add("replies", replies);
            writer.write(GSON.toJson(responseBody));

        } catch (Exception e) {
            writeError(writer, "Failed to compute S2 covering: " + e.getMessage());
        }
    }

    /**
     * Compute S2 covering cell IDs for a circular region. Unlike the v3 function,
     * no level filtering or promotion is applied — cells are returned at whatever
     * levels the coverer selects.
     */
    static JsonArray computeCovering(double centerLat, double centerLng, double radiusMeters) {
        S2LatLng center = S2LatLng.fromDegrees(centerLat, centerLng);
        S1Angle radiusAngle = S1Angle.radians(radiusMeters / EARTH_RADIUS_METERS);
        S2Cap searchRegion = S2Cap.fromAxisAngle(center.toPoint(), radiusAngle);

        S2RegionCoverer coverer = S2RegionCoverer.builder()
                .setMinLevel(MIN_LEVEL)
                .setMaxLevel(MAX_LEVEL)
                .setMaxCells(MAX_CELLS)
                .build();

        S2CellUnion covering = coverer.getCovering(searchRegion);

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
