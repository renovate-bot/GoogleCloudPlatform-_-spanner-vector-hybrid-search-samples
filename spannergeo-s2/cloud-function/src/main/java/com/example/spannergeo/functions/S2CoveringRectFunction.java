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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Cloud Run function backing the Spanner Remote UDF {@code geo.s2_covering_rect}.
 *
 * <p>Given a bounding box (minLat, minLng, maxLat, maxLng), computes an S2 covering
 * for the rectangular search region and returns the covering cell IDs. These cell IDs
 * can be joined against the {@code PointOfInterestLocationIndex} table to find
 * candidate POIs within the bounding box.
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
public class S2CoveringRectFunction implements HttpFunction {

    private static final Gson GSON = new Gson();

    /**
     * S2 cell levels we index at. Must match the levels used when inserting data
     * into PointOfInterestLocationIndex.
     * Level 12 ~ 3.3km, Level 14 ~ 800m, Level 16 ~ 150m.
     */
    private static final Set<Integer> INDEX_LEVELS = Set.of(12, 14, 16);

    private static final int MIN_LEVEL = 12;
    private static final int MAX_LEVEL = 16;

    /**
     * Maximum number of cells in the covering. Higher values give tighter coverings
     * (fewer false positives) but produce larger SQL IN/BETWEEN clauses.
     * We use 50 here (higher than the client-side default of 20) because the
     * coverer may produce cells at levels we filter out, so we start with more
     * to have enough after filtering.
     */
    private static final int MAX_CELLS = 50;

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
                    writeError(writer, "minLat must be <= maxLat, got: "
                            + minLat + " > " + maxLat);
                    return;
                }
                if (minLng > maxLng) {
                    writeError(writer, "minLng must be <= maxLng, got: "
                            + minLng + " > " + maxLng);
                    return;
                }

                List<String> coveringCellIds = computeCoveringCellIds(
                        minLat, minLng, maxLat, maxLng);

                // Build a JSON array of string-encoded cell IDs
                JsonArray cellIdArray = new JsonArray();
                for (String cellId : coveringCellIds) {
                    cellIdArray.add(cellId);
                }
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
     * Compute S2 covering cell IDs for a rectangular region, filtered to only include
     * cells at our index levels (12, 14, 16).
     *
     * <p>The S2RegionCoverer may produce cells at any level between minLevel and
     * maxLevel. Since our index only stores tokens at levels 12, 14, and 16, we
     * filter out cells at other levels (e.g., 13, 15). For each filtered-out cell,
     * we replace it with its parent at the nearest coarser index level to ensure
     * coverage is not lost.
     *
     * @param minLat  southern latitude bound in degrees
     * @param minLng  western longitude bound in degrees
     * @param maxLat  northern latitude bound in degrees
     * @param maxLng  eastern longitude bound in degrees
     * @return list of S2 cell IDs as string representations of signed INT64 values
     */
    static List<String> computeCoveringCellIds(double minLat, double minLng,
                                                double maxLat, double maxLng) {
        // Create an S2LatLngRect from the southwest and northeast corners
        S2LatLngRect rect = new S2LatLngRect(
                S2LatLng.fromDegrees(minLat, minLng),
                S2LatLng.fromDegrees(maxLat, maxLng));

        // Compute a covering. We use MAX_CELLS=50 because some cells may be at
        // non-index levels and will be promoted to a coarser index level, which
        // can merge duplicates and reduce the final count.
        S2RegionCoverer coverer = S2RegionCoverer.builder()
                .setMinLevel(MIN_LEVEL)
                .setMaxLevel(MAX_LEVEL)
                .setMaxCells(MAX_CELLS)
                .build();

        S2CellUnion covering = coverer.getCovering(rect);

        // Filter to only include cells at our index levels (12, 14, 16).
        // Cells at non-index levels (13, 15) are promoted to the nearest coarser
        // index level so we don't miss any coverage.
        List<S2CellId> filteredCells = new ArrayList<>();
        for (S2CellId cellId : covering) {
            int level = cellId.level();
            if (INDEX_LEVELS.contains(level)) {
                // Cell is already at an index level -- use it directly
                filteredCells.add(cellId);
            } else {
                // Promote to the nearest coarser index level.
                // E.g., level 13 -> level 12, level 15 -> level 14.
                S2CellId promoted = promoteToIndexLevel(cellId);
                if (promoted != null) {
                    filteredCells.add(promoted);
                }
            }
        }

        // Deduplicate: promotion can produce duplicate cell IDs (e.g., two level-13
        // siblings may both promote to the same level-12 parent)
        return filteredCells.stream()
                .distinct()
                .map(cellId -> String.valueOf(cellId.id()))
                .toList();
    }

    /**
     * Promote a cell to the nearest coarser index level.
     * For example, a level-13 cell is promoted to its level-12 parent.
     * Returns null if the cell is coarser than all index levels (shouldn't happen
     * given our coverer configuration, but handled for safety).
     */
    private static S2CellId promoteToIndexLevel(S2CellId cellId) {
        int level = cellId.level();
        // Walk up from current level to find the nearest index level
        for (int indexLevel = level - 1; indexLevel >= MIN_LEVEL; indexLevel--) {
            if (INDEX_LEVELS.contains(indexLevel)) {
                return cellId.parent(indexLevel);
            }
        }
        // Cell is coarser than our coarsest index level -- use the coarsest
        return cellId.parent(MIN_LEVEL);
    }

    /** Write a JSON error response. */
    private static void writeError(BufferedWriter writer, String message) throws IOException {
        JsonObject error = new JsonObject();
        error.addProperty("errorMessage", message);
        writer.write(GSON.toJson(error));
    }
}
