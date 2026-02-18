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
import com.google.common.geometry.S2LatLng;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Cloud Run function backing the Spanner Remote UDF {@code s2_distance}.
 *
 * <p>Computes the great-circle distance in meters between two points on Earth
 * using the S2 Geometry library. This replaces the inline Haversine formula in
 * SQL queries, making them more readable and slightly more accurate (S2 uses a
 * more precise angular distance calculation than the Haversine approximation).
 *
 * <p>Wire protocol (Spanner Remote UDF batch format):
 * <pre>
 * Request:  {"requestId": "...", "calls": [[lat1, lng1, lat2, lng2], ...]}
 * Response: {"replies": [distance_meters_1, distance_meters_2, ...]}
 * </pre>
 *
 * <p>Distances are returned as JSON numbers (FLOAT64). Unlike cell IDs, distances
 * are well within JSON's safe numeric range so no string encoding is needed.
 */
public class S2DistanceFunction implements HttpFunction {

    private static final Gson GSON = new Gson();

    /** Earth's mean radius in meters, used to convert angular distance to meters. */
    private static final double EARTH_RADIUS_METERS = 6_371_000.0;

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
                double lat1 = callArgs.get(0).getAsDouble();
                double lng1 = callArgs.get(1).getAsDouble();
                double lat2 = callArgs.get(2).getAsDouble();
                double lng2 = callArgs.get(3).getAsDouble();

                // Validate latitude/longitude ranges
                if (lat1 < -90 || lat1 > 90 || lat2 < -90 || lat2 > 90) {
                    writeError(writer, "Latitude must be between -90 and 90");
                    return;
                }
                if (lng1 < -180 || lng1 > 180 || lng2 < -180 || lng2 > 180) {
                    writeError(writer, "Longitude must be between -180 and 180");
                    return;
                }

                double distanceMeters = computeDistanceMeters(lat1, lng1, lat2, lng2);
                replies.add(distanceMeters);
            }

            JsonObject responseBody = new JsonObject();
            responseBody.add("replies", replies);
            writer.write(GSON.toJson(responseBody));

        } catch (Exception e) {
            writeError(writer, "Failed to compute distance: " + e.getMessage());
        }
    }

    /**
     * Compute the great-circle distance between two points using S2.
     *
     * <p>{@code S2LatLng.getDistance()} returns an {@code S1Angle} representing the
     * angular distance between the two points on the unit sphere. Multiplying by
     * Earth's radius converts this to meters.
     *
     * @param lat1 latitude of the first point in degrees
     * @param lng1 longitude of the first point in degrees
     * @param lat2 latitude of the second point in degrees
     * @param lng2 longitude of the second point in degrees
     * @return distance in meters
     */
    static double computeDistanceMeters(double lat1, double lng1, double lat2, double lng2) {
        S2LatLng point1 = S2LatLng.fromDegrees(lat1, lng1);
        S2LatLng point2 = S2LatLng.fromDegrees(lat2, lng2);

        // S2LatLng.getDistance() returns the angle subtended at the center of the
        // unit sphere. Multiply by Earth's radius to get surface distance in meters.
        return point1.getDistance(point2).radians() * EARTH_RADIUS_METERS;
    }

    /** Write a JSON error response. */
    private static void writeError(BufferedWriter writer, String message) throws IOException {
        JsonObject error = new JsonObject();
        error.addProperty("errorMessage", message);
        writer.write(GSON.toJson(error));
    }
}
