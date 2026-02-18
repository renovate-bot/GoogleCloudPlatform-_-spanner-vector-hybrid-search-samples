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

package com.example.spannergeo;

import com.example.spannergeo.model.Location;
import com.google.cloud.spanner.*;
import com.google.common.geometry.S2CellId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Data Access Object for geo-spatial operations on Spanner.
 * Uses the v3 token index schema (PointOfInterest + PointOfInterestLocationIndex).
 *
 * All queries use parameterized SQL — no string concatenation.
 */
public class SpannerGeoDao {

    private final DatabaseClient dbClient;

    public SpannerGeoDao(DatabaseClient dbClient) {
        this.dbClient = dbClient;
    }

    /**
     * Insert a location along with its S2 index tokens.
     * This writes to both PointOfInterest and PointOfInterestLocationIndex
     * in a single transaction.
     */
    public void insertLocation(Location location) {
        List<S2CellId> cellIds = S2Util.encodeCellIds(
                location.getLatitude(), location.getLongitude());

        List<Mutation> mutations = new ArrayList<>();

        // Insert the parent row
        mutations.add(Mutation.newInsertOrUpdateBuilder("PointOfInterest")
                .set("PoiId").to(location.getPoiId())
                .set("Name").to(location.getName())
                .set("Category").to(location.getCategory())
                .set("Latitude").to(location.getLatitude())
                .set("Longitude").to(location.getLongitude())
                .build());

        // Insert one index row per S2 cell level
        for (S2CellId cellId : cellIds) {
            mutations.add(Mutation.newInsertOrUpdateBuilder("PointOfInterestLocationIndex")
                    .set("PoiId").to(location.getPoiId())
                    .set("S2CellId").to(cellId.id())
                    .set("CellLevel").to(cellId.level())
                    .build());
        }

        dbClient.write(mutations);
    }

    /**
     * Insert multiple locations in a single transaction (batch insert).
     */
    public void insertLocations(List<Location> locations) {
        List<Mutation> mutations = new ArrayList<>();

        for (Location location : locations) {
            List<S2CellId> cellIds = S2Util.encodeCellIds(
                    location.getLatitude(), location.getLongitude());

            mutations.add(Mutation.newInsertOrUpdateBuilder("PointOfInterest")
                    .set("PoiId").to(location.getPoiId())
                    .set("Name").to(location.getName())
                    .set("Category").to(location.getCategory())
                    .set("Latitude").to(location.getLatitude())
                    .set("Longitude").to(location.getLongitude())
                    .build());

            for (S2CellId cellId : cellIds) {
                mutations.add(Mutation.newInsertOrUpdateBuilder("PointOfInterestLocationIndex")
                        .set("PoiId").to(location.getPoiId())
                        .set("S2CellId").to(cellId.id())
                        .set("CellLevel").to(cellId.level())
                        .build());
            }
        }

        dbClient.write(mutations);
    }

    /**
     * Radius search: find all POIs within `radiusMeters` of (centerLat, centerLng).
     *
     * Algorithm:
     *   1. Compute S2 covering cells for the search circle
     *   2. Build a SQL query with BETWEEN clauses for each cell range
     *   3. Post-filter results with Haversine distance
     *   4. Sort by distance ascending
     */
    public List<LocationResult> radiusSearch(double centerLat, double centerLng,
                                              double radiusMeters) {
        List<S2Util.CellIdRange> ranges = S2Util.computeCovering(
                centerLat, centerLng, radiusMeters);

        if (ranges.isEmpty()) {
            return List.of();
        }

        // Build the dynamic OR clauses for the covering ranges.
        // Each range maps to: idx.S2CellId BETWEEN @min_N AND @max_N
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT poi.PoiId, poi.Name, poi.Category, poi.Latitude, poi.Longitude ");
        sql.append("FROM PointOfInterest poi ");
        sql.append("WHERE poi.PoiId IN (");
        // DISTINCT is important: a POI has tokens at multiple cell levels,
        // so the same PoiId can match multiple covering ranges.
        sql.append("  SELECT DISTINCT idx.PoiId FROM PointOfInterestLocationIndex idx WHERE ");

        for (int i = 0; i < ranges.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append("idx.S2CellId BETWEEN @min_").append(i)
               .append(" AND @max_").append(i);
        }

        sql.append(")");

        Statement.Builder stmtBuilder = Statement.newBuilder(sql.toString());
        for (int i = 0; i < ranges.size(); i++) {
            stmtBuilder.bind("min_" + i).to(ranges.get(i).getMin());
            stmtBuilder.bind("max_" + i).to(ranges.get(i).getMax());
        }
        Statement statement = stmtBuilder.build();

        // Execute the query and post-filter with Haversine
        List<LocationResult> results = new ArrayList<>();
        try (ResultSet rs = dbClient.singleUse().executeQuery(statement)) {
            while (rs.next()) {
                double lat = rs.getDouble("Latitude");
                double lng = rs.getDouble("Longitude");
                double distance = S2Util.haversineDistance(centerLat, centerLng, lat, lng);

                // Post-filter: only include results within the actual radius
                if (distance <= radiusMeters) {
                    Location loc = new Location(
                            rs.getString("PoiId"),
                            rs.getString("Name"),
                            rs.isNull("Category") ? null : rs.getString("Category"),
                            lat, lng);
                    results.add(new LocationResult(loc, distance));
                }
            }
        }

        // Sort by distance
        results.sort(Comparator.comparingDouble(LocationResult::getDistanceMeters));
        return results;
    }

    /**
     * Bounding box search: find all POIs within the given rectangle.
     */
    public List<Location> bboxSearch(double minLat, double minLng,
                                      double maxLat, double maxLng) {
        List<S2Util.CellIdRange> ranges = S2Util.computeCoveringRect(
                minLat, minLng, maxLat, maxLng);

        if (ranges.isEmpty()) {
            return List.of();
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT poi.PoiId, poi.Name, poi.Category, poi.Latitude, poi.Longitude ");
        sql.append("FROM PointOfInterest poi ");
        sql.append("WHERE poi.PoiId IN (");
        sql.append("  SELECT DISTINCT idx.PoiId FROM PointOfInterestLocationIndex idx WHERE ");

        for (int i = 0; i < ranges.size(); i++) {
            if (i > 0) sql.append(" OR ");
            sql.append("idx.S2CellId BETWEEN @min_").append(i)
               .append(" AND @max_").append(i);
        }
        sql.append(")");

        // Post-filter with exact bounding box
        sql.append(" AND poi.Latitude BETWEEN @minLat AND @maxLat");
        sql.append(" AND poi.Longitude BETWEEN @minLng AND @maxLng");
        sql.append(" ORDER BY poi.Name");

        Statement.Builder stmtBuilder = Statement.newBuilder(sql.toString());
        for (int i = 0; i < ranges.size(); i++) {
            stmtBuilder.bind("min_" + i).to(ranges.get(i).getMin());
            stmtBuilder.bind("max_" + i).to(ranges.get(i).getMax());
        }
        stmtBuilder.bind("minLat").to(minLat);
        stmtBuilder.bind("maxLat").to(maxLat);
        stmtBuilder.bind("minLng").to(minLng);
        stmtBuilder.bind("maxLng").to(maxLng);

        List<Location> results = new ArrayList<>();
        try (ResultSet rs = dbClient.singleUse().executeQuery(stmtBuilder.build())) {
            while (rs.next()) {
                results.add(new Location(
                        rs.getString("PoiId"),
                        rs.getString("Name"),
                        rs.isNull("Category") ? null : rs.getString("Category"),
                        rs.getDouble("Latitude"),
                        rs.getDouble("Longitude")));
            }
        }
        return results;
    }

    /**
     * Approximate k-Nearest Neighbors: find the k closest POIs to a point.
     *
     * Strategy: start with a small radius, expand until we have >= k results.
     * This is an iterative approach — each iteration doubles the search radius.
     */
    public List<LocationResult> knnSearch(double centerLat, double centerLng,
                                           int k, double initialRadiusMeters) {
        double radius = initialRadiusMeters;
        List<LocationResult> results = List.of();

        // Expand the search radius until we find enough candidates
        for (int attempt = 0; attempt < 8; attempt++) {
            results = radiusSearch(centerLat, centerLng, radius);
            if (results.size() >= k) {
                break;
            }
            radius *= 2; // Double the radius each iteration
        }

        // Return only the top k results
        if (results.size() > k) {
            results = results.subList(0, k);
        }
        return results;
    }

    /**
     * Radius search using Remote UDFs: find all POIs within {@code radiusMeters}
     * of ({@code centerLat}, {@code centerLng}).
     *
     * <p>Unlike {@link #radiusSearch}, this method has zero S2 library dependency.
     * Both the covering computation and the distance calculation happen server-side
     * via Spanner Remote UDFs ({@code geo.s2_covering} and {@code geo.s2_distance}).
     * The client only provides lat/lng/radius as SQL parameters.
     *
     * <p>This requires the Remote UDFs to be deployed. If they are not deployed,
     * this method throws a {@link SpannerException}.
     *
     * @param centerLat    latitude of the search center in degrees
     * @param centerLng    longitude of the search center in degrees
     * @param radiusMeters search radius in meters
     * @return matching locations sorted by distance ascending
     */
    public List<LocationResult> radiusSearchWithUdf(double centerLat, double centerLng,
                                                     double radiusMeters) {
        // The entire S2 covering + post-filtering pipeline runs in SQL.
        // s2_covering() computes covering cells server-side via Cloud Run function.
        // s2_distance() computes great-circle distance server-side for post-filtering.
        // Remote UDFs live in the "geo" schema. The UNNEST of a Remote UDF result
        // must be done via a subquery — Spanner requires materializing the array first.
        String sql = """
                WITH candidates AS (
                  SELECT DISTINCT poi.PoiId, poi.Name, poi.Category, poi.Latitude, poi.Longitude
                  FROM (SELECT geo.s2_covering(@centerLat, @centerLng, @radiusMeters) AS cells),
                       UNNEST(cells) AS covering_cell
                  JOIN PointOfInterestLocationIndex idx ON idx.S2CellId = covering_cell
                  JOIN PointOfInterest poi ON poi.PoiId = idx.PoiId
                ),
                with_distance AS (
                  SELECT c.PoiId, c.Name, c.Category, c.Latitude, c.Longitude,
                         geo.s2_distance(c.Latitude, c.Longitude, @centerLat, @centerLng) AS distance_meters
                  FROM candidates c
                )
                SELECT * FROM with_distance
                WHERE distance_meters <= @radiusMeters
                ORDER BY distance_meters
                """;

        Statement statement = Statement.newBuilder(sql)
                .bind("centerLat").to(centerLat)
                .bind("centerLng").to(centerLng)
                .bind("radiusMeters").to(radiusMeters)
                .build();

        List<LocationResult> results = new ArrayList<>();
        try (ResultSet rs = dbClient.singleUse().executeQuery(statement)) {
            while (rs.next()) {
                Location loc = new Location(
                        rs.getString("PoiId"),
                        rs.getString("Name"),
                        rs.isNull("Category") ? null : rs.getString("Category"),
                        rs.getDouble("Latitude"),
                        rs.getDouble("Longitude"));
                double distance = rs.getDouble("distance_meters");
                results.add(new LocationResult(loc, distance));
            }
        }

        // Results are already sorted by distance_meters from the ORDER BY clause
        return results;
    }

    /**
     * Bounding box search using Remote UDFs: find all POIs within the given rectangle.
     *
     * <p>Unlike {@link #bboxSearch}, this method has zero S2 library dependency.
     * The covering computation happens server-side via the Spanner Remote UDF
     * {@code geo.s2_covering_rect}. The client only provides the bounding box
     * coordinates as SQL parameters.
     *
     * <p>This requires the Remote UDFs to be deployed. If they are not deployed,
     * this method throws a {@link SpannerException}.
     *
     * @param minLat  southern latitude bound in degrees
     * @param minLng  western longitude bound in degrees
     * @param maxLat  northern latitude bound in degrees
     * @param maxLng  eastern longitude bound in degrees
     * @return matching locations within the bounding box, sorted by name
     */
    public List<Location> bboxSearchWithUdf(double minLat, double minLng,
                                             double maxLat, double maxLng) {
        // geo.s2_covering_rect() computes covering cells for the rectangle server-side.
        // The UNNEST of a Remote UDF result must be done via a subquery -- Spanner
        // requires materializing the array first.
        String sql = """
                SELECT DISTINCT
                    poi.PoiId, poi.Name, poi.Category, poi.Latitude, poi.Longitude
                FROM (SELECT geo.s2_covering_rect(@minLat, @minLng, @maxLat, @maxLng) AS cells),
                     UNNEST(cells) AS covering_cell
                JOIN PointOfInterestLocationIndex idx ON idx.S2CellId = covering_cell
                JOIN PointOfInterest poi ON poi.PoiId = idx.PoiId
                WHERE poi.Latitude BETWEEN @minLat AND @maxLat
                  AND poi.Longitude BETWEEN @minLng AND @maxLng
                ORDER BY poi.Name
                """;

        Statement statement = Statement.newBuilder(sql)
                .bind("minLat").to(minLat)
                .bind("minLng").to(minLng)
                .bind("maxLat").to(maxLat)
                .bind("maxLng").to(maxLng)
                .build();

        List<Location> results = new ArrayList<>();
        try (ResultSet rs = dbClient.singleUse().executeQuery(statement)) {
            while (rs.next()) {
                results.add(new Location(
                        rs.getString("PoiId"),
                        rs.getString("Name"),
                        rs.isNull("Category") ? null : rs.getString("Category"),
                        rs.getDouble("Latitude"),
                        rs.getDouble("Longitude")));
            }
        }
        return results;
    }

    /**
     * Approximate k-Nearest Neighbors using Remote UDFs: find the k closest POIs.
     *
     * <p>Uses the same iterative expansion strategy as {@link #knnSearch}, but
     * delegates to {@link #radiusSearchWithUdf} instead of {@link #radiusSearch}.
     * This means the entire pipeline (covering computation, distance calculation,
     * post-filtering) runs server-side with zero S2 library dependency.
     *
     * @param centerLat          latitude of the search center in degrees
     * @param centerLng          longitude of the search center in degrees
     * @param k                  number of nearest neighbors to return
     * @param initialRadiusMeters starting search radius in meters (doubles each iteration)
     * @return the k closest locations sorted by distance ascending
     */
    public List<LocationResult> knnSearchWithUdf(double centerLat, double centerLng,
                                                   int k, double initialRadiusMeters) {
        double radius = initialRadiusMeters;
        List<LocationResult> results = List.of();

        // Expand the search radius until we find enough candidates
        for (int attempt = 0; attempt < 8; attempt++) {
            results = radiusSearchWithUdf(centerLat, centerLng, radius);
            if (results.size() >= k) {
                break;
            }
            radius *= 2; // Double the radius each iteration
        }

        // Return only the top k results
        if (results.size() > k) {
            results = results.subList(0, k);
        }
        return results;
    }

    /**
     * A location paired with its distance from a search point.
     */
    public static class LocationResult {
        private final Location location;
        private final double distanceMeters;

        public LocationResult(Location location, double distanceMeters) {
            this.location = location;
            this.distanceMeters = distanceMeters;
        }

        public Location getLocation()      { return location; }
        public double getDistanceMeters()   { return distanceMeters; }

        @Override
        public String toString() {
            return String.format("  %.0fm - %s (%s) [%.6f, %.6f]",
                    distanceMeters,
                    location.getName(),
                    location.getCategory(),
                    location.getLatitude(),
                    location.getLongitude());
        }
    }
}
