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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * End-to-end demo: seeds sample data into Spanner, then runs geo-spatial queries
 * in two modes:
 *
 * <ol>
 *   <li><b>Client-side</b> -- the application uses the S2 library to compute
 *       covering cells and Haversine distances before querying Spanner.</li>
 *   <li><b>Remote UDF</b> -- all S2 logic runs server-side via Cloud Run
 *       functions invoked as Spanner Remote UDFs. The client sends only
 *       lat/lng/radius parameters.</li>
 * </ol>
 *
 * Both approaches exercise the same three query types (radius, bounding box,
 * k-NN) with identical search parameters so the reader can compare results.
 *
 * <p>Usage (env vars from .env or shell, or CLI args as fallback):
 * <pre>
 *   # Option 1: Set env vars (or use .env file with a wrapper)
 *   export SPANNER_PROJECT_ID=my-project
 *   export SPANNER_INSTANCE_ID=my-instance
 *   export SPANNER_DATABASE_ID=my-database
 *   mvn exec:java
 *
 *   # Option 2: Pass as CLI args
 *   mvn exec:java -Dexec.args="PROJECT_ID INSTANCE_ID DATABASE_ID"
 * </pre>
 */
public class App {

    public static void main(String[] args) {
        String projectId = envOrArg("SPANNER_PROJECT_ID", args, 0);
        String instanceId = envOrArg("SPANNER_INSTANCE_ID", args, 1);
        String databaseId = envOrArg("SPANNER_DATABASE_ID", args, 2);

        if (projectId == null || instanceId == null || databaseId == null) {
            System.err.println("Usage: Set SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, "
                    + "SPANNER_DATABASE_ID env vars, or pass as CLI args.");
            System.exit(1);
        }

        SpannerOptions options = SpannerOptions.newBuilder()
                .setProjectId(projectId)
                .build();

        try (Spanner spanner = options.getService()) {
            DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
            DatabaseClient dbClient = spanner.getDatabaseClient(db);
            SpannerGeoDao dao = new SpannerGeoDao(dbClient);

            // Step 1: Seed data (writes both v3 tokens and v4 leaf cell ID)
            seedData(dao);

            // Step 2: Client-side v3 queries (S2 library computes coverings on the client)
            runClientSideQueries(dao);

            // Step 3: Remote UDF v3 queries (S2 logic runs server-side via Cloud Functions)
            runUdfQueries(dao);

            // Step 4: Client-side v4 queries (range scans on leaf cell ID)
            runClientSideQueriesV4(dao);

            // Step 5: Remote UDF v4 queries (bitwise range computation in SQL)
            runUdfQueriesV4(dao);

            System.out.println("\nDone.");
        }
    }

    // ----- Data seeding --------------------------------------------------------

    /**
     * Seed the database with 15 well-known San Francisco landmarks.
     * Uses INSERT_OR_UPDATE so re-running the demo is idempotent.
     */
    private static void seedData(SpannerGeoDao dao) {
        System.out.println("=== Seeding sample data (San Francisco landmarks) ===\n");
        List<Location> sampleLocations = createSampleData();
        dao.insertLocations(sampleLocations);
        System.out.println("Inserted " + sampleLocations.size() + " locations.\n");
    }

    // =====================================================================
    // Client-side queries -- the application uses the S2 library to compute
    // covering cells and bind them as query parameters.
    // =====================================================================

    /**
     * Run all three client-side query types: radius, bounding box, k-NN.
     * S2 covering computation and distance filtering happen in Java.
     */
    private static void runClientSideQueries(SpannerGeoDao dao) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("  CLIENT-SIDE QUERIES (S2 computed on the application)");
        System.out.println("=".repeat(60));

        // Shared search parameters -- near Union Square, San Francisco
        double searchLat = 37.7880, searchLng = -122.4075;
        double radiusMeters = 2000;
        double minLat = 37.775, minLng = -122.420;
        double maxLat = 37.795, maxLng = -122.400;

        runClientRadiusSearch(dao, searchLat, searchLng, radiusMeters);
        runClientBboxSearch(dao, minLat, minLng, maxLat, maxLng);
        runClientKnnSearch(dao, searchLat, searchLng);
    }

    /**
     * Radius search: find POIs within {@code radiusMeters} of the given point.
     * The S2 covering and Haversine post-filter both run on the client.
     */
    private static void runClientRadiusSearch(SpannerGeoDao dao,
                                              double searchLat, double searchLng,
                                              double radiusMeters) {
        System.out.println("\n--- Radius Search ---");
        System.out.printf("Center: (%.4f, %.4f), Radius: %.0fm%n%n",
                searchLat, searchLng, radiusMeters);

        List<SpannerGeoDao.LocationResult> results =
                dao.radiusSearch(searchLat, searchLng, radiusMeters);
        printLocationResults(results);
    }

    /**
     * Bounding box search: find POIs within the given lat/lng rectangle.
     * The S2 covering runs on the client; exact bbox filtering is in SQL.
     */
    private static void runClientBboxSearch(SpannerGeoDao dao,
                                            double minLat, double minLng,
                                            double maxLat, double maxLng) {
        System.out.println("\n--- Bounding Box Search ---");
        System.out.printf("Box: (%.3f, %.3f) to (%.3f, %.3f)%n%n",
                minLat, minLng, maxLat, maxLng);

        List<Location> results = dao.bboxSearch(minLat, minLng, maxLat, maxLng);
        printLocations(results);
    }

    /**
     * Approximate k-NN: find the 3 closest POIs by iteratively expanding radius.
     */
    private static void runClientKnnSearch(SpannerGeoDao dao,
                                           double searchLat, double searchLng) {
        System.out.println("\n--- Approximate k-NN Search (k=3) ---");
        System.out.printf("Center: (%.4f, %.4f)%n%n", searchLat, searchLng);

        List<SpannerGeoDao.LocationResult> results =
                dao.knnSearch(searchLat, searchLng, 3, 500);
        if (results.isEmpty()) {
            System.out.println("No results found.");
        } else {
            System.out.println("Closest 3 location(s):");
            results.forEach(System.out::println);
        }
    }

    // =====================================================================
    // Remote UDF queries -- all S2 logic runs server-side via Cloud Run
    // functions. The client sends only lat/lng/radius parameters.
    // Requires Remote UDFs to be deployed (see deploy/deploy-function.sh).
    // =====================================================================

    /**
     * Run all three UDF-based query types: radius, bounding box, k-NN.
     * Each is wrapped in try-catch so the demo works whether or not the
     * Remote UDFs are deployed.
     */
    private static void runUdfQueries(SpannerGeoDao dao) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("  REMOTE UDF QUERIES (S2 computed server-side)");
        System.out.println("=".repeat(60));

        // Same search parameters as client-side queries for comparison
        double searchLat = 37.7880, searchLng = -122.4075;
        double radiusMeters = 2000;
        double minLat = 37.775, minLng = -122.420;
        double maxLat = 37.795, maxLng = -122.400;

        runUdfRadiusSearch(dao, searchLat, searchLng, radiusMeters);
        runUdfBboxSearch(dao, minLat, minLng, maxLat, maxLng);
        runUdfKnnSearch(dao, searchLat, searchLng);
    }

    /**
     * Radius search using Remote UDFs. The geo.s2_covering() and
     * geo.s2_distance() UDFs handle covering computation and distance
     * filtering entirely in SQL.
     */
    private static void runUdfRadiusSearch(SpannerGeoDao dao,
                                           double searchLat, double searchLng,
                                           double radiusMeters) {
        System.out.println("\n--- Radius Search with Remote UDFs ---");
        System.out.printf("Center: (%.4f, %.4f), Radius: %.0fm%n%n",
                searchLat, searchLng, radiusMeters);

        try {
            List<SpannerGeoDao.LocationResult> results =
                    dao.radiusSearchWithUdf(searchLat, searchLng, radiusMeters);
            printLocationResults(results);
        } catch (Exception e) {
            printUdfError("radius search", e);
        }
    }

    /**
     * Bounding box search using Remote UDFs. The geo.s2_covering_rect()
     * UDF computes covering cells for the rectangle server-side.
     */
    private static void runUdfBboxSearch(SpannerGeoDao dao,
                                         double minLat, double minLng,
                                         double maxLat, double maxLng) {
        System.out.println("\n--- Bounding Box Search with Remote UDFs ---");
        System.out.printf("Box: (%.3f, %.3f) to (%.3f, %.3f)%n%n",
                minLat, minLng, maxLat, maxLng);

        try {
            List<Location> results =
                    dao.bboxSearchWithUdf(minLat, minLng, maxLat, maxLng);
            printLocations(results);
        } catch (Exception e) {
            printUdfError("bounding box search", e);
        }
    }

    /**
     * Approximate k-NN search using Remote UDFs.
     */
    private static void runUdfKnnSearch(SpannerGeoDao dao,
                                        double searchLat, double searchLng) {
        System.out.println("\n--- Approximate k-NN Search with Remote UDFs (k=3) ---");
        System.out.printf("Center: (%.4f, %.4f)%n%n", searchLat, searchLng);

        try {
            List<SpannerGeoDao.LocationResult> results =
                    dao.knnSearchWithUdf(searchLat, searchLng, 3, 500);
            if (results.isEmpty()) {
                System.out.println("No results found.");
            } else {
                System.out.println("Closest 3 location(s):");
                results.forEach(System.out::println);
            }
        } catch (Exception e) {
            printUdfError("k-NN search", e);
        }
    }

    // =====================================================================
    // v4 Client-side queries — range scans on the leaf-level S2 Cell ID.
    // No interleaved table, no JOINs, no DISTINCT.
    // =====================================================================

    private static void runClientSideQueriesV4(SpannerGeoDao dao) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("  V4 CLIENT-SIDE QUERIES (range scans on leaf cell)");
        System.out.println("=".repeat(60));

        double searchLat = 37.7880, searchLng = -122.4075;
        double radiusMeters = 2000;
        double minLat = 37.775, minLng = -122.420;
        double maxLat = 37.795, maxLng = -122.400;

        System.out.println("\n--- v4 Radius Search ---");
        System.out.printf("Center: (%.4f, %.4f), Radius: %.0fm%n%n",
                searchLat, searchLng, radiusMeters);
        List<SpannerGeoDao.LocationResult> radiusResults =
                dao.radiusSearchV4(searchLat, searchLng, radiusMeters);
        printLocationResults(radiusResults);

        System.out.println("\n--- v4 Bounding Box Search ---");
        System.out.printf("Box: (%.3f, %.3f) to (%.3f, %.3f)%n%n",
                minLat, minLng, maxLat, maxLng);
        List<Location> bboxResults = dao.bboxSearchV4(minLat, minLng, maxLat, maxLng);
        printLocations(bboxResults);

        System.out.println("\n--- v4 Approximate k-NN Search (k=3) ---");
        System.out.printf("Center: (%.4f, %.4f)%n%n", searchLat, searchLng);
        List<SpannerGeoDao.LocationResult> knnResults =
                dao.knnSearchV4(searchLat, searchLng, 3, 500);
        if (knnResults.isEmpty()) {
            System.out.println("No results found.");
        } else {
            System.out.println("Closest 3 location(s):");
            knnResults.forEach(System.out::println);
        }
    }

    // =====================================================================
    // v4 Remote UDF queries — bitwise range computation in SQL.
    // Same UDFs as v3, but covering cells are converted to ranges inline.
    // =====================================================================

    private static void runUdfQueriesV4(SpannerGeoDao dao) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("  V4 REMOTE UDF QUERIES (bitwise ranges in SQL)");
        System.out.println("=".repeat(60));

        double searchLat = 37.7880, searchLng = -122.4075;
        double radiusMeters = 2000;
        double minLat = 37.775, minLng = -122.420;
        double maxLat = 37.795, maxLng = -122.400;

        System.out.println("\n--- v4 Radius Search with Remote UDFs ---");
        System.out.printf("Center: (%.4f, %.4f), Radius: %.0fm%n%n",
                searchLat, searchLng, radiusMeters);
        try {
            List<SpannerGeoDao.LocationResult> results =
                    dao.radiusSearchWithUdfV4(searchLat, searchLng, radiusMeters);
            printLocationResults(results);
        } catch (Exception e) {
            printUdfError("v4 radius search", e);
        }

        System.out.println("\n--- v4 Bounding Box Search with Remote UDFs ---");
        System.out.printf("Box: (%.3f, %.3f) to (%.3f, %.3f)%n%n",
                minLat, minLng, maxLat, maxLng);
        try {
            List<Location> results =
                    dao.bboxSearchWithUdfV4(minLat, minLng, maxLat, maxLng);
            printLocations(results);
        } catch (Exception e) {
            printUdfError("v4 bounding box search", e);
        }

        System.out.println("\n--- v4 Approximate k-NN Search with Remote UDFs (k=3) ---");
        System.out.printf("Center: (%.4f, %.4f)%n%n", searchLat, searchLng);
        try {
            List<SpannerGeoDao.LocationResult> results =
                    dao.knnSearchWithUdfV4(searchLat, searchLng, 3, 500);
            if (results.isEmpty()) {
                System.out.println("No results found.");
            } else {
                System.out.println("Closest 3 location(s):");
                results.forEach(System.out::println);
            }
        } catch (Exception e) {
            printUdfError("v4 k-NN search", e);
        }
    }

    // ----- Output helpers --------------------------------------------------

    /** Print a list of LocationResults (with distance). */
    private static void printLocationResults(List<SpannerGeoDao.LocationResult> results) {
        if (results.isEmpty()) {
            System.out.println("No results found.");
        } else {
            System.out.println("Found " + results.size() + " location(s):");
            results.forEach(System.out::println);
        }
    }

    /** Print a list of Locations (without distance). */
    private static void printLocations(List<Location> results) {
        if (results.isEmpty()) {
            System.out.println("No results found.");
        } else {
            System.out.println("Found " + results.size() + " location(s):");
            results.forEach(System.out::println);
        }
    }

    /** Print a user-friendly error when a UDF query fails. */
    private static void printUdfError(String queryType, Exception e) {
        System.out.println("Remote UDFs not deployed, skipping " + queryType + ".");
        System.out.println("  Deploy with: ./deploy/deploy-function.sh");
        System.out.println("  Error: " + e.getMessage());
    }

    // ----- Sample data -----------------------------------------------------

    /**
     * 15 well-known San Francisco landmarks used as seed data.
     *
     * PoiIds are deterministic UUIDs derived from the landmark name, so
     * re-running the demo upserts the same rows (via INSERT_OR_UPDATE)
     * rather than creating duplicates.
     */
    private static List<Location> createSampleData() {
        return List.of(
            new Location(deterministicId("Golden Gate Bridge"), "Golden Gate Bridge",
                    "landmark", 37.8199, -122.4783),
            new Location(deterministicId("Fisherman's Wharf"), "Fisherman's Wharf",
                    "landmark", 37.8080, -122.4177),
            new Location(deterministicId("Coit Tower"), "Coit Tower",
                    "landmark", 37.8024, -122.4058),
            new Location(deterministicId("Ferry Building"), "Ferry Building",
                    "landmark", 37.7956, -122.3935),
            new Location(deterministicId("Union Square"), "Union Square",
                    "shopping", 37.7880, -122.4075),
            new Location(deterministicId("Chinatown Gate"), "Chinatown Gate",
                    "landmark", 37.7908, -122.4058),
            new Location(deterministicId("Transamerica Pyramid"), "Transamerica Pyramid",
                    "landmark", 37.7952, -122.4028),
            new Location(deterministicId("AT&T Park (Oracle Park)"), "AT&T Park (Oracle Park)",
                    "sports", 37.7786, -122.3893),
            new Location(deterministicId("Moscone Center"), "Moscone Center",
                    "convention", 37.7840, -122.4010),
            new Location(deterministicId("City Hall"), "City Hall",
                    "government", 37.7793, -122.4193),
            new Location(deterministicId("Alamo Square (Painted Ladies)"), "Alamo Square (Painted Ladies)",
                    "landmark", 37.7764, -122.4340),
            new Location(deterministicId("Twin Peaks"), "Twin Peaks",
                    "nature", 37.7544, -122.4477),
            new Location(deterministicId("Dolores Park"), "Dolores Park",
                    "park", 37.7596, -122.4269),
            new Location(deterministicId("Palace of Fine Arts"), "Palace of Fine Arts",
                    "landmark", 37.8020, -122.4484),
            new Location(deterministicId("Alcatraz Island"), "Alcatraz Island",
                    "landmark", 37.8267, -122.4230)
        );
    }

    /** Generate a deterministic UUID from a name so re-runs upsert instead of duplicating. */
    private static String deterministicId(String name) {
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** Return env var if set, otherwise fall back to CLI arg at the given index. */
    private static String envOrArg(String envVar, String[] args, int argIndex) {
        String val = System.getenv(envVar);
        if (val != null && !val.isEmpty()) {
            return val;
        }
        return argIndex < args.length ? args[argIndex] : null;
    }
}
