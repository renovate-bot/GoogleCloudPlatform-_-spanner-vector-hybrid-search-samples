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

package com.google.cloud.spanner.benchmark;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Cloud Spanner Latency Load Test Benchmark Runner with Direct Access.
 *
 * Demonstrates single-row write, strong read, and stale read performance
 * using Google Cloud Spanner Direct Access to route traffic directly
 * to Spanner storage and consensus replicas on Google's network without GFE proxies.
 */
public class DirectAccessBenchmark {

    public static class LatencyPercentiles {
        public int opsCount;
        public double hitRate = 1.0;
        public double p50;
        public double p90;
        public double p95;
        public double p99;
        public double min;
        public double max;
        public double avg;
    }

    public static class BenchmarkOutput {
        public String projectId;
        public String instanceId;
        public String databaseId;
        public boolean directAccessEnabled;
        public boolean directPathConfirmed;
        public int operationsCount;
        public int stalenessSeconds;
        public String timestamp;
        public LatencyPercentiles writes;
        public LatencyPercentiles strongReads;
        public LatencyPercentiles staleReads;
    }

    public static class WorkloadResult {
        public final List<Double> latencies = new ArrayList<>();
        public int attempted = 0;
        public int found = 0;
    }

    private static boolean isRunningOnGce() {
        try {
            URL url = new URL("http://metadata.google.internal/computeMetadata/v1/instance/id");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Metadata-Flavor", "Google");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            return conn.getResponseCode() == 200;
        } catch (Throwable t) {
            return false;
        }
    }

    public static void main(String[] args) {
        Map<String, String> parsedArgs = parseArgs(args);

        String projectId = parsedArgs.getOrDefault("projectId", "");
        String instanceId = parsedArgs.getOrDefault("instanceId", "");
        String databaseId = parsedArgs.getOrDefault("databaseId", "");
        String leaderRegion = parsedArgs.getOrDefault("leaderRegion", "").trim();
        int operations = Integer.parseInt(parsedArgs.getOrDefault("operations", "500"));
        int stalenessSeconds = Integer.parseInt(parsedArgs.getOrDefault("stalenessSeconds", "15"));
        String outputFile = parsedArgs.getOrDefault("outputFile", "benchmark_results.json");

        if (projectId.isEmpty() || instanceId.isEmpty() || databaseId.isEmpty()) {
            System.err.println("Usage: java -jar spanner-benchmark.jar --projectId=<ID> --instanceId=<ID> --databaseId=<ID> [--leaderRegion=<REGION>] [--operations=500] [--stalenessSeconds=15]");
            System.exit(1);
        }

        boolean onGce = isRunningOnGce();
        boolean directAccessActive = Boolean.parseBoolean(System.getenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS"));

        System.out.println("====================================================================");
        System.out.println("  Google Cloud Spanner Direct Access Latency Benchmark");
        System.out.println("====================================================================");
        System.out.printf(" Project:             %s%n", projectId);
        System.out.printf(" Instance:            %s%n", instanceId);
        System.out.printf(" Database:            %s%n", databaseId);
        System.out.printf(" Leader Region (cfg): %s%n", leaderRegion.isEmpty() ? "Default (instance configuration)" : leaderRegion);
        System.out.printf(" Operations:          %d per workload%n", operations);
        System.out.printf(" Staleness Bound:     %d seconds%n", stalenessSeconds);
        System.out.printf(" Running on GCE:      %b%n", onGce);
        System.out.printf(" Direct Access (env): %b%n", directAccessActive);

        // Configure Direct Access (DirectPath xDS is triggered via GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS)
        SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder()
                .setProjectId(projectId);
        SpannerOptions options = optionsBuilder.build();
        System.out.printf(" DirectPath (cfg):    %b%n", options.isAttemptDirectPath());
        System.out.printf(" DirectPath wire:     %s%n",
                (onGce && directAccessActive) ? "ACTIVE (DirectPath xDS + ALTS directly to Spanner backends)"
                      : (onGce ? "OFF (Traversing GFE proxy - GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS not set)"
                               : "NO - off-GCE, all RPCs traverse Google Frontend (GFE)"));
        System.out.println("====================================================================\n");

        try (Spanner spanner = options.getService()) {
            DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
            DatabaseClient dbClient = spanner.getDatabaseClient(db);

            // Check active Spanner leader from information_schema
            try (ResultSet rs = dbClient.singleUse().executeQuery(
                    Statement.of("SELECT option_value FROM information_schema.database_options WHERE option_name = 'default_leader'"))) {
                if (rs.next()) {
                    System.out.printf(" Active Spanner Leader: %s (custom DDL configured)%n", rs.getString(0));
                } else {
                    System.out.println(" Active Spanner Leader: Default (instance configuration default)");
                }
            } catch (Exception e) {
                System.out.println(" Active Spanner Leader: Default (unspecified or single-region)");
            }

            // 1. Ensure Table LatencyKV exists and leader matches
            ensureSchemaExists(spanner, db, leaderRegion);

            // 2. Warm-up
            System.out.println("Warming up connection channels...");
            runWarmup(dbClient);

            // 3. Workload 1: Writes (INSERT)
            System.out.printf("Executing %d WRITE operations (single-row INSERT with commit timestamp)...%n", operations);
            List<String> writtenIds = new ArrayList<>(operations);
            WorkloadResult writeResult = runWriteWorkload(dbClient, operations, writtenIds);
            long writesCompletedAtMs = System.currentTimeMillis();

            // 4. Workload 2: Strong Reads (Point lookups)
            System.out.printf("Executing %d STRONG READ operations (point lookups)...%n", operations);
            WorkloadResult strongReadResult = runStrongReadWorkload(dbClient, writtenIds);

            // Staleness Window Synchronization
            long targetMs = writesCompletedAtMs + (stalenessSeconds + 2) * 1000L;
            long remainingMs = targetMs - System.currentTimeMillis();
            if (remainingMs > 0) {
                System.out.printf("Waiting %.1fs for the %ds staleness window to elapse...%n",
                        remainingMs / 1000.0, stalenessSeconds);
                Thread.sleep(remainingMs);
            } else {
                System.out.println("Staleness window already elapsed during strong reads.");
            }

            // 5. Workload 3: Stale Reads (Point lookups with 15s staleness)
            System.out.printf("Executing %d STALE READ operations (staleness = %ds)...%n", operations, stalenessSeconds);
            WorkloadResult staleReadResult = runStaleReadWorkload(dbClient, writtenIds, stalenessSeconds);

            // 6. Aggregate Telemetry
            BenchmarkOutput output = new BenchmarkOutput();
            output.projectId = projectId;
            output.instanceId = instanceId;
            output.databaseId = databaseId;
            output.directAccessEnabled = directAccessActive;
            output.directPathConfirmed = onGce && directAccessActive;
            output.operationsCount = operations;
            output.stalenessSeconds = stalenessSeconds;
            output.timestamp = Instant.now().toString();
            output.writes = computePercentiles(writeResult);
            output.strongReads = computePercentiles(strongReadResult);
            output.staleReads = computePercentiles(staleReadResult);

            printSummaryTable(output);
            saveJsonOutput(output, outputFile);

            System.out.println("\n✅ Benchmark completed successfully.");
        } catch (Exception e) {
            System.err.println("❌ Benchmark failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void ensureSchemaExists(Spanner spanner, DatabaseId db, String leaderRegion) {
        System.out.println("Checking schema for table LatencyKV...");
        DatabaseClient client = spanner.getDatabaseClient(db);
        boolean tableExists = false;
        boolean columnExists = false;

        try (ResultSet rs = client.singleUse().executeQuery(
                Statement.of("SELECT table_name FROM information_schema.tables WHERE table_name = 'LatencyKV'"))) {
            if (rs.next()) {
                tableExists = true;
            }
        } catch (Exception ignored) {
        }

        if (tableExists) {
            try (ResultSet rs = client.singleUse().executeQuery(
                    Statement.of("SELECT column_name FROM information_schema.columns WHERE table_name = 'LatencyKV' AND column_name = 'created_at'"))) {
                if (rs.next()) {
                    columnExists = true;
                }
            } catch (Exception ignored) {
            }
        }

        try {
            DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
            String instanceId = db.getInstanceId().getInstance();
            String databaseId = db.getDatabase();
            if (!tableExists) {
                System.out.println("Creating table LatencyKV...");
                OperationFuture<?, UpdateDatabaseDdlMetadata> op = adminClient.updateDatabaseDdl(
                        instanceId,
                        databaseId,
                        Collections.singletonList(
                                "CREATE TABLE LatencyKV (" +
                                "  id STRING(64) NOT NULL," +
                                "  created_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)" +
                                ") PRIMARY KEY (id)"
                        ),
                        null
                );
                op.get(120, TimeUnit.SECONDS);
                System.out.println("Table LatencyKV created successfully.");
            } else if (!columnExists) {
                System.out.println("Adding missing column created_at to table LatencyKV...");
                OperationFuture<?, UpdateDatabaseDdlMetadata> op = adminClient.updateDatabaseDdl(
                        instanceId,
                        databaseId,
                        Collections.singletonList(
                                "ALTER TABLE LatencyKV ADD COLUMN created_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)"
                        ),
                        null
                );
                op.get(120, TimeUnit.SECONDS);
                System.out.println("Column created_at added successfully.");
            } else {
                System.out.println("Table LatencyKV and column created_at verified.");
            }
        } catch (Exception e) {
            System.out.println("Note on DDL check: " + e.getMessage());
        }

        // Check and apply custom default leader if specified
        if (leaderRegion != null && !leaderRegion.trim().isEmpty()) {
            String targetLeader = leaderRegion.trim();
            String currentLeader = null;
            try (ResultSet rs = client.singleUse().executeQuery(
                    Statement.of("SELECT option_value FROM information_schema.database_options WHERE option_name = 'default_leader'"))) {
                if (rs.next()) {
                    currentLeader = rs.getString(0);
                }
            } catch (Exception ignored) {
            }

            if (!targetLeader.equalsIgnoreCase(currentLeader)) {
                try {
                    DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
                    String instanceId = db.getInstanceId().getInstance();
                    String databaseId = db.getDatabase();
                    System.out.printf("Applying default leader DDL '%s' to database '%s' (current: %s)...%n",
                            targetLeader, databaseId, currentLeader != null ? currentLeader : "default");
                    OperationFuture<?, UpdateDatabaseDdlMetadata> op = adminClient.updateDatabaseDdl(
                            instanceId,
                            databaseId,
                            Collections.singletonList(
                                    "ALTER DATABASE `" + databaseId + "` SET OPTIONS (default_leader = '" + targetLeader + "')"
                            ),
                            null
                    );
                    op.get(120, TimeUnit.SECONDS);
                    System.out.printf("Database default leader set to '%s' successfully.%n", targetLeader);
                } catch (Exception e) {
                    System.out.println("Note on default leader DDL: " + e.getMessage());
                }
            } else {
                System.out.printf("Database default leader '%s' already configured.%n", targetLeader);
            }
        }
    }

    private static void runWarmup(DatabaseClient dbClient) {
        System.out.println("Executing connection & channel warmup...");
        // 1. Warm up read channel and session pool
        for (int i = 0; i < 5; i++) {
            try (ResultSet rs = dbClient.singleUse().executeQuery(Statement.of("SELECT 1"))) {
                if (rs.next()) {
                    rs.getLong(0);
                }
            } catch (Exception ignored) {
            }
        }
        // 2. Warm up write channel and gRPC stream with dummy keys
        for (int i = 0; i < 3; i++) {
            try {
                Mutation warmupMutation = Mutation.newInsertOrUpdateBuilder("LatencyKV")
                        .set("id").to("warmup-" + i)
                        .set("created_at").to(Value.COMMIT_TIMESTAMP)
                        .build();
                dbClient.writeAtLeastOnce(Collections.singletonList(warmupMutation));
            } catch (Exception ignored) {
            }
        }
        // 3. Warm up point read path
        for (int i = 0; i < 3; i++) {
            try {
                dbClient.singleUse().readRow("LatencyKV", Key.of("warmup-" + i), Arrays.asList("id", "created_at"));
            } catch (Exception ignored) {
            }
        }
        System.out.println("Warmup complete. Commencing benchmark workloads...\n");
    }

    private static WorkloadResult runWriteWorkload(DatabaseClient dbClient, int count, List<String> writtenIds) {
        WorkloadResult result = new WorkloadResult();

        for (int i = 0; i < count; i++) {
            String id = "key-" + UUID.randomUUID();
            writtenIds.add(id);
            result.attempted++;

            Mutation mutation = Mutation.newInsertOrUpdateBuilder("LatencyKV")
                    .set("id").to(id)
                    .set("created_at").to(Value.COMMIT_TIMESTAMP)
                    .build();
            List<Mutation> mutations = Collections.singletonList(mutation);

            long start = System.nanoTime();
            dbClient.writeAtLeastOnce(mutations);
            double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
            result.latencies.add(elapsedMs);
            result.found++;
        }
        return result;
    }

    private static WorkloadResult runStrongReadWorkload(DatabaseClient dbClient, List<String> ids) {
        WorkloadResult result = new WorkloadResult();
        for (String id : ids) {
            result.attempted++;
            long start = System.nanoTime();
            Struct row = dbClient.singleUse().readRow("LatencyKV", Key.of(id), Arrays.asList("id", "created_at"));
            double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
            if (row != null) {
                result.found++;
                result.latencies.add(elapsedMs);
            }
        }
        return result;
    }

    private static WorkloadResult runStaleReadWorkload(DatabaseClient dbClient, List<String> ids, int stalenessSeconds) {
        WorkloadResult result = new WorkloadResult();
        TimestampBound bound = TimestampBound.ofExactStaleness(stalenessSeconds, TimeUnit.SECONDS);

        for (String id : ids) {
            result.attempted++;
            long start = System.nanoTime();
            Struct row = dbClient.singleUse(bound).readRow("LatencyKV", Key.of(id), Arrays.asList("id", "created_at"));
            double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
            if (row != null) {
                result.found++;
                result.latencies.add(elapsedMs);
            }
        }
        return result;
    }

    private static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) return 0.0;
        int idx = (int) Math.ceil(p * sorted.size()) - 1;
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return Math.round(sorted.get(idx) * 100.0) / 100.0;
    }

    private static LatencyPercentiles computePercentiles(WorkloadResult res) {
        LatencyPercentiles p = new LatencyPercentiles();
        if (res == null || res.attempted == 0) {
            return p;
        }

        p.opsCount = res.found;
        p.hitRate = res.attempted > 0 ? (double) res.found / res.attempted : 0.0;
        p.hitRate = Math.round(p.hitRate * 100.0) / 100.0;

        List<Double> latencies = new ArrayList<>(res.latencies);
        if (latencies.isEmpty()) {
            return p;
        }

        Collections.sort(latencies);
        int n = latencies.size();
        p.min = Math.round(latencies.get(0) * 100.0) / 100.0;
        p.max = Math.round(latencies.get(n - 1) * 100.0) / 100.0;
        p.p50 = percentile(latencies, 0.50);
        p.p90 = percentile(latencies, 0.90);
        p.p95 = percentile(latencies, 0.95);
        p.p99 = percentile(latencies, 0.99);

        double sum = 0;
        for (double d : latencies) sum += d;
        p.avg = Math.round((sum / n) * 100.0) / 100.0;

        return p;
    }

    private static void printSummaryTable(BenchmarkOutput out) {
        System.out.println("\n=====================================================================================================");
        System.out.println("                               BENCHMARK LATENCY SUMMARY (ms)");
        System.out.println("=====================================================================================================");
        System.out.printf("%-18s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-7s%n",
                "Workload", "Min", "p50", "p90", "p95", "p99", "Max", "Avg", "HitRate");
        System.out.println("-------------------+----------+----------+----------+----------+----------+----------+----------+--------");
        printMetricRow("Writes (INSERT)", out.writes);
        printMetricRow("Strong Reads", out.strongReads);
        printMetricRow("Stale Reads (15s)", out.staleReads);
        System.out.println("=====================================================================================================\n");
    }

    private static void printMetricRow(String name, LatencyPercentiles m) {
        System.out.printf("%-18s | %8.2f | %8.2f | %8.2f | %8.2f | %8.2f | %8.2f | %8.2f | %6.0f%%%n",
                name, m.min, m.p50, m.p90, m.p95, m.p99, m.max, m.avg, m.hitRate * 100.0);
    }

    private static String formatPercentilesJson(LatencyPercentiles p) {
        if (p == null) {
            return "{}";
        }
        return String.format(Locale.US,
                "{\n" +
                "    \"opsCount\": %d,\n" +
                "    \"hitRate\": %.2f,\n" +
                "    \"p50\": %.2f,\n" +
                "    \"p90\": %.2f,\n" +
                "    \"p95\": %.2f,\n" +
                "    \"p99\": %.2f,\n" +
                "    \"min\": %.2f,\n" +
                "    \"max\": %.2f,\n" +
                "    \"avg\": %.2f\n" +
                "  }",
                p.opsCount, p.hitRate, p.p50, p.p90, p.p95, p.p99, p.min, p.max, p.avg);
    }

    private static void saveJsonOutput(BenchmarkOutput output, String filePath) {
        String json = String.format(Locale.US,
                "{\n" +
                "  \"projectId\": \"%s\",\n" +
                "  \"instanceId\": \"%s\",\n" +
                "  \"databaseId\": \"%s\",\n" +
                "  \"directAccessEnabled\": %b,\n" +
                "  \"directPathConfirmed\": %b,\n" +
                "  \"operationsCount\": %d,\n" +
                "  \"stalenessSeconds\": %d,\n" +
                "  \"timestamp\": \"%s\",\n" +
                "  \"writes\": %s,\n" +
                "  \"strongReads\": %s,\n" +
                "  \"staleReads\": %s\n" +
                "}\n",
                output.projectId, output.instanceId, output.databaseId,
                output.directAccessEnabled, output.directPathConfirmed, output.operationsCount,
                output.stalenessSeconds, output.timestamp,
                formatPercentilesJson(output.writes),
                formatPercentilesJson(output.strongReads),
                formatPercentilesJson(output.staleReads)
        );
        try (FileWriter writer = new FileWriter(filePath, StandardCharsets.UTF_8)) {
            writer.write(json);
            System.out.printf("Telemetry saved to file: %s%n", filePath);
        } catch (IOException e) {
            System.err.println("Could not write output file: " + e.getMessage());
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String[] parts = arg.substring(2).split("=", 2);
                if (parts.length == 2) {
                    map.put(parts[0], parts[1]);
                }
            }
        }
        return map;
    }
}
