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
package com.spanner.benchmark.benchmark;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.spanner.benchmark.config.AppConfig;
import com.spanner.benchmark.model.CustomerInsightKey;
import com.spanner.benchmark.model.CustomerInsightPhoneKey;
import com.spanner.benchmark.util.CliArguments;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class BenchmarkCommand {
  private static final List<String> READ_COLUMNS =
      List.of(
          "cust_id",
          "acct_no",
          "phone_number",
          "insight_category",
          "insight_name",
          "insight_values",
          "updated_by",
          "updated_at");

  private BenchmarkCommand() {}

  public static void run(AppConfig config, DatabaseClient client, String[] args) throws Exception {
    CliArguments cli = CliArguments.parse(args);
    BenchmarkTable benchmarkTable = BenchmarkTable.fromName(cli.getString("table", "all"));
    String scenarioName = cli.getString("scenario", "core-read-paths");
    String selectionModeName = cli.getString("selection-mode", "random");
    long selectionSeed = Long.parseLong(cli.getString("selection-seed", "42"));
    int warmupIterations = cli.getInt("warmup", config.defaultWarmupIterations());
    int measuredIterations = cli.getInt("iterations", config.defaultMeasuredIterations());
    int concurrency = cli.getInt("concurrency", config.defaultConcurrency());
    String concurrencySweepArg = cli.getString("concurrency-sweep", "");
    int sampleSize = cli.getInt("sample-size", 200);
    int stalenessSeconds = cli.getInt("staleness-seconds", config.defaultStalenessSeconds());

    SelectionMode selectionMode = SelectionMode.fromName(selectionModeName);

    List<ConsistencyMode> consistencyModes =
        List.of(
            new ConsistencyMode("strong", TimestampBound.strong()),
            new ConsistencyMode(
                "stale-" + stalenessSeconds + "s",
                TimestampBound.ofExactStaleness(stalenessSeconds, TimeUnit.SECONDS)));

    List<Scenario> scenarios = new ArrayList<>();
    List<TableBenchmarkPlan> tablePlans = loadSelectedTablePlans(client, benchmarkTable, sampleSize);
    for (TableBenchmarkPlan tablePlan : tablePlans) {
      for (ConsistencyMode consistencyMode : consistencyModes) {
        scenarios.addAll(
            buildScenariosForMode(
                client,
                tablePlan,
                consistencyMode,
                scenarioName,
                selectionMode,
                selectionSeed));
      }
    }

    if (scenarios.isEmpty()) {
      throw new IllegalArgumentException(
          "Unsupported scenario '"
              + scenarioName
              + "'. Supported values: core-read-paths, customer-account, phone-number, exact-primary-key-sql, customer-only, full-key-read, all");
    }

    List<Integer> concurrencyLevels = parseConcurrencyLevels(concurrency, concurrencySweepArg);
    System.out.printf(
        "Running benchmark with table=%s, scenario=%s, selectionMode=%s, selectionSeed=%d, warmup=%d, iterations=%d, concurrency=%s, sampleSize=%d, staleReadSeconds=%d%n",
        benchmarkTable.displayName(),
        scenarioName,
        selectionMode.displayName(),
        selectionSeed,
        warmupIterations,
        measuredIterations,
        formatConcurrencyLevels(concurrencyLevels),
        sampleSize,
        stalenessSeconds);

    List<SweepResult> sweepResults = new ArrayList<>();
    for (int currentConcurrency : concurrencyLevels) {
      for (Scenario scenario : scenarios) {
        BenchmarkResult result =
            runScenario(scenario, warmupIterations, measuredIterations, currentConcurrency);
        printResult(result, currentConcurrency);
        sweepResults.add(new SweepResult(currentConcurrency, result));
      }
    }

    if (concurrencyLevels.size() > 1) {
      printSweepReport(sweepResults);
    }
  }

  private static List<TableBenchmarkPlan> loadSelectedTablePlans(
      DatabaseClient client, BenchmarkTable benchmarkTable, int sampleSize) {
    List<TableBenchmarkPlan> plans = new ArrayList<>();
    for (TableSpec tableSpec : benchmarkTable.tableSpecs()) {
      TableBenchmarkPlan plan = tableSpec.loadPlan(client, sampleSize);
      if (plan.isEmpty()) {
        throw new IllegalStateException(
            "No rows found in " + tableSpec.tableName() + ". Run populate first.");
      }
      plans.add(plan);
    }
    return plans;
  }

  private static List<Scenario> buildScenariosForMode(
      DatabaseClient client,
      TableBenchmarkPlan tablePlan,
      ConsistencyMode consistencyMode,
      String scenarioName,
      SelectionMode selectionMode,
      long selectionSeed) {
    List<Scenario> scenarios = new ArrayList<>();
    switch (tablePlan.tableSpec()) {
      case CUSTOMER_INSIGHTS ->
          buildCustomerInsightsScenarios(
              client, tablePlan, consistencyMode, scenarioName, selectionMode, selectionSeed, scenarios);
      case CUSTOMER_INSIGHTS_PHONE ->
          buildCustomerInsightsPhoneScenarios(
              client, tablePlan, consistencyMode, scenarioName, selectionMode, selectionSeed, scenarios);
    }
    return scenarios;
  }

  private static void buildCustomerInsightsScenarios(
      DatabaseClient client,
      TableBenchmarkPlan tablePlan,
      ConsistencyMode consistencyMode,
      String scenarioName,
      SelectionMode selectionMode,
      long selectionSeed,
      List<Scenario> scenarios) {
    CustomerInsightsSamples samples = tablePlan.customerInsightsSamples();
    switch (scenarioName) {
      case "core-read-paths" -> {
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-cust-id-and-acct-no"),
                consistencyMode.name(),
                () ->
                    executeCustomerAccountQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.customerAccounts(), selectionMode, selectionSeed))));
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-full-primary-key"),
                consistencyMode.name(),
                () ->
                    executeFullPrimaryKeyQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.keys(), selectionMode, selectionSeed))));
      }
      case "customer-account" ->
          scenarios.add(
              new Scenario(
                  scenarioLabel(tablePlan.tableSpec(), "sql-by-cust-id-and-acct-no"),
                  consistencyMode.name(),
                  () ->
                      executeCustomerAccountQuery(
                          client,
                          tablePlan.tableSpec().tableName(),
                          consistencyMode.timestampBound(),
                          sampler(samples.customerAccounts(), selectionMode, selectionSeed))));
      case "exact-primary-key-sql" ->
          scenarios.add(
              new Scenario(
                  scenarioLabel(tablePlan.tableSpec(), "sql-by-full-primary-key"),
                  consistencyMode.name(),
                  () ->
                      executeFullPrimaryKeyQuery(
                          client,
                          tablePlan.tableSpec().tableName(),
                          consistencyMode.timestampBound(),
                          sampler(samples.keys(), selectionMode, selectionSeed))));
      case "customer-only" ->
          scenarios.add(
              new Scenario(
                  scenarioLabel(tablePlan.tableSpec(), "sql-by-cust-id"),
                  consistencyMode.name(),
                  () ->
                      executeCustomerQuery(
                          client,
                          tablePlan.tableSpec().tableName(),
                          consistencyMode.timestampBound(),
                          sampler(samples.customerIds(), selectionMode, selectionSeed))));
      case "full-key-read" ->
          scenarios.add(
              new Scenario(
                  scenarioLabel(tablePlan.tableSpec(), "point-read-full-key"),
                  consistencyMode.name(),
                  () ->
                      executePointRead(
                          client,
                          tablePlan.tableSpec().tableName(),
                          consistencyMode.timestampBound(),
                          sampler(samples.keys(), selectionMode, selectionSeed))));
      case "all" -> {
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "point-read-full-key"),
                consistencyMode.name(),
                () ->
                    executePointRead(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.keys(), selectionMode, selectionSeed))));
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-cust-id"),
                consistencyMode.name(),
                () ->
                    executeCustomerQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.customerIds(), selectionMode, selectionSeed))));
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-cust-id-and-acct-no"),
                consistencyMode.name(),
                () ->
                    executeCustomerAccountQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.customerAccounts(), selectionMode, selectionSeed))));
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-full-primary-key"),
                consistencyMode.name(),
                () ->
                    executeFullPrimaryKeyQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.keys(), selectionMode, selectionSeed))));
      }
      default -> {}
    }
  }

  private static void buildCustomerInsightsPhoneScenarios(
      DatabaseClient client,
      TableBenchmarkPlan tablePlan,
      ConsistencyMode consistencyMode,
      String scenarioName,
      SelectionMode selectionMode,
      long selectionSeed,
      List<Scenario> scenarios) {
    CustomerInsightsPhoneSamples samples = tablePlan.customerInsightsPhoneSamples();
    switch (scenarioName) {
      case "core-read-paths" -> {
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-phone-number"),
                consistencyMode.name(),
                () ->
                    executePhoneNumberQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.phoneNumbers(), selectionMode, selectionSeed))));
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-full-primary-key"),
                consistencyMode.name(),
                () ->
                    executePhonePrimaryKeyQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.keys(), selectionMode, selectionSeed))));
      }
      case "phone-number" ->
          scenarios.add(
              new Scenario(
                  scenarioLabel(tablePlan.tableSpec(), "sql-by-phone-number"),
                  consistencyMode.name(),
                  () ->
                      executePhoneNumberQuery(
                          client,
                          tablePlan.tableSpec().tableName(),
                          consistencyMode.timestampBound(),
                          sampler(samples.phoneNumbers(), selectionMode, selectionSeed))));
      case "exact-primary-key-sql" ->
          scenarios.add(
              new Scenario(
                  scenarioLabel(tablePlan.tableSpec(), "sql-by-full-primary-key"),
                  consistencyMode.name(),
                  () ->
                      executePhonePrimaryKeyQuery(
                          client,
                          tablePlan.tableSpec().tableName(),
                          consistencyMode.timestampBound(),
                          sampler(samples.keys(), selectionMode, selectionSeed))));
      case "full-key-read" ->
          scenarios.add(
              new Scenario(
                  scenarioLabel(tablePlan.tableSpec(), "point-read-full-key"),
                  consistencyMode.name(),
                  () ->
                      executePhonePointRead(
                          client,
                          tablePlan.tableSpec().tableName(),
                          consistencyMode.timestampBound(),
                          sampler(samples.keys(), selectionMode, selectionSeed))));
      case "all" -> {
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "point-read-full-key"),
                consistencyMode.name(),
                () ->
                    executePhonePointRead(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.keys(), selectionMode, selectionSeed))));
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-phone-number"),
                consistencyMode.name(),
                () ->
                    executePhoneNumberQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.phoneNumbers(), selectionMode, selectionSeed))));
        scenarios.add(
            new Scenario(
                scenarioLabel(tablePlan.tableSpec(), "sql-by-full-primary-key"),
                consistencyMode.name(),
                () ->
                    executePhonePrimaryKeyQuery(
                        client,
                        tablePlan.tableSpec().tableName(),
                        consistencyMode.timestampBound(),
                        sampler(samples.keys(), selectionMode, selectionSeed))));
      }
      default -> {}
    }
  }

  private static CustomerInsightsSamples loadCustomerInsightsSamples(
      DatabaseClient client, String tableName, int sampleSize) {
    List<CustomerInsightKey> keys = new ArrayList<>();
    try (ResultSet rs =
        client
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT cust_id, acct_no, phone_number, insight_category, insight_name "
                        + "FROM "
                        + tableName
                        + " "
                        + "ORDER BY cust_id, acct_no, phone_number, insight_category, insight_name "
                        + "LIMIT "
                        + sampleSize))) {
      while (rs.next()) {
        keys.add(
            new CustomerInsightKey(
                rs.getString("cust_id"),
                rs.getString("acct_no"),
                rs.getString("phone_number"),
                rs.getString("insight_category"),
                rs.getString("insight_name")));
      }
    }

    List<String> customerIds = new ArrayList<>();
    try (ResultSet rs =
        client
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT DISTINCT cust_id "
                        + "FROM " + tableName + " ORDER BY cust_id LIMIT "
                        + sampleSize))) {
      while (rs.next()) {
        customerIds.add(rs.getString("cust_id"));
      }
    }

    List<CustomerAccount> customerAccounts = new ArrayList<>();
    try (ResultSet rs =
        client
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT DISTINCT cust_id, acct_no "
                        + "FROM " + tableName + " ORDER BY cust_id, acct_no LIMIT "
                        + sampleSize))) {
      while (rs.next()) {
        customerAccounts.add(new CustomerAccount(rs.getString("cust_id"), rs.getString("acct_no")));
      }
    }

    return new CustomerInsightsSamples(keys, customerIds, customerAccounts);
  }

  private static CustomerInsightsPhoneSamples loadCustomerInsightsPhoneSamples(
      DatabaseClient client, String tableName, int sampleSize) {
    List<CustomerInsightPhoneKey> keys = new ArrayList<>();
    try (ResultSet rs =
        client
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT phone_number, insight_category, insight_name "
                        + "FROM "
                        + tableName
                        + " "
                        + "ORDER BY phone_number, insight_category, insight_name "
                        + "LIMIT "
                        + sampleSize))) {
      while (rs.next()) {
        keys.add(
            new CustomerInsightPhoneKey(
                rs.getString("phone_number"),
                rs.getString("insight_category"),
                rs.getString("insight_name")));
      }
    }

    List<String> phoneNumbers = new ArrayList<>();
    try (ResultSet rs =
        client
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT DISTINCT phone_number "
                        + "FROM " + tableName + " ORDER BY phone_number LIMIT "
                        + sampleSize))) {
      while (rs.next()) {
        phoneNumbers.add(rs.getString("phone_number"));
      }
    }

    return new CustomerInsightsPhoneSamples(keys, phoneNumbers);
  }

  private static String scenarioLabel(TableSpec tableSpec, String scenarioName) {
    return tableSpec.tableName() + "." + scenarioName;
  }

  private static <T> Sampler<T> sampler(List<T> items, SelectionMode selectionMode, long selectionSeed) {
    return switch (selectionMode) {
      case RANDOM -> new RandomSampler<>(items);
      case RANDOM_WITHOUT_REPLACEMENT -> new RandomWithoutReplacementSampler<>(items, selectionSeed);
      case ROUND_ROBIN -> new RoundRobinSampler<>(items);
      case SHUFFLE_ONCE -> new ShuffleOnceSampler<>(items, selectionSeed);
    };
  }

  private static int executePointRead(
      DatabaseClient client,
      String tableName,
      TimestampBound timestampBound,
      Sampler<CustomerInsightKey> sampler) {
    CustomerInsightKey key = sampler.next();
    Struct row = client.singleUse(timestampBound).readRow(tableName, key.asSpannerKey(), READ_COLUMNS);
    if (row == null) {
      throw new IllegalStateException(
          "Point read returned no row for table " + tableName + " and key: " + key);
    }
    row.getString("cust_id");
    return 1;
  }

  private static int executePhonePointRead(
      DatabaseClient client,
      String tableName,
      TimestampBound timestampBound,
      Sampler<CustomerInsightPhoneKey> sampler) {
    CustomerInsightPhoneKey key = sampler.next();
    Struct row = client.singleUse(timestampBound).readRow(tableName, key.asSpannerKey(), READ_COLUMNS);
    if (row == null) {
      throw new IllegalStateException(
          "Point read returned no row for table " + tableName + " and key: " + key);
    }
    row.getString("phone_number");
    return 1;
  }

  private static int executeCustomerQuery(
      DatabaseClient client, String tableName, TimestampBound timestampBound, Sampler<String> sampler) {
    String custId = sampler.next();
    Statement statement =
        Statement.newBuilder(
                "SELECT cust_id, acct_no, phone_number, insight_category, insight_name "
                    + "FROM " + tableName + " WHERE cust_id = @custId")
            .bind("custId")
            .to(custId)
            .build();
    return consumeRows(client, timestampBound, statement);
  }

  private static int executeCustomerAccountQuery(
      DatabaseClient client,
      String tableName,
      TimestampBound timestampBound,
      Sampler<CustomerAccount> sampler) {
    CustomerAccount account = sampler.next();
    Statement statement =
        Statement.newBuilder(
                "SELECT cust_id, acct_no, phone_number, insight_category, insight_name "
                    + "FROM " + tableName + " WHERE cust_id = @custId AND acct_no = @acctNo")
            .bind("custId")
            .to(account.custId())
            .bind("acctNo")
            .to(account.acctNo())
            .build();
    return consumeRows(client, timestampBound, statement);
  }

  private static int executeFullPrimaryKeyQuery(
      DatabaseClient client,
      String tableName,
      TimestampBound timestampBound,
      Sampler<CustomerInsightKey> sampler) {
    CustomerInsightKey key = sampler.next();
    Statement statement =
        Statement.newBuilder(
                "SELECT cust_id, acct_no, phone_number, insight_category, insight_name "
                    + "FROM "
                    + tableName
                    + " "
                    + "WHERE cust_id = @custId "
                    + "AND acct_no = @acctNo "
                    + "AND phone_number = @phoneNumber "
                    + "AND insight_category = @insightCategory "
                    + "AND insight_name = @insightName")
            .bind("custId")
            .to(key.custId())
            .bind("acctNo")
            .to(key.acctNo())
            .bind("phoneNumber")
            .to(key.phoneNumber())
            .bind("insightCategory")
            .to(key.insightCategory())
            .bind("insightName")
            .to(key.insightName())
            .build();
    return consumeRows(client, timestampBound, statement);
  }

  private static int executePhoneNumberQuery(
      DatabaseClient client, String tableName, TimestampBound timestampBound, Sampler<String> sampler) {
    String phoneNumber = sampler.next();
    Statement statement =
        Statement.newBuilder(
                "SELECT cust_id, acct_no, phone_number, insight_category, insight_name "
                    + "FROM " + tableName + " WHERE phone_number = @phoneNumber")
            .bind("phoneNumber")
            .to(phoneNumber)
            .build();
    return consumeRows(client, timestampBound, statement);
  }

  private static int executePhonePrimaryKeyQuery(
      DatabaseClient client,
      String tableName,
      TimestampBound timestampBound,
      Sampler<CustomerInsightPhoneKey> sampler) {
    CustomerInsightPhoneKey key = sampler.next();
    Statement statement =
        Statement.newBuilder(
                "SELECT cust_id, acct_no, phone_number, insight_category, insight_name "
                    + "FROM "
                    + tableName
                    + " "
                    + "WHERE phone_number = @phoneNumber "
                    + "AND insight_category = @insightCategory "
                    + "AND insight_name = @insightName")
            .bind("phoneNumber")
            .to(key.phoneNumber())
            .bind("insightCategory")
            .to(key.insightCategory())
            .bind("insightName")
            .to(key.insightName())
            .build();
    return consumeRows(client, timestampBound, statement);
  }

  private static int consumeRows(
      DatabaseClient client, TimestampBound timestampBound, Statement statement) {
    int rows = 0;
    try (ResultSet rs = client.singleUse(timestampBound).executeQuery(statement)) {
      while (rs.next()) {
        rows++;
      }
    }
    if (rows == 0) {
      throw new IllegalStateException("Query returned zero rows.");
    }
    return rows;
  }

  private static BenchmarkResult runScenario(
      Scenario scenario, int warmupIterations, int measuredIterations, int concurrency)
      throws InterruptedException, ExecutionException {
    for (int i = 0; i < warmupIterations; i++) {
      scenario.action().call();
    }

    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    List<Future<Measurement>> futures = new ArrayList<>(measuredIterations);
    long startedAt = System.nanoTime();
    try {
      for (int i = 0; i < measuredIterations; i++) {
        futures.add(executor.submit(timed(scenario.action())));
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.MINUTES);
    }

    long totalElapsed = System.nanoTime() - startedAt;
    List<Measurement> measurements = new ArrayList<>(measuredIterations);
    for (Future<Measurement> future : futures) {
      measurements.add(future.get());
    }
    return BenchmarkResult.from(
        scenario.name(), scenario.consistencyMode(), measurements, totalElapsed);
  }

  private static Callable<Measurement> timed(ScenarioAction action) {
    return () -> {
      long startedAt = System.nanoTime();
      int rowsReturned = action.call();
      return new Measurement(System.nanoTime() - startedAt, rowsReturned);
    };
  }

  private static void printResult(BenchmarkResult result, int concurrency) {
    System.out.printf(
        "%nScenario: %s%n"
            + "  consistency: %s%n"
            + "  concurrency: %d%n"
            + "  count: %d%n"
            + "  throughput ops/sec: %.2f%n"
            + "  rows returned: min=%d avg=%.2f p50=%.2f p95=%.2f p99=%.2f max=%d%n"
            + "  latency ms: min=%.2f avg=%.2f p50=%.2f p95=%.2f p99=%.2f max=%.2f%n",
        result.name(),
        result.consistencyMode(),
        concurrency,
        result.count(),
        result.throughputOpsPerSecond(),
        result.minRows(),
        result.avgRows(),
        result.p50Rows(),
        result.p95Rows(),
        result.p99Rows(),
        result.maxRows(),
        result.minMs(),
        result.avgMs(),
        result.p50Ms(),
        result.p95Ms(),
        result.p99Ms(),
        result.maxMs());
  }

  private static List<Integer> parseConcurrencyLevels(
      int fallbackConcurrency, String concurrencySweepArg) {
    if (concurrencySweepArg == null || concurrencySweepArg.isBlank()) {
      return List.of(fallbackConcurrency);
    }

    List<Integer> levels = new ArrayList<>();
    for (String part : concurrencySweepArg.split(",")) {
      String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int parsed = Integer.parseInt(trimmed);
      if (parsed <= 0) {
        throw new IllegalArgumentException("Concurrency values must be positive: " + trimmed);
      }
      if (!levels.contains(parsed)) {
        levels.add(parsed);
      }
    }

    if (levels.isEmpty()) {
      throw new IllegalArgumentException(
          "Concurrency sweep produced no valid values. Example: --concurrency-sweep 1,2,4,8");
    }
    return levels;
  }

  private static String formatConcurrencyLevels(List<Integer> concurrencyLevels) {
    if (concurrencyLevels.size() == 1) {
      return Integer.toString(concurrencyLevels.get(0));
    }
    return concurrencyLevels.toString();
  }

  private static void printSweepReport(List<SweepResult> sweepResults) {
    int scenarioWidth = "scenario".length();
    int consistencyWidth = "consistency".length();
    for (SweepResult sweepResult : sweepResults) {
      scenarioWidth = Math.max(scenarioWidth, sweepResult.result().name().length());
      consistencyWidth = Math.max(consistencyWidth, sweepResult.result().consistencyMode().length());
    }

    String rowFormat =
        "%-" + scenarioWidth + "s | %-" + consistencyWidth + "s | %11s | %18s | %8s | %8s | %8s | %8s | %8s%n";
    String valueFormat =
        "%-" + scenarioWidth + "s | %-" + consistencyWidth + "s | %11d | %18.2f | %8.2f | %8.2f | %8.2f | %8.2f | %8.2f%n";

    System.out.println("\nConcurrency sweep report:");
    System.out.printf(
        rowFormat,
        "scenario",
        "consistency",
        "concurrency",
        "throughput_ops_sec",
        "avg_rows",
        "p95_rows",
        "avg_ms",
        "p95_ms",
        "p99_ms");

    sweepResults.stream()
        .sorted(
            Comparator.comparing((SweepResult result) -> result.result().name())
                .thenComparing(result -> result.result().consistencyMode())
                .thenComparingInt(SweepResult::concurrency))
        .forEach(
            sweepResult -> {
              BenchmarkResult result = sweepResult.result();
              System.out.printf(
                  valueFormat,
                  result.name(),
                  result.consistencyMode(),
                  sweepResult.concurrency(),
                  result.throughputOpsPerSecond(),
                  result.avgRows(),
                  result.p95Rows(),
                  result.avgMs(),
                  result.p95Ms(),
                  result.p99Ms());
            });
  }

  @FunctionalInterface
  private interface ScenarioAction {
    int call();
  }

  private interface Sampler<T> {
    T next();
  }

  private static final class RandomSampler<T> implements Sampler<T> {
    private final List<T> items;

    private RandomSampler(List<T> items) {
      this.items = items;
    }

    @Override
    public T next() {
      return items.get(ThreadLocalRandom.current().nextInt(items.size()));
    }
  }

  private static final class RoundRobinSampler<T> implements Sampler<T> {
    private final List<T> items;
    private final AtomicInteger index;

    private RoundRobinSampler(List<T> items) {
      this.items = items;
      this.index = new AtomicInteger();
    }

    @Override
    public T next() {
      return items.get(Math.floorMod(index.getAndIncrement(), items.size()));
    }
  }

  private static final class RandomWithoutReplacementSampler<T> implements Sampler<T> {
    private final List<T> items;
    private final Random random;
    private final AtomicInteger index;

    private RandomWithoutReplacementSampler(List<T> items, long seed) {
      this.items = new ArrayList<>(items);
      this.random = new Random(seed);
      Collections.shuffle(this.items, this.random);
      this.index = new AtomicInteger();
    }

    @Override
    public synchronized T next() {
      int currentIndex = index.getAndIncrement();
      if (currentIndex >= items.size()) {
        Collections.shuffle(items, random);
        index.set(1);
        currentIndex = 0;
      }
      return items.get(currentIndex);
    }
  }

  private static final class ShuffleOnceSampler<T> implements Sampler<T> {
    private final List<T> items;
    private final AtomicInteger index;

    private ShuffleOnceSampler(List<T> items, long seed) {
      this.items = new ArrayList<>(items);
      Collections.shuffle(this.items, new Random(seed));
      this.index = new AtomicInteger();
    }

    @Override
    public T next() {
      return items.get(Math.floorMod(index.getAndIncrement(), items.size()));
    }
  }

  private enum SelectionMode {
    RANDOM,
    RANDOM_WITHOUT_REPLACEMENT,
    ROUND_ROBIN,
    SHUFFLE_ONCE;

    static SelectionMode fromName(String name) {
      return switch (name) {
        case "random" -> RANDOM;
        case "random-without-replacement" -> RANDOM_WITHOUT_REPLACEMENT;
        case "round-robin" -> ROUND_ROBIN;
        case "shuffle-once" -> SHUFFLE_ONCE;
        default ->
            throw new IllegalArgumentException(
                "Unsupported selection mode '" + name + "'. Supported values: random, random-without-replacement, round-robin, shuffle-once");
      };
    }

    String displayName() {
      return switch (this) {
        case RANDOM -> "random";
        case RANDOM_WITHOUT_REPLACEMENT -> "random-without-replacement";
        case ROUND_ROBIN -> "round-robin";
        case SHUFFLE_ONCE -> "shuffle-once";
      };
    }
  }

  private record Scenario(String name, String consistencyMode, ScenarioAction action) {}

  private record ConsistencyMode(String name, TimestampBound timestampBound) {}

  private record CustomerAccount(String custId, String acctNo) {}

  private record CustomerInsightsSamples(
      List<CustomerInsightKey> keys,
      List<String> customerIds,
      List<CustomerAccount> customerAccounts) {}

  private record CustomerInsightsPhoneSamples(
      List<CustomerInsightPhoneKey> keys,
      List<String> phoneNumbers) {}

  private record TableBenchmarkPlan(
      TableSpec tableSpec,
      CustomerInsightsSamples customerInsightsSamples,
      CustomerInsightsPhoneSamples customerInsightsPhoneSamples) {

    boolean isEmpty() {
      return switch (tableSpec) {
        case CUSTOMER_INSIGHTS -> customerInsightsSamples.keys().isEmpty();
        case CUSTOMER_INSIGHTS_PHONE -> customerInsightsPhoneSamples.keys().isEmpty();
      };
    }
  }

  private record SweepResult(int concurrency, BenchmarkResult result) {}

  private record Measurement(long durationNanos, int rowsReturned) {}

  private enum BenchmarkTable {
    CUSTOMER_INSIGHTS(List.of(TableSpec.CUSTOMER_INSIGHTS)),
    CUSTOMER_INSIGHTS_PHONE(List.of(TableSpec.CUSTOMER_INSIGHTS_PHONE)),
    ALL(List.of(TableSpec.CUSTOMER_INSIGHTS, TableSpec.CUSTOMER_INSIGHTS_PHONE));

    private final List<TableSpec> tableSpecs;

    BenchmarkTable(List<TableSpec> tableSpecs) {
      this.tableSpecs = tableSpecs;
    }

    static BenchmarkTable fromName(String name) {
      return switch (name) {
        case "customer_insights" -> CUSTOMER_INSIGHTS;
        case "customer_insights_phone" -> CUSTOMER_INSIGHTS_PHONE;
        case "all" -> ALL;
        default ->
            throw new IllegalArgumentException(
                "Unsupported table '" + name + "'. Supported values: customer_insights, customer_insights_phone, all");
      };
    }

    List<TableSpec> tableSpecs() {
      return tableSpecs;
    }

    String displayName() {
      return switch (this) {
        case CUSTOMER_INSIGHTS -> "customer_insights";
        case CUSTOMER_INSIGHTS_PHONE -> "customer_insights_phone";
        case ALL -> "all";
      };
    }
  }

  private enum TableSpec {
    CUSTOMER_INSIGHTS("customer_insights"),
    CUSTOMER_INSIGHTS_PHONE("customer_insights_phone");

    private final String tableName;

    TableSpec(String tableName) {
      this.tableName = tableName;
    }

    String tableName() {
      return tableName;
    }

    TableBenchmarkPlan loadPlan(DatabaseClient client, int sampleSize) {
      return switch (this) {
        case CUSTOMER_INSIGHTS ->
            new TableBenchmarkPlan(this, loadCustomerInsightsSamples(client, tableName, sampleSize), null);
        case CUSTOMER_INSIGHTS_PHONE ->
            new TableBenchmarkPlan(this, null, loadCustomerInsightsPhoneSamples(client, tableName, sampleSize));
      };
    }
  }

  private record BenchmarkResult(
      String name,
      String consistencyMode,
      int count,
      double throughputOpsPerSecond,
      int minRows,
      double avgRows,
      double p50Rows,
      double p95Rows,
      double p99Rows,
      int maxRows,
      double minMs,
      double avgMs,
      double p50Ms,
      double p95Ms,
      double p99Ms,
      double maxMs) {

    static BenchmarkResult from(
        String name, String consistencyMode, List<Measurement> measurements, long totalElapsedNanos) {
      List<Long> sortedDurations = new ArrayList<>(measurements.size());
      List<Integer> sortedRows = new ArrayList<>(measurements.size());
      double totalMs = 0;
      long totalRows = 0;

      for (Measurement measurement : measurements) {
        sortedDurations.add(measurement.durationNanos());
        sortedRows.add(measurement.rowsReturned());
        totalMs += nanosToMillis(measurement.durationNanos());
        totalRows += measurement.rowsReturned();
      }

      Collections.sort(sortedDurations);
      Collections.sort(sortedRows);

      double count = measurements.size();
      return new BenchmarkResult(
          name,
          consistencyMode,
          measurements.size(),
          measurements.size() / (totalElapsedNanos / 1_000_000_000.0),
          sortedRows.get(0),
          totalRows / count,
          percentileInt(sortedRows, 0.50),
          percentileInt(sortedRows, 0.95),
          percentileInt(sortedRows, 0.99),
          sortedRows.get(sortedRows.size() - 1),
          nanosToMillis(sortedDurations.get(0)),
          totalMs / count,
          percentileMillis(sortedDurations, 0.50),
          percentileMillis(sortedDurations, 0.95),
          percentileMillis(sortedDurations, 0.99),
          nanosToMillis(sortedDurations.get(sortedDurations.size() - 1)));
    }

    private static double percentileMillis(List<Long> sortedNanos, double percentile) {
      int index = percentileIndex(sortedNanos.size(), percentile);
      return nanosToMillis(sortedNanos.get(index));
    }

    private static double percentileInt(List<Integer> sortedValues, double percentile) {
      int index = percentileIndex(sortedValues.size(), percentile);
      return sortedValues.get(index);
    }

    private static int percentileIndex(int size, double percentile) {
      int index = (int) Math.ceil(percentile * size) - 1;
      return Math.max(0, Math.min(index, size - 1));
    }

    private static double nanosToMillis(long nanos) {
      return nanos / 1_000_000.0;
    }
  }
}
