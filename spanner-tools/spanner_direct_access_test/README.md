# Spanner Benchmark POC

This repository contains a small Java 17 / Maven benchmark scaffold for evaluating Google Cloud Spanner read and query performance against the `customer_insights` and `customer_insights_phone` tables in [schema.sql](schema.sql).

Configuration is loaded from `.env`, authentication uses ADC from the VM, and `GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS` is expected to be set explicitly.

**NOTE**: It's important to set 'GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS' to true to realize the latency benefits of "DIRECT PATH".

**IMPORTANT**: Use the .env.example to create a .env file at the same folder level. This is required and please fill in the values so that it matches your environment.

## Spanner Background

This repository assumes you already have a Google Cloud project, a Spanner instance, and a database available for the benchmark. If you need to set those up or want the canonical product references, start with these official docs:

- Spanner product documentation: https://cloud.google.com/spanner/docs
- Set up Application Default Credentials (ADC): https://cloud.google.com/docs/authentication/provide-credentials-adc
- Create and manage Spanner instances: https://cloud.google.com/spanner/docs/create-manage-instances
- Create and query a database: https://cloud.google.com/spanner/docs/create-query-database-console
- Make schema updates with DDL: https://cloud.google.com/spanner/docs/schema-updates
- Spanner client libraries overview: https://cloud.google.com/spanner/docs/reference/libraries
- Java client library reference: https://cloud.google.com/java/docs/reference/google-cloud-spanner/latest/overview

For this benchmark specifically:

- the instance and database identifiers are read from `.env`
- authentication is expected to come from ADC rather than an embedded service-account key
- the intended schema for this repo lives in [`schema.sql`](schema.sql)
- schema changes can be applied with `gcloud spanner databases ddl update`, which is also the approach described in the Spanner schema-update documentation

If you are starting from scratch, the usual flow is:

1. Create or choose a Google Cloud project.
2. Create a Spanner instance in the target region or instance configuration.
3. Create a database in that instance.
4. Apply the schema from [`schema.sql`](schema.sql).
5. Create a local `.env` from [`.env.example`](.env.example).
6. Verify ADC is available in the shell where you will run the benchmark.
7. Compile, populate, and run benchmark scenarios from this repository.

## Schema Overview

The benchmark currently targets two tables with the same payload columns but different primary-key shapes:

- `customer_insights`: keyed by `cust_id, acct_no, phone_number, insight_category, insight_name`
- `customer_insights_phone`: keyed by `phone_number, insight_category, insight_name`

Both tables currently contain these columns:

- `cust_id STRING(36) NOT NULL`
- `acct_no STRING(36) NOT NULL`
- `phone_number STRING(20) NOT NULL`
- `insight_category STRING(50) NOT NULL`
- `insight_name STRING(100) NOT NULL`
- `insight_values STRING(MAX)`
- `updated_by STRING(100)`
- `updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)`

The benchmark is set up this way so the same logical data can be exercised through two different key orders:

- `customer_insights` supports customer/account-oriented access paths
- `customer_insights_phone` supports phone-oriented access paths

That makes it easier to compare how key order changes point-read behavior, prefix scans, and SQL filtering selectivity while keeping the row contents the same.

Also see [schema.sql](schema.sql).

## Java Source Layout

The Java code in this repository is intentionally small and organized around a few focused classes:

- [`src/main/java/com/spanner/benchmark/Main.java`](src/main/java/com/spanner/benchmark/Main.java): CLI entry point that loads configuration, creates the Spanner client, and dispatches to `populate` or `benchmark`
- [`src/main/java/com/spanner/benchmark/benchmark/BenchmarkCommand.java`](src/main/java/com/spanner/benchmark/benchmark/BenchmarkCommand.java): benchmark runner that loads sample keys, executes the configured scenarios, and reports latency, row-count, throughput, and concurrency-sweep results
- [`src/main/java/com/spanner/benchmark/populate/PopulateCommand.java`](src/main/java/com/spanner/benchmark/populate/PopulateCommand.java): synthetic data loader that generates benchmark rows and writes them into both `customer_insights` and `customer_insights_phone`
- [`src/main/java/com/spanner/benchmark/spanner/SpannerClientFactory.java`](src/main/java/com/spanner/benchmark/spanner/SpannerClientFactory.java): central place for building the Spanner client with project, endpoint, emulator, and direct-access settings
- [`src/main/java/com/spanner/benchmark/config/AppConfig.java`](src/main/java/com/spanner/benchmark/config/AppConfig.java): typed application configuration loaded from `.env` and environment variables
- [`src/main/java/com/spanner/benchmark/config/EnvFileLoader.java`](src/main/java/com/spanner/benchmark/config/EnvFileLoader.java): minimal `.env` parser used by `AppConfig`
- [`src/main/java/com/spanner/benchmark/util/CliArguments.java`](src/main/java/com/spanner/benchmark/util/CliArguments.java): small command-line option parser for `--flag value` style arguments
- [`src/main/java/com/spanner/benchmark/model/CustomerInsightKey.java`](src/main/java/com/spanner/benchmark/model/CustomerInsightKey.java): key model for point reads and exact-key SQL against `customer_insights`
- [`src/main/java/com/spanner/benchmark/model/CustomerInsightPhoneKey.java`](src/main/java/com/spanner/benchmark/model/CustomerInsightPhoneKey.java): key model for point reads and exact-key SQL against `customer_insights_phone`

At a high level, execution flows like this:

1. [`Main.java`](src/main/java/com/spanner/benchmark/Main.java) loads config and opens a Spanner client.
2. [`PopulateCommand.java`](src/main/java/com/spanner/benchmark/populate/PopulateCommand.java) or [`BenchmarkCommand.java`](src/main/java/com/spanner/benchmark/benchmark/BenchmarkCommand.java) handles the requested command.
3. [`SpannerClientFactory.java`](src/main/java/com/spanner/benchmark/spanner/SpannerClientFactory.java) controls how the Java client connects to Spanner.
4. [`AppConfig.java`](src/main/java/com/spanner/benchmark/config/AppConfig.java) and [`EnvFileLoader.java`](src/main/java/com/spanner/benchmark/config/EnvFileLoader.java) provide runtime settings from `.env`.


## Commands

Compile:

```bash
mvn compile
```

Build an executable jar:

```bash
mvn package
```

Recommended execution path:

```bash
java -jar target/spanner-benchmark-poc.jar benchmark
```

Populate synthetic seed data:

```bash
mvn exec:java -Dexec.args="populate --profile small --truncate-first"
```

Run the basic benchmark:

```bash
mvn exec:java -Dexec.args="benchmark --scenario core-read-paths --warmup 10 --iterations 100 --concurrency 1 --sample-size 200"
```

Run the benchmark with an explicit stale-read window:

```bash
mvn exec:java -Dexec.args="benchmark --scenario core-read-paths --warmup 10 --iterations 100 --concurrency 1 --sample-size 200 --staleness-seconds 15"
```

Preferred runtime path for clean CLI execution:

```bash
java -jar target/spanner-benchmark-poc.jar populate --profile small --truncate-first
java -jar target/spanner-benchmark-poc.jar benchmark --scenario core-read-paths --warmup 10 --iterations 100 --concurrency 1 --sample-size 200 --staleness-seconds 15
```

## Maven Exec Note

`mvn exec:java` may emit a shutdown warning like `NoClassDefFoundError: io/opentelemetry/sdk/common/CompletableResultCode` after the benchmark completes.

That warning is from the Maven exec launcher and its in-process classloader cleanup, not from the benchmark logic or Spanner query execution itself.

For real benchmark runs, prefer the packaged jar:

```bash
mvn package
java -jar target/spanner-benchmark-poc.jar benchmark
```

## Benchmark Workflow

Seed a simple dataset into both tables:

```bash
java -jar target/spanner-benchmark-poc.jar populate --profile small --truncate-first
```

Seed a dataset with many `cust_id + acct_no` combinations and about 250 rows per account pair in both tables:

```bash
java -jar target/spanner-benchmark-poc.jar populate --profile read-heavy-250 --truncate-first
```

Seed a dataset with many `cust_id + acct_no` combinations and about 1000 rows per account pair in both tables:

```bash
java -jar target/spanner-benchmark-poc.jar populate --profile read-heavy-1000 --truncate-first
```

You can also override any profile dimension directly. Example: 100 account pairs with 250 rows per pair:

```bash
java -jar target/spanner-benchmark-poc.jar populate --profile read-heavy-250 --truncate-first --customers 20 --accounts-per-customer 5
```

Run the default core read paths benchmark across both tables:

```bash
java -jar target/spanner-benchmark-poc.jar benchmark --warmup 10 --iterations 100 --concurrency 1 --sample-size 200 --staleness-seconds 15
```

Limit benchmark execution to one table when needed:

```bash
java -jar target/spanner-benchmark-poc.jar benchmark --table customer_insights --scenario core-read-paths --warmup 10 --iterations 100 --concurrency 1 --sample-size 200 --staleness-seconds 15
java -jar target/spanner-benchmark-poc.jar benchmark --table customer_insights_phone --scenario core-read-paths --warmup 10 --iterations 100 --concurrency 1 --sample-size 200 --staleness-seconds 15
```

Run only the `cust_id + acct_no` query benchmark:

```bash
java -jar target/spanner-benchmark-poc.jar benchmark --scenario customer-account --warmup 10 --iterations 100 --concurrency 1 --sample-size 200 --staleness-seconds 15
java -jar target/spanner-benchmark-poc.jar benchmark --scenario customer-account --warmup 10 --iterations 100 --concurrency 10 --sample-size 200 --staleness-seconds 15
java -jar target/spanner-benchmark-poc.jar benchmark --scenario customer-account --selection-mode random-without-replacement --selection-seed 42 --warmup 100 --iterations 2000 --concurrency 10 --sample-size 1000 --staleness-seconds 15
java -jar target/spanner-benchmark-poc.jar benchmark --scenario customer-account --selection-mode round-robin --warmup 100 --iterations 1000 --concurrency 10 --sample-size 200 --staleness-seconds 15
```

The benchmark output now includes per-query row-returned stats alongside latency and throughput.

Run a built-in concurrency sweep with a compact report:

```bash
java -jar target/spanner-benchmark-poc.jar benchmark --scenario customer-account --selection-mode random-without-replacement --selection-seed 42 --warmup 1000 --iterations 2000 --concurrency-sweep 1,2,4,8,16 --sample-size 1000 --staleness-seconds 15
```

When `--concurrency-sweep` is set, the benchmark runs each listed concurrency level in order and prints a summary table at the end for easier comparison.

Selection modes:

- `random`: random with replacement
- `random-without-replacement`: randomized coverage without repeats until the sampled set is exhausted, then reshuffle
- `round-robin`: deterministic cycling through the sampled keys
- `shuffle-once`: deterministic shuffle once, then cycle through that order

Run only the full primary-key SQL benchmark:

```bash
java -jar target/spanner-benchmark-poc.jar benchmark --scenario exact-primary-key-sql --warmup 10 --iterations 100 --concurrency 1 --sample-size 200 --staleness-seconds 15
```

## Benchmark Terms

`warmup` means the number of query executions to run before measurements are recorded.

Why this matters:

- early requests can include one-time effects like JVM activity, connection/session setup, and client-side initialization
- those first executions are often not representative of steady-state read latency
- warmup helps separate startup effects from the numbers you want to compare

In this benchmark:

- `--warmup 10` means run 10 unmeasured executions first
- `--iterations 100` means then record 100 measured executions
- `--concurrency 10` means up to 10 measured executions can be in flight at the same time
- `--concurrency-sweep 1,2,4,8` means run the same benchmark repeatedly at each listed concurrency and then print a comparison report

Practical guidance:

- use small warmup values for quick smoke tests
- use larger warmup values when comparing runs seriously
- keep warmup, iterations, concurrency, sample size, and selection mode the same when comparing results across data shapes or consistency modes

`sample-size` controls how many keys or key prefixes are loaded into the benchmark's in-memory sample set before execution starts.

Why this matters:

- the benchmark does not choose from the whole table on every iteration
- it first loads a bounded sample of candidate keys or `cust_id + acct_no` pairs
- query executions are then selected from that sample set

Practical guidance:

- larger sample sizes reduce the chance that a small subset of keys dominates the run
- smaller sample sizes are faster to initialize but can bias the workload toward fewer repeated keys
- keep sample size fixed when comparing two benchmark runs

`selection-mode` controls how benchmark iterations choose from the sampled keys.

Available modes:

- `random`: random with replacement; the same key can be picked multiple times
- `random-without-replacement`: randomized order with no repeats until the sample is exhausted, then reshuffle using the same seeded RNG
- `round-robin`: deterministic cycling through the sampled keys in order
- `shuffle-once`: deterministic shuffle once, then cycle through that shuffled order

Practical guidance:

- use `random` when you want a simple randomized workload
- use `random-without-replacement` when you want broad coverage with less immediate key reuse
- use `round-robin` when you want strict deterministic coverage
- use `shuffle-once` when you want deterministic but less order-biased coverage
- use `random-without-replacement` plus a fixed seed for concurrency sweeps so each run covers the sample broadly without immediate key reuse
- if comparing runs closely, keep both `selection-mode` and `selection-seed` fixed

## Current Benchmark Coverage

The initial default benchmark focus is:

- SQL queries filtered by `cust_id` and `acct_no` with strong consistency
- SQL queries filtered by `cust_id` and `acct_no` with stale consistency
- SQL queries filtered by full primary key with strong consistency
- SQL queries filtered by full primary key with stale consistency

Additional scenario names currently supported:

- `core-read-paths`
- `customer-account`
- `phone-number`
- `exact-primary-key-sql`
- `customer-only`
- `full-key-read`
- `all`

Table selection values:

- `all`: run scenarios for both `customer_insights` and `customer_insights_phone`
- `customer_insights`: run only the original customer/account keyed table scenarios
- `customer_insights_phone`: run only the phone-keyed table scenarios

This is only the first pass. The intent is to refine the scenario set once the target read patterns and reporting requirements are finalized.

## Data Shape Notes

For this schema, rows returned by the `cust_id + acct_no` query are driven by:

`rows per customer-account = phone-numbers-per-account * categories * names-per-category`

The populate command now prints:

- total expected rows across both tables
- total distinct `cust_id + acct_no` pairs
- rows per `cust_id + acct_no` pair

For deterministic benchmark datasets, use `--truncate-first` so the selected profile replaces existing table contents instead of accumulating with prior loads.

Useful presets:

- `small`: lightweight smoke-test dataset
- `read-heavy-250`: many customer/account combinations with about 250 rows per account pair
- `read-heavy-1000`: many customer/account combinations with about 1000 rows per account pair
