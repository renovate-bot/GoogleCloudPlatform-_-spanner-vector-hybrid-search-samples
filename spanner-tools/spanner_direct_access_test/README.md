# Spanner Benchmark POC

This repository contains a small Java 17 / Maven benchmark scaffold for evaluating Google Cloud Spanner read and query performance against the `customer_insights` and `customer_insights_phone` tables in [schema.sql](/home/karthit_google_com/code/kt-spanner-stuff/verizon_soi_poc/schema.sql).

Configuration is loaded from `.env`, authentication uses ADC from the VM, and `GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS` is expected to be set explicitly.

NOTE: It's important to set 'GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS' to true to realize the latency benefits of "DIRECT PATH".

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
