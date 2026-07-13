# Spanner Tools

[Back to Spanner Samples](../README.md)

Utility tools and applications for managing and operating Google Cloud Spanner.

## Samples

| Sample | Description | Technologies |
|--------|-------------|--------------|
| [spanner-split-mgr (Java)](spanner-split-mgr/java/README.md) | Java and Spring Boot Web UI for managing Spanner split points. | Java, Spring Boot, Thymeleaf, SQLite |
| [spanner-split-mgr (Python)](spanner-split-mgr/python/README.md) | Python Web UI for managing Spanner split points. | Python, FastAPI, Alpine.js, SQLite |
| [spanner_bulk_load](spanner_bulk_load/README.md) | Bulk-loading implementations for Spanner. | Dataflow |
| [spanner_cdc_loadtest](spanner_cdc_loadtest/README.md) | Java-based load generator for Cloud Spanner to test Change Data Capture (CDC) and general performance with various load strategies. Support for Cloud Run Jobs. | Java 17, Maven, Docker, Cloud Run Jobs |
| [spanner_direct_access_test](spanner_direct_access_test/README.md) | Benchmark scaffold for evaluating Spanner read and query performance. | Java 17, Maven, Spanner direct access |
| [spanner_noise_maker](spanner_noise_maker/README.md) | Load generator for Cloud Spanner. Supports data seeding, standard random traffic, sequential key hotspot generation and transaction lock contention simulation. Runs on Cloud Run Jobs. | Python 3.11, Docker, Cloud Run Jobs |
| [spanner_quicksink](spanner_quicksink/README.md) | Multi-Threaded application to read Spanner Change Streams and emit them to various sinks (File, Spanner, BigQuery) with optional transactional consistency buffer. | Java 17, Maven, Spanner Change Streams, BigQuery |
