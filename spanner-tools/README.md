# Spanner Tools

Utility tools and applications for managing and operating Google Cloud Spanner.

## Samples

| Sample | Description | Technologies |
|--------|-------------|--------------|
| [spanner-split-mgr](spanner-split-mgr/) | Web UI for managing Spanner split points. Provides local staging, batch sync, and visual status indicators for split point operations. Useful for performance tuning and load balancing. | FastAPI, Alpine.js, TailwindCSS, SQLite |
| [spanner_quicksink](spanner_quicksink/) | Multi-Threaded application to read Spanner ChangeStreams and emit them to various sinks (File, Spanner, BigQuery) with optional transactional consistency buffer. | Java 17, Maven, Spanner Change Streams, BigQuery |
| [spanner_cdc_loadtest](spanner_cdc_loadtest/) | Java-based load generator for Cloud Spanner to test Change Data Capture (CDC) and general performance with various load strategies. Support for Cloud Run Jobs. | Java 17, Maven, Docker, Cloud Run Jobs |
