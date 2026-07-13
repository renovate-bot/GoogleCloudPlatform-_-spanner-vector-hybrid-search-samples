# Data Movement with Spark

This sample demonstrates the direct Elasticsearch-to-Spanner data movement path for the less common case where Elasticsearch itself is the practical source dataset. The execution target is Managed Service for Apache Spark on Google Cloud. The job reads a flat Elasticsearch index with the Elasticsearch Hadoop Spark connector, transforms source documents into a Spanner table shape, and writes rows with the Spark Spanner connector.

The sample is intentionally small. It proves the mechanics of bulk extraction, schema translation, Spanner writes, and post-load FTS validation before scaling the pattern to customer indices.

## Why Spark

Spark is a good fit for one-time or repeatable bulk movement because it gives the migration team a distributed execution engine between Elasticsearch and Spanner. Instead of writing a bespoke scroll loop and a bespoke Spanner writer, the sample uses existing Spark connectors for both sides and keeps the migration logic in a small PySpark transformation.

The practical advantages are:

- Parallel reads from Elasticsearch through the Elasticsearch Hadoop connector.
- A single transformation layer for renaming fields, casting types, dropping unused fields, and handling simple validation.
- Distributed writes into Spanner through the Spark Spanner connector.
- A serverless execution option with Managed Service for Apache Spark, avoiding long-lived cluster management for migration jobs.
- A reusable shape for dry runs, test loads, production bulk loads, and post-load validation.

This pattern is still a bulk migration pattern, not continuous replication. Use it when the source dataset can be snapshotted or when the application can tolerate a bulk load plus validation and cutover workflow.

## Connectors

This sample uses two Spark data source connectors.

| Side | Connector | Spark usage | Purpose |
| :---- | :---- | :---- | :---- |
| Source | Elasticsearch Hadoop Spark connector | `spark.read.format("es")` | Reads Elasticsearch documents into a Spark DataFrame. |
| Target | Spark Spanner connector | `df.write.format("cloud-spanner")` | Writes the transformed DataFrame into a Spanner table. |

The submit script supplies both connector artifacts with `spark.jars.packages`:

```bash
--properties=^#^spark.jars.packages=com.google.cloud.spark.spanner:spark-3.5-spanner:1.4.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.15.3
```

The `^#^` prefix is gcloud's alternate delimiter syntax. It prevents the comma between Maven coordinates from being parsed as two separate `--properties` entries.

Runtime `2.3` is the default because it runs Spark 3.5 with Scala 2.13, matching the connector coordinates used here. If you change the Managed Spark runtime, update both connector coordinates so the Spark and Scala versions still match.

## Extraction Partitioning and Read Saturation

The extraction plan should be sized around Elasticsearch read throughput, not just Spark executor count. Spark can only saturate the source when the Elasticsearch connector creates enough independent read tasks and each task receives a reasonably balanced amount of work.

Use two levels of read parallelism:

1. Connector-native shard and sliced-scroll partitions. The Elasticsearch Hadoop connector normally derives input partitions from the source shards. On Elasticsearch versions that support scroll slicing, `es.input.max.docs.per.partition` tells the connector to split large shards into smaller input slices. This is the first tuning knob because it does not require application-specific partition ranges. `es.scroll.size` controls how many documents each task asks for per scroll request.
2. Field-partitioned queries. When shard and slice partitioning still leaves Spark underutilized, or when the migration needs business-aligned chunks for validation and retries, run multiple disjoint Elasticsearch queries over a partition field and union the DataFrames in Spark. The sample job supports exact-value partitions and range partitions for this pattern.

Good Elasticsearch partition fields have these properties:

- They are mapped as `keyword`, numeric, or `date` fields so Elasticsearch can evaluate exact or range filters efficiently. Avoid analyzed `text` fields.
- They are present on nearly all documents that must be migrated. Null or missing-heavy fields create a leftover partition that is easy to forget.
- They are stable for the migration snapshot. Avoid fields that can change while a bulk extract is running unless the source index is frozen or the query has an explicit point-in-time boundary.
- They have enough cardinality or range spread to create more partitions than the current shard count, without producing many tiny partitions.
- They are not badly skewed. A field with one very large value and many tiny values will leave one slow task. Split large values again by date, numeric range, or another field.
- They are exhaustive and disjoint. Every source document should match exactly one partition query unless the run intentionally filters out part of the source.

To identify candidates, inspect mappings and counts before choosing ranges:

```bash
curl -fsS "${ELASTICSEARCH_URL}/${ELASTICSEARCH_INDEX}/_mapping?pretty"

curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  "${ELASTICSEARCH_URL}/${ELASTICSEARCH_INDEX}/_field_caps?fields=tenant_id,updated_at,status"

curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  "${ELASTICSEARCH_URL}/${ELASTICSEARCH_INDEX}/_search?size=0&pretty" \
  -d '{"aggs":{"status_counts":{"terms":{"field":"status","size":20}}}}'

curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  "${ELASTICSEARCH_URL}/${ELASTICSEARCH_INDEX}/_search?size=0&pretty" \
  -d '{"aggs":{"updated_at_days":{"date_histogram":{"field":"updated_at","calendar_interval":"day"}}}}'
```

For this small fixture, `status` is useful only to demonstrate exact-value partitioning:

```bash
SPARK_ES_INPUT_MAX_DOCS_PER_PARTITION=50000
SPARK_ES_SCROLL_SIZE=2000
SPARK_ES_PARTITION_FIELD=status
SPARK_ES_PARTITION_VALUES=active,inactive
```

In a real production product index, `status` alone would often be too low-cardinality and skew-prone. A timestamp or numeric range is usually a better first candidate for large extracts:

```bash
SPARK_ES_PARTITION_FIELD=updated_at
SPARK_ES_PARTITION_RANGES=2026-05-01T00:00:00Z..2026-05-04T00:00:00Z,2026-05-04T00:00:00Z..2026-05-07T00:00:00Z
```

The job combines each partition filter with `SPARK_ES_QUERY` when that base query is set. Use this to keep a migration-scope filter, such as tenant or active index-generation filters, while still splitting the read into smaller source-side partitions.

Before production cutover, compare the sum of source `_count` results for every partition with the count for the unpartitioned migration query. During the run, watch Elasticsearch search thread pools, rejected requests, scroll context pressure, heap, CPU, network, Spark task skew, and Spanner write backlog. Increase read parallelism only while Elasticsearch remains the bottleneck and the cluster has headroom.

## Source and Target Shape

The Elasticsearch fixture contains flat product documents:

```json
{
  "tenant_id": "tenant-a",
  "product_id": "trail-1",
  "title": "Waterproof trail running shoe",
  "description": "Lightweight waterproof trail shoe for wet mountain runs.",
  "category": "footwear",
  "status": "active",
  "price_cents": 12999,
  "updated_at": "2026-05-01T12:00:00Z"
}
```

The target Spanner table stores typed columns and generated FTS token columns:

```sql
CREATE TABLE SparkMigratedProducts (
  TenantId STRING(36) NOT NULL,
  ProductId STRING(64) NOT NULL,
  Title STRING(MAX) NOT NULL,
  Description STRING(MAX),
  Category STRING(64),
  Status STRING(32),
  PriceCents INT64,
  UpdatedAt TIMESTAMP,
  TitleTokens TOKENLIST AS (
    TOKENIZE_FULLTEXT(Title, token_category=>"title")
  ) HIDDEN,
  DescriptionTokens TOKENLIST AS (
    TOKENIZE_FULLTEXT(Description)
  ) HIDDEN,
  SearchTokens TOKENLIST AS (
    TOKENLIST_CONCAT([TitleTokens, DescriptionTokens])
  ) HIDDEN
) PRIMARY KEY (TenantId, ProductId);
```

The Spark job writes only the base columns. Spanner computes the hidden generated `TOKENLIST` columns and maintains the search index.

## Schema Translation

The PySpark job is deliberately explicit about field mapping:

```python
migrated = source.select(
    F.col("tenant_id").cast("string").alias("TenantId"),
    F.col("product_id").cast("string").alias("ProductId"),
    F.col("title").cast("string").alias("Title"),
    F.col("description").cast("string").alias("Description"),
    F.col("category").cast("string").alias("Category"),
    F.col("status").cast("string").alias("Status"),
    F.col("price_cents").cast("long").alias("PriceCents"),
    F.to_timestamp("updated_at").alias("UpdatedAt"),
)
```

For real migrations, do not mechanically copy every Elasticsearch field into Spanner. We recommend that you start from the target query contract and classify source fields as search text, exact filter, sort, facet, payload, metadata, or drop. Then translate only the fields needed by the target Spanner schema.

Common schema translation decisions include:

- Cast Elasticsearch numeric and date values into the Spanner column types expected by the target schema.
- Preserve stable primary-key fields. This sample uses `(TenantId, ProductId)`.
- Keep full-text source fields as normal `STRING` columns, then generate `TOKENLIST` columns in Spanner rather than loading token columns from Spark.
- Flatten or redesign nested objects and arrays before the Spanner write.

The sample job checks that required columns are present and drops duplicate rows by primary key before writing:

```python
required_columns = ["TenantId", "ProductId", "Title"]
write_df = migrated.dropDuplicates(["TenantId", "ProductId"])
```

For production jobs, it is recommended that you add stronger data quality checks for nullability, key uniqueness, enum values, timestamp parsing, and rejected-row reporting.

## Spanner Writer Configuration

The Spanner writer in `spark/es_to_spanner.py` uses:

```python
write_df.write.format("cloud-spanner")     .option("projectId", args.spanner_project_id)     .option("instanceId", args.spanner_instance_id)     .option("databaseId", args.spanner_database_id)     .option("table", args.spanner_table)     .option("mutationType", args.mutation_type)     .option("mutationsPerTransaction", args.mutations_per_transaction)     .option("numWriteThreads", args.num_write_threads)     .option("maxPendingTransactions", args.max_pending_transactions)     .option("assumeIdempotentRows", args.assume_idempotent_rows)     .option("enablePartialRowUpdates", args.enable_partial_row_updates)     .mode(args.write_mode)     .save()
```

The important options are:

| Option | Sample default | Why it matters |
| :---- | :---- | :---- |
| `projectId`, `instanceId`, `databaseId` | From `.env` | Identifies the target Spanner database. |
| `table` | `SparkMigratedProducts` | Target table for the DataFrame write. |
| `mutationType` | `insert_or_update` | Makes reruns tolerant of existing primary keys. Use `insert` when duplicates should fail fast. |
| `mutationsPerTransaction` | `1000` | Controls Spanner transaction batching. Tune with row width and mutation limits in mind. |
| `numWriteThreads` | `8` | Controls parallel write workers inside the connector. Increase carefully with Spanner capacity. |
| `maxPendingTransactions` | `20` | Limits queued write pressure. Higher values can improve throughput but increase memory and retry pressure. |
| `assumeIdempotentRows` | `false` | Can enable a higher-throughput at-least-once write path when duplicate writes are safe. Validate before enabling. |
| `enablePartialRowUpdates` | `true` | Required for this FTS schema because Spark writes only base columns while Spanner computes hidden generated token columns. |
| Spark save mode | `append` | Controls whether Spark appends, overwrites, ignores, or errors for the target write. |

The first validation run exposed why `enablePartialRowUpdates=true` matters. Without it, Spark tried to write every column in the Spanner table schema, including hidden generated `TOKENLIST` columns such as `TitleTokens`, and failed before any rows were loaded.

## Managed Spark Setup

Managed Service for Apache Spark runs the PySpark job as a Dataproc Serverless batch. The sample wrapper is:

```bash
samples/data-movement/scripts/submit_managed_spark_batch.sh
```

Configure `.env` with at least:

```bash
GOOGLE_CLOUD_PROJECT=...
GOOGLE_CLOUD_REGION=us-central1
GCS_BUCKET=your-spark-deps-bucket

ELASTICSEARCH_URL=https://your-es-endpoint:9243
ELASTICSEARCH_USERNAME=...
ELASTICSEARCH_PASSWORD=...
ELASTICSEARCH_INDEX=sample_data_movement

SPANNER_INSTANCE_ID=...
SPANNER_DATABASE_ID=...
```

Useful optional settings:

```bash
MANAGED_SPARK_RUNTIME_VERSION=2.3
MANAGED_SPARK_SERVICE_ACCOUNT=spark-runner@PROJECT_ID.iam.gserviceaccount.com
MANAGED_SPARK_SUBNET=projects/PROJECT_ID/regions/us-central1/subnetworks/SUBNET
SPARK_ES_RESOURCE=sample_data_movement
SPARK_ES_INPUT_MAX_DOCS_PER_PARTITION=50000
SPARK_ES_SCROLL_SIZE=2000
SPARK_ES_PARTITION_FIELD=status
SPARK_ES_PARTITION_VALUES=active,inactive
SPARK_SPANNER_CONNECTOR_PACKAGE=com.google.cloud.spark.spanner:spark-3.5-spanner:1.4.0
SPARK_ELASTICSEARCH_CONNECTOR_PACKAGE=org.elasticsearch:elasticsearch-spark-30_2.13:8.15.3
SPARK_SPANNER_MUTATION_TYPE=insert_or_update
SPARK_SPANNER_ENABLE_PARTIAL_ROW_UPDATES=true
```

The wrapper expands the `.env` settings into a `gcloud dataproc batches submit pyspark` command. You normally run the wrapper rather than typing this command by hand, but this shape is useful for debugging connector package versions and Spark arguments:

```bash
gcloud dataproc batches submit pyspark \
  samples/data-movement/spark/es_to_spanner.py \
  --version=2.3 \
  --deps-bucket="${GCS_BUCKET}" \
  --properties=^#^spark.jars.packages=com.google.cloud.spark.spanner:spark-3.5-spanner:1.4.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.15.3 \
  -- \
  --es-nodes=... \
  --es-port=... \
  --es-resource=sample_data_movement \
  --es-input-max-docs-per-partition=50000 \
  --es-scroll-size=2000 \
  --es-partition-field=status \
  --es-partition-values=active,inactive \
  --spanner-project-id=... \
  --spanner-instance-id=... \
  --spanner-database-id=... \
  --spanner-table=SparkMigratedProducts
```

Everything before `--` configures Dataproc Serverless and Spark. Everything after `--` is passed to `spark/es_to_spanner.py`.

## Operational Requirements

There are two identities to think about:

- The submitter identity, which runs `gcloud dataproc batches submit pyspark`.
- The runtime service account, which runs the Spark workload.

The submitter needs permission to create Dataproc Serverless batches, such as a role containing `dataproc.batches.create`. In the validation run for this sample, `roles/dataproc.admin` on the submitting service account satisfied the batch creation requirement.

The runtime service account needs:

- Dataproc Worker permissions. Managed Spark failed startup without `roles/dataproc.worker` on the runtime account.
- Spanner write permissions on the target database, such as Spanner database user permissions appropriate for the target environment.
- Cloud Storage object permissions on the dependency or staging bucket used by the batch.

Managed Spark must also be able to reach the Elasticsearch HTTP endpoint. A local Docker Elasticsearch endpoint on `localhost` is useful for fixture development, but it is not reachable from serverless Spark. For a real run, use an Elasticsearch endpoint reachable from the Spark network, such as Elastic Cloud, a self-managed cluster behind an internal load balancer, or another reachable HTTPS endpoint. In the local validation run, the batch used the VM's internal address for the Docker-backed Elasticsearch service.

## Run and Validate the Sample

Run the sample in this order.

1. Apply the target Spanner schema:

   ```bash
   samples/scripts/apply_spanner_sample_schema.sh data-movement
   ```

2. Load the Elasticsearch fixture into the configured source index:

   ```bash
   samples/scripts/load_elasticsearch_sample.sh data-movement
   ```

3. If you need to rerun the Spark load against the same tenant rows, clear the sample rows first:

   ```bash
   samples/data-movement/scripts/cleanup_spanner_rows.sh
   ```

4. Submit the Managed Spark batch:

   ```bash
   samples/data-movement/scripts/submit_managed_spark_batch.sh
   ```

   The driver log should report the number of rows written. When field partitioning is enabled, it should also report the number of partitioned queries and the resulting source DataFrame partition count.

5. Check loaded row counts by status:

   ```bash
   samples/scripts/run_spanner_query.sh data-movement count-by-status
   ```

   Expected result for the checked-in fixture:

   ```text
   Status    ProductCount
   active    4
   inactive  1
   ```

6. Run a search query against the generated FTS token columns:

   ```bash
   samples/scripts/run_spanner_query.sh data-movement search-waterproof-trail
   ```

   Expected product IDs for the checked-in fixture:

   ```text
   hike-1
   jacket-1
   trail-1
   ```

This validates the important end-to-end behavior: Spark loaded base rows into Spanner, Spanner computed the generated token columns, and the search index can answer an FTS query over the loaded data.

The partitioned-read path was also validated with the checked-in fixture on Managed Spark. The validation run loaded the five-document Elasticsearch fixture, confirmed `status` partitions of `active:4` and `inactive:1`, submitted Spark with `SPARK_ES_INPUT_MAX_DOCS_PER_PARTITION=2`, `SPARK_ES_SCROLL_SIZE=2`, `SPARK_ES_PARTITION_FIELD=status`, and `SPARK_ES_PARTITION_VALUES=active,inactive`, and the driver reported `Elasticsearch source DataFrame partitions: 3` before writing five rows to Spanner.

## Production Notes

Use this as a mechanical starting point, not as a final production pipeline. For production migration runs:

- Run against a source endpoint that can tolerate Spark scroll reads. Start with connector-native shard and sliced-scroll tuning, then add field-partitioned queries when Spark still cannot keep enough source reads in flight.
- Prove partition coverage before loading: the sum of partition `_count` results should match the unpartitioned migration query count, and partition filters should be mutually exclusive.
- Keep only fields required by the target schema. Add explicit transformations for nested objects, arrays, enums, and type coercions before writing to Spanner.
- Decide whether retries should be idempotent. If duplicate writes are acceptable, `insert_or_update` and `assumeIdempotentRows` can be useful; if duplicates should fail, use stricter write semantics.
- Size Spanner write concurrency with `mutationsPerTransaction`, `numWriteThreads`, and `maxPendingTransactions`, then validate write throughput and abort behavior with representative data.
- Treat command-line Elasticsearch passwords as a sample convenience only. Prefer a managed secret delivery pattern for real migrations.
- Reconcile source counts, Spark output counts, and Spanner row counts before cutover.
- Run representative FTS queries after loading to validate that generated token columns and search indexes behave as expected.
