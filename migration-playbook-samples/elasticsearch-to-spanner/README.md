# Samples

Samples are small, runnable examples for the scenarios in the playbook's
`Target Schema and Query Design` section. Each topic should use the same file
layout so the Elastic source behavior and the Spanner target design can be
reviewed side by side.

```text
samples/<topic>/
  README.md
  elasticsearch/
    index.json
    documents.ndjson
    queries/
      <query>.json
  spanner/
    schema.sql
    queries/
      <query>.sql
```

## Local Elasticsearch

Start the shared Docker-backed Elasticsearch source:

```bash
samples/scripts/start_elasticsearch.sh
```

Load a sample into the source:

```bash
samples/scripts/load_elasticsearch_sample.sh stemming
```

Run one of the sample queries:

```bash
samples/scripts/run_elasticsearch_query.sh stemming search-running
```

The default index name is `sample_<topic>`, for example `sample_stemming`.
Pass an index name as the final argument to the load or query scripts to
override that default. You can also set `SAMPLE_ELASTICSEARCH_INDEX` for a
temporary override without changing the scenario-level `ELASTICSEARCH_INDEX`
used elsewhere in the repo.

## Spanner

The Spanner scripts use the repo root `.env` values for
`SPANNER_PROJECT_ID`, `SPANNER_INSTANCE_ID`, and `SPANNER_DATABASE_ID`.

Apply a sample schema:

```bash
samples/scripts/apply_spanner_sample_schema.sh stemming
```

Seed sample rows:

```bash
samples/scripts/seed_spanner_sample.sh stemming
```

Run a sample query:

```bash
samples/scripts/run_spanner_query.sh stemming search-running
```

The query runner replaces `@tenant_id` with `SAMPLE_TENANT_ID`, defaulting to `tenant-a`. It also applies `SAMPLE_SPANNER_OPTIMIZER_VERSION`, defaulting to `6`, as a single runner-level statement hint so individual SQL files do not need optimizer hints.

## Current Samples

| Sample | Focus |
| :---- | :---- |
| `stemming` | English analyzer behavior compared with Spanner enhanced query mode |
| `fuzzy-matching` | Elasticsearch edit-distance fuzziness compared with Spanner n-gram search |
| `faceting-aggregations` | Search result facets, array facets, range buckets, and metrics |
| `custom-dictionaries` | Elasticsearch custom vocabulary compared with Spanner enhanced query mode |
| `data-movement` | Spark bulk extraction from Elasticsearch and load into Spanner on Managed Service for Apache Spark |
