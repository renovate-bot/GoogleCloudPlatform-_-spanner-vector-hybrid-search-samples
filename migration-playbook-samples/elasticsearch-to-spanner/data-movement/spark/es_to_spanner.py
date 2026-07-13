#!/usr/bin/env python3
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Bulk-copy a flat Elasticsearch index into a Spanner FTS table with Spark."""

import argparse
import json
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract flat product documents from Elasticsearch and write them to Cloud Spanner."
    )
    parser.add_argument("--es-nodes", required=True, help="Elasticsearch host or comma-separated hosts.")
    parser.add_argument("--es-port", default="9200", help="Elasticsearch HTTP port.")
    parser.add_argument("--es-resource", required=True, help="Elasticsearch index or index/type resource.")
    parser.add_argument("--es-query", help="Optional Elasticsearch Query DSL JSON string.")
    parser.add_argument(
        "--es-input-max-docs-per-partition",
        help="Optional es.input.max.docs.per.partition value for connector-native sliced scroll partitions.",
    )
    parser.add_argument(
        "--es-scroll-size",
        help="Optional es.scroll.size value controlling documents returned per scroll request per task.",
    )
    parser.add_argument(
        "--es-partition-field",
        help="Optional keyword, numeric, or date field used to build disjoint partitioned ES queries.",
    )
    parser.add_argument(
        "--es-partition-values",
        help="Comma-separated exact values for --es-partition-field. Each value becomes one partitioned query.",
    )
    parser.add_argument(
        "--es-partition-ranges",
        help=(
            "Comma-separated ranges for --es-partition-field. Use lower..upper, lower.., or ..upper. "
            "Ranges use gte for the lower bound and lt for the upper bound."
        ),
    )
    parser.add_argument("--es-use-ssl", action="store_true", help="Use HTTPS when connecting to Elasticsearch.")
    parser.add_argument(
        "--es-wan-only",
        default="true",
        choices=["true", "false"],
        help="Use only configured Elasticsearch nodes. Keep true for managed or load-balanced endpoints.",
    )
    parser.add_argument("--es-username", help="Optional Elasticsearch basic auth username.")
    parser.add_argument("--es-password", help="Optional Elasticsearch basic auth password.")
    parser.add_argument("--spanner-project-id", required=True)
    parser.add_argument("--spanner-instance-id", required=True)
    parser.add_argument("--spanner-database-id", required=True)
    parser.add_argument("--spanner-table", default="SparkMigratedProducts")
    parser.add_argument("--write-mode", default="append", choices=["append", "overwrite", "ignore", "error"])
    parser.add_argument(
        "--mutation-type",
        default="insert_or_update",
        choices=["insert", "insert_or_update", "replace", "update"],
    )
    parser.add_argument("--mutations-per-transaction", default="1000")
    parser.add_argument("--num-write-threads", default="8")
    parser.add_argument("--max-pending-transactions", default="20")
    parser.add_argument(
        "--assume-idempotent-rows",
        default="false",
        choices=["true", "false"],
        help="Enable the Spanner connector's higher-throughput at-least-once write path.",
    )
    parser.add_argument("--enable-partial-row-updates", default="true", choices=["true", "false"])
    return parser.parse_args()


def build_es_options(args: argparse.Namespace) -> Dict[str, str]:
    options = {
        "es.nodes": args.es_nodes,
        "es.port": args.es_port,
        "es.resource": args.es_resource,
        "es.nodes.wan.only": args.es_wan_only,
        "es.nodes.discovery": "false" if args.es_wan_only == "true" else "true",
        "es.read.field.as.array.include": "",
    }
    if args.es_input_max_docs_per_partition:
        options["es.input.max.docs.per.partition"] = args.es_input_max_docs_per_partition
    if args.es_scroll_size:
        options["es.scroll.size"] = args.es_scroll_size
    if args.es_use_ssl:
        options["es.net.ssl"] = "true"
    if args.es_username:
        options["es.net.http.auth.user"] = args.es_username
    if args.es_password:
        options["es.net.http.auth.pass"] = args.es_password
    if args.es_query:
        options["es.query"] = args.es_query
    return options


def parse_literal(value: str) -> Any:
    value = value.strip()
    if not value:
        raise ValueError("Partition values and range bounds cannot be empty.")
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def build_base_query_body(es_query: Optional[str]) -> Dict[str, Any]:
    if not es_query:
        return {"query": {"match_all": {}}}

    try:
        parsed = json.loads(es_query)
    except json.JSONDecodeError as exc:
        raise ValueError("--es-query must be a JSON Query DSL string when partitioned reads are enabled.") from exc

    if not isinstance(parsed, dict):
        raise ValueError("--es-query must decode to a JSON object.")
    if "query" in parsed:
        return dict(parsed)
    return {"query": parsed}


def add_filter_to_query_body(base_body: Dict[str, Any], filter_clause: Dict[str, Any]) -> Dict[str, Any]:
    body = dict(base_body)
    base_query = body.get("query", {"match_all": {}})
    body["query"] = {"bool": {"must": [base_query], "filter": [filter_clause]}}
    return body


def build_partition_queries(args: argparse.Namespace) -> List[str]:
    if not args.es_partition_field and not args.es_partition_values and not args.es_partition_ranges:
        return []
    if not args.es_partition_field:
        raise ValueError("--es-partition-field is required when partition values or ranges are set.")
    if bool(args.es_partition_values) == bool(args.es_partition_ranges):
        raise ValueError("Set exactly one of --es-partition-values or --es-partition-ranges.")

    base_body = build_base_query_body(args.es_query)
    queries = []

    if args.es_partition_values:
        for raw_value in args.es_partition_values.split(","):
            value = parse_literal(raw_value)
            filter_clause = {"term": {args.es_partition_field: value}}
            queries.append(json.dumps(add_filter_to_query_body(base_body, filter_clause), separators=(",", ":")))

    if args.es_partition_ranges:
        for raw_range in args.es_partition_ranges.split(","):
            parts = raw_range.split("..")
            if len(parts) != 2:
                raise ValueError(f"Invalid range partition {raw_range!r}. Expected lower..upper.")
            lower, upper = (part.strip() for part in parts)
            range_spec: Dict[str, Any] = {}
            if lower:
                range_spec["gte"] = parse_literal(lower)
            if upper:
                range_spec["lt"] = parse_literal(upper)
            if not range_spec:
                raise ValueError(f"Invalid open range partition {raw_range!r}. Set at least one bound.")
            filter_clause = {"range": {args.es_partition_field: range_spec}}
            queries.append(json.dumps(add_filter_to_query_body(base_body, filter_clause), separators=(",", ":")))

    return queries


def load_source(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    options = build_es_options(args)
    partition_queries = build_partition_queries(args)
    if not partition_queries:
        return spark.read.format("es").options(**options).load(args.es_resource)

    if args.es_query:
        options.pop("es.query", None)

    print(
        f"Reading Elasticsearch resource {args.es_resource} with "
        f"{len(partition_queries)} partitioned queries on {args.es_partition_field}"
    )
    partitioned_reads = []
    for index, query in enumerate(partition_queries, start=1):
        query_options = dict(options)
        query_options["es.query"] = query
        print(f"Creating Elasticsearch read partition {index}/{len(partition_queries)}")
        partitioned_reads.append(spark.read.format("es").options(**query_options).load(args.es_resource))

    source = partitioned_reads[0]
    for partitioned_read in partitioned_reads[1:]:
        source = source.unionByName(partitioned_read, allowMissingColumns=True)
    return source


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("elastic-to-spanner-bulk-load").getOrCreate()

    source = load_source(spark, args)
    print(f"Elasticsearch source DataFrame partitions: {source.rdd.getNumPartitions()}")

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

    required_columns = ["TenantId", "ProductId", "Title"]
    malformed_count = migrated.filter(
        " OR ".join(f"{column} IS NULL" for column in required_columns)
    ).count()
    if malformed_count:
        raise ValueError(
            f"Found {malformed_count} rows missing one of required columns: {required_columns}"
        )

    write_df = migrated.dropDuplicates(["TenantId", "ProductId"])
    print(f"Writing {write_df.count()} rows to Spanner table {args.spanner_table}")

    (
        write_df.write.format("cloud-spanner")
        .option("projectId", args.spanner_project_id)
        .option("instanceId", args.spanner_instance_id)
        .option("databaseId", args.spanner_database_id)
        .option("table", args.spanner_table)
        .option("mutationType", args.mutation_type)
        .option("mutationsPerTransaction", args.mutations_per_transaction)
        .option("numWriteThreads", args.num_write_threads)
        .option("maxPendingTransactions", args.max_pending_transactions)
        .option("assumeIdempotentRows", args.assume_idempotent_rows)
        .option("enablePartialRowUpdates", args.enable_partial_row_updates)
        .mode(args.write_mode)
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
