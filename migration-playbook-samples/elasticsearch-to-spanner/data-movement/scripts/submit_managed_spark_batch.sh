#!/usr/bin/env bash
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

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: samples/data-movement/scripts/submit_managed_spark_batch.sh

Submits the Elasticsearch-to-Spanner PySpark bulk load job to Managed Service
for Apache Spark serverless using values from .env.

Required .env values:
  GOOGLE_CLOUD_PROJECT or SPANNER_PROJECT_ID
  GOOGLE_CLOUD_REGION or MANAGED_SPARK_REGION
  GCS_BUCKET or MANAGED_SPARK_DEPS_BUCKET
  ELASTICSEARCH_URL or SPARK_ES_NODES/SPARK_ES_PORT
  SPANNER_INSTANCE_ID
  SPANNER_DATABASE_ID

Optional .env values:
  MANAGED_SPARK_RUNTIME_VERSION
  MANAGED_SPARK_SERVICE_ACCOUNT
  MANAGED_SPARK_SUBNET
  SPARK_ES_RESOURCE
  SPARK_ES_QUERY
  SPARK_ES_INPUT_MAX_DOCS_PER_PARTITION
  SPARK_ES_SCROLL_SIZE
  SPARK_ES_PARTITION_FIELD
  SPARK_ES_PARTITION_VALUES
  SPARK_ES_PARTITION_RANGES
  SPARK_SPANNER_CONNECTOR_PACKAGE
  SPARK_ELASTICSEARCH_CONNECTOR_PACKAGE
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  source "${ROOT_DIR}/.env"
  set +a
fi

PROJECT_ID="${SPANNER_PROJECT_ID:-${GOOGLE_CLOUD_PROJECT:-}}"
REGION="${MANAGED_SPARK_REGION:-${GOOGLE_CLOUD_REGION:-}}"
DEPS_BUCKET="${MANAGED_SPARK_DEPS_BUCKET:-${GCS_BUCKET:-}}"
RUNTIME_VERSION="${MANAGED_SPARK_RUNTIME_VERSION:-2.3}"
SPANNER_CONNECTOR_PACKAGE="${SPARK_SPANNER_CONNECTOR_PACKAGE:-com.google.cloud.spark.spanner:spark-3.5-spanner:1.4.0}"
ES_CONNECTOR_PACKAGE="${SPARK_ELASTICSEARCH_CONNECTOR_PACKAGE:-org.elasticsearch:elasticsearch-spark-30_2.13:${ELASTIC_STACK_VERSION:-8.15.3}}"
ES_RESOURCE="${SPARK_ES_RESOURCE:-${ELASTICSEARCH_INDEX:-sample_data_movement}}"
SPANNER_TABLE="${SPARK_SPANNER_TABLE:-SparkMigratedProducts}"
JOB_FILE="${ROOT_DIR}/samples/data-movement/spark/es_to_spanner.py"
BATCH_ID="${SPARK_DATA_MOVEMENT_BATCH_ID:-es-spanner-$(date -u +%Y%m%d%H%M%S)}"

: "${PROJECT_ID:?SPANNER_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required}"
: "${REGION:?MANAGED_SPARK_REGION or GOOGLE_CLOUD_REGION is required}"
: "${DEPS_BUCKET:?MANAGED_SPARK_DEPS_BUCKET or GCS_BUCKET is required}"
: "${SPANNER_INSTANCE_ID:?SPANNER_INSTANCE_ID is required}"
: "${SPANNER_DATABASE_ID:?SPANNER_DATABASE_ID is required}"

if [[ -n "${SPARK_ES_NODES:-}" ]]; then
  ES_NODES="${SPARK_ES_NODES}"
  ES_PORT="${SPARK_ES_PORT:-9200}"
  ES_USE_SSL="${SPARK_ES_USE_SSL:-false}"
else
  : "${ELASTICSEARCH_URL:?ELASTICSEARCH_URL or SPARK_ES_NODES is required}"
  ES_ENDPOINT="${ELASTICSEARCH_URL#http://}"
  ES_ENDPOINT="${ES_ENDPOINT#https://}"
  ES_ENDPOINT="${ES_ENDPOINT%%/*}"
  if [[ "${ELASTICSEARCH_URL}" == https://* ]]; then
    ES_USE_SSL="true"
    DEFAULT_ES_PORT="443"
  else
    ES_USE_SSL="false"
    DEFAULT_ES_PORT="9200"
  fi
  ES_NODES="${ES_ENDPOINT%%:*}"
  if [[ "${ES_ENDPOINT}" == *:* ]]; then
    ES_PORT="${ES_ENDPOINT##*:}"
  else
    ES_PORT="${DEFAULT_ES_PORT}"
  fi
fi

SPARK_ARGS=(
  "--es-nodes=${ES_NODES}"
  "--es-port=${ES_PORT}"
  "--es-resource=${ES_RESOURCE}"
  "--spanner-project-id=${PROJECT_ID}"
  "--spanner-instance-id=${SPANNER_INSTANCE_ID}"
  "--spanner-database-id=${SPANNER_DATABASE_ID}"
  "--spanner-table=${SPANNER_TABLE}"
)

if [[ "${ES_USE_SSL}" == "true" ]]; then
  SPARK_ARGS+=("--es-use-ssl")
fi
if [[ -n "${ELASTICSEARCH_USERNAME:-}" ]]; then
  SPARK_ARGS+=("--es-username=${ELASTICSEARCH_USERNAME}")
fi
if [[ -n "${ELASTICSEARCH_PASSWORD:-}" ]]; then
  SPARK_ARGS+=("--es-password=${ELASTICSEARCH_PASSWORD}")
fi
if [[ -n "${SPARK_ES_QUERY:-}" ]]; then
  SPARK_ARGS+=("--es-query=${SPARK_ES_QUERY}")
fi
if [[ -n "${SPARK_ES_INPUT_MAX_DOCS_PER_PARTITION:-}" ]]; then
  SPARK_ARGS+=("--es-input-max-docs-per-partition=${SPARK_ES_INPUT_MAX_DOCS_PER_PARTITION}")
fi
if [[ -n "${SPARK_ES_SCROLL_SIZE:-}" ]]; then
  SPARK_ARGS+=("--es-scroll-size=${SPARK_ES_SCROLL_SIZE}")
fi
if [[ -n "${SPARK_ES_PARTITION_FIELD:-}" ]]; then
  SPARK_ARGS+=("--es-partition-field=${SPARK_ES_PARTITION_FIELD}")
fi
if [[ -n "${SPARK_ES_PARTITION_VALUES:-}" ]]; then
  SPARK_ARGS+=("--es-partition-values=${SPARK_ES_PARTITION_VALUES}")
fi
if [[ -n "${SPARK_ES_PARTITION_RANGES:-}" ]]; then
  SPARK_ARGS+=("--es-partition-ranges=${SPARK_ES_PARTITION_RANGES}")
fi
if [[ -n "${SPARK_SPANNER_WRITE_MODE:-}" ]]; then
  SPARK_ARGS+=("--write-mode=${SPARK_SPANNER_WRITE_MODE}")
fi
if [[ -n "${SPARK_SPANNER_MUTATION_TYPE:-}" ]]; then
  SPARK_ARGS+=("--mutation-type=${SPARK_SPANNER_MUTATION_TYPE}")
fi
if [[ -n "${SPARK_SPANNER_ASSUME_IDEMPOTENT_ROWS:-}" ]]; then
  SPARK_ARGS+=("--assume-idempotent-rows=${SPARK_SPANNER_ASSUME_IDEMPOTENT_ROWS}")
fi
if [[ -n "${SPARK_SPANNER_ENABLE_PARTIAL_ROW_UPDATES:-}" ]]; then
  SPARK_ARGS+=("--enable-partial-row-updates=${SPARK_SPANNER_ENABLE_PARTIAL_ROW_UPDATES}")
fi

GCLOUD_ARGS=(
  dataproc batches submit pyspark "${JOB_FILE}"
  "--batch=${BATCH_ID}"
  "--project=${PROJECT_ID}"
  "--region=${REGION}"
  "--version=${RUNTIME_VERSION}"
  "--deps-bucket=${DEPS_BUCKET}"
  "--properties=^#^spark.jars.packages=${SPANNER_CONNECTOR_PACKAGE},${ES_CONNECTOR_PACKAGE}"
)

if [[ -n "${MANAGED_SPARK_SERVICE_ACCOUNT:-}" ]]; then
  GCLOUD_ARGS+=("--service-account=${MANAGED_SPARK_SERVICE_ACCOUNT}")
fi
if [[ -n "${MANAGED_SPARK_SUBNET:-}" ]]; then
  GCLOUD_ARGS+=("--subnet=${MANAGED_SPARK_SUBNET}")
fi

gcloud "${GCLOUD_ARGS[@]}" -- "${SPARK_ARGS[@]}"
