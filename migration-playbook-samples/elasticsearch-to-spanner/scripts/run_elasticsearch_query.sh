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
Usage: samples/scripts/run_elasticsearch_query.sh SAMPLE_ID QUERY [INDEX_NAME]

QUERY can be an absolute/relative JSON file path or a query name under
samples/SAMPLE_ID/elasticsearch/queries. The .json suffix is optional.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 2 ]]; then
  usage
  exit 0
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  source "${ROOT_DIR}/.env"
  set +a
fi

SAMPLE_ID="$1"
QUERY_ARG="$2"
DEFAULT_INDEX="sample_${SAMPLE_ID//-/_}"
INDEX_NAME="${3:-${SAMPLE_ELASTICSEARCH_INDEX:-${DEFAULT_INDEX}}}"
SAMPLE_DIR="${ROOT_DIR}/samples/${SAMPLE_ID}"
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:${ELASTICSEARCH_LOCAL_PORT:-9200}}"

if [[ -f "${QUERY_ARG}" ]]; then
  QUERY_FILE="${QUERY_ARG}"
else
  QUERY_NAME="${QUERY_ARG%.json}"
  QUERY_FILE="${SAMPLE_DIR}/elasticsearch/queries/${QUERY_NAME}.json"
fi

if [[ ! -f "${QUERY_FILE}" ]]; then
  printf 'Missing Elasticsearch query file: %s\n' "${QUERY_FILE}" >&2
  exit 1
fi

curl -fsS -X GET \
  -H 'Content-Type: application/json' \
  "${ELASTICSEARCH_URL}/${INDEX_NAME}/_search?pretty" \
  -d @"${QUERY_FILE}"
