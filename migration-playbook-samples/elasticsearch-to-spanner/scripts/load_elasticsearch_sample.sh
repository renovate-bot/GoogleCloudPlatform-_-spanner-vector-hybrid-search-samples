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
Usage: samples/scripts/load_elasticsearch_sample.sh SAMPLE_ID [INDEX_NAME]

Loads samples/SAMPLE_ID/elasticsearch/index.json and documents.ndjson into the
local Docker-backed Elasticsearch source.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 ]]; then
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
DEFAULT_INDEX="sample_${SAMPLE_ID//-/_}"
INDEX_NAME="${2:-${SAMPLE_ELASTICSEARCH_INDEX:-${DEFAULT_INDEX}}}"
SAMPLE_DIR="${ROOT_DIR}/samples/${SAMPLE_ID}"
INDEX_FILE="${SAMPLE_DIR}/elasticsearch/index.json"
DOCUMENTS_FILE="${SAMPLE_DIR}/elasticsearch/documents.ndjson"
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:${ELASTICSEARCH_LOCAL_PORT:-9200}}"

if [[ ! -f "${INDEX_FILE}" ]]; then
  printf 'Missing Elasticsearch index file: %s\n' "${INDEX_FILE}" >&2
  exit 1
fi

if [[ ! -f "${DOCUMENTS_FILE}" ]]; then
  printf 'Missing Elasticsearch documents file: %s\n' "${DOCUMENTS_FILE}" >&2
  exit 1
fi

printf 'Waiting for Elasticsearch at %s\n' "${ELASTICSEARCH_URL}"
until curl -fsS "${ELASTICSEARCH_URL}/_cluster/health" >/dev/null; do
  sleep 2
done

printf 'Resetting index %s\n' "${INDEX_NAME}"
curl -fsS -X DELETE "${ELASTICSEARCH_URL}/${INDEX_NAME}" >/dev/null || true
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  "${ELASTICSEARCH_URL}/${INDEX_NAME}" \
  -d @"${INDEX_FILE}" >/dev/null

printf 'Loading sample documents from %s\n' "${DOCUMENTS_FILE}"
RESPONSE_FILE="$(mktemp)"
curl -fsS -X POST \
  -H 'Content-Type: application/x-ndjson' \
  "${ELASTICSEARCH_URL}/${INDEX_NAME}/_bulk?refresh=true" \
  --data-binary @"${DOCUMENTS_FILE}" >"${RESPONSE_FILE}"

if command -v jq >/dev/null 2>&1; then
  jq '{errors: .errors, items: (.items | length)}' "${RESPONSE_FILE}"
  if ! jq -e '.errors == false' "${RESPONSE_FILE}" >/dev/null; then
    printf 'Bulk load reported errors. Full response is in %s\n' "${RESPONSE_FILE}" >&2
    exit 1
  fi
else
  cat "${RESPONSE_FILE}"
fi

printf 'Final count:\n'
curl -fsS "${ELASTICSEARCH_URL}/${INDEX_NAME}/_count"
printf '\n'
