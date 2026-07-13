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
Usage: samples/scripts/seed_spanner_sample.sh SAMPLE_ID

Runs samples/SAMPLE_ID/spanner/cleanup.sql, when present, followed by
samples/SAMPLE_ID/spanner/seed-data.sql against the Spanner database defined by
SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, and SPANNER_DATABASE_ID in .env.
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

: "${SPANNER_PROJECT_ID:?SPANNER_PROJECT_ID is required}"
: "${SPANNER_INSTANCE_ID:?SPANNER_INSTANCE_ID is required}"
: "${SPANNER_DATABASE_ID:?SPANNER_DATABASE_ID is required}"

SAMPLE_ID="$1"
SEED_FILE="${ROOT_DIR}/samples/${SAMPLE_ID}/spanner/seed-data.sql"
CLEANUP_FILE="${ROOT_DIR}/samples/${SAMPLE_ID}/spanner/cleanup.sql"

if [[ ! -f "${SEED_FILE}" ]]; then
  printf 'Missing Spanner seed file: %s\n' "${SEED_FILE}" >&2
  exit 1
fi

if [[ -f "${CLEANUP_FILE}" ]]; then
  SQL="$(< "${CLEANUP_FILE}")"
  gcloud spanner databases execute-sql "${SPANNER_DATABASE_ID}" \
    --instance="${SPANNER_INSTANCE_ID}" \
    --project="${SPANNER_PROJECT_ID}" \
    --sql="${SQL}"
fi

SQL="$(< "${SEED_FILE}")"
gcloud spanner databases execute-sql "${SPANNER_DATABASE_ID}" \
  --instance="${SPANNER_INSTANCE_ID}" \
  --project="${SPANNER_PROJECT_ID}" \
  --sql="${SQL}"
