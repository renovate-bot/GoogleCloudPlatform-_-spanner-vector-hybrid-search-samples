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
Usage: samples/scripts/apply_spanner_sample_schema.sh SAMPLE_ID

Applies samples/SAMPLE_ID/spanner/schema.sql to the Spanner database defined
by SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, and SPANNER_DATABASE_ID in .env.
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
SCHEMA_FILE="${ROOT_DIR}/samples/${SAMPLE_ID}/spanner/schema.sql"

if [[ ! -f "${SCHEMA_FILE}" ]]; then
  printf 'Missing Spanner schema file: %s\n' "${SCHEMA_FILE}" >&2
  exit 1
fi

gcloud spanner databases ddl update "${SPANNER_DATABASE_ID}" \
  --instance="${SPANNER_INSTANCE_ID}" \
  --project="${SPANNER_PROJECT_ID}" \
  --ddl-file="${SCHEMA_FILE}"
