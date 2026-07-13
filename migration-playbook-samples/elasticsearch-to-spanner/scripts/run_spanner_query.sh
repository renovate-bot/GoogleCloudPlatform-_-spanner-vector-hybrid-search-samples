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
Usage: samples/scripts/run_spanner_query.sh SAMPLE_ID QUERY

Runs samples/SAMPLE_ID/spanner/queries/QUERY.sql against the Spanner database
defined by SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, and SPANNER_DATABASE_ID in
.env. The @tenant_id placeholder is replaced with SAMPLE_TENANT_ID, defaulting
to tenant-a. SAMPLE_SPANNER_OPTIMIZER_VERSION defaults to 6 and is applied as a
single statement hint by this runner.
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

: "${SPANNER_PROJECT_ID:?SPANNER_PROJECT_ID is required}"
: "${SPANNER_INSTANCE_ID:?SPANNER_INSTANCE_ID is required}"
: "${SPANNER_DATABASE_ID:?SPANNER_DATABASE_ID is required}"

SAMPLE_ID="$1"
QUERY_NAME="${2%.sql}"
QUERY_FILE="${ROOT_DIR}/samples/${SAMPLE_ID}/spanner/queries/${QUERY_NAME}.sql"
TENANT_ID="${SAMPLE_TENANT_ID:-tenant-a}"

if [[ ! -f "${QUERY_FILE}" ]]; then
  printf 'Missing Spanner query file: %s\n' "${QUERY_FILE}" >&2
  exit 1
fi

SQL="$(sed "s/@tenant_id/'${TENANT_ID}'/g" "${QUERY_FILE}")"
OPTIMIZER_VERSION="${SAMPLE_SPANNER_OPTIMIZER_VERSION-6}"
if [[ -n "${OPTIMIZER_VERSION}" && ! "${SQL}" =~ ^[[:space:]]*@\{OPTIMIZER_VERSION= ]]; then
  SQL="@{OPTIMIZER_VERSION=${OPTIMIZER_VERSION}}"$'\n'"${SQL}"
fi

gcloud spanner databases execute-sql "${SPANNER_DATABASE_ID}" \
  --instance="${SPANNER_INSTANCE_ID}" \
  --project="${SPANNER_PROJECT_ID}" \
  --sql="${SQL}"
