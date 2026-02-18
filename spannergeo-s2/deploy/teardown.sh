#!/bin/bash
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

# =============================================================================
# teardown.sh — Delete Cloud Functions and clean up IAM bindings
# =============================================================================
# Removes the Cloud Functions deployed by deploy-function.sh. The IAM bindings
# granted by grant-permissions.sh are automatically cleaned up when the Cloud
# Run services are deleted (since the bindings are on the services themselves).
#
# This script does NOT:
#   - Delete the Spanner instance or database (those may be shared/pre-existing)
#   - Drop the Remote UDF definitions from Spanner (requires DDL, see below)
#   - Disable GCP APIs (other services in the project may depend on them)
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#
# Usage:
#   ./teardown.sh                     # Uses project from gcloud config
#   ./teardown.sh --project my-proj   # Explicit project ID
# =============================================================================
set -euo pipefail

# --- Configuration ---
REGION="us-central1"

# Function names to delete (must match what was deployed)
COVERING_FUNCTION_NAME="s2-covering"
DISTANCE_FUNCTION_NAME="s2-distance"
COVERING_RECT_FUNCTION_NAME="s2-covering-rect"

# --- Parse flags ---
PROJECT_ID=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --project)
            PROJECT_ID="$2"
            shift 2
            ;;
        --project=*)
            PROJECT_ID="${1#*=}"
            shift
            ;;
        *)
            echo "Unknown flag: $1"
            echo "Usage: $0 [--project PROJECT_ID]"
            exit 1
            ;;
    esac
done

# --- Resolve project ID ---
if [[ -z "$PROJECT_ID" ]]; then
    PROJECT_ID=$(gcloud config get-value project 2>/dev/null) || true
    if [[ -z "$PROJECT_ID" ]]; then
        echo "ERROR: No project specified and no default project configured."
        echo "Run: gcloud config set project YOUR_PROJECT_ID"
        echo "  or: $0 --project YOUR_PROJECT_ID"
        exit 1
    fi
fi

echo "============================================="
echo "Tearing down S2 Cloud Functions"
echo "  Project: ${PROJECT_ID}"
echo "  Region:  ${REGION}"
echo "============================================="
echo ""

# --- Step 1: Delete s2-covering function ---
# --quiet suppresses the interactive confirmation prompt.
# If the function doesn't exist, gcloud will return an error. We continue
# anyway (|| true) so the script can clean up the remaining resources.
echo "Step 1: Deleting function: ${COVERING_FUNCTION_NAME}..."
if gcloud functions describe "${COVERING_FUNCTION_NAME}" \
    --gen2 --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud functions delete "${COVERING_FUNCTION_NAME}" \
        --gen2 \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet
    echo "  Deleted: ${COVERING_FUNCTION_NAME}"
else
    echo "  Function ${COVERING_FUNCTION_NAME} not found — skipping."
fi
echo ""

# --- Step 2: Delete s2-distance function ---
echo "Step 2: Deleting function: ${DISTANCE_FUNCTION_NAME}..."
if gcloud functions describe "${DISTANCE_FUNCTION_NAME}" \
    --gen2 --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud functions delete "${DISTANCE_FUNCTION_NAME}" \
        --gen2 \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet
    echo "  Deleted: ${DISTANCE_FUNCTION_NAME}"
else
    echo "  Function ${DISTANCE_FUNCTION_NAME} not found — skipping."
fi
echo ""

# --- Step 3: Delete s2-covering-rect function ---
echo "Step 3: Deleting function: ${COVERING_RECT_FUNCTION_NAME}..."
if gcloud functions describe "${COVERING_RECT_FUNCTION_NAME}" \
    --gen2 --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud functions delete "${COVERING_RECT_FUNCTION_NAME}" \
        --gen2 \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet
    echo "  Deleted: ${COVERING_RECT_FUNCTION_NAME}"
else
    echo "  Function ${COVERING_RECT_FUNCTION_NAME} not found — skipping."
fi
echo ""

echo "============================================="
echo "Teardown complete."
echo "============================================="
echo ""
echo "IMPORTANT: The Spanner Remote UDF definitions still exist in the"
echo "database. The functions they reference have been deleted, so any"
echo "query calling them will fail. To clean up the UDF definitions,"
echo "run the following DDL against your Spanner database:"
echo ""
echo "  DROP FUNCTION IF EXISTS geo.s2_covering;"
echo "  DROP FUNCTION IF EXISTS geo.s2_distance;"
echo "  DROP FUNCTION IF EXISTS geo.s2_covering_rect;"
echo "  DROP SCHEMA IF EXISTS geo;"
echo ""
echo "You can execute this via the gcloud CLI:"
echo "  gcloud spanner databases ddl update DATABASE_NAME \\"
echo "    --instance=INSTANCE_NAME \\"
echo "    --ddl='DROP FUNCTION IF EXISTS geo.s2_covering' \\"
echo "    --ddl='DROP FUNCTION IF EXISTS geo.s2_distance' \\"
echo "    --ddl='DROP FUNCTION IF EXISTS geo.s2_covering_rect' \\"
echo "    --ddl='DROP SCHEMA IF EXISTS geo' \\"
echo "    --project=${PROJECT_ID}"
