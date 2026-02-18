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
# grant-permissions.sh — Grant Spanner service agent invoker access
# =============================================================================
# Grants the Spanner service agent the Cloud Run Invoker role on each Cloud Run
# service backing our Gen 2 Cloud Functions. This is the critical IAM binding
# that allows Spanner Remote UDFs to invoke the functions.
#
# How it works:
#   - Spanner uses a Google-managed service agent to call Remote UDF endpoints.
#     The service agent has the form:
#       service-PROJECT_NUMBER@gcp-sa-spanner.iam.gserviceaccount.com
#
#   - Gen 2 Cloud Functions are backed by Cloud Run services. The IAM binding
#     must be on the Cloud Run service (roles/run.invoker), NOT on the Cloud
#     Function resource itself.
#
#   - When Spanner invokes a Remote UDF, the service agent obtains an OIDC
#     token for the endpoint URL and includes it in the request. The Cloud Run
#     service verifies the token and checks that the caller has the invoker
#     role. Without this binding, Spanner receives PERMISSION_DENIED.
#
# Common mistakes this script avoids:
#   - Granting roles/cloudfunctions.invoker instead of roles/run.invoker
#     (Gen 2 functions need the Cloud Run role)
#   - Granting the role to the wrong service account (e.g., the default
#     compute service account instead of the Spanner service agent)
#   - Granting the role at the project level instead of on the specific
#     Cloud Run services (this script follows least privilege)
#
# Prerequisites:
#   - Cloud Functions already deployed (run deploy-function.sh first)
#   - gcloud CLI installed and authenticated with permissions to modify
#     IAM policies on Cloud Run services (roles/run.admin or equivalent)
#
# Usage:
#   ./grant-permissions.sh                     # Uses project from gcloud config
#   ./grant-permissions.sh --project my-proj   # Explicit project ID
# =============================================================================
set -euo pipefail

# --- Configuration ---
REGION="us-central1"

# Cloud Run service names (same as the Gen 2 Cloud Function names)
COVERING_SERVICE="s2-covering"
DISTANCE_SERVICE="s2-distance"
COVERING_RECT_SERVICE="s2-covering-rect"

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
echo "Granting Spanner IAM permissions"
echo "  Project: ${PROJECT_ID}"
echo "  Region:  ${REGION}"
echo "============================================="
echo ""

# --- Step 1: Look up the project number ---
# The Spanner service agent email uses the project NUMBER, not the project ID.
echo "Step 1: Looking up project number for ${PROJECT_ID}..."
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')
echo "  Project number: ${PROJECT_NUMBER}"
echo ""

# --- Step 2: Construct the Spanner service agent email ---
SPANNER_SA="service-${PROJECT_NUMBER}@gcp-sa-spanner.iam.gserviceaccount.com"
echo "Step 2: Spanner service agent identified"
echo "  ${SPANNER_SA}"
echo ""

# --- Step 3: Grant Cloud Run Invoker on s2-covering ---
# This allows the Spanner service agent to send authenticated HTTPS requests
# to the s2-covering Cloud Run service with a valid OIDC token.
echo "Step 3: Granting roles/run.invoker on ${COVERING_SERVICE}..."
echo "  Member:  serviceAccount:${SPANNER_SA}"
echo "  Role:    roles/run.invoker"
echo "  Service: ${COVERING_SERVICE} (${REGION})"
gcloud run services add-iam-policy-binding "${COVERING_SERVICE}" \
    --region="${REGION}" \
    --member="serviceAccount:${SPANNER_SA}" \
    --role="roles/run.invoker" \
    --project="${PROJECT_ID}"
echo "  Granted."
echo ""

# --- Step 4: Grant Cloud Run Invoker on s2-distance ---
echo "Step 4: Granting roles/run.invoker on ${DISTANCE_SERVICE}..."
echo "  Member:  serviceAccount:${SPANNER_SA}"
echo "  Role:    roles/run.invoker"
echo "  Service: ${DISTANCE_SERVICE} (${REGION})"
gcloud run services add-iam-policy-binding "${DISTANCE_SERVICE}" \
    --region="${REGION}" \
    --member="serviceAccount:${SPANNER_SA}" \
    --role="roles/run.invoker" \
    --project="${PROJECT_ID}"
echo "  Granted."
echo ""

# --- Step 5: Grant Cloud Run Invoker on s2-covering-rect ---
echo "Step 5: Granting roles/run.invoker on ${COVERING_RECT_SERVICE}..."
echo "  Member:  serviceAccount:${SPANNER_SA}"
echo "  Role:    roles/run.invoker"
echo "  Service: ${COVERING_RECT_SERVICE} (${REGION})"
gcloud run services add-iam-policy-binding "${COVERING_RECT_SERVICE}" \
    --region="${REGION}" \
    --member="serviceAccount:${SPANNER_SA}" \
    --role="roles/run.invoker" \
    --project="${PROJECT_ID}"
echo "  Granted."
echo ""

echo "============================================="
echo "IAM permissions granted successfully."
echo "============================================="
echo ""
echo "Summary of bindings:"
echo "  ${SPANNER_SA}"
echo "    -> roles/run.invoker on Cloud Run service: ${COVERING_SERVICE}"
echo "    -> roles/run.invoker on Cloud Run service: ${DISTANCE_SERVICE}"
echo "    -> roles/run.invoker on Cloud Run service: ${COVERING_RECT_SERVICE}"
echo ""
echo "The Spanner service agent can now invoke all three Cloud Functions"
echo "via authenticated HTTPS with OIDC tokens."
echo ""
echo "Next step: Create the Remote UDF definitions in Spanner"
echo "  Update the endpoint URLs in sample/infra/udf_definition.sql"
echo "  then execute the DDL against your Spanner database."
