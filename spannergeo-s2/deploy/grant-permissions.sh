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
# grant-permissions.sh — Grant Spanner service agent permissions for Remote UDFs
# =============================================================================
# Grants the Spanner service agent the Spanner API Service Agent role at the
# project level. This is the IAM binding that allows Spanner Remote UDFs to
# invoke Cloud Run endpoints (including Gen 2 Cloud Functions).
#
# How it works:
#   - Spanner uses a Google-managed service agent to call Remote UDF endpoints.
#     The service agent has the form:
#       service-PROJECT_NUMBER@gcp-sa-spanner.iam.gserviceaccount.com
#
#   - The service agent needs the roles/spanner.serviceAgent role on the
#     project. This role includes the permissions needed to invoke Cloud Run
#     services used as Remote UDF backends.
#
#   - When Spanner invokes a Remote UDF, the service agent obtains an OIDC
#     token for the endpoint URL and includes it in the request. The Cloud Run
#     service verifies the token and checks that the caller has the necessary
#     permissions. Without this binding, Spanner receives PERMISSION_DENIED.
#
# Reference:
#   https://cloud.google.com/spanner/docs/cloud-run-remote-function
#
# Prerequisites:
#   - Cloud Functions already deployed (run deploy-function.sh first)
#   - gcloud CLI installed and authenticated with permissions to modify
#     IAM policies on the project (roles/resourcemanager.projectIamAdmin
#     or equivalent)
#
# Usage:
#   ./grant-permissions.sh                     # Uses project from gcloud config
#   ./grant-permissions.sh --project my-proj   # Explicit project ID
# =============================================================================
set -euo pipefail

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

# --- Step 3: Grant roles/spanner.serviceAgent at the project level ---
# This role grants the Spanner service agent the permissions it needs to
# invoke Cloud Run endpoints used as Remote UDF backends, including:
#   - Obtaining OIDC tokens for authenticated HTTPS calls
#   - Invoking Cloud Run services in the project
#
# This is a project-level binding (not per-service), which means the Spanner
# service agent can invoke any Cloud Run service in the project that is
# referenced by a Remote UDF. If you need tighter scoping, consult the
# Spanner Remote Functions documentation.
echo "Step 3: Granting roles/spanner.serviceAgent on project ${PROJECT_ID}..."
echo "  Member:   serviceAccount:${SPANNER_SA}"
echo "  Role:     roles/spanner.serviceAgent"
echo "  Resource: project/${PROJECT_ID}"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SPANNER_SA}" \
    --role="roles/spanner.serviceAgent" \
    --condition=None \
    --quiet > /dev/null
echo "  Granted."
echo ""

echo "============================================="
echo "IAM permissions granted successfully."
echo "============================================="
echo ""
echo "Summary:"
echo "  ${SPANNER_SA}"
echo "    -> roles/spanner.serviceAgent on project: ${PROJECT_ID}"
echo ""
echo "The Spanner service agent can now invoke Cloud Run endpoints"
echo "used as Remote UDF backends in this project."
echo ""
echo "Next step: Create the Remote UDF definitions in Spanner"
echo "  Update the endpoint URLs in sample/infra/udf_definition.sql"
echo "  then execute the DDL against your Spanner database."
