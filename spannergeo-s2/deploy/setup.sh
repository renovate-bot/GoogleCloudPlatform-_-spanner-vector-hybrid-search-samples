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
# setup.sh — Enable GCP APIs for Spanner Remote UDF deployment
# =============================================================================
# Enables the GCP APIs required to deploy Cloud Functions (Gen 2) that back
# Spanner Remote UDFs. This script is idempotent — safe to run multiple times.
#
# APIs enabled:
#   - Cloud Functions         (deploy Gen 2 functions)
#   - Cloud Build             (build function source during deployment)
#   - Cloud Run               (Gen 2 functions run on Cloud Run)
#   - Cloud Spanner           (the database calling the Remote UDFs)
#   - Artifact Registry       (store container images built by Cloud Build)
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Sufficient permissions to enable APIs (roles/serviceusage.serviceUsageAdmin
#     or roles/owner on the project)
#
# Usage:
#   ./setup.sh                     # Uses project from gcloud config
#   ./setup.sh --project my-proj   # Explicit project ID
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
echo "Setting up GCP APIs for project: ${PROJECT_ID}"
echo "============================================="
echo ""

# --- List of required APIs ---
APIS=(
    "cloudfunctions.googleapis.com"
    "cloudbuild.googleapis.com"
    "run.googleapis.com"
    "spanner.googleapis.com"
    "artifactregistry.googleapis.com"
)

# --- Enable each API ---
# gcloud services enable is idempotent: re-enabling an already-enabled API
# is a no-op. This makes the script safe to run multiple times.
for api in "${APIS[@]}"; do
    echo "Enabling API: ${api}"
    gcloud services enable "${api}" --project="${PROJECT_ID}"
done

echo ""
echo "============================================="
echo "All required APIs enabled successfully."
echo "============================================="
echo ""
echo "Next step: Deploy the Cloud Functions"
echo "  ./deploy-function.sh --project ${PROJECT_ID}"
