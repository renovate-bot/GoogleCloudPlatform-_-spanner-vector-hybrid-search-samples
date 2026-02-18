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
# deploy-function.sh — Deploy S2 Cloud Functions for Spanner Remote UDFs
# =============================================================================
# Deploys three Gen 2 Cloud Functions that back the Spanner Remote UDFs:
#
#   1. s2-covering       — Computes S2 cell coverings for a circular search region.
#                          Entry point: com.example.spannergeo.functions.S2CoveringFunction
#
#   2. s2-distance       — Computes great-circle distance between two lat/lng points.
#                          Entry point: com.example.spannergeo.functions.S2DistanceFunction
#
#   3. s2-covering-rect  — Computes S2 cell coverings for a rectangular (lat/lng) region.
#                          Entry point: com.example.spannergeo.functions.S2CoveringRectFunction
#
# All functions are HTTP-triggered, require authentication, and use the Java 17
# runtime. Gen 2 functions run on Cloud Run, which provides better concurrency
# handling and min-instance controls.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - APIs enabled (run setup.sh first)
#   - Maven (mvn) installed for building the Java source
#   - Cloud Function Java source in sample/cloud-function/
#
# Usage:
#   ./deploy-function.sh                     # Uses project from gcloud config
#   ./deploy-function.sh --project my-proj   # Explicit project ID
# =============================================================================
set -euo pipefail

# --- Configuration ---
REGION="us-central1"
RUNTIME="java17"
MEMORY="512MB"
TIMEOUT="60s"

# Function names and their entry points
COVERING_FUNCTION_NAME="s2-covering"
COVERING_ENTRY_POINT="com.example.spannergeo.functions.S2CoveringFunction"

DISTANCE_FUNCTION_NAME="s2-distance"
DISTANCE_ENTRY_POINT="com.example.spannergeo.functions.S2DistanceFunction"

COVERING_RECT_FUNCTION_NAME="s2-covering-rect"
COVERING_RECT_ENTRY_POINT="com.example.spannergeo.functions.S2CoveringRectFunction"

# --- Resolve paths relative to this script ---
# This ensures the script works regardless of where it's invoked from.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLOUD_FUNCTION_DIR="${SCRIPT_DIR}/../cloud-function"

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

# --- Verify source directory exists ---
if [[ ! -d "$CLOUD_FUNCTION_DIR" ]]; then
    echo "ERROR: Cloud Function source directory not found: ${CLOUD_FUNCTION_DIR}"
    exit 1
fi

if [[ ! -f "${CLOUD_FUNCTION_DIR}/pom.xml" ]]; then
    echo "ERROR: pom.xml not found in ${CLOUD_FUNCTION_DIR}"
    echo "The Cloud Function Maven project must exist before deploying."
    exit 1
fi

echo "============================================="
echo "Deploying S2 Cloud Functions"
echo "  Project:  ${PROJECT_ID}"
echo "  Region:   ${REGION}"
echo "  Runtime:  ${RUNTIME}"
echo "  Memory:   ${MEMORY}"
echo "  Timeout:  ${TIMEOUT}"
echo "  Source:   ${CLOUD_FUNCTION_DIR}"
echo "============================================="
echo ""

# --- Step 1: Build the Cloud Function ---
# Maven build ensures the source compiles before we attempt deployment.
# gcloud also builds during deploy, but pre-building catches errors faster
# and gives clearer error messages.
echo "Step 1: Building Cloud Function source with Maven..."
echo "  Running: mvn -f ${CLOUD_FUNCTION_DIR}/pom.xml package -q -DskipTests"
mvn -f "${CLOUD_FUNCTION_DIR}/pom.xml" package -q -DskipTests
echo "  Build successful."
echo ""

# --- Step 2: Deploy s2-covering function ---
echo "Step 2: Deploying function: ${COVERING_FUNCTION_NAME}"
echo "  Entry point: ${COVERING_ENTRY_POINT}"
gcloud functions deploy "${COVERING_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --runtime="${RUNTIME}" \
    --source="${CLOUD_FUNCTION_DIR}" \
    --entry-point="${COVERING_ENTRY_POINT}" \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory="${MEMORY}" \
    --timeout="${TIMEOUT}" \
    --project="${PROJECT_ID}"
echo "  Deployed: ${COVERING_FUNCTION_NAME}"
echo ""

# --- Step 3: Deploy s2-distance function ---
echo "Step 3: Deploying function: ${DISTANCE_FUNCTION_NAME}"
echo "  Entry point: ${DISTANCE_ENTRY_POINT}"
gcloud functions deploy "${DISTANCE_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --runtime="${RUNTIME}" \
    --source="${CLOUD_FUNCTION_DIR}" \
    --entry-point="${DISTANCE_ENTRY_POINT}" \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory="${MEMORY}" \
    --timeout="${TIMEOUT}" \
    --project="${PROJECT_ID}"
echo "  Deployed: ${DISTANCE_FUNCTION_NAME}"
echo ""

# --- Step 4: Deploy s2-covering-rect function ---
echo "Step 4: Deploying function: ${COVERING_RECT_FUNCTION_NAME}"
echo "  Entry point: ${COVERING_RECT_ENTRY_POINT}"
gcloud functions deploy "${COVERING_RECT_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --runtime="${RUNTIME}" \
    --source="${CLOUD_FUNCTION_DIR}" \
    --entry-point="${COVERING_RECT_ENTRY_POINT}" \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory="${MEMORY}" \
    --timeout="${TIMEOUT}" \
    --project="${PROJECT_ID}"
echo "  Deployed: ${COVERING_RECT_FUNCTION_NAME}"
echo ""

# --- Step 5: Retrieve and display function URLs ---
# Gen 2 functions have their URL in serviceConfig.uri. These URLs are needed
# for the Spanner Remote UDF CREATE FUNCTION ... OPTIONS (endpoint = '...') DDL.
echo "Step 5: Retrieving deployed function URLs..."

COVERING_URL=$(gcloud functions describe "${COVERING_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --format='value(serviceConfig.uri)' \
    --project="${PROJECT_ID}")

DISTANCE_URL=$(gcloud functions describe "${DISTANCE_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --format='value(serviceConfig.uri)' \
    --project="${PROJECT_ID}")

COVERING_RECT_URL=$(gcloud functions describe "${COVERING_RECT_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --format='value(serviceConfig.uri)' \
    --project="${PROJECT_ID}")

echo ""
echo "============================================="
echo "Deployment complete!"
echo "============================================="
echo ""
echo "S2 Covering Function URL:       ${COVERING_URL}"
echo "S2 Distance Function URL:       ${DISTANCE_URL}"
echo "S2 Covering Rect Function URL:  ${COVERING_RECT_URL}"
echo ""
echo "Use these URLs in your Remote UDF definitions"
echo "(sample/infra/udf_definition.sql). Replace PLACEHOLDER_URL"
echo "with the URL for the corresponding function."
echo ""
echo "Next step: Grant IAM permissions for Spanner"
echo "  ./grant-permissions.sh --project ${PROJECT_ID}"
