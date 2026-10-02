#!/usr/bin/env bash
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

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for formatting
BLUE='\033[1;34m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
RED='\033[1;31m'
NC='\033[0m' # No Color

PROJECT_ID=""
DRY_RUN=false
VERBOSITY="info"
AUTO_APPROVE=false

usage() {
    echo -e "${BLUE}Cloud Spanner Deployment Explorer - Google App Engine Flex Deployment${NC}"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -p, --project PROJECT_ID   Specify Google Cloud Project ID"
    echo "  -d, --dry-run              Validate configuration and print deployment commands without executing"
    echo "  -v, --debug                Deploy with verbose debugging logs (--verbosity=debug)"
    echo "  -y, --yes                  Skip interactive confirmation prompt"
    echo "  -h, --help                 Display this help message"
    echo ""
    exit 0
}

# Parse command line options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--project)
            PROJECT_ID="$2"
            shift 2
            ;;
        -p=*|--project=*)
            PROJECT_ID="${1#*=}"
            shift
            ;;
        --verbosity=*)
            VERBOSITY="${1#*=}"
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -v|--debug)
            VERBOSITY="debug"
            shift
            ;;
        -y|--yes)
            AUTO_APPROVE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}  Google Cloud Spanner Deployment Explorer - App Engine Deployment      ${NC}"
echo -e "${BLUE}========================================================================${NC}"

# Step 1: Verify gcloud CLI
if ! command -v gcloud &> /dev/null; then
    if [[ "$DRY_RUN" == true ]]; then
        echo -e "${YELLOW}⚠️ 'gcloud' CLI tool is not found in PATH (continuing in dry-run mode)...${NC}"
    else
        echo -e "${RED}❌ 'gcloud' CLI tool is not found in PATH.${NC}"
        echo "   Please install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
fi

# Step 2: Determine Project ID dynamically (Zero hardcoded project IDs)
if [[ -z "$PROJECT_ID" ]]; then
    PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
fi

if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "(unset)" ]]; then
    echo -e "${YELLOW}⚠️ No active Google Cloud project configured.${NC}"
    read -rp "Enter Google Cloud Project ID to deploy to: " PROJECT_ID
fi

if [[ -z "$PROJECT_ID" ]]; then
    echo -e "${RED}❌ A valid Google Cloud Project ID is required for deployment.${NC}"
    exit 1
fi

# Validate project ID format (Google Cloud naming rule)
if [[ ! "$PROJECT_ID" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]]; then
    echo -e "${YELLOW}⚠️ Warning: '$PROJECT_ID' may not conform to standard GCP project ID format.${NC}"
fi

GAE_SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"
STAGING_BUCKET="gs://staging.${PROJECT_ID}.appspot.com"

echo -e "🎯 Target Project:      ${GREEN}${PROJECT_ID}${NC}"
echo -e "🤖 GAE Service Account: ${GREEN}${GAE_SERVICE_ACCOUNT}${NC}"
echo -e "🪣 Staging Bucket:      ${GREEN}${STAGING_BUCKET}${NC}"
echo -e "⚙️  Runtime:             ${GREEN}Custom Docker container (env: flex)${NC}"
echo -e "🔒 Feature Policy:      ${GREEN}enable_load_testing: false, enable_ai: false (Topology Explorer only on App Engine)${NC}"
echo ""

# Step 3: IAM & Permissions Checklist ("IAM Nightmare" mitigation)
echo -e "${YELLOW}📋 IAM & Permissions Preflight Checklist:${NC}"
echo "   App Engine Flexible requires the GAE service account to hold the following roles:"
echo "   1. Artifact Registry Create-on-Push Writer (roles/artifactregistry.createOnPushWriter)"
echo "   2. Compute Admin (roles/compute.admin)"
echo "   3. Storage Admin (roles/storage.admin)"
echo "   4. Storage Owner on staging bucket: ${STAGING_BUCKET}"
echo ""
echo "   If permissions are missing, grant them via:"
echo "     gcloud projects add-iam-policy-binding ${PROJECT_ID} \\"
echo "       --member=\"serviceAccount:${GAE_SERVICE_ACCOUNT}\" \\"
echo "       --role=\"roles/artifactregistry.createOnPushWriter\""
echo "     gcloud projects add-iam-policy-binding ${PROJECT_ID} \\"
echo "       --member=\"serviceAccount:${GAE_SERVICE_ACCOUNT}\" \\"
echo "       --role=\"roles/compute.admin\""
echo "     gcloud projects add-iam-policy-binding ${PROJECT_ID} \\"
echo "       --member=\"serviceAccount:${GAE_SERVICE_ACCOUNT}\" \\"
echo "       --role=\"roles/storage.admin\""
echo ""

# Step 4: Ensure production frontend bundle is compiled
STATIC_INDEX="$ROOT_DIR/backend/app/static/index.html"
if [[ ! -f "$STATIC_INDEX" ]]; then
    echo -e "📦 Production frontend bundle missing. Compiling frontend..."
    if command -v npm &> /dev/null; then
        (cd "$ROOT_DIR/frontend" && npm install && npm run build)
        echo -e "${GREEN}✅ Frontend compilation finished.${NC}"
    else
        echo -e "${YELLOW}⚠️ 'npm' not found. App will run with API endpoints only or use existing static assets.${NC}"
    fi
else
    echo -e "${GREEN}✅ Static frontend bundle verified ($STATIC_INDEX).${NC}"
fi

# Step 5: Validate app.yaml configuration
if [[ ! -f "$ROOT_DIR/app.yaml" ]]; then
    echo -e "${RED}❌ $ROOT_DIR/app.yaml not found.${NC}"
    exit 1
fi

# Check that load testing is disabled in app.yaml
if ! grep -q 'ENABLE_LOAD_TESTING: "false"' "$ROOT_DIR/app.yaml"; then
    echo -e "${YELLOW}⚠️ Warning: ENABLE_LOAD_TESTING is not set to false in app.yaml.${NC}"
fi

DEPLOY_CMD="gcloud app deploy app.yaml --project=${PROJECT_ID} --verbosity=${VERBOSITY}"

echo ""
echo -e "${BLUE}Deployment Command:${NC}"
echo "  $DEPLOY_CMD"
echo ""

if [[ "$DRY_RUN" == true ]]; then
    echo -e "${GREEN}🔎 [DRY RUN] Configuration and preflight checks completed successfully.${NC}"
    echo "   To deploy, run: $0 --project=${PROJECT_ID}"
    exit 0
fi

if [[ "$AUTO_APPROVE" != true ]]; then
    read -rp "Proceed with deployment to Google App Engine? (y/N): " CONFIRM
    if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
        echo "Deployment canceled by user."
        exit 0
    fi
fi

echo -e "🚀 Starting App Engine Flexible deployment..."
cd "$ROOT_DIR"

# Execute gcloud app deploy
if eval "$DEPLOY_CMD"; then
    echo ""
    echo -e "${GREEN}========================================================================${NC}"
    echo -e "${GREEN}✅ Deployment to App Engine Flex succeeded!                             ${NC}"
    echo -e "${GREEN}========================================================================${NC}"
    echo -e "🌐 Application URL: https://${PROJECT_ID}.uc.r.appspot.com"
    echo -e "🔒 Latency Benchmarking is turned OFF (topology explorer & sizing active)."
    echo ""
else
    echo ""
    echo -e "${RED}❌ Deployment encountered an error.${NC}"
    echo ""
    echo -e "${YELLOW}💡 Troubleshooting common App Engine Flex issues:${NC}"
    echo "   1. TLS/Client Certificate Alert ('tlsv13 alert certificate required'):"
    echo "      Run:"
    echo "        CLOUDSDK_CONTEXT_AWARE_USE_CLIENT_CERTIFICATE=false gcloud app deploy app.yaml"
    echo ""
    echo "   2. Permission Denied on Staging Bucket:"
    echo "      Grant Storage Admin to the GAE service account:"
    echo "        gcloud projects add-iam-policy-binding ${PROJECT_ID} \\"
    echo "          --member=\"serviceAccount:${GAE_SERVICE_ACCOUNT}\" \\"
    echo "          --role=\"roles/storage.admin\""
    echo ""
    echo "   3. Detailed logs: re-run with --debug flag:"
    echo "      $0 --project=${PROJECT_ID} --debug"
    exit 1
fi
