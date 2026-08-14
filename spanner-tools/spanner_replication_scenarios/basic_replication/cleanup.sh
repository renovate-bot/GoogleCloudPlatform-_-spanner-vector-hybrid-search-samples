#!/bin/bash
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

set -e

# Load configuration
if [[ -f "config.env" ]]; then
    # Simple export of variables from config.env
    set -a
    source config.env
    set +a
else
    echo "Error: config.env not found."
    exit 1
fi

if [[ -z "$PROJECT_ID" ]]; then
    PROJECT_ID=$(gcloud config get-value project)
fi

echo "========================================="
echo "Cleaning up Spanner Replication Demo..."
echo "========================================="

echo "1. Cancelling Dataflow Jobs..."
# Cancel bulk migration job if it happens to still be running
BULK_JOB_ID=$(gcloud dataflow jobs list --project="${PROJECT_ID}" --region="${REGION}" --status=active --filter="name=bulk-migration-job" --format="value(JOB_ID)")
if [[ -n "$BULK_JOB_ID" ]]; then
    echo "Cancelling bulk-migration-job ($BULK_JOB_ID)..."
    gcloud dataflow jobs cancel "$BULK_JOB_ID" --project="${PROJECT_ID}" --region="${REGION}"
fi

# Cancel CDC replication job
CDC_JOB_ID=$(gcloud dataflow jobs list --project="${PROJECT_ID}" --region="${REGION}" --status=active --filter="name=cdc-replication-job" --format="value(JOB_ID)")
if [[ -n "$CDC_JOB_ID" ]]; then
    echo "Cancelling cdc-replication-job ($CDC_JOB_ID)..."
    gcloud dataflow jobs cancel "$CDC_JOB_ID" --project="${PROJECT_ID}" --region="${REGION}"
fi

echo "2. Destroying Terraform Infrastructure (Spanner Instances & GCS Bucket)..."
terraform destroy -auto-approve -no-color -var="project_id=${PROJECT_ID}"

echo "3. Removing local temp files & state..."
rm -f start_timestamp.txt export_folder.txt .demo_state.json spanner_shard_config.json

echo "========================================="
echo "Cleanup completed successfully!"
echo "========================================="
