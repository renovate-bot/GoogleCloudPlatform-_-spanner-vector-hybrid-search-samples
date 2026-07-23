# Spanner-to-Spanner Continuous Replication Demo

This repository contains an interactive demonstration of a **Zero-Downtime Migration** and **Continuous Data Replication** across two Google Cloud Spanner databases. 

It provisions a Spanner schema (tables: `Users`, `Products`, `Orders`, `OrderItems`), uses the `Cloud_Spanner_to_GCS_Avro` and `GCS_Avro_to_Cloud_Spanner` templates to perform a historical bulk migration, and then automatically kicks off `spanner-to-sourcedb` ChangeStreams replication starting at the exact timestamp the snapshot was taken, ensuring zero data loss.

The demo Python app is a wizard that manages the underlying Terraform and Dataflow jobs while providing real-time status updates!

---

## Quickstart Guide

### Prerequisites
Before running the demo, ensure you have the following installed and authenticated:
- **Google Cloud SDK (`gcloud`)**: Authenticated with `gcloud auth application-default login` and `gcloud auth login`.
- **Terraform**: `~> 5.0`
- **Python 3.9+**

### 1. Configure the Environment
The application uses an environment file (`config.env`) to configure project, regions, and instance names.

1. Copy the template configuration file:
   ```bash
   cp config.env.example config.env
   ```
2. Open `config.env` and customize the variables as needed. See the [Configuration Parameters](#configuration-parameters) section below for details on each parameter.

### 2. Install Dependencies
It is recommended to use a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Interactive Demo
Start the interactive Spanner Replication Demo:
```bash
python3 spanner_replication_demo.py
```

The UI will display a live dashboard and prompt you to step through the following phases:
1. **Setup Infrastructure:** Applies the Terraform configuration to spin up two Spanner instances (source & target) and a GCS staging bucket.
2. **Seed Initial Data:** Inserts thousands of sample records into the source database.
3. **Bulk Export to GCS:** Captures the current timestamp, then launches a Dataflow job to copy historical data from the source to a GCS staging bucket.
4. **Bulk Import from GCS:** Launches a Dataflow job to load the exported data into the target Spanner instance.
5. **CDC Replication:** Kicks off continuous ChangeStream replication. 
6. **Simulate Live Traffic:** Generates new random transactions on the source database so you can observe the stream & replication.
7. **Validate Data Consistency:** Runs queries against both the source and target to verify record counts match perfectly.
8. **Cleanup Environment:** Tears down all created resources.

### 4. Cleanup
When finished with the demo, you can also clean up the provisioned resources with the following command:
```bash
./cleanup.sh
```

---

## Manual Execution (Alternative to TUI)

If you prefer to run the replication pipeline steps manually in your terminal instead of using the interactive TUI, you can execute the commands below.

### Setup Environment Variables
Load the configurations from `config.env` into your shell environment (as created for the TUI previous):
```bash
# Load environment configurations
set -a && source config.env && set +a
```

---

### Step-by-Step Commands

#### 1. Setup Infrastructure
Deploy the Spanner instances and GCS staging bucket using Terraform:
```bash
terraform init
terraform apply -auto-approve -var="project_id=$PROJECT_ID"
```

Once deployment completes, retrieve and export the generated GCS staging bucket name:
```bash
export TEMP_BUCKET=$(terraform output -raw dataflow_temp_bucket)
echo "Staging Bucket: $TEMP_BUCKET"
```

#### 2. Seed Initial Data
Generate users, products, and simulated orders on the Source database:
```bash
python3 bulk_load.py --project="$PROJECT_ID" --instance="$SOURCE_INSTANCE" --database="$SOURCE_DB"
```

#### 3. Bulk Export to GCS
Initiate a Dataflow job to export the tables to GCS. We capture the current time in UTC to use as our replication start timestamp, generate a random run ID, and set GCS paths:
```bash
# Capture the snapshot start timestamp (in UTC ISO 8601 format)
export START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "Start Timestamp: $START_TIMESTAMP"

export EXPORT_ID=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')
export EXPORT_FOLDER="$TEMP_BUCKET/export-$EXPORT_ID"

gcloud dataflow jobs run "spanner-export-$EXPORT_ID" \
  --gcs-location="gs://dataflow-templates-$REGION/latest/Cloud_Spanner_to_GCS_Avro" \
  --region="$REGION" \
  --parameters="instanceId=$SOURCE_INSTANCE,databaseId=$SOURCE_DB,outputDir=$EXPORT_FOLDER" \
  --format=json
```
*Wait for this Dataflow job to complete.*

Once completed, locate the timestamped subfolder created in GCS:
```bash
# Locate the generated export folder
export TRUE_EXPORT_FOLDER=$(gcloud storage ls "$EXPORT_FOLDER/" | grep -E '\/$' | head -n 1)
echo "True Export Folder: $TRUE_EXPORT_FOLDER"
```

#### 4. Bulk Import to Destination Spanner
Load the historical export into the Destination Spanner database:
```bash
export IMPORT_ID=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')

gcloud dataflow jobs run "spanner-import-$IMPORT_ID" \
  --gcs-location="gs://dataflow-templates-$REGION/latest/GCS_Avro_to_Cloud_Spanner" \
  --region="$REGION" \
  --parameters="instanceId=$DEST_INSTANCE,databaseId=$DEST_DB,inputDir=$TRUE_EXPORT_FOLDER" \
  --format=json
```
*Wait for this Dataflow job to complete.*

#### 5. CDC Replication (Streaming)
Configure destination shards and start the continuous change stream replication.

Create the shard connection configuration file:
```bash
cat <<EOF > spanner_shard_config.json
[
  {
    "projectId": "$PROJECT_ID",
    "instanceId": "$DEST_INSTANCE",
    "databaseId": "$DEST_DB"
  }
]
EOF
```

Upload the configuration file to GCS:
```bash
gcloud storage cp spanner_shard_config.json "$TEMP_BUCKET/config.json"
```

Submit the streaming replication job to Dataflow using the Flex Template:
```bash
gcloud dataflow flex-template run "cdc-replication-job" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --template-file-gcs-location="gs://dataflow-templates-$REGION/latest/flex/Spanner_to_SourceDb" \
  --parameters="changeStreamName=streamall,instanceId=$SOURCE_INSTANCE,databaseId=$SOURCE_DB,sourceType=spanner,spannerProjectId=$PROJECT_ID,metadataInstance=$DEST_INSTANCE,metadataDatabase=$DEST_DB,sourceShardsFilePath=$TEMP_BUCKET/config.json,deadLetterQueueDirectory=$TEMP_BUCKET/dlq,startTimestamp=$START_TIMESTAMP" \
  --temp-location="$TEMP_BUCKET/temp" \
  --worker-machine-type="n2-standard-4" \
  --max-workers=5 \
  --staging-location="$TEMP_BUCKET/staging" \
  --format=json
```

*Wait for this Dataflow job to startup. This can take 7-10minutes*

#### 6. Simulate Live Traffic
Generate random load on the Source Spanner instance:
```bash
python3 generate_load.py --project="$PROJECT_ID" --instance="$SOURCE_INSTANCE" --database="$SOURCE_DB" --count=100
```

#### 7. Validate Data Consistency
Compare counts and consistency between the source and destination instances to verify replication:
```bash
python3 validate_data.py --project="$PROJECT_ID" --source-instance="$SOURCE_INSTANCE" --source-database="$SOURCE_DB" --dest-instance="$DEST_INSTANCE" --dest-database="$DEST_DB"
```

#### 8. Cleanup
Tear down the running Dataflow pipelines and Cloud Spanner databases:
```bash
./cleanup.sh
```

---

## Configuration Parameters

The `config.env` file supports the following parameters:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PROJECT_ID` | The Google Cloud Project ID. If left empty, the application queries the active `gcloud` CLI profile configuration. | `""` |
| `REGION` | The Google Cloud region where the Dataflow jobs are executed and templates are sourced. | `"us-central1"` |
| `SOURCE_INSTANCE` | The ID of the source Cloud Spanner instance. *Note: If deploying via Terraform in Step 1, this must match the instance name defined in `spanner.tf`.* | `"source-instance"` |
| `SOURCE_DB` | The ID of the source Cloud Spanner database. | `"source-db"` |
| `DEST_INSTANCE` | The ID of the destination Cloud Spanner instance. *Note: If deploying via Terraform in Step 1, this must match the instance name defined in `spanner.tf`.* | `"dest-instance"` |
| `DEST_DB` | The ID of the destination Cloud Spanner database. | `"dest-db"` |
| `DATAFLOW_RUNNER` | Execution environment for the templates. Set to `remote` to run on managed Google Cloud Dataflow service, or `local` to compile templates locally and run via DirectRunner. | `"remote"` |
| `SPANNER_DISABLE_BUILTIN_METRICS` | Suppresses OpenTelemetry metrics warning/error logs inside python spanner client. | `"true"` |
