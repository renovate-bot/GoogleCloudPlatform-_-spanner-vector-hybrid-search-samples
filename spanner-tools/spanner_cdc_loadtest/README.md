# Cloud Spanner CDC Load Test

A Java-based load generator for Cloud Spanner, designed to test Change Data Capture (CDC) and general database performance. It supports various strategies to simulate different types of load.

## Strategies

| Strategy | Description |
| :--- | :--- |
| **RANDOM** | Standard Insert/Update/Delete on random IDs. |
| **SEQUENTIAL** | Strict sequence of Insert -> 10 Updates -> Delete on a unique ID. High churn per ID. |
| **HOTSPOT** | High contention on a single shared row (Read-Write Transaction). |
| **ATOMICITY** | Simulates money transfer between two shared rows (Read-Write Transaction). |
| **SATURATION** | Large batch inserts (~1MB per op) to saturate network/CPU. |
| **INTEGRITY** | Rapid Insert/Delete cycles on a fixed pool of keys (Resurrection testing). |
| **READ_HEAVY** | 90% Read (point lookup), 10% Insert. Maintains local cache of 10k recent keys. |
| **MIXED** | Cycles through all the above strategies across workers. |

## Local Usage

### Prerequisites
- Java 17+
- Maven 3.8+
- Google Cloud SDK (`gcloud`)

### Build
```bash
mvn clean package -DskipTests
```

### Command-Line Options

| Option | Long Option | Required | Default | Description |
| :--- | :--- | :---: | :--- | :--- |
| `-p` | `--project` | **Yes** | — | Google Cloud Project ID |
| `-i` | `--instance` | **Yes** | — | Cloud Spanner Instance ID |
| `-d` | `--database` | **Yes** | — | Cloud Spanner Database ID |
| `-c` | `--concurrency` | No | `10` | Number of concurrent worker threads |
| `-s` | `--strategy` | No | `RANDOM` | Load strategy (`RANDOM`, `SEQUENTIAL`, `HOTSPOT`, `ATOMICITY`, `SATURATION`, `INTEGRITY`, `READ_HEAVY`, `MIXED`). See [Strategies](#strategies). |
| | `--duration` | No | `60` | Duration of test in seconds |
| | `--create-schema` | No | `false` | Automatically creates target table (`LoadTestTable`) if it does not exist |
| `-h` | `--help` | No | — | Show help message and exit |
| `-V` | `--version` | No | — | Print version information and exit |

### Run Locally

Set your environment variables:
```bash
export PROJECT_ID="your-project-id"
export SPANNER_INSTANCE="your-instance-id"
export SPANNER_DATABASE="your-database-id"
```

**Run with custom parameters:**
```bash
java -jar target/spanner-cdc-loadtest-1.0-SNAPSHOT.jar \
  -p "${PROJECT_ID}" \
  -i "${SPANNER_INSTANCE}" \
  -d "${SPANNER_DATABASE}" \
  -c 10 \
  -s MIXED \
  --duration 60 \
  --create-schema
```

**Run with defaults (10 threads, RANDOM strategy, 60s duration):**
```bash
java -jar target/spanner-cdc-loadtest-1.0-SNAPSHOT.jar \
  -p "${PROJECT_ID}" \
  -i "${SPANNER_INSTANCE}" \
  -d "${SPANNER_DATABASE}"
```
*Note: Schema creation is disabled by default. Pass `--create-schema` on initial run to automatically create the table schema.*

---

## Cloud Run Jobs Deployment

Run massive load tests serverlessly using Cloud Run Jobs.

### 1. Variables
Set your environment variables:
```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
# Use Artifact Registry (pkg.dev), NOT gcr.io
export REPO_NAME="spanner-loadtest-repo"
export IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/spanner-loadtest:latest"
export SPANNER_INSTANCE="your-instance-id"
export SPANNER_DATABASE="your-database-id"
```

### 2. Initial Setup (One-time)
Create the Artifact Registry repository:
```bash
gcloud artifacts repositories create ${REPO_NAME} \
    --repository-format=docker \
    --location=${REGION} \
    --description="Docker repository for Spanner Load Test"
```

### 3. Build and Push
Build the container image using Cloud Build:
```bash
gcloud builds submit --tag ${IMAGE_NAME}
```

### 4. Create the Job
Create the Cloud Run Job. Note the `--args` syntax requires a single comma-separated string.

```bash
gcloud run jobs create spanner-loadtest \
  --image ${IMAGE_NAME} \
  --region ${REGION} \
  --cpu=4 \
  --memory=8Gi \
  --task-timeout=1h \
  --args="-p,${PROJECT_ID},-i,${SPANNER_INSTANCE},-d,${SPANNER_DATABASE},-c,5,-s,MIXED,--duration,60"
```
*Note: Ensure `--task-timeout` is greater than your load test `--duration`.*

### 5. Execute the Load Test
Run the job. You can override arguments (e.g., to increase concurrency or change strategy) using the `--args` flag.

**Example: Run with 200 Total Threads**
(50 threads per task * 4 tasks)

```bash
gcloud run jobs execute spanner-loadtest \
  --region ${REGION} \
  --tasks 4 \
  --task-timeout=30m \
  --args="-p,${PROJECT_ID},-i,${SPANNER_INSTANCE},-d,${SPANNER_DATABASE},-c,50,-s,HOTSPOT,--duration,300"
```

### 6. Update Configuration (Resources)
To change the CPU or Memory limits for an existing job:
```bash
gcloud run jobs update spanner-loadtest \
  --cpu=8 \
  --memory=16Gi \
  --region=${REGION}
```
**Important:** These resources (8 CPU, 16Gi Memory) are allocated **PER TASK**.
If you execute this job with `--tasks 10`, you will provision **10 separate containers**, each with 8 vCPUs (Total: 80 vCPUs).
Rates are billed per vCPU-second and GB-second for each task.

### Troubleshooting
- **Error: `None of [grpclb] specified`**: The shaded JAR is missing `ServicesResourceTransformer`. Ensure it's in `pom.xml` and rebuild.
- **Error: `denied: gcr.io repo does not exist`**: Use Artifact Registry (`pkg.dev`) as shown above.


---

## End-to-End Quickstart

### Variables
```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export REPO_NAME="spanner-loadtest-repo"
export IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/spanner-loadtest:latest"

export SPANNER_INSTANCE="your-instance-id"
export SPANNER_DATABASE="your-database-id"
```

### Initial setup

```bash
gcloud artifacts repositories create ${REPO_NAME} \
    --repository-format=docker \
    --location=${REGION} \
    --description="Docker repository for Spanner Load Test"

gcloud builds submit --tag ${IMAGE_NAME}

gcloud run jobs create spanner-loadtest \
  --image ${IMAGE_NAME} \
  --region ${REGION} \
  --cpu=4 \
  --memory=8Gi \
  --task-timeout=1h \
  --args="-p,${PROJECT_ID},-i,${SPANNER_INSTANCE},-d,${SPANNER_DATABASE},-c,4,-s,MIXED,--duration,600"

gcloud run jobs execute spanner-loadtest --region ${REGION} --tasks 50
```

### Rebuild after changes
```bash
gcloud builds submit --tag ${IMAGE_NAME}
gcloud run jobs execute spanner-loadtest --region ${REGION} --tasks 2
```