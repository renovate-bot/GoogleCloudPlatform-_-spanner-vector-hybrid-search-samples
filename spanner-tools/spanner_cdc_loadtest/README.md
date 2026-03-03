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

### Run Locally
```bash
java -jar target/spanner-cdc-loadtest-1.0-SNAPSHOT.jar \
  -p <PROJECT_ID> \
  -i <INSTANCE_ID> \
  -d <DATABASE_ID> \
  -c 10 \
  -s MIXED \
  -c 10 \
  -s MIXED \
  --duration 60 \
  --create-schema
```
*Note: Schema creation is disabled by default. Use `--create-schema` to enable it.*

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
  --region=${REGION}
```
**Important:** These resources (8 CPU, 16Gi Memory) are allocated **PER TASK**.
If you execute this job with `--tasks 10`, you will provision **10 separate containers**, each with 8 vCPUs (Total: 80 vCPUs).
Rates are billed per vCPU-second and GB-second for each task.

### Troubleshooting
- **Error: `None of [grpclb] specified`**: The shaded JAR is missing `ServicesResourceTransformer`. Ensure it's in `pom.xml` and rebuild.
- **Error: `denied: gcr.io repo does not exist`**: Use Artifact Registry (`pkg.dev`) as shown above.


# End-to-End

## Variables
```
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export IMAGE_NAME="gcr.io/${PROJECT_ID}/spanner-loadtest:latest"

export SPANNER_INSTANCE="your-instance-id"
export SPANNER_DATABASE="your-database-id"
```

## Initial setup

```
gcloud artifacts repositories create spanner-loadtest-repo \
    --repository-format=docker \
    --location=${REGION} \
    --description="Docker repository for Spanner Load Test"

export IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/spanner-loadtest-repo/spanner-loadtest:latest"

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

## Rebuild after changes
```
gcloud builds submit --tag ${IMAGE_NAME}
gcloud run jobs execute spanner-loadtest --region ${REGION} --tasks 2
```