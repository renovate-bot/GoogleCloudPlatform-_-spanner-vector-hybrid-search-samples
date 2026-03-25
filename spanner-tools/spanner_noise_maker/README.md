# Spanner Noise Maker

A Python-based load generator for Cloud Spanner, designed to run as Cloud Run Tasks.

## Features
- **Seed Mode**: Generates a configurable amount of initial data (Users, Products, Orders).
- **Noise Mode**: Generates continuous random load including inserts, updates, and 5 diverse query shapes.
- **Hotspot Mode**: Generates a moving hotspot by inserting only new orders with sequential (timestamp-based) keys.
- **Lock Mode**: Simulates lock contention by executing transactions that hold exclusive locks for an extended period (2s).
- **Serverless Scaling**: Built for Cloud Run Tasks to scale load generation horizontally.

## Local Usage

### Prerequisites
- Python 3.11+
- Google Cloud Spanner instance and database

### Setup
```bash
pip install -r requirements.txt
```

### Run Seeding
```bash
python3 main.py --project <PROJECT_ID> --instance <INSTANCE_ID> --database <DATABASE_ID> \
    --mode seed --users 10000 --products 5000 --orders 50000
```

### Run Noise Load
```bash
python3 main.py --project <PROJECT_ID> --instance <INSTANCE_ID> --database <DATABASE_ID> \
    --mode noise --duration 300
```

### Run Hotspot Load
```bash
python3 main.py --project <PROJECT_ID> --instance <INSTANCE_ID> --database <DATABASE_ID> \
    --mode hotspot --duration 300
```

### Run Lock Load
```bash
python3 main.py --project <PROJECT_ID> --instance <INSTANCE_ID> --database <DATABASE_ID> \
    --mode lock --order-id 123 --duration 300
```

## Cloud Run Tasks Deployment

### 0. Prerequists

* Spanner instance & database needs to be created
* Schema needs to be created in the database
  
### 1. Variables
```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export REPO_NAME="spanner-noise-repo"
export IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/spanner-noise-maker:latest"
export SPANNER_INSTANCE="your-instance-id"
export SPANNER_DATABASE="your-database-id"
```

### 2. Initial Setup
```bash
gcloud artifacts repositories create ${REPO_NAME} \
    --repository-format=docker \
    --location=${REGION} \
    --description="Docker repository for Spanner Noise Maker"
```

### 3. Build and Push
```bash
gcloud builds submit --tag ${IMAGE_NAME}
```

### 4. Create the Cloud Run Job
```bash
gcloud run jobs create spanner-noise-job \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --cpu=2 \
    --memory=4Gi \
    --task-timeout=1h \
    --set-env-vars="PROJECT_ID=${PROJECT_ID},INSTANCE_ID=${SPANNER_INSTANCE},DATABASE_ID=${SPANNER_DATABASE}"
```

### 5.1 Execute the Seed
```bash
# Run with 1 concurrent tasks
gcloud run jobs execute spanner-noise-job \
    --region ${REGION} \
    --tasks 1 \
    --update-env-vars="MODE=seed,USERS=20000,PRODUCTS=10000,ORDERS=100000"
```

### 5.2 Execute the Noise Test
```bash
# Run with 10 concurrent tasks
gcloud run jobs execute spanner-noise-job \
    --region ${REGION} \
    --tasks 10 \
    --update-env-vars="MODE=noise,DURATION=600"
```

### 5.3 Execute the Hotspotting Test
```bash
# Run with 100 concurrent tasks so its high enough to induce actual moving hotspots
gcloud run jobs execute spanner-noise-job \
    --region ${REGION} \
    --tasks 100 \
    --update-env-vars="MODE=hotspot,DURATION=600"
```

### 5.4 Execute the Lock Simulation Test
```bash
# Run with multiple concurrent tasks using the SAME order ID to create a lock storm
gcloud run jobs execute spanner-noise-job \
    --region ${REGION} \
    --tasks 10 \
    --update-env-vars="MODE=lock,ORDER_ID=123,DURATION=600"
```

## Schema
The tool expects the following schema:
```sql
CREATE TABLE Users (
  UserId STRING(36) NOT NULL,
  Username STRING(100),
  JoinedAt TIMESTAMP,
) PRIMARY KEY (UserId);

CREATE TABLE Products (
  ProductId STRING(36) NOT NULL,
  OwnerId STRING(36),
  Price INT64,
  Category STRING(50),
) PRIMARY KEY (ProductId);

CREATE TABLE Orders (
  OrderId STRING(36) NOT NULL,
  BuyerId STRING(36) NOT NULL,
  ProductId STRING(36) NOT NULL,
  OrderTime TIMESTAMP NOT NULL,
  Amount INT64,
) PRIMARY KEY (OrderId);
```
