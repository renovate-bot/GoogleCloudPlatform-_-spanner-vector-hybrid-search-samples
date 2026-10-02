#!/bin/bash
# Copyright 2026 Google LLC
# DBExplorer GCE Regional Benchmark Client Runner Startup Script

set -e
exec > >(tee -a /var/log/dbexplorer-benchmark.log /dev/console) 2>&1

echo "===================================================================="
echo "  DBExplorer Regional GCE Client Benchmark Runner"
echo "  Timestamp: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "  Hostname:  $(hostname)"
echo "===================================================================="

# 1. Fetch parameters from GCE Instance Metadata
META_URL="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
PROJECT_ID=$(curl -s -f -H "Metadata-Flavor: Google" "${META_URL}/spanner_project_id" || echo "")
INSTANCE_ID=$(curl -s -f -H "Metadata-Flavor: Google" "${META_URL}/spanner_instance_id" || echo "")
DATABASE_ID=$(curl -s -f -H "Metadata-Flavor: Google" "${META_URL}/spanner_database_id" || echo "")
LEADER_REGION=$(curl -s -f -H "Metadata-Flavor: Google" "${META_URL}/spanner_leader_region" || echo "")
OPERATIONS=$(curl -s -f -H "Metadata-Flavor: Google" "${META_URL}/benchmark_operations" || echo "1000")
STALENESS=$(curl -s -f -H "Metadata-Flavor: Google" "${META_URL}/benchmark_staleness" || echo "15")
JAR_GCS_PATH=$(curl -s -f -H "Metadata-Flavor: Google" "${META_URL}/jar_gcs_path" || echo "")

echo "Configured Parameters:"
echo "  Project ID:    ${PROJECT_ID}"
echo "  Instance ID:   ${INSTANCE_ID}"
echo "  Database ID:   ${DATABASE_ID}"
echo "  Leader Region: ${LEADER_REGION}"
echo "  Operations:    ${OPERATIONS}"
echo "  Staleness:     ${STALENESS}s"
echo "  JAR GCS Path:  ${JAR_GCS_PATH}"

report_error() {
    local err_msg="$1"
    echo "ERROR: ${err_msg}"
    echo "DBEXPLORER_BENCHMARK_ERROR: ${err_msg}"
    curl -s -X PUT -H "Metadata-Flavor: Google" \
        --data "{\"error\": \"${err_msg}\"}" \
        "http://metadata.google.internal/computeMetadata/v1/instance/guest-attributes/dbexplorer/results" || true
    exit 1
}

# Trap unexpected errors and report immediately to Guest Attributes and console
trap 'report_error "Startup script failed unexpectedly at line ${LINENO} with exit code $?"' ERR

if [ -z "${PROJECT_ID}" ] || [ -z "${INSTANCE_ID}" ] || [ -z "${DATABASE_ID}" ]; then
    report_error "Missing required Spanner instance/database metadata."
fi

# 2. Neutralize background OSConfigAgent and package managers that cause lock collisions
echo "Disabling background OS policies and package management locks..."
systemctl stop google-osconfig-agent 2>/dev/null || true
systemctl mask google-osconfig-agent 2>/dev/null || true
systemctl stop unattended-upgrades 2>/dev/null || true
systemctl mask unattended-upgrades 2>/dev/null || true
systemctl stop apt-daily.service apt-daily.timer apt-daily-upgrade.service apt-daily-upgrade.timer 2>/dev/null || true
systemctl mask apt-daily.service apt-daily.timer apt-daily-upgrade.service apt-daily-upgrade.timer 2>/dev/null || true

# Terminate any background apt/dpkg processes spawned at boot
pkill -9 -f 'apt-get|apt|dpkg|unattended-upgrade|osconfig' 2>/dev/null || true
sleep 1

# Clean up stale locks and repair dpkg state
rm -f /var/lib/apt/lists/lock /var/lib/dpkg/lock /var/lib/dpkg/lock-frontend /var/cache/apt/archives/lock 2>/dev/null || true
rm -rf /var/lib/apt/lists/partial/* 2>/dev/null || true
dpkg --configure -a --force-confdef --force-confold 2>/dev/null || true

# 3. Ensure JRE (Java 17+) is installed with bounded lock detection and retries
wait_for_apt_lock() {
    local timeout=30
    local elapsed=0
    echo "Checking for background package management locks..."
    while true; do
        local locked=0
        if command -v fuser &>/dev/null; then
            if fuser /var/lib/apt/lists/lock /var/lib/dpkg/lock /var/lib/dpkg/lock-frontend &>/dev/null; then
                locked=1
            fi
        elif command -v lsof &>/dev/null; then
            if lsof /var/lib/apt/lists/lock /var/lib/dpkg/lock /var/lib/dpkg/lock-frontend &>/dev/null; then
                locked=1
            fi
        fi
        if [ $locked -eq 0 ]; then
            break
        fi
        echo "Waiting for background apt/dpkg lock to release (${elapsed}s elapsed)..."
        sleep 3
        elapsed=$((elapsed + 3))
        if [ $elapsed -ge $timeout ]; then
            echo "Warning: Background lock held for ${timeout}s. Terminating conflicting processes to proceed..."
            pkill -9 -f 'apt-get|apt|dpkg|unattended-upgrade|osconfig' 2>/dev/null || true
            sleep 1
            rm -f /var/lib/apt/lists/lock /var/lib/dpkg/lock /var/lib/dpkg/lock-frontend /var/cache/apt/archives/lock 2>/dev/null || true
            rm -rf /var/lib/apt/lists/partial/* 2>/dev/null || true
            dpkg --configure -a --force-confdef --force-confold 2>/dev/null || true
            break
        fi
    done
}

install_java_with_retry() {
    if command -v java &> /dev/null; then
        echo "Java runtime already present."
        return 0
    fi

    echo "Java not found. Installing openjdk-17-jre-headless via apt-get..."
    export DEBIAN_FRONTEND=noninteractive

    local max_attempts=5
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        wait_for_apt_lock
        echo "Attempt $attempt of $max_attempts: updating apt caches and installing Java..."
        if apt-get update -y -q -o DPkg::Lock::Timeout=60 && \
           (apt-get install -y -q -o DPkg::Lock::Timeout=60 openjdk-17-jre-headless curl || \
            apt-get install -y -q -o DPkg::Lock::Timeout=60 default-jre-headless curl); then
            echo "Java successfully installed."
            return 0
        fi
        echo "apt-get encountered an issue (attempt $attempt/$max_attempts). Retrying in 5 seconds..."
        sleep 5
        attempt=$((attempt + 1))
    done

    report_error "Failed to install Java runtime after $max_attempts attempts."
}

install_java_with_retry

JAVA_VERSION=$(java -version 2>&1 | head -n 1)
echo "Java runtime verified: ${JAVA_VERSION}"

# 3. Download Benchmark Shaded JAR from Cloud Storage
RUNNER_JAR="/tmp/spanner-benchmark-runner.jar"
echo "Fetching shaded benchmark JAR from ${JAR_GCS_PATH}..."

TOKEN=$(curl -s -f -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
    | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)

if [ -z "${TOKEN}" ]; then
    report_error "Failed to acquire GCP service account token from instance metadata."
fi

# Parse bucket and object path from gs://bucket/path/to/file.jar
BUCKET_NAME=$(echo "${JAR_GCS_PATH}" | sed -e 's|^gs://||' -e 's|/.*||')
OBJECT_PATH=$(echo "${JAR_GCS_PATH}" | sed -e "s|^gs://${BUCKET_NAME}/||")
ENCODED_OBJECT=$(echo "${OBJECT_PATH}" | sed -e 's|/|%2F|g')

HTTP_CODE=$(curl -s -w "%{http_code}" -H "Authorization: Bearer ${TOKEN}" \
    "https://storage.googleapis.com/storage/v1/b/${BUCKET_NAME}/o/${ENCODED_OBJECT}?alt=media" \
    -o "${RUNNER_JAR}")

if [ "${HTTP_CODE}" -ne 200 ] || [ ! -s "${RUNNER_JAR}" ]; then
    report_error "Failed to download JAR from ${JAR_GCS_PATH} (HTTP ${HTTP_CODE})."
fi

JAR_SIZE=$(wc -c < "${RUNNER_JAR}")
echo "Runner JAR successfully staged: ${JAR_SIZE} bytes."

# 4. Execute Latency Workload
RESULTS_FILE="/tmp/benchmark_results.json"
rm -f "${RESULTS_FILE}"

# Explicitly enable DirectPath / Direct Access in Google Cloud Java Spanner SDK
export GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS=true
export GOOGLE_CLOUD_ENABLE_DIRECT_PATH=true

echo "Executing DirectAccessBenchmark workload (Direct Access enabled)..."
set +e
java -jar "${RUNNER_JAR}" \
    --projectId="${PROJECT_ID}" \
    --instanceId="${INSTANCE_ID}" \
    --databaseId="${DATABASE_ID}" \
    --operations="${OPERATIONS}" \
    --stalenessSeconds="${STALENESS}" \
    --leaderRegion="${LEADER_REGION}" \
    --outputFile="${RESULTS_FILE}"
JAVA_EXIT_CODE=$?
set -e

echo "Workload process exited with code: ${JAVA_EXIT_CODE}"

# 5. Emit Results via Guest Attributes and Serial Console
if [ ${JAVA_EXIT_CODE} -eq 0 ] && [ -s "${RESULTS_FILE}" ]; then
    echo "Workload succeeded. Publishing results to GCE Guest Attributes..."
    curl -s -X PUT -H "Metadata-Flavor: Google" \
        --data-binary @"${RESULTS_FILE}" \
        "http://metadata.google.internal/computeMetadata/v1/instance/guest-attributes/dbexplorer/results"
    
    echo "DBEXPLORER_BENCHMARK_OUTPUT_BEGIN"
    cat "${RESULTS_FILE}"
    echo ""
    echo "DBEXPLORER_BENCHMARK_OUTPUT_END"
    echo "Runner completed successfully."
else
    report_error "DirectAccessBenchmark failed with exit code ${JAVA_EXIT_CODE}."
fi
