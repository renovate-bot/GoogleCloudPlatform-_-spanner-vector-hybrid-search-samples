#!/bin/bash

# Ensure REGION is set; defaults to us-central1 if empty
REGION="${REGION:-us-central1}"

echo "Launching 8 instances of the Spanner load test in ${REGION}..."

for i in {1..80}
do
   echo "Starting job execution #$i..."
   gcloud run jobs execute spanner-loadtest --region "${REGION}" --tasks 100 &
done

# Wait for all background processes to finish (optional)
wait

echo "All jobs have been triggered."
