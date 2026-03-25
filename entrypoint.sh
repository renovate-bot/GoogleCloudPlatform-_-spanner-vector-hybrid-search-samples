#!/bin/bash

# Default values for environment variables
PROJECT_ID="${PROJECT_ID}"
INSTANCE_ID="${INSTANCE_ID}"
DATABASE_ID="${DATABASE_ID}"
MODE="${MODE:-noise}"
DURATION="${DURATION:-60}"
USERS="${USERS:-10000}"
PRODUCTS="${PRODUCTS:-5000}"
ORDERS="${ORDERS:-50000}"

# Execute the Python script with mapped arguments
python3 main.py \
    --project "${PROJECT_ID}" \
    --instance "${INSTANCE_ID}" \
    --database "${DATABASE_ID}" \
    --mode "${MODE}" \
    --duration "${DURATION}" \
    --users "${USERS}" \
    --products "${PRODUCTS}" \
    --orders "${ORDERS}" \
    ${ORDER_ID:+--order-id "$ORDER_ID"}
