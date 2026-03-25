import os
# Silence noisy OpenTelemetry/Spanner metrics BEFORE any other imports
os.environ["GOOGLE_CLOUD_SPANNER_OMIT_RPC_METRICS"] = "True"

import argparse
import logging
import sys
from google.cloud import spanner
from seeder import run_seeder
from noise import run_noise
from hotspot import run_hotspot
from lock_simulator import run_lock_test

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
# Suppress noisy OpenTelemetry and Spanner metrics errors
logging.getLogger("opentelemetry").setLevel(logging.CRITICAL)
logging.getLogger("google.cloud.spanner_v1.metrics").setLevel(logging.CRITICAL)
logging.getLogger("google.cloud.monitoring_v3").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Spanner Noise Maker - Load Test Tool")
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--instance", required=True, help="Spanner Instance ID")
    parser.add_argument("--database", required=True, help="Spanner Database ID")
    parser.add_argument("--mode", choices=["seed", "noise", "hotspot", "lock"], required=True, help="Execution mode")
    parser.add_argument("--order-id", help="Order ID to use for lock simulation (required for mode=lock)")
    
    # Seeding parameters
    parser.add_argument("--users", type=int, default=10000, help="Number of users to seed")
    parser.add_argument("--products", type=int, default=5000, help="Number of products to seed")
    parser.add_argument("--orders", type=int, default=50000, help="Number of orders to seed")
    parser.add_argument("--threads", type=int, default=10, help="Number of worker threads for parallel seeding")
    
    # Noise parameters
    parser.add_argument("--duration", type=int, default=60, help="Duration of noise generation in seconds")
    
    args = parser.parse_args()

    client = spanner.Client(project=args.project)
    instance = client.instance(args.instance)
    database = instance.database(args.database)

    if args.mode == "seed":
        run_seeder(database, args.users, args.products, args.orders, args.threads)
    elif args.mode == "noise":
        run_noise(database, args.duration)
    elif args.mode == "hotspot":
        run_hotspot(database, args.duration)
    elif args.mode == "lock":
        if not args.order_id:
            logger.error("--order-id is required when mode is 'lock'")
            sys.exit(1)
        run_lock_test(database, args.order_id, args.duration)

if __name__ == "__main__":
    main()
