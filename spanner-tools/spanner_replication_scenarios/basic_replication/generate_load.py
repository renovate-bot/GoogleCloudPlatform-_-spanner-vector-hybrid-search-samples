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

import argparse
import random
import uuid
import time
from google.cloud import spanner

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--count", type=int, default=100, help="Number of business events to generate")
    args = parser.parse_args()

    client = spanner.Client(project=args.project)
    db = client.instance(args.instance).database(args.database)

    print(f"Starting load generation on {args.instance}/{args.database} (inserting {args.count} records)...")

    try:
        for i in range(args.count):
            uid = str(uuid.uuid4())
            pid = str(uuid.uuid4())
            order_id = str(uuid.uuid4())
            
            with db.batch() as batch:
                batch.insert(
                    table="Users",
                    columns=("UserId", "FullName", "Email", "CreatedAt"),
                    values=[(uid, f"LiveUser {uid[:8]}", f"live_{uid[:8]}@example.com", spanner.COMMIT_TIMESTAMP)]
                )
                batch.insert(
                    table="Products",
                    columns=("ProductId", "Name", "Description", "Price", "InventoryCount"),
                    values=[(pid, f"LiveProduct {pid[:8]}", "Live Desc", 42.0, 100)]
                )
                batch.insert(
                    table="Orders",
                    columns=("OrderId", "UserId", "OrderDate", "TotalAmount", "Status"),
                    values=[(order_id, uid, spanner.COMMIT_TIMESTAMP, 42.0, "PENDING")]
                )
                batch.insert(
                    table="OrderItems",
                    columns=("OrderId", "ProductId", "Quantity", "UnitPrice"),
                    values=[(order_id, pid, 1, 42.0)]
                )
            
            print(f"[{i+1}/{args.count}] Inserted User, Product, Order, and OrderItem (OrderId: {order_id})")
            
    except KeyboardInterrupt:
        print("\nStopped load generation early.")
        
    print(f"\nFinished inserting {args.count} business events.")

if __name__ == "__main__":
    main()
