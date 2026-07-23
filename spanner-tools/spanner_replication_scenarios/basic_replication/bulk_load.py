import argparse
import random
import uuid
import datetime
import concurrent.futures
from google.cloud import spanner

def worker(database, batch_size):
    users = []
    products = []
    orders = []
    order_items = []
    
    # Generate some users and products first to use in orders
    user_ids = [str(uuid.uuid4()) for _ in range(batch_size)]
    product_ids = [str(uuid.uuid4()) for _ in range(batch_size)]

    for uid in user_ids:
        users.append((uid, f"User {uid[:8]}", f"{uid[:8]}@example.com", spanner.COMMIT_TIMESTAMP))
    
    for pid in product_ids:
        products.append((pid, f"Product {pid[:8]}", "Description", round(random.uniform(10.0, 500.0), 2), random.randint(0, 1000)))

    # Generate orders for users
    for uid in user_ids:
        if random.random() > 0.5: # 50% chance a user has an order
            order_id = str(uuid.uuid4())
            total = 0.0
            
            # Generate items for the order
            num_items = random.randint(1, 5)
            for _ in range(num_items):
                pid = random.choice(product_ids)
                qty = random.randint(1, 3)
                price = round(random.uniform(10.0, 500.0), 2)
                order_items.append((order_id, pid, qty, price))
                total += qty * price
                
            orders.append((order_id, uid, spanner.COMMIT_TIMESTAMP, total, "COMPLETED"))

    with database.batch() as batch:
        batch.insert_or_update(
            table="Users",
            columns=("UserId", "FullName", "Email", "CreatedAt"),
            values=users
        )
        batch.insert_or_update(
            table="Products",
            columns=("ProductId", "Name", "Description", "Price", "InventoryCount"),
            values=products
        )
        if orders:
            batch.insert_or_update(
                table="Orders",
                columns=("OrderId", "UserId", "OrderDate", "TotalAmount", "Status"),
                values=orders
            )
        if order_items:
            batch.insert_or_update(
                table="OrderItems",
                columns=("OrderId", "ProductId", "Quantity", "UnitPrice"),
                values=order_items
            )
    return len(users)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--batches", type=int, default=50, help="Number of batches to run")
    parser.add_argument("--batch_size", type=int, default=200, help="Number of users/products per batch")
    args = parser.parse_args()

    client = spanner.Client(project=args.project)
    db = client.instance(args.instance).database(args.database)

    print(f"Starting bulk load of {args.batches * args.batch_size} entities...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(worker, db, args.batch_size) for _ in range(args.batches)]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Batch failed: {e}")
    print("Bulk load completed.")

if __name__ == "__main__":
    main()
