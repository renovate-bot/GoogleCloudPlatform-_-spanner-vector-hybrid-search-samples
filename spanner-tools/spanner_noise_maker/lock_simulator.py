import logging
import random
import time
import uuid
from google.cloud import spanner
from data_generator import generate_order

logger = logging.getLogger(__name__)

def run_lock_test(database, order_id, duration_seconds=60):
    logger.info(f"Starting lock simulation load (Order {order_id}) for {duration_seconds} seconds...")
    end_time = time.time() + duration_seconds
    
    # Use separate snapshots for each ID fetch to avoid "single-use snapshot" reuse error
    with database.snapshot() as snapshot:
        user_ids = [row[0] for row in snapshot.execute_sql("SELECT UserId FROM Users LIMIT 1000")]
    
    with database.snapshot() as snapshot:
        product_ids = [row[0] for row in snapshot.execute_sql("SELECT ProductId FROM Products LIMIT 1000")]

    if not user_ids or not product_ids:
        logger.error("No users or products found in database. Cannot create order fallback. Please seed data first.")
        return

    logger.info(f"Found {len(user_ids)} users and {len(product_ids)} products to use for order fallback.")

    while time.time() < end_time:
        try:
            buyer_id = random.choice(user_ids)
            product_id = random.choice(product_ids)
            
            o = generate_order(buyer_id, product_id)
            o["OrderId"] = order_id # Use the specified order ID
            o["Amount"] = random.randint(1, 10000) # Ensure it's a changing random value
            
            def lock_txn(transaction):
                # SELECT before write to acquire shared lock
                results = transaction.execute_sql(
                    "SELECT OrderId FROM Orders WHERE OrderId = @id",
                    params={"id": o["OrderId"]},
                    param_types={"id": spanner.param_types.STRING}
                )
                list(results) # Consume to ensure shared lock is acquired
                
                # Wait 2 seconds before write
                logger.debug(f"Waiting 2 seconds before write for Order {order_id}...")
                time.sleep(2)
                
                # Native Spanner GoogleSQL INSERT OR UPDATE
                transaction.execute_update(
                    "INSERT OR UPDATE INTO Orders (OrderId, BuyerId, ProductId, OrderTime, Amount) VALUES (@id, @buyer, @product, @time, @amount)",
                    params={"id": o["OrderId"], "buyer": o["BuyerId"], "product": o["ProductId"], "time": o["OrderTime"], "amount": o["Amount"]},
                    param_types={"id": spanner.param_types.STRING, "buyer": spanner.param_types.STRING, "product": spanner.param_types.STRING, "time": spanner.param_types.TIMESTAMP, "amount": spanner.param_types.INT64}
                )
            
            database.run_in_transaction(lock_txn)
            
        except Exception as e:
            logger.error(f"Error during lock simulation: {e}")
            # Avoid tight loop on repeated failure
            time.sleep(1)

    logger.info("Lock simulation complete.")
