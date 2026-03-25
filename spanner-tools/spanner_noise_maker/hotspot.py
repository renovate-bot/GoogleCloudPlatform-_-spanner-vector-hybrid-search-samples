import logging
import random
import time
import uuid
from google.cloud import spanner
from data_generator import generate_order

logger = logging.getLogger(__name__)

def run_hotspot(database, duration_seconds=60):
    logger.info(f"Starting moving hotspot load (Orders only) for {duration_seconds} seconds...")
    end_time = time.time() + duration_seconds
    
    # Use separate snapshots for each ID fetch to avoid "single-use snapshot" reuse error
    with database.snapshot() as snapshot:
        user_ids = [row[0] for row in snapshot.execute_sql("SELECT UserId FROM Users LIMIT 1000")]
    
    with database.snapshot() as snapshot:
        product_ids = [row[0] for row in snapshot.execute_sql("SELECT ProductId FROM Products LIMIT 1000")]

    if not user_ids or not product_ids:
        logger.error("No users or products found in database. Cannot create orders. Please seed data first.")
        return

    logger.info(f"Found {len(user_ids)} users and {len(product_ids)} products to use for orders.")

    while time.time() < end_time:
        try:
            buyer_id = random.choice(user_ids)
            product_id = random.choice(product_ids)
            
            # Generate order data
            o = generate_order(buyer_id, product_id)
            
            # Overwrite OrderId with sequential timestamp + random suffix to ensure hotspot
            # Microsecond timestamp (16 digits)
            timestamp_ms = int(time.time() * 1000000)
            short_uuid = uuid.uuid4().hex[:6]
            order_id = f"{timestamp_ms}_{short_uuid}"
            
            o["OrderId"] = order_id[:36] # Ensure it fits in STRING(36)
            
            database.run_in_transaction(lambda t: t.execute_update(
                "INSERT INTO Orders (OrderId, BuyerId, ProductId, OrderTime, Amount) VALUES (@id, @buyer, @product, @time, @amount)",
                params={"id": o["OrderId"], "buyer": o["BuyerId"], "product": o["ProductId"], "time": o["OrderTime"], "amount": o["Amount"]},
                param_types={"id": spanner.param_types.STRING, "buyer": spanner.param_types.STRING, "product": spanner.param_types.STRING, "time": spanner.param_types.TIMESTAMP, "amount": spanner.param_types.INT64}
            ))
            
        except Exception as e:
            logger.error(f"Error during hotspot generation: {e}")
            # Avoid tight loop on repeated failure
            time.sleep(1)

    logger.info("Moving hotspot load complete.")
