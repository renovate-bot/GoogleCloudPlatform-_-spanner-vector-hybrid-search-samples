import logging
import random
import concurrent.futures
from google.cloud import spanner
from data_generator import generate_user, generate_product, generate_order

logger = logging.getLogger(__name__)

def seed_users(database, count, batch_size=10000, threads=10):
    logger.info(f"Seeding {count} users using Batch SQL with {threads} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        for i in range(0, count, batch_size):
            current_batch = min(batch_size, count - i)
            users = [generate_user() for _ in range(current_batch)]
            
            def insert_users(transaction, user_list):
                statements = []
                for u in user_list:
                    statements.append((
                        "INSERT INTO Users (UserId, Username, JoinedAt) VALUES (@id, @name, @joined)",
                        {"id": u["UserId"], "name": u["Username"], "joined": u["JoinedAt"]},
                        {"id": spanner.param_types.STRING, "name": spanner.param_types.STRING, "joined": spanner.param_types.TIMESTAMP}
                    ))
                transaction.batch_update(statements)
            
            def worker(u_list, b_idx):
                import threading
                database.run_in_transaction(insert_users, u_list)
                logger.info(f"[{threading.current_thread().name}] Users batch {b_idx} committed")
            
            batch_idx = i // batch_size + 1
            futures.append(executor.submit(worker, users, batch_idx))
        
        completed = 0
        total = len(futures)
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Raise exception if thread failed
            completed += 1
            logger.info(f"Users seeding progress: {completed}/{total} total batches completed")
    logger.info(f"Completed seeding {count} users")

def seed_products(database, count, user_ids, batch_size=10000, threads=10):
    logger.info(f"Seeding {count} products using Batch SQL with {threads} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        for i in range(0, count, batch_size):
            current_batch = min(batch_size, count - i)
            products = [generate_product(owner_id=random.choice(user_ids) if user_ids else None) for _ in range(current_batch)]
            
            def insert_products(transaction, product_list):
                statements = []
                for p in product_list:
                    statements.append((
                        "INSERT INTO Products (ProductId, OwnerId, Price, Category) VALUES (@id, @owner, @price, @cat)",
                        {"id": p["ProductId"], "owner": p["OwnerId"], "price": p["Price"], "cat": p["Category"]},
                        {"id": spanner.param_types.STRING, "owner": spanner.param_types.STRING, "price": spanner.param_types.INT64, "cat": spanner.param_types.STRING}
                    ))
                transaction.batch_update(statements)
            
            def worker(p_list, b_idx):
                import threading
                database.run_in_transaction(insert_products, p_list)
                logger.info(f"[{threading.current_thread().name}] Products batch {b_idx} committed")
            
            batch_idx = i // batch_size + 1
            futures.append(executor.submit(worker, products, batch_idx))
        
        completed = 0
        total = len(futures)
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Raise exception if thread failed
            completed += 1
            logger.info(f"Products seeding progress: {completed}/{total} total batches completed")
    logger.info(f"Completed seeding {count} products")

def seed_orders(database, count, user_ids, product_ids, batch_size=10000, threads=10):
    logger.info(f"Seeding {count} orders using Batch SQL with {threads} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        for i in range(0, count, batch_size):
            current_batch = min(batch_size, count - i)
            orders = [generate_order(buyer_id=random.choice(user_ids), product_id=random.choice(product_ids)) for _ in range(current_batch)]
            
            def insert_orders(transaction, order_list):
                statements = []
                for o in order_list:
                    statements.append((
                        "INSERT INTO Orders (OrderId, BuyerId, ProductId, OrderTime, Amount) VALUES (@id, @buyer, @product, @time, @amount)",
                        {"id": o["OrderId"], "buyer": o["BuyerId"], "product": o["ProductId"], "time": o["OrderTime"], "amount": o["Amount"]},
                        {"id": spanner.param_types.STRING, "buyer": spanner.param_types.STRING, "product": spanner.param_types.STRING, "time": spanner.param_types.TIMESTAMP, "amount": spanner.param_types.INT64}
                    ))
                transaction.batch_update(statements)
            
            def worker(o_list, b_idx):
                import threading
                database.run_in_transaction(insert_orders, o_list)
                logger.info(f"[{threading.current_thread().name}] Orders batch {b_idx} committed")
            
            batch_idx = i // batch_size + 1
            futures.append(executor.submit(worker, orders, batch_idx))
        
        completed = 0
        total = len(futures)
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Raise exception if thread failed
            completed += 1
            logger.info(f"Orders seeding progress: {completed}/{total} total batches completed")
    logger.info(f"Completed seeding {count} orders")

def run_seeder(database, user_count, product_count, order_count, threads=10):
    seed_users(database, user_count, threads=threads)
    
    user_ids = []
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("SELECT UserId FROM Users TABLESAMPLE BERNOULLI (50 PERCENT) LIMIT 1000")
        for row in results:
            user_ids.append(row[0])
            
    seed_products(database, product_count, user_ids, threads=threads)
    
    product_ids = []
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("SELECT ProductId FROM Products TABLESAMPLE BERNOULLI (50 PERCENT) LIMIT 1000")
        for row in results:
            product_ids.append(row[0])
            
    seed_orders(database, order_count, user_ids, product_ids, threads=threads)
    logger.info("Seeding complete.")
