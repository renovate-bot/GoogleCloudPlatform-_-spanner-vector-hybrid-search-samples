import logging
import random
import time
from datetime import datetime, timezone
from google.cloud import spanner
from data_generator import generate_user, generate_product, generate_order

logger = logging.getLogger(__name__)

def run_noise(database, duration_seconds=60):
    logger.info(f"Starting noise generation (SQL-only) for {duration_seconds} seconds...")
    end_time = time.time() + duration_seconds
    
    # Use separate snapshots for each ID fetch to avoid "single-use snapshot" reuse error
    with database.snapshot() as snapshot:
        user_ids = [row[0] for row in snapshot.execute_sql("SELECT UserId FROM Users TABLESAMPLE BERNOULLI (10 PERCENT) LIMIT 1000")]
    
    with database.snapshot() as snapshot:
        product_ids = [row[0] for row in snapshot.execute_sql("SELECT ProductId FROM Products TABLESAMPLE BERNOULLI (10 PERCENT) LIMIT 1000")]

    if not user_ids or not product_ids:
        logger.warning("No data found in tables. Noise generation might be limited.")

    query_shapes = [
        "SELECT * FROM Users WHERE UserId = @id",
        "SELECT * FROM Orders WHERE BuyerId = @id LIMIT 10",
        "SELECT Category, AVG(Price) FROM Products GROUP BY Category",
        """
        SELECT u.Username, o.OrderTime, p.Price 
        FROM Users u 
        JOIN Orders o ON u.UserId = o.BuyerId 
        JOIN Products p ON o.ProductId = p.ProductId 
        WHERE u.UserId = @id
        """,
        "SELECT * FROM Products WHERE Price > @price AND Category = @category LIMIT 5"
    ]

    while time.time() < end_time:
        action = random.choice(["insert", "update", "query"])
        
        try:
            if action == "insert":
                table = random.choice(["Users", "Products", "Orders"])
                if table == "Users":
                    u = generate_user()
                    database.run_in_transaction(lambda t: t.execute_update(
                        "INSERT INTO Users (UserId, Username, JoinedAt) VALUES (@id, @name, @joined)",
                        params={"id": u["UserId"], "name": u["Username"], "joined": u["JoinedAt"]},
                        param_types={"id": spanner.param_types.STRING, "name": spanner.param_types.STRING, "joined": spanner.param_types.TIMESTAMP}
                    ))
                elif table == "Products":
                    p = generate_product(owner_id=random.choice(user_ids) if user_ids else None)
                    database.run_in_transaction(lambda t: t.execute_update(
                        "INSERT INTO Products (ProductId, OwnerId, Price, Category) VALUES (@id, @owner, @price, @cat)",
                        params={"id": p["ProductId"], "owner": p["OwnerId"], "price": p["Price"], "cat": p["Category"]},
                        param_types={"id": spanner.param_types.STRING, "owner": spanner.param_types.STRING, "price": spanner.param_types.INT64, "cat": spanner.param_types.STRING}
                    ))
                elif table == "Orders" and user_ids and product_ids:
                    o = generate_order(random.choice(user_ids), random.choice(product_ids))
                    database.run_in_transaction(lambda t: t.execute_update(
                        "INSERT INTO Orders (OrderId, BuyerId, ProductId, OrderTime, Amount) VALUES (@id, @buyer, @product, @time, @amount)",
                        params={"id": o["OrderId"], "buyer": o["BuyerId"], "product": o["ProductId"], "time": o["OrderTime"], "amount": o["Amount"]},
                        param_types={"id": spanner.param_types.STRING, "buyer": spanner.param_types.STRING, "product": spanner.param_types.STRING, "time": spanner.param_types.TIMESTAMP, "amount": spanner.param_types.INT64}
                    ))

            elif action == "update":
                table = random.choice(["Users", "Products"])
                if table == "Users" and user_ids:
                    database.run_in_transaction(lambda t: t.execute_update(
                        "UPDATE Users SET Username = @name WHERE UserId = @id",
                        params={"name": "updated_" + str(random.randint(1,1000)), "id": random.choice(user_ids)},
                        param_types={"name": spanner.param_types.STRING, "id": spanner.param_types.STRING}
                    ))
                elif table == "Products" and product_ids:
                    database.run_in_transaction(lambda t: t.execute_update(
                        "UPDATE Products SET Price = @price WHERE ProductId = @id",
                        params={"price": random.randint(10, 2000), "id": random.choice(product_ids)},
                        param_types={"price": spanner.param_types.INT64, "id": spanner.param_types.STRING}
                    ))

            elif action == "query":
                shape_idx = random.randint(0, len(query_shapes)-1)
                sql = query_shapes[shape_idx]
                params = {}
                param_types = {}
                
                if shape_idx in [0, 1, 3] and user_ids:
                    params = {"id": random.choice(user_ids)}
                    param_types = {"id": spanner.param_types.STRING}
                elif shape_idx == 4:
                    params = {"price": random.randint(50, 500), "category": "category_" + str(random.randint(1,10))}
                    param_types = {"price": spanner.param_types.INT64, "category": spanner.param_types.STRING}
                
                with database.snapshot() as snapshot:
                    results = snapshot.execute_sql(sql, params=params, param_types=param_types)
                    list(results)

        except Exception as e:
            logger.error(f"Error during noise generation: {e}")

    logger.info("Noise generation complete.")
