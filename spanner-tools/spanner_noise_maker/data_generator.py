import uuid
import random
from datetime import datetime, timezone, timedelta
from faker import Faker

fake = Faker()

def generate_user():
    return {
        "UserId": str(uuid.uuid4()),
        "Username": fake.user_name()[:100],
        "JoinedAt": datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
    }

def generate_product(owner_id=None):
    return {
        "ProductId": str(uuid.uuid4()),
        "OwnerId": owner_id or str(uuid.uuid4()),
        "Price": random.randint(10, 1000),
        "Category": fake.word()[:50]
    }

def generate_order(buyer_id, product_id):
    return {
        "OrderId": str(uuid.uuid4()),
        "BuyerId": buyer_id,
        "ProductId": product_id,
        "OrderTime": datetime.now(timezone.utc),
        "Amount": random.randint(1, 5)
    }
