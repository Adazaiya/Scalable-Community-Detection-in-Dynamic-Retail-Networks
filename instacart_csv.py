import csv
import random
from datetime import datetime, timedelta

# ----------------------------
# Configurable parameters
# ----------------------------
NUM_USERS = 10
NUM_PRODUCTS = 15
NUM_ORDERS = 20
MAX_ITEMS_PER_ORDER = 5
START_DATE = datetime(2017, 1, 1)

# ----------------------------
# Generate products.csv
# ----------------------------
products = [
    {"product_id": 101, "product_name": "Organic Whole Milk", "aisle": "dairy", "department": "dairy eggs"},
    {"product_id": 102, "product_name": "Banana", "aisle": "fresh fruits", "department": "produce"},
    {"product_id": 103, "product_name": "Cheerios", "aisle": "cereal", "department": "breakfast"},
    {"product_id": 104, "product_name": "Large Eggs", "aisle": "dairy", "department": "dairy eggs"},
    {"product_id": 105, "product_name": "Bread", "aisle": "bakery", "department": "bakery"},
    {"product_id": 106, "product_name": "Almond Milk", "aisle": "dairy", "department": "dairy eggs"},
    {"product_id": 107, "product_name": "Orange Juice", "aisle": "beverages", "department": "beverages"},
    {"product_id": 108, "product_name": "Yogurt", "aisle": "dairy", "department": "dairy eggs"},
    {"product_id": 109, "product_name": "Granola", "aisle": "cereal", "department": "breakfast"},
    {"product_id": 110, "product_name": "Butter", "aisle": "dairy", "department": "dairy eggs"},
    {"product_id": 111, "product_name": "Coffee", "aisle": "beverages", "department": "beverages"},
    {"product_id": 112, "product_name": "Apple", "aisle": "fresh fruits", "department": "produce"},
    {"product_id": 113, "product_name": "Spinach", "aisle": "fresh vegetables", "department": "produce"},
    {"product_id": 114, "product_name": "Cheddar Cheese", "aisle": "dairy", "department": "dairy eggs"},
    {"product_id": 115, "product_name": "Cereal Bar", "aisle": "cereal", "department": "breakfast"}
]

with open("products.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["product_id","product_name","aisle","department"])
    writer.writeheader()
    for p in products:
        writer.writerow(p)

# ----------------------------
# Generate orders.csv
# ----------------------------
orders = []
for order_id in range(1, NUM_ORDERS+1):
    user_id = random.randint(1, NUM_USERS)
    order_date = START_DATE + timedelta(days=random.randint(0, 30))
    order_time = f"{random.randint(8,20)}:{random.choice(['00','15','30','45'])}"
    orders.append({"order_id": order_id, "user_id": user_id, "order_date": order_date.strftime("%Y-%m-%d"), "order_time": order_time})

with open("orders.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["order_id","user_id","order_date","order_time"])
    writer.writeheader()
    for o in orders:
        writer.writerow(o)

# ----------------------------
# Generate order_products.csv
# ----------------------------
order_products = []
for order in orders:
    num_items = random.randint(2, MAX_ITEMS_PER_ORDER)
    product_choices = random.sample(products, num_items)
    for idx, product in enumerate(product_choices, start=1):
        order_products.append({
            "order_id": order["order_id"],
            "product_id": product["product_id"],
            "add_to_cart_order": idx
        })

with open("order_products.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["order_id","product_id","add_to_cart_order"])
    writer.writeheader()
    for op in order_products:
        writer.writerow(op)

print("CSV files generated: orders.csv, products.csv, order_products.csv")
