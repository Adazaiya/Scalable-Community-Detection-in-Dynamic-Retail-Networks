# instacart_csv.py
import csv
import random
from datetime import datetime, timedelta

# =====PARAMETERS FOR SPARSE GRAPH =====
NUM_USERS = 150
NUM_PRODUCTS = 120
NUM_ORDERS = 1000
MAX_ITEMS_PER_ORDER = 6
START_DATE = datetime(2017, 1, 1)

# Expanded product catalog (more categories = sparser connections)
PRODUCT_CATALOG = {
    'dairy': [
        'Organic Whole Milk', 'Almond Milk', 'Soy Milk', 'Large Eggs', 'Yogurt', 
        'Greek Yogurt', 'Butter', 'Cream Cheese', 'Cheddar Cheese', 'Mozzarella',
        'Sour Cream', 'Cottage Cheese', 'Half and Half'
    ],
    'produce': [
        'Banana', 'Apple', 'Orange', 'Strawberries', 'Blueberries', 'Grapes',
        'Lettuce', 'Spinach', 'Tomato', 'Carrot', 'Broccoli', 'Cucumber',
        'Avocado', 'Bell Pepper', 'Onion', 'Garlic', 'Lemon', 'Lime', 'Kale'
    ],
    'breakfast': [
        'Cheerios', 'Granola', 'Oatmeal', 'Cereal Bar', 'Pancake Mix',
        'Maple Syrup', 'Bagels', 'English Muffins', 'Protein Bar', 'Muesli'
    ],
    'beverages': [
        'Orange Juice', 'Apple Juice', 'Coffee', 'Tea', 'Green Tea',
        'Soda', 'Sparkling Water', 'Energy Drink', 'Bottled Water', 
        'Iced Tea', 'Lemonade', 'Coconut Water'
    ],
    'bakery': [
        'Bread', 'Whole Wheat Bread', 'Sourdough', 'Rolls', 'Croissants', 
        'Muffins', 'Cookies', 'Brownies', 'Baguette', 'Pita Bread'
    ],
    'meat': [
        'Chicken Breast', 'Ground Beef', 'Pork Chops', 'Salmon', 'Tuna',
        'Shrimp', 'Turkey', 'Bacon', 'Sausage', 'Deli Ham', 'Steak'
    ],
    'snacks': [
        'Chips', 'Pretzels', 'Crackers', 'Popcorn', 'Trail Mix',
        'Candy', 'Chocolate Bar', 'Nuts', 'Granola Bar', 'Rice Cakes', 'Beef Jerky'
    ],
    'frozen': [
        'Ice Cream', 'Frozen Pizza', 'Frozen Vegetables', 'Frozen Fruit',
        'Ice Pops', 'Frozen Meals', 'Frozen Waffles', 'Frozen Fries', 'Frozen Berries'
    ],
    'pantry': [
        'Pasta', 'Rice', 'Beans', 'Tomato Sauce', 'Soup', 'Cereal',
        'Peanut Butter', 'Jelly', 'Olive Oil', 'Flour', 'Sugar', 'Salt', 'Vinegar'
    ],
    'personal_care': [
        'Shampoo', 'Conditioner', 'Soap', 'Toothpaste', 'Deodorant',
        'Lotion', 'Tissues', 'Paper Towels', 'Toilet Paper', 'Razors'
    ],
    'household': [
        'Dish Soap', 'Laundry Detergent', 'All-Purpose Cleaner', 'Sponges',
        'Trash Bags', 'Aluminum Foil', 'Plastic Wrap', 'Ziplock Bags'
    ]
}

# Generate products
products = []
product_id = 101

for category, items in PRODUCT_CATALOG.items():
    for item_name in items:
        if len(products) < NUM_PRODUCTS:
            products.append({
                "product_id": product_id,
                "product_name": item_name,
                "aisle": category,
                "department": category
            })
            product_id += 1

print("Generating products...")
with open("products.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["product_id","product_name","aisle","department"])
    writer.writeheader()
    writer.writerows(products)

# More diverse shopping patterns
SHOPPING_PATTERNS = {
    'breakfast': {
        'primary': ['dairy', 'breakfast'],
        'secondary': ['beverages', 'produce', 'bakery'],
        'weight': 0.15
    },
    'meal_prep': {
        'primary': ['meat', 'produce'],
        'secondary': ['pantry', 'dairy', 'frozen'],
        'weight': 0.20
    },
    'healthy': {
        'primary': ['produce'],
        'secondary': ['dairy', 'beverages', 'breakfast'],
        'weight': 0.15
    },
    'convenience': {
        'primary': ['frozen', 'snacks'],
        'secondary': ['beverages', 'bakery'],
        'weight': 0.10
    },
    'weekly': {
        'primary': ['dairy', 'produce', 'meat'],
        'secondary': ['pantry', 'bakery', 'beverages', 'snacks', 'frozen'],
        'weight': 0.20
    },
    'essentials': {
        'primary': ['personal_care', 'household'],
        'secondary': ['dairy', 'produce'],
        'weight': 0.10
    },
    'random': {
        'primary': list(PRODUCT_CATALOG.keys()),
        'secondary': [],
        'weight': 0.10
    }
}

def get_shopping_pattern():
    rand = random.random()
    cumulative = 0
    for pattern, config in SHOPPING_PATTERNS.items():
        cumulative += config['weight']
        if rand <= cumulative:
            return pattern, config
    return 'weekly', SHOPPING_PATTERNS['weekly']

# Generate orders
orders = []
print("Generating orders...")
for order_id in range(1, NUM_ORDERS + 1):
    user_id = random.randint(1, NUM_USERS)
    order_date = START_DATE + timedelta(days=random.randint(0, 90))
    order_time = f"{random.randint(8,20)}:{random.choice(['00','15','30','45'])}"
    orders.append({
        "order_id": order_id,
        "user_id": user_id,
        "order_date": order_date.strftime("%Y-%m-%d"),
        "order_time": order_time
    })

with open("orders.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["order_id","user_id","order_date","order_time"])
    writer.writeheader()
    writer.writerows(orders)

# Generate order_products
order_products = []
print("Generating order items...")

for order in orders:
    pattern_name, pattern_config = get_shopping_pattern()
    num_items = random.randint(3, MAX_ITEMS_PER_ORDER)
    
    selected = []
    for _ in range(num_items):

        if random.random() < 0.50:
            if pattern_config['primary']:
                categories = pattern_config['primary']
            else:
                categories = list(PRODUCT_CATALOG.keys())
        else:
            if pattern_config['secondary']:
                categories = pattern_config['secondary']
            else:
                categories = list(PRODUCT_CATALOG.keys())
        
        cat = random.choice(categories)
        cat_products = [p for p in products if p['aisle'] == cat]
        
        if cat_products:
            if random.random() < 0.8:
                selected.append(random.choice(cat_products))
            else:
                selected.append(random.choice(products))
    
    unique = list({p['product_id']: p for p in selected}.values())
    
    for idx, product in enumerate(unique, 1):
        order_products.append({
            "order_id": order["order_id"],
            "product_id": product["product_id"],
            "add_to_cart_order": idx
        })

with open("order_products.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["order_id","product_id","add_to_cart_order"])
    writer.writeheader()
    writer.writerows(order_products)

# Calculate expected density
total_possible_edges = (NUM_PRODUCTS * (NUM_PRODUCTS - 1)) / 2
avg_items_per_order = len(order_products) / NUM_ORDERS
avg_edges_per_order = (avg_items_per_order * (avg_items_per_order - 1)) / 2
estimated_edges = NUM_ORDERS * avg_edges_per_order * 0.1
estimated_density = estimated_edges / total_possible_edges * 100

print("\nSparse dataset generated.")
print(f"Products: {len(products)}")
print(f"Categories: {len(PRODUCT_CATALOG)}")
print(f"Users: {NUM_USERS}")
print(f"Orders: {len(orders)}")
print(f"Order items: {len(order_products)}")
print(f"Average items per order: {len(order_products)/len(orders):.1f}")
print(f"Estimated density: ~{estimated_density:.1f}%")
print("\nKey improvements for sparsity:")
print(" - 120 products (more variety)")
print(" - 1000 orders (more data)")
print(" - 50/50 primary and secondary split")
print(" - 11 categories (more distribution)")
print(" - 20 percent random product selection")
print("\nRun: python3 instacart_spark_pipeline.py")

