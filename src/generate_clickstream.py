import csv
import json
import random
from datetime import datetime, timedelta
import uuid

# --- Configuration & Setup ---
random.seed(42)

TARGET_TOTAL_EVENTS = 100_000
OUTPUT_FILE = "clickstream_events.json1"
PRODUCT_CATALOG_FILE = "product_catalog.csv"
NUM_USERS = 5000

# Product Catalog with Categories and Brands
PRODUCTS = [
    {"id": "P101", "name": "Wireless Mouse", "brand": "LogiTech", "category": "Accessories", "price": 29.99},
    {"id": "P102", "name": "Mechanical Keyboard", "brand": "KeyChron", "category": "Accessories", "price": 89.99},
    {"id": "P103", "name": "UltraWide Monitor", "brand": "LG", "category": "Electronics", "price": 349.99},
    {"id": "P104", "name": "USB-C Hub", "brand": "Anker", "category": "Electronics", "price": 45.50},
    {"id": "P105", "name": "Ergonomic Chair", "brand": "HermanMiller", "category": "Furniture", "price": 199.99},
    {"id": "P106", "name": "Standing Desk", "brand": "FlexiSpot", "category": "Furniture", "price": 499.99},
]

CATEGORIES = sorted(set(p["category"] for p in PRODUCTS))

def generate_product_catalog_csv(filename=PRODUCT_CATALOG_FILE):
    """Generates the product catalog CSV file including brands."""
    fieldnames = ["id", "name", "brand", "category", "price"]
    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for product in PRODUCTS:
                writer.writerow(product)
        print(f"Success! Generated product catalog and saved to {filename}")
    except Exception as e:
        print(f"An error occurred while generating the product catalog CSV: {e}")

def get_user_favorite_category(user_id):
    """Deterministic favorite category based on user_id."""
    uid_num = int(user_id.split("_")[1])
    return CATEGORIES[uid_num % len(CATEGORIES)]

def get_gap(event_type):
    """Gaps shrink as the user descends the funnel."""
    if event_type == "page_view":
        return random.randint(30, 180)
    elif event_type == "product_view":
        return random.randint(15, 90)
    elif event_type == "add_to_cart":
        return random.randint(10, 45)
    elif event_type == "purchase":
        return random.randint(5, 30)
    return 10

def generate_session():
    user_id = f"user_{random.randint(1, NUM_USERS)}"
    favorite_category = get_user_favorite_category(user_id)
    
    end_date = datetime(2026, 9, 9, 23, 59, 59)
    start_date = end_date - timedelta(days=7)
    session_start = start_date + timedelta(seconds=random.randint(0, 7 * 86400))
    target_event_count = random.randint(6, 15)
    session_id = str(uuid.uuid4())
    
    events = []

    # 1. BROWSING STAGE
    num_page_views = random.randint(1, 3)
    for _ in range(num_page_views):
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": "page_view",
            "product_id": None,
            "product_name": None,
            "category": None,
            "price": None
        })
        if len(events) >= target_event_count:
            break

    # 2. BROWSE-ONLY CHECK
    if len(events) < target_event_count and random.random() < 0.20:
        return assign_timestamps(events, session_start)

    # 3. FUNNEL ATTEMPTS LOOP
    while len(events) < target_event_count:
        # Pick product (biased to user's favorite category)
        if random.random() < 0.70:
            candidates = [p for p in PRODUCTS if p["category"] == favorite_category]
            if not candidates:
                candidates = PRODUCTS
        else:
            candidates = PRODUCTS
        product = random.choice(candidates)

        # VIEW STAGE (1..2 product views)
        num_pviews = random.randint(1, 2)
        for _ in range(num_pviews):
            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "session_id": session_id,
                "event_type": "product_view",
                "product_id": product["id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": None
            })
            if len(events) >= target_event_count:
                break
        if len(events) >= target_event_count:
            break

        # CART STAGE
        if random.random() < 0.30:
            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "session_id": session_id,
                "event_type": "add_to_cart",
                "product_id": product["id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"]
            })
            if len(events) >= target_event_count:
                break

            # PURCHASE STAGE
            if random.random() < 0.40:
                events.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_type": "purchase",
                    "product_id": product["id"],
                    "product_name": product["name"],
                    "category": product["category"],
                    "price": product["price"]
                })
                if len(events) >= target_event_count:
                    break

    return assign_timestamps(events, session_start)

def assign_timestamps(events, session_start):
    t = session_start
    for event in events:
        t += timedelta(seconds=get_gap(event["event_type"]))
        event["timestamp"] = t.isoformat()
    return events

def main():
    # 1. Generate product catalog CSV file
    generate_product_catalog_csv()

    # 2. Generate clickstream events
    print(f"Generating ~{TARGET_TOTAL_EVENTS:,} clickstream events...")
    
    all_events = []
    while len(all_events) < TARGET_TOTAL_EVENTS:
        session_events = generate_session()
        all_events.extend(session_events)

    # Sessions are atomic — never split one. We accept a small overshoot.
    # Sort so output is time-ordered across sessions (matches real log arrival).
    all_events.sort(key=lambda e: e["timestamp"])

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        for event in all_events:
            file.write(json.dumps(event) + "\n")

    print(f"Success! Generated {len(all_events):,} events and saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()