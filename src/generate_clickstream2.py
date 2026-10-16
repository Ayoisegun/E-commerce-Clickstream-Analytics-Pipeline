import csv
import json
import random
from datetime import datetime, timedelta
import uuid

# --- Configuration & Setup ---
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

TARGET_TOTAL_EVENTS = 5_000_000
OUTPUT_FILE = "clickstream_events.jsonl"
PRODUCT_CATALOG_FILE = "product_catalog.csv"

# Scale parameters
NUM_USERS = 35_000
NUM_DAYS = 30

# Data-Quality / Corruption Rates (Configurable between 0.1% and 1%)
DUPLICATE_RATE = 0.003        # 0.3% duplicate event injection
MISSING_VALUE_RATE = 0.005    # 0.5% missing optional fields
INVALID_PRICE_RATE = 0.002    # 0.2% invalid/anomalous purchase prices
MALFORMED_RECORD_RATE = 0.002 # 0.2% malformed JSON syntax lines
LATE_EVENT_RATE = 0.003       # 0.3% out-of-order / late timestamps

# --- Expanded Product Catalog (~75 Products across Categories & Brands) ---
CATEGORIES_AND_BRANDS = {
    "Accessories": ["LogiTech", "KeyChron", "Razer", "Anker", "Belkin"],
    "Electronics": ["LG", "Dell", "Samsung", "Sony", "Apple", "Asus"],
    "Furniture": ["HermanMiller", "FlexiSpot", "Steelcase", "IKEA", "Autonomous"],
    "Apparel": ["Nike", "Adidas", "Puma", "UnderArmour", "NewBalance"],
    "Home & Kitchen": ["Ninja", "InstantPot", "Keurig", "Breville", "Dyson"]
}

PRODUCT_NAMES = {
    "Accessories": ["Wireless Mouse", "Mechanical Keyboard", "RGB Mousepad", "USB-C Hub", "Webcam 1080p", "Laptop Stand", "External SSD 1TB", "Bluetooth Speaker"],
    "Electronics": ["UltraWide Monitor 34\"", "4K Gaming Monitor", "Noise-Canceling Headphones", "Portable Power Bank", "Wireless Earbuds", "Thunderbolt Dock", "Smart Tablet"],
    "Ergonomic Chair": ["Ergonomic Chair", "Standing Desk", "Mesh Office Chair", "Executive Desk", "Drafting Stool", "Cable Management Tray", "Monitor Arm"],
    "Apparel": ["Running Shoes", "Compression Shirt", "Athletic Joggers", "Training Shorts", "Performance Hoodie", "Gym Duffel Bag"],
    "Home & Kitchen": ["Air Fryer XL", "Smart Coffee Maker", "Espresso Machine", "Cordless Vacuum", "Blender Pro", "Electric Kettle", "Smart Air Purifier"]
}

def generate_product_catalog():
    """Generates an expanded product catalog of ~75 products."""
    products = []
    prod_id_counter = 101
    
    for category, brands in CATEGORIES_AND_BRANDS.items():
        # Fallback names list selector
        names_pool = PRODUCT_NAMES.get(category, ["Standard Item A", "Standard Item B", "Standard Item C"])
        for i in range(15):  # ~15 products per category = 75 products
            p_id = f"P{prod_id_counter}"
            brand = brands[i % len(brands)]
            name_base = names_pool[i % len(names_pool)]
            name = f"{brand} {name_base}"
            
            # Realistic pricing based on category
            base_price = 29.99
            if category == "Electronics": base_price = 199.99
            elif category == "Furniture": base_price = 249.99
            elif category == "Home & Kitchen": base_price = 89.99
            elif category == "Apparel": base_price = 59.99
            
            price = round(base_price * random.uniform(0.6, 2.5), 2)
            
            products.append({
                "id": p_id,
                "name": name,
                "brand": brand,
                "category": category,
                "price": price
            })
            prod_id_counter += 1
            
    return products

PRODUCTS = generate_product_catalog()
CATEGORIES = sorted(list(CATEGORIES_AND_BRANDS.keys()))

def generate_product_catalog_csv(filename=PRODUCT_CATALOG_FILE):
    """Exports the product catalog to CSV including brands."""
    fieldnames = ["id", "name", "brand", "category", "price"]
    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for product in PRODUCTS:
                writer.writerow(product)
        print(f"Success! Generated product catalog CSV with {len(PRODUCTS)} items -> {filename}")
    except Exception as e:
        print(f"Error generating product catalog CSV: {e}")

# --- User Persona Profiles (Behavioral Consistency) ---
def initialize_user_profiles():
    profiles = {}
    for i in range(1, NUM_USERS + 1):
        uid = f"user_{i}"
        profiles[uid] = {
            "favorite_category": random.choice(CATEGORIES),
            # Propensity weights for conversion behavior
            "funnel_tendency": random.choices(
                ["browser_only", "cart_abandoner", "buyer"],
                weights=[0.40, 0.35, 0.25]
            )[0],
            # Activity volume profile
            "activity_weight": random.expovariate(1.0) + 0.2
        }
    return profiles

USER_PROFILES = initialize_user_profiles()

def get_gap(event_type):
    """Gaps shrink as user descends the funnel."""
    if event_type == "page_view":
        return random.randint(20, 120)
    elif event_type == "product_view":
        return random.randint(10, 60)
    elif event_type == "add_to_cart":
        return random.randint(5, 30)
    elif event_type == "purchase":
        return random.randint(3, 15)
    return 10

def generate_session(day_start_time):
    """Generates a realistic session sequence based on user behavioral profile."""
    user_id = f"user_{random.randint(1, NUM_USERS)}"
    profile = USER_PROFILES[user_id]
    favorite_category = profile["favorite_category"]
    tendency = profile["funnel_tendency"]
    
    # Random session offset within the day (0 to 24 hours)
    session_start = day_start_time + timedelta(seconds=random.randint(0, 86399))
    session_id = str(uuid.uuid4())
    
    events = []
    
    # 1. BROWSING STAGE (Always starts with page views)
    num_page_views = random.randint(1, 4)
    for _ in range(num_page_views):
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": "page_view",
            "product_id": None,
            "product_name": None,
            "brand": None,
            "category": None,
            "price": None
        })
    
    # If user profile is strictly browser-only, stop here
    if tendency == "browser_only" and random.random() < 0.8:
        return assign_timestamps(events, session_start)

    # 2. PRODUCT INTERACTION STAGE
    # Select products biased toward favorite category
    category_candidates = [p for p in PRODUCTS if p["category"] == favorite_category]
    other_candidates = [p for p in PRODUCTS if p["category"] != favorite_category]
    
    num_interactions = random.randint(1, 4)
    purchased_item = None
    added_item = None
    
    for _ in range(num_interactions):
        if random.random() < 0.75 and category_candidates:
            product = random.choice(category_candidates)
        else:
            product = random.choice(other_candidates if other_candidates else PRODUCTS)
            
        # Product Views (1 to 3 views per product interaction)
        for _ in range(random.randint(1, 2)):
            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "session_id": session_id,
                "event_type": "product_view",
                "product_id": product["id"],
                "product_name": product["name"],
                "brand": product["brand"],
                "category": product["category"],
                "price": product["price"]
            })
        
        # Decide cart action based on profile
        if tendency != "browser_only" and random.random() < 0.60:
            added_item = product
            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "session_id": session_id,
                "event_type": "add_to_cart",
                "product_id": product["id"],
                "product_name": product["name"],
                "brand": product["brand"],
                "category": product["category"],
                "price": product["price"]
            })
            
            # Decide purchase action based on profile
            if tendency == "buyer" and random.random() < 0.65:
                purchased_item = product
                events.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_type": "purchase",
                    "product_id": product["id"],
                    "product_name": product["name"],
                    "brand": product["brand"],
                    "category": product["category"],
                    "price": product["price"]
                })
                break # Stop funnel after successful purchase
                
    return assign_timestamps(events, session_start)

def assign_timestamps(events, session_start):
    t = session_start
    for event in events:
        t += timedelta(seconds=get_gap(event["event_type"]))
        event["timestamp"] = t.isoformat()
    return events

def apply_data_quality_anomalies(event):
    """Applies configurable, controlled data quality corruptions to an event record."""
    # 1. Missing optional fields
    if random.random() < MISSING_VALUE_RATE:
        event["brand"] = None
        event["category"] = None
        
    # 2. Invalid price on purchase events
    if event["event_type"] == "purchase" and random.random() < INVALID_PRICE_RATE:
        event["price"] = round(random.choice([-10.0, 0.0, 99999.99]), 2)
        
    # 3. Unexpected / Unknown event types
    if random.random() < 0.001:
        event["event_type"] = "unknown_click_action"
        
    return event

def main():
    print("--- Starting E-Commerce Clickstream Data Generator ---")
    generate_product_catalog_csv()
    
    end_date = datetime(2026, 9, 9, 23, 59, 59)
    start_date = end_date - timedelta(days=NUM_DAYS)
    
    print(f"Target Events: {TARGET_TOTAL_EVENTS:,} across {NUM_DAYS} days ({start_date.date()} to {end_date.date()})")
    print(f"Active Users: {NUM_USERS:,} | Catalog Size: {len(PRODUCTS)} products\n")
    
    total_events_written = 0
    current_day = start_date
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f_out:
        while total_events_written < TARGET_TOTAL_EVENTS and current_day <= end_date:
            day_end = current_day + timedelta(days=1)
            day_events = []
            
            # Scale daily traffic with weekend peaks (Friday/Saturday get 30% more traffic)
            is_weekend = current_day.weekday() in [4, 5]
            daily_target_share = (TARGET_TOTAL_EVENTS / NUM_DAYS) * (1.3 if is_weekend else 0.9)
            
            # Generate sessions for the current day until daily share is met
            while len(day_events) < daily_target_share and total_events_written + len(day_events) < TARGET_TOTAL_EVENTS:
                session_events = generate_session(current_day)
                day_events.extend(session_events)
                
            # Chronologically sort events for this day to preserve strict log arrival order
            day_events.sort(key=lambda x: x["timestamp"])
            
            # Stream-write day's events with data quality corruptions & anomalies applied
            for event in day_events:
                # Apply data quality issues
                event = apply_data_quality_anomalies(event)
                
                # Check for duplicate event injection
                is_duplicate = random.random() < DUPLICATE_RATE
                
                # Check for late-arriving / out-of-order timestamp injection
                if random.random() < LATE_EVENT_RATE:
                    dt = datetime.fromisoformat(event["timestamp"])
                    event["timestamp"] = (dt - timedelta(hours=random.randint(2, 24))).isoformat()

                # Check for malformed JSON line injection
                if random.random() < MALFORMED_RECORD_RATE:
                    f_out.write('{"event_id": "malformed_json_row_corrupted_syntax", "user_id": ' + f'"{event["user_id"]}"\n')
                    total_events_written += 1
                    continue

                f_out.write(json.dumps(event) + "\n")
                total_events_written += 1
                
                if is_duplicate:
                    f_out.write(json.dumps(event) + "\n") # Write duplicate row
                    total_events_written += 1

            progress_pct = (total_events_written / TARGET_TOTAL_EVENTS) * 100
            print(f"[{current_day.date()}] Day completed. Total events written: {total_events_written:,} / {TARGET_TOTAL_EVENTS:,} ({progress_pct:.1f}%)")
            
            current_day = day_end

    print(f"\nSuccess! Generated ~{total_events_written:,} records and saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()