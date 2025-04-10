import time
import json
import random
from datetime import datetime
import pytz

# Define the local timezone (UTC+2, e.g., Europe/Madrid).
local_tz = pytz.timezone("Europe/Madrid")

# Initialize so that the first transaction is TX0100001
transaction_counter = 100000

def is_store_open(now):
    """
    Checks if the current time (in local_tz) is within operating hours (9 AM - 9 PM)
    Monday through Saturday. Returns True if open, False if closed.
    """
    # Sunday in Python's datetime => weekday() == 6
    # Monday => 0 ... Saturday => 5
    if now.weekday() == 6:  # Sunday
        return False
    if now.hour < 9 or now.hour >= 21:  # Before 9 AM or after 9 PM
        return False
    return True

def generate_transaction():
    """
    Generates a single transaction record with:
      - transaction_id
      - supermarket_id (STORE0001 to STORE1500)
      - transaction_date (current local time in Europe/Madrid)
      - items: list of {"sku": ..., "quantity": ...}
    """
    global transaction_counter
    transaction_counter += 1

    # Ensure a 7-digit numeric portion, so 100001 => "0100001" => TX0100001.
    transaction_id = f"TX{transaction_counter:07d}"

    # Pick a random store between STORE0001 and STORE1500 (or 5 in your test).
    store_number = random.randint(1, 5)
    supermarket_id = f"STORE{store_number:04d}"

    # Get current datetime in the specified local timezone.
    transaction_date = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")

    # Randomly decide how many distinct SKUs are in this transaction.
    num_items = random.randint(1, 20)
    items = []
    for _ in range(num_items):
        sku_number = random.randint(1, 12000)
        sku_id = f"SKU{sku_number:05d}"
        quantity = random.choices([1, 2, 3, 4, 5], weights=[70, 15, 10, 4, 1])[0]
        items.append({"sku": sku_id, "quantity": quantity})

    transaction_record = {
        "transaction_id": transaction_id,
        "supermarket_id": supermarket_id,
        "transaction_date": transaction_date,
        "items": items
    }

    return transaction_record

def main():
    print("Starting real-time transaction generator. Press Ctrl+C to stop.")
    
    while True:
        # Use the local timezone for our decision logic.
        now = datetime.now(local_tz)
        
        if not is_store_open(now):
            print("Stores are currently closed. Waiting 1 hour to check again...")
            time.sleep(3600)
            continue

        # Store is open: generate a transaction record.
        transaction = generate_transaction()
        # Print the JSON representation to stdout (or do whatever you like here).
        print(json.dumps(transaction, ensure_ascii=False))
        
        # Sleep a random interval between transactions to simulate real-time variation.
        time.sleep(random.uniform(60, 180))

if __name__ == "__main__":
    main()
