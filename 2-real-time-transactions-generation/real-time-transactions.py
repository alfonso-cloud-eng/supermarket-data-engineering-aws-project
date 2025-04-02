import time
import json
import random
from datetime import datetime

# Global transaction counter (or you can use a UUID instead).
transaction_counter = 0

def is_store_open(now):
    """
    Checks if the current time is within operating hours (9 AM - 9 PM) 
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
      - transaction_date (current local time, assuming Spain time on the server)
      - items: list of {"sku": ..., "quantity": ...}
    """
    global transaction_counter
    transaction_counter += 1

    # Transaction ID could also be a UUID, but here we'll just use a numeric counter.
    transaction_id = f"TX{transaction_counter:07d}"

    # Pick a random store between STORE0001 and STORE1500
    store_number = random.randint(1, 1500)
    supermarket_id = f"STORE{store_number:04d}"

    # Current datetime as transaction date (assuming local machine is in Spain time).
    transaction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Randomly decide how many distinct SKUs are in this transaction
    num_items = random.randint(1, 20)

    items = []
    # We'll sample SKUs for realism. Make sure we don't pick duplicates (optional).
    # But let's keep it simple and allow duplicates occasionally if you prefer.
    # If you'd rather not allow duplicates, use random.sample.
    for _ in range(num_items):
        sku_number = random.randint(1, 12000)
        sku_id = f"SKU{sku_number:05d}"
        quantity = random.choices([1, 2, 3, 4, 5], weights=[70, 15, 10, 4, 1])[0] # e.g., up to 5 units of any given item
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
        now = datetime.now()
        
        if not is_store_open(now):
            # If store is closed, wait 1 hour before checking again.
            # Alternatively, you could sleep until exactly 9 AM the next day.
            print("Stores are currently closed. Waiting 1 hour to check again...")
            time.sleep(3600)
            continue

        # Store is open, so generate a transaction.
        transaction = generate_transaction()
        
        # Convert to JSON string and print to terminal
        print(json.dumps(transaction, ensure_ascii=False))
        
        # Sleep a random interval between transactions to simulate real-time variation.
        # For example, 1 to 5 seconds. Adjust as needed for your use case.
        time.sleep(random.uniform(0.1, 0.5))

if __name__ == "__main__":
    main()
