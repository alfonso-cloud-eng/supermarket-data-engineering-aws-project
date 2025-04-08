import csv
import json
import random
from datetime import datetime, timedelta

# Constants
NUM_TRANSACTIONS = 100000
NUM_STORES = 5
NUM_SKUS = 12000

# Weighted choices for quantity: most common is 1.
QUANTITY_CHOICES = [1, 2, 3, 4, 5]
QUANTITY_WEIGHTS = [70, 15, 10, 4, 1]

def generate_random_datetime(start, end):
    """
    Returns a random datetime between start and end.
    """
    # total seconds between start and end
    delta = end - start
    random_seconds = random.uniform(0, delta.total_seconds())
    return start + timedelta(seconds=random_seconds)

def generate_items():
    """
    Generate a list of items for a transaction.
    Number of items is random (e.g., 1 to 12 distinct items).
    Each item includes a SKU (from SKU00001 to SKU12000) and a quantity 
    (most commonly 1, with weighted probabilities for higher quantities).
    """
    num_items = random.randint(1, 12)
    items = []
    for _ in range(num_items):
        sku_number = random.randint(1, NUM_SKUS)
        sku = f"SKU{sku_number:05d}"
        quantity = random.choices(QUANTITY_CHOICES, weights=QUANTITY_WEIGHTS)[0]
        items.append({"sku": sku, "quantity": quantity})
    return items

def main():
    # Determine the date range: from one year ago to now.
    now = datetime.now()
    one_year_ago = now - timedelta(days=365)

    # Generate random transaction datetimes.
    transactions = []
    for i in range(1, NUM_TRANSACTIONS + 1):
        # Generate a random date between one_year_ago and now.
        tx_datetime = generate_random_datetime(one_year_ago, now)
        # Format the datetime as a string.
        tx_date_str = tx_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        # Generate transaction fields.
        transaction_id = f"TX{i:07d}"
        store_number = random.randint(1, NUM_STORES)
        supermarket_id = f"STORE{store_number:04d}"
        items = generate_items()
        
        transaction = {
            "transaction_id": transaction_id,
            "supermarket_id": supermarket_id,
            "transaction_date": tx_date_str,
            "items": items
        }
        transactions.append(transaction)

    # Sort transactions by transaction_date
    transactions.sort(key=lambda x: x["transaction_date"])

    # Export to CSV file: each row will have the JSON representation of the transaction fields.
    csv_filename = "past_transactions.csv"
    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        # We'll use CSV columns for each field.
        fieldnames = ["transaction_id", "supermarket_id", "transaction_date", "items"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for tx in transactions:
            # Convert the "items" list to a JSON string.
            tx["items"] = json.dumps(tx["items"], ensure_ascii=False)
            writer.writerow(tx)
    
    print(f"CSV file '{csv_filename}' with {NUM_TRANSACTIONS} transactions has been generated.")

if __name__ == "__main__":
    main()
