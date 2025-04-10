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
    delta = end - start
    random_seconds = random.uniform(0, delta.total_seconds())
    return start + timedelta(seconds=random_seconds)

def generate_items():
    """
    Generate a list of items for a transaction.
    Number of items is random (1 to 12 distinct items).
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

    # Step 1: Generate the raw data (no transaction_id yet).
    transactions = []
    for _ in range(NUM_TRANSACTIONS):
        tx_datetime = generate_random_datetime(one_year_ago, now)
        tx_date_str = tx_datetime.strftime("%Y-%m-%d %H:%M:%S")
        store_number = random.randint(1, NUM_STORES)
        supermarket_id = f"STORE{store_number:04d}"
        items = generate_items()

        transactions.append({
            "transaction_date": tx_date_str,
            "supermarket_id": supermarket_id,
            "items": items
        })

    # Step 2: Sort the transactions by their date string.
    # (Since we used strftime, it sorts lexically the same as actual datetimes in YYYY-MM-DD HH:MM:SS format.)
    transactions.sort(key=lambda x: x["transaction_date"])

    # Step 3: Assign transaction IDs in ascending order to match the sorted timestamps.
    for i, tx in enumerate(transactions, start=1):
        tx["transaction_id"] = f"TX{i:07d}"

    # Step 4: Write to CSV
    csv_filename = "past_transactions.csv"
    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["transaction_id", "supermarket_id", "transaction_date", "items"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for tx in transactions:
            # Convert the "items" list to a JSON string before writing.
            tx_copy = tx.copy()
            tx_copy["items"] = json.dumps(tx["items"], ensure_ascii=False)
            writer.writerow(tx_copy)

    print(f"CSV file '{csv_filename}' with {NUM_TRANSACTIONS} transactions has been generated.")

if __name__ == "__main__":
    main()
