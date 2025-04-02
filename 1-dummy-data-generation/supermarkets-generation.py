import csv
from faker import Faker

def generate_supermarkets_csv(filename="supermarkets.csv", num_supermarkets=1500):
    """
    Generates a CSV file with two columns:
        - supermarket_id: a unique identifier for each supermarket
        - supermarket_name: the supermarket's street address (street name and building number only)
    """
    fake = Faker("es_ES")
    
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # Write header row.
        writer.writerow(["supermarket_id", "supermarket_name"])
        
        # Generate rows.
        for i in range(1, num_supermarkets + 1):
            supermarket_id = f"STORE{i:04d}"
            # Build the address using only street name and building number.
            supermarket_name = f"{fake.street_name()} {fake.building_number()}"
            writer.writerow([supermarket_id, supermarket_name])
    
    print(f"CSV file '{filename}' with {num_supermarkets} supermarkets has been generated.")

if __name__ == "__main__":
    generate_supermarkets_csv()
