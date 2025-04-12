import csv
import random
from faker import Faker
from faker.providers import BaseProvider

# Define a custom provider to generate supermarket product names
class SupermarketProvider(BaseProvider):
    def supermarket_product(self):
        product_data = {
            "produce": {
                "adjectives": ["Organic", "Fresh", "Local", "Seasonal", "Ripe"],
                "nouns": ["Apple", "Banana", "Tomato", "Lettuce", "Carrot", "Cucumber"]
            },
            "cleaning": {
                "adjectives": ["Powerful", "Eco-Friendly", "Sparkling", "Deep-Clean", "Multi-Surface"],
                "nouns": ["Detergent", "Cleaner", "Disinfectant", "Soap", "Sanitizer"]
            },
            "pet": {
                "adjectives": ["Nutritious", "Delicious", "Healthy", "Premium", "Wholesome"],
                "nouns": ["Dog Food", "Cat Food", "Pet Treats", "Puppy Chow", "Kitten Kibble"]
            },
            "cosmetics": {
                "adjectives": ["Luxury", "Silky", "Radiant", "Smooth", "Elegant"],
                "nouns": ["Lotion", "Cream", "Perfume", "Foundation", "Lipstick"]
            },
            "bakery": {
                "adjectives": ["Freshly-Baked", "Artisan", "Crispy", "Fluffy", "Homemade"],
                "nouns": ["Bread", "Croissant", "Baguette", "Muffin", "Bagel"]
            },
            "dairy": {
                "adjectives": ["Creamy", "Fresh", "Organic", "Rich", "Farm-Fresh"],
                "nouns": ["Milk", "Cheese", "Yogurt", "Butter", "Cream"]
            },
            "beverages": {
                "adjectives": ["Icy", "Refreshing", "Sparkling", "Chilled", "Zesty"],
                "nouns": ["Soda", "Juice", "Tea", "Coffee", "Water"]
            },
            "snacks": {
                "adjectives": ["Crunchy", "Salty", "Sweet", "Spicy", "Savory"],
                "nouns": ["Chips", "Popcorn", "Cookies", "Nuts", "Crackers"]
            }
        }
        category = self.random_element(elements=list(product_data.keys()))
        adjectives = product_data[category]["adjectives"]
        nouns = product_data[category]["nouns"]
        return f"{self.random_element(elements=adjectives)} {self.random_element(elements=nouns)}"

# Initialize Faker and add our custom provider
fake = Faker()
fake.add_provider(SupermarketProvider)

# Define the CSV filename
filename = "skus.csv"

with open(filename, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["sku", "name of article", "price"])
    
    for i in range(1, 12001):
        sku = f"SKU{i:05d}"
        name = fake.supermarket_product()
        
        # 90% chance to use a low price range (0.5 to 10€)
        if random.random() < 0.9:
            price = round(random.uniform(0.5, 10), 2)
        else:
            # 10% chance to have a higher price (10 to 100€)
            price = round(random.uniform(10, 100), 2)
            
        writer.writerow([sku, name, price])

print(f"CSV file '{filename}' with 12,000 SKUs has been generated.")
