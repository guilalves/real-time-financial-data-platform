"""Generate synthetic financial product data."""
import argparse
import json
import random
from datetime import timedelta
from pathlib import Path

from faker import Faker

from config.settings import SEED

fake = Faker()
Faker.seed(SEED + 2)
random.seed(SEED + 2)

PRODUCT_CATALOG = [
    ("PRD001", "Basic Checking Account", "checking", 0.0),
    ("PRD002", "Premium Savings Account", "savings", 4.5),
    ("PRD003", "High-Yield Savings", "savings", 5.2),
    ("PRD004", "Student Checking", "checking", 0.0),
    ("PRD005", "Business Checking", "checking", 0.0),
    ("PRD006", "Investment Portfolio - Conservative", "investment", 6.5),
    ("PRD007", "Investment Portfolio - Moderate", "investment", 9.0),
    ("PRD008", "Investment Portfolio - Aggressive", "investment", 12.5),
    ("PRD009", "Classic Credit Card", "credit", 18.9),
    ("PRD010", "Platinum Credit Card", "credit", 15.9),
    ("PRD011", "Travel Rewards Card", "credit", 17.5),
    ("PRD012", "Cashback Credit Card", "credit", 19.9),
    ("PRD013", "Personal Loan - Fixed", "loan", 8.5),
    ("PRD014", "Personal Loan - Variable", "loan", 7.2),
    ("PRD015", "Home Equity Line", "loan", 6.8),
    ("PRD016", "Auto Loan", "loan", 5.9),
    ("PRD017", "SME Business Loan", "loan", 9.5),
    ("PRD018", "Mortgage - 30yr Fixed", "mortgage", 6.5),
    ("PRD019", "Mortgage - 15yr Fixed", "mortgage", 5.9),
    ("PRD020", "Mortgage - ARM 5/1", "mortgage", 5.2),
]


def generate_product(row: tuple) -> dict:
    product_id, name, category, interest_rate = row
    created = fake.date_time_between(start_date="-10y", end_date="-1y")
    return {
        "product_id": product_id,
        "product_name": name,
        "category": category,
        "interest_rate": interest_rate,
        "status": random.choices(["active", "discontinued"], weights=[90, 10])[0],
        "minimum_balance": round(random.choice([0, 500, 1000, 5000, 10000]), 2),
        "created_at": created.isoformat(),
        "updated_at": (
            created + timedelta(days=random.randint(0, 730))
        ).isoformat(),
    }


def generate_products() -> list[dict]:
    return [generate_product(row) for row in PRODUCT_CATALOG]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic products")
    parser.add_argument("--output", type=str, default="data/raw/products.json")
    args = parser.parse_args()

    products = generate_products()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(products, f, indent=2)

    print(f"Generated {len(products)} products → {output_path}")


if __name__ == "__main__":
    main()
