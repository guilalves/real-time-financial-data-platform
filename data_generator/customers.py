"""Generate synthetic customer data."""
import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from config.settings import (
    CURRENCIES,
    CUSTOMER_SEGMENTS,
    DEFAULT_CUSTOMER_COUNT,
    SEED,
)

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)


def generate_customer(customer_id: int) -> dict:
    created = fake.date_time_between(start_date="-5y", end_date="now")
    return {
        "customer_id": f"CUS{customer_id:08d}",
        "name": fake.name(),
        "age": random.randint(18, 80),
        "country": fake.country_code(representation="alpha-2"),
        "segment": random.choice(CUSTOMER_SEGMENTS),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "preferred_currency": random.choice(CURRENCIES[:4]),
        "created_at": created.isoformat(),
        "updated_at": (
            created + timedelta(days=random.randint(0, 365))
        ).isoformat(),
    }


def generate_customers(count: int) -> list[dict]:
    return [generate_customer(i + 1) for i in range(count)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic customers")
    parser.add_argument("--records", type=int, default=DEFAULT_CUSTOMER_COUNT)
    parser.add_argument("--output", type=str, default="data/raw/customers.json")
    args = parser.parse_args()

    customers = generate_customers(args.records)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(customers, f, indent=2)

    print(f"Generated {len(customers)} customers → {output_path}")


if __name__ == "__main__":
    main()
