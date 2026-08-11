"""Generate synthetic account data."""
import argparse
import json
import random
from datetime import timedelta
from pathlib import Path

from faker import Faker

from config.settings import (
    ACCOUNT_TYPES,
    CURRENCIES,
    DEFAULT_ACCOUNT_COUNT,
    DEFAULT_CUSTOMER_COUNT,
    SEED,
)

fake = Faker()
Faker.seed(SEED + 1)
random.seed(SEED + 1)


def generate_account(account_id: int, customer_count: int) -> dict:
    created = fake.date_time_between(start_date="-5y", end_date="now")
    account_type = random.choice(ACCOUNT_TYPES)
    return {
        "account_id": f"ACC{account_id:08d}",
        "customer_id": f"CUS{random.randint(1, customer_count):08d}",
        "account_type": account_type,
        "status": random.choices(
            ["active", "inactive", "blocked"], weights=[85, 10, 5]
        )[0],
        "balance": round(random.uniform(0, 500_000), 2),
        "currency": random.choice(CURRENCIES[:4]),
        "created_at": created.isoformat(),
        "updated_at": (
            created + timedelta(days=random.randint(0, 365))
        ).isoformat(),
    }


def generate_accounts(count: int, customer_count: int) -> list[dict]:
    return [generate_account(i + 1, customer_count) for i in range(count)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic accounts")
    parser.add_argument("--records", type=int, default=DEFAULT_ACCOUNT_COUNT)
    parser.add_argument("--customers", type=int, default=DEFAULT_CUSTOMER_COUNT)
    parser.add_argument("--output", type=str, default="data/raw/accounts.json")
    args = parser.parse_args()

    accounts = generate_accounts(args.records, args.customers)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(accounts, f, indent=2)

    print(f"Generated {len(accounts)} accounts → {output_path}")


if __name__ == "__main__":
    main()
