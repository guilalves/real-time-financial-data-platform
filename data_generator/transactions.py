"""
Generate synthetic transaction data with intentional engineering challenges:
  - ~2% duplicate events (simulating Kafka at-least-once delivery)
  - ~5% late-arriving events (event_time up to 45 min before processing_time)
  - ~1% records with injected null fields (for data quality validation)
  - Schema evolution: v1 fields always present; v2/v3 fields added at configurable rate
"""
import argparse
import copy
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from config.settings import (
    CURRENCIES,
    DEFAULT_ACCOUNT_COUNT,
    DEFAULT_CUSTOMER_COUNT,
    DEFAULT_TRANSACTION_COUNT,
    DUPLICATE_RATE,
    LATE_ARRIVAL_RATE,
    MERCHANT_CATEGORIES,
    NULL_INJECTION_RATE,
    SEED,
    TRANSACTION_TYPES,
)

fake = Faker()
Faker.seed(SEED + 3)
random.seed(SEED + 3)

MERCHANTS = [
    "Amazon", "Walmart", "Target", "Starbucks", "McDonald's",
    "Shell", "BP", "Uber", "Lyft", "Netflix", "Spotify",
    "Apple Store", "Google Play", "Best Buy", "Home Depot",
    "CVS Pharmacy", "Walgreens", "Delta Airlines", "Marriott",
    "Airbnb", "DoorDash", "Instacart", "Whole Foods",
]

PAYMENT_METHODS = ["credit_card", "debit_card", "pix", "wire_transfer", "cash"]


def _base_transaction(
    account_count: int,
    customer_count: int,
    now: datetime,
    schema_version: int,
) -> dict:
    event_time = now - timedelta(seconds=random.randint(0, 59))
    record: dict = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC{random.randint(1, account_count):08d}",
        "customer_id": f"CUS{random.randint(1, customer_count):08d}",
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "amount": round(random.uniform(0.5, 15_000), 2),
        "currency": random.choices(CURRENCIES, weights=[50, 20, 15, 8, 3, 2, 2])[0],
        "merchant": random.choice(MERCHANTS),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "timestamp": event_time.isoformat(),
        "status": random.choices(
            ["completed", "pending", "failed"],
            weights=[88, 10, 2],
        )[0],
    }

    # Schema v2: payment_method column added
    if schema_version >= 2:
        record["payment_method"] = random.choice(PAYMENT_METHODS)

    # Schema v3: device_fingerprint and ip_country added
    if schema_version >= 3:
        record["device_fingerprint"] = fake.sha256() if random.random() > 0.1 else None
        record["ip_country"] = fake.country_code(representation="alpha-2")

    return record


def _inject_null(record: dict) -> dict:
    """Randomly nullify a mandatory field for DQ testing."""
    field = random.choice(["transaction_id", "customer_id", "amount", "timestamp"])
    record[field] = None
    return record


def generate_transactions(
    count: int,
    account_count: int,
    customer_count: int,
    schema_version: int = 2,
) -> list[dict]:
    now = datetime.utcnow()
    records: list[dict] = []

    for _ in range(count):
        record = _base_transaction(account_count, customer_count, now, schema_version)

        # Simulate late arrival: event_time shifted back 10–45 minutes
        if random.random() < LATE_ARRIVAL_RATE:
            original_ts = datetime.fromisoformat(record["timestamp"])
            late_ts = original_ts - timedelta(minutes=random.randint(10, 45))
            record["timestamp"] = late_ts.isoformat()
            record["_is_late_arrival"] = True
        else:
            record["_is_late_arrival"] = False

        # Inject null field for DQ challenge
        if random.random() < NULL_INJECTION_RATE:
            record = _inject_null(record)

        records.append(record)

    # Inject duplicates at the end (simulating at-least-once Kafka delivery)
    duplicate_count = int(count * DUPLICATE_RATE)
    duplicates = [copy.deepcopy(random.choice(records)) for _ in range(duplicate_count)]
    records.extend(duplicates)

    # Shuffle so duplicates aren't clustered at the end
    random.shuffle(records)
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic transactions")
    parser.add_argument("--records", type=int, default=DEFAULT_TRANSACTION_COUNT)
    parser.add_argument("--accounts", type=int, default=DEFAULT_ACCOUNT_COUNT)
    parser.add_argument("--customers", type=int, default=DEFAULT_CUSTOMER_COUNT)
    parser.add_argument("--schema-version", type=int, default=2, choices=[1, 2, 3])
    parser.add_argument("--output", type=str, default="data/raw/transactions.json")
    args = parser.parse_args()

    transactions = generate_transactions(
        args.records,
        args.accounts,
        args.customers,
        args.schema_version,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(transactions, f, indent=2)

    total = len(transactions)
    dupes = sum(1 for t in transactions if transactions.count(t) > 1)
    late = sum(1 for t in transactions if t.get("_is_late_arrival"))
    nulls = sum(
        1 for t in transactions
        if any(t.get(f) is None for f in ["transaction_id", "customer_id", "amount", "timestamp"])
    )
    print(f"Generated {total:,} transactions → {output_path}")
    print(f"  Late arrivals : ~{late:,} ({late/total:.1%})")
    print(f"  Injected nulls: ~{nulls:,} ({nulls/total:.1%})")
    print(f"  Schema version: v{args.schema_version}")


if __name__ == "__main__":
    main()
