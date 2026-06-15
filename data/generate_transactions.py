"""
Generate sample raw payment transactions as JSON Lines (one JSON object per line).

This simulates the kind of messy, real-world event data that lands in GCS / Pub/Sub
before any cleaning. We deliberately inject a few bad records so our pipeline's
validation step has something to catch.

Run:  python3 generate_transactions.py --count 1000 --out raw_transactions.jsonl
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone

MERCHANTS = ["amazon", "walmart", "uber", "netflix", "shell", "tim_hortons", "loblaws"]
CURRENCIES = ["CAD", "USD", "EUR"]
STATUSES = ["approved", "declined", "pending"]


def make_record(i: int, now: datetime) -> dict:
    ts = now - timedelta(seconds=random.randint(0, 7 * 24 * 3600))
    rec = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"cust_{random.randint(1, 5000):05d}",
        "merchant": random.choice(MERCHANTS),
        "amount": round(random.uniform(1.0, 2500.0), 2),
        "currency": random.choice(CURRENCIES),
        "status": random.choice(STATUSES),
        "event_time": ts.isoformat(),
    }

    # Inject ~5% bad data on purpose, to exercise validation:
    roll = random.random()
    if roll < 0.02:
        rec["amount"] = -abs(rec["amount"])          # negative amount (invalid)
    elif roll < 0.035:
        rec["currency"] = "XYZ"                        # unknown currency
    elif roll < 0.05:
        del rec["customer_id"]                         # missing required field
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--out", default="raw_transactions.jsonl")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    with open(args.out, "w") as f:
        for i in range(args.count):
            f.write(json.dumps(make_record(i, now)) + "\n")
    print(f"Wrote {args.count} records to {args.out}")


if __name__ == "__main__":
    main()
