"""Publish synthetic payment events to a Pub/Sub topic.

Each event is stamped with the current time so it lands in the active window of
the streaming pipeline.

Usage:
    python publish_events.py --project PROJECT --topic payment-events --count 300 --rate 50
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

MERCHANTS = ["amazon", "walmart", "uber", "netflix", "shell", "tim_hortons", "loblaws"]
CURRENCIES = ["CAD", "USD", "EUR"]
STATUSES = ["approved", "declined", "pending"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", default="payment-events")
    ap.add_argument("--count", type=int, default=300)
    ap.add_argument("--rate", type=float, default=50, help="messages per second")
    args = ap.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)

    sleep = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    for _ in range(args.count):
        rec = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"cust_{random.randint(1, 5000):05d}",
            "merchant": random.choice(MERCHANTS),
            "amount": round(random.uniform(1.0, 2500.0), 2),
            "currency": random.choice(CURRENCIES),
            "status": random.choice(STATUSES),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }
        publisher.publish(topic_path, json.dumps(rec).encode("utf-8"))
        sent += 1
        if sleep:
            time.sleep(sleep)
    print(f"Published {sent} events to {topic_path}")


if __name__ == "__main__":
    main()
