"""Streaming ingestion pipeline: Pub/Sub to BigQuery.

Consumes payment events from a Pub/Sub subscription, validates them, aggregates
transaction count and total amount per merchant over fixed 30-second windows,
and appends the results to BigQuery.

Pub/Sub stamps each message with its publish time, so the fixed windows group
events by arrival time. For event-time semantics, supply a timestamp attribute
on the messages and Beam will use it for windowing and watermarks.

Usage:
    python streaming_transactions.py \
        --project PROJECT \
        --subscription projects/PROJECT/subscriptions/payment-events-sub \
        --output_table PROJECT:payments.streaming_merchant_agg \
        --temp_location gs://PROJECT-payments-lake/tmp
"""
import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

VALID_CURRENCIES = {"CAD", "USD", "EUR"}


def parse_and_keep_valid(msg_bytes):
    """Decode and validate a Pub/Sub message, yielding only valid records."""
    try:
        rec = json.loads(msg_bytes.decode("utf-8"))
    except Exception:
        return
    if (
        rec.get("customer_id")
        and isinstance(rec.get("amount"), (int, float))
        and rec["amount"] > 0
        and rec.get("currency") in VALID_CURRENCIES
    ):
        yield rec


class CountSum(beam.CombineFn):
    """Accumulate transaction count and total amount in a single pass."""

    def create_accumulator(self):
        return (0, 0.0)

    def add_input(self, acc, amount):
        c, s = acc
        return (c + 1, s + amount)

    def merge_accumulators(self, accs):
        c = sum(a[0] for a in accs)
        s = sum(a[1] for a in accs)
        return (c, s)

    def extract_output(self, acc):
        c, s = acc
        return (c, round(s, 2))


class ToBqRow(beam.DoFn):
    """Build a BigQuery row, attaching the window start timestamp."""

    def process(self, element, window=beam.DoFn.WindowParam):
        merchant, (count, total) = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "merchant": merchant,
            "txn_count": count,
            "total_amount": total,
        }


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subscription", required=True)
    ap.add_argument("--output_table", required=True)
    args, beam_args = ap.parse_known_args()

    options = PipelineOptions(beam_args, save_main_session=True)
    options.view_as(StandardOptions).streaming = True

    schema = "window_start:TIMESTAMP,merchant:STRING,txn_count:INTEGER,total_amount:FLOAT"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.subscription)
            | "ParseValidate" >> beam.FlatMap(parse_and_keep_valid)
            | "Window30s" >> beam.WindowInto(FixedWindows(30))
            | "ToKV" >> beam.Map(lambda r: (r["merchant"], r["amount"]))
            | "CountSumPerMerchant" >> beam.CombinePerKey(CountSum())
            | "ToBqRow" >> beam.ParDo(ToBqRow())
            | "WriteBQ" >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARN)
    run()
