"""Batch ingestion pipeline for raw payment transactions.

Reads newline-delimited JSON, validates each record against business rules, and
writes three outputs:

  * staging                  - cleaned and enriched valid records
  * mart_merchant_totals     - total amount aggregated per merchant
  * rejects                  - invalid records with an error reason (dead-letter)

Invalid records are routed to the dead-letter output instead of being dropped,
so no data is lost and a single malformed record never fails the job.

The pipeline runs unchanged on the local DirectRunner or on Cloud Dataflow
(add --runner=DataflowRunner --project --region --temp_location).

Usage:
    python batch_transactions.py \
        --input ../data/raw_transactions.jsonl \
        --output_dir ../data/out
"""
import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

VALID_CURRENCIES = {"CAD", "USD", "EUR"}


class ParseAndValidate(beam.DoFn):
    """Parse a JSON line and validate it.

    Valid records go to the main output; invalid records are emitted to the
    'rejects' tagged output with the list of failed rules.
    """

    REJECTS = "rejects"

    def process(self, line):
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            yield beam.pvalue.TaggedOutput(self.REJECTS, {"raw": line, "error": "bad_json"})
            return

        errors = []
        if not rec.get("customer_id"):
            errors.append("missing_customer_id")
        if not isinstance(rec.get("amount"), (int, float)) or rec.get("amount", 0) <= 0:
            errors.append("invalid_amount")
        if rec.get("currency") not in VALID_CURRENCIES:
            errors.append("invalid_currency")

        if errors:
            rec["errors"] = errors
            yield beam.pvalue.TaggedOutput(self.REJECTS, rec)
        else:
            yield rec


def enrich(rec: dict) -> dict:
    """Normalize the merchant field and derive an amount band."""
    rec["merchant"] = rec["merchant"].strip().lower()
    rec["amount_band"] = (
        "small" if rec["amount"] < 50 else "medium" if rec["amount"] < 500 else "large"
    )
    return rec


def to_merchant_kv(rec: dict):
    return (rec["merchant"], rec["amount"])


def format_mart_row(kv) -> str:
    merchant, total = kv
    return json.dumps({"merchant": merchant, "total_amount": round(total, 2)})


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output_dir", required=True)
    args, beam_args = ap.parse_known_args()

    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        lines = p | "ReadRaw" >> beam.io.ReadFromText(args.input)

        parsed = lines | "ParseValidate" >> beam.ParDo(
            ParseAndValidate()
        ).with_outputs(ParseAndValidate.REJECTS, main="valid")

        valid = parsed.valid
        rejects = parsed[ParseAndValidate.REJECTS]

        # Staging: cleaned and enriched valid records.
        staging = valid | "Enrich" >> beam.Map(enrich)
        (
            staging
            | "StagingToJson" >> beam.Map(json.dumps)
            | "WriteStaging" >> beam.io.WriteToText(
                f"{args.output_dir}/staging", file_name_suffix=".jsonl"
            )
        )

        # Mart: total amount per merchant.
        (
            staging
            | "ToMerchantKV" >> beam.Map(to_merchant_kv)
            | "SumPerMerchant" >> beam.CombinePerKey(sum)
            | "FormatMart" >> beam.Map(format_mart_row)
            | "WriteMart" >> beam.io.WriteToText(
                f"{args.output_dir}/mart_merchant_totals", file_name_suffix=".jsonl"
            )
        )

        # Dead-letter: invalid records retained for inspection and reprocessing.
        (
            rejects
            | "RejectsToJson" >> beam.Map(json.dumps)
            | "WriteRejects" >> beam.io.WriteToText(
                f"{args.output_dir}/rejects", file_name_suffix=".jsonl"
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
