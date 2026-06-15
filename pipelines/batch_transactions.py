"""
Project 1 — Milestone 1:  Apache Beam BATCH pipeline (runs locally on DirectRunner)

What this teaches (and what you'll be asked in the interview):
  * Pipeline / PCollection / PTransform  -> the core Beam model
  * Map vs FlatMap vs ParDo               -> element-wise transforms
  * Branching a PCollection + tagged outputs (valid vs rejected records)
  * CombinePerKey / GroupByKey            -> aggregations (the "GROUP BY" of Beam)
  * Why the SAME code runs locally (DirectRunner) and on GCP (DataflowRunner)

Pipeline does what a real "raw -> staging -> mart" layer does:
  read raw JSONL  ->  parse  ->  VALIDATE (split good/bad)
                                   |-> clean/enrich good records  -> write staging
                                   |-> aggregate by merchant      -> write mart
                                   `-> write rejects to a dead-letter file

Run:
  python3 batch_transactions.py \
      --input ../data/raw_transactions.jsonl \
      --output_dir ../data/out
"""
import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

VALID_CURRENCIES = {"CAD", "USD", "EUR"}


# ---- A ParDo (DoFn) that parses + validates each line --------------------------
# We emit good records to the main output and bad ones to a tagged "rejects"
# output. This is the standard "dead-letter queue" pattern interviewers love.
class ParseAndValidate(beam.DoFn):
    REJECTS = "rejects"

    def process(self, line):
        # 1) Parse JSON safely
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            yield beam.pvalue.TaggedOutput(self.REJECTS, {"raw": line, "error": "bad_json"})
            return

        # 2) Validate business rules
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
            yield rec  # main (valid) output


def enrich(rec: dict) -> dict:
    """Light transformation: add a derived column + normalize. Returns a new dict."""
    rec["merchant"] = rec["merchant"].strip().lower()
    rec["amount_band"] = (
        "small" if rec["amount"] < 50 else "medium" if rec["amount"] < 500 else "large"
    )
    return rec


def to_merchant_kv(rec: dict):
    """Map each record to (merchant, amount) so we can aggregate per merchant."""
    return (rec["merchant"], rec["amount"])


def format_mart_row(kv) -> str:
    merchant, total = kv
    return json.dumps({"merchant": merchant, "total_amount": round(total, 2)})


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output_dir", required=True)
    args, beam_args = ap.parse_known_args()

    # PipelineOptions carries runner config. With no --runner flag it uses
    # DirectRunner (local). On GCP you'd pass --runner=DataflowRunner + project/region.
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        # Read raw lines -> a PCollection[str]
        lines = p | "ReadRaw" >> beam.io.ReadFromText(args.input)

        # Parse + validate, splitting into main (valid) and 'rejects' outputs
        parsed = lines | "ParseValidate" >> beam.ParDo(
            ParseAndValidate()
        ).with_outputs(ParseAndValidate.REJECTS, main="valid")

        valid = parsed.valid
        rejects = parsed[ParseAndValidate.REJECTS]

        # --- Staging branch: clean/enrich valid records, write out ---
        staging = valid | "Enrich" >> beam.Map(enrich)
        (
            staging
            | "StagingToJson" >> beam.Map(json.dumps)
            | "WriteStaging" >> beam.io.WriteToText(
                f"{args.output_dir}/staging", file_name_suffix=".jsonl"
            )
        )

        # --- Mart branch: total amount per merchant (CombinePerKey == GROUP BY SUM) ---
        (
            staging
            | "ToMerchantKV" >> beam.Map(to_merchant_kv)
            | "SumPerMerchant" >> beam.CombinePerKey(sum)
            | "FormatMart" >> beam.Map(format_mart_row)
            | "WriteMart" >> beam.io.WriteToText(
                f"{args.output_dir}/mart_merchant_totals", file_name_suffix=".jsonl"
            )
        )

        # --- Dead-letter branch: keep the bad records for inspection ---
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
