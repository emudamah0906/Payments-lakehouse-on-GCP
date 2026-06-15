"""
Project 1 — Milestone 5:  Cloud Function (2nd gen) — event-driven ingestion.

Trigger: a file is finalized (uploaded) in the landing GCS bucket.
Action:  load that JSONL file into BigQuery payments.raw_landing (append).

This is the "Cloud Functions for lightweight, event-based processing" + the
"file-arrival trigger" pattern from the JD and resume. In a bigger system the
function would instead kick off a Dataflow job; here it loads directly to keep
the demo tight.
"""
import os

import functions_framework
from google.cloud import bigquery


@functions_framework.cloud_event
def load_to_bq(cloud_event):
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]

    # Only act on JSON Lines files (ignore folders, temp files, etc.)
    if not name.endswith(".jsonl"):
        print(f"Skipping non-jsonl object: {name}")
        return

    uri = f"gs://{bucket}/{name}"
    dataset = os.environ.get("BQ_DATASET", "payments")
    table = f"{dataset}.raw_landing"

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_uri(uri, table, job_config=job_config)
    load_job.result()  # wait for completion
    out = client.get_table(table)
    print(f"Loaded {uri} -> {table}. Table now has {out.num_rows} rows.")
