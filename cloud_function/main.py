"""Cloud Function (gen2): load files from the landing bucket into BigQuery.

Triggered when an object is finalized in the landing GCS bucket. JSON Lines files
are loaded into payments.raw_landing (append); other objects are ignored.
"""
import os

import functions_framework
from google.cloud import bigquery


@functions_framework.cloud_event
def load_to_bq(cloud_event):
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]

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
    load_job.result()
    out = client.get_table(table)
    print(f"Loaded {uri} -> {table}. Table now has {out.num_rows} rows.")
