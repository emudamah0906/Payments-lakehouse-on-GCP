"""
Project 1 — Milestone 4:  Data Quality & Reconciliation checks on BigQuery.

This is a lightweight version of what Great Expectations / dbt tests do: declare
"expectations" about the data, run them, fail loudly if any break. In production
you'd wire the exit code into the pipeline/orchestrator (Airflow) and alert on it.

Checks implemented:
  1. Volume      - staging is non-empty
  2. Not-null    - key columns never null in staging
  3. Validity    - all amounts > 0; all currencies in the allowed set
  4. Uniqueness  - transaction_id is unique in staging
  5. Reconciliation - raw_count == staging_count + rejects_count  (no data lost)
  6. Freshness   - dead-letter rate is within an acceptable threshold

Run:
  GOOGLE_CLOUD_PROJECT=<project> python run_checks.py
"""
import os
import sys
from google.cloud import bigquery

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
DS = "payments"
ALLOWED_CURRENCIES = ("CAD", "USD", "EUR")
MAX_DEADLETTER_RATE = 0.10  # fail if >10% of records are rejected

client = bigquery.Client(project=PROJECT)
results = []


def check(name, sql, predicate, detail_fn):
    """Run a query, apply predicate to the single result row, record pass/fail."""
    row = list(client.query(sql))[0]
    ok = predicate(row)
    results.append((ok, name, detail_fn(row)))


def t(name):  # fully-qualified table
    return f"`{PROJECT}.{DS}.{name}`"


# 1. Volume
check("staging_non_empty",
      f"SELECT COUNT(*) AS n FROM {t('staging_transactions')}",
      lambda r: r.n > 0,
      lambda r: f"{r.n} rows")

# 2. Not-null key columns
check("no_null_keys",
      f"""SELECT COUNTIF(transaction_id IS NULL OR customer_id IS NULL
                         OR amount IS NULL) AS bad
          FROM {t('staging_transactions')}""",
      lambda r: r.bad == 0,
      lambda r: f"{r.bad} null-key rows")

# 3a. Validity: positive amounts
check("amounts_positive",
      f"SELECT COUNTIF(amount <= 0) AS bad FROM {t('staging_transactions')}",
      lambda r: r.bad == 0,
      lambda r: f"{r.bad} non-positive amounts")

# 3b. Validity: known currencies
check("currencies_valid",
      f"""SELECT COUNTIF(currency NOT IN {ALLOWED_CURRENCIES}) AS bad
          FROM {t('staging_transactions')}""",
      lambda r: r.bad == 0,
      lambda r: f"{r.bad} invalid-currency rows")

# 4. Uniqueness of transaction_id
check("transaction_id_unique",
      f"""SELECT COUNT(*) - COUNT(DISTINCT transaction_id) AS dupes
          FROM {t('staging_transactions')}""",
      lambda r: r.dupes == 0,
      lambda r: f"{r.dupes} duplicate ids")

# 5. Reconciliation: nothing lost between raw and (staging + rejects)
check("reconciliation_raw_vs_outputs",
      f"""SELECT
            (SELECT COUNT(*) FROM {t('raw_transactions')})  AS raw_n,
            (SELECT COUNT(*) FROM {t('staging_transactions')}) AS stg_n,
            (SELECT COUNT(*) FROM {t('rejects')})            AS rej_n""",
      lambda r: r.raw_n == r.stg_n + r.rej_n,
      lambda r: f"raw={r.raw_n}, staging={r.stg_n}, rejects={r.rej_n}")

# 6. Dead-letter rate within threshold
check("deadletter_rate_ok",
      f"""SELECT SAFE_DIVIDE(
            (SELECT COUNT(*) FROM {t('rejects')}),
            (SELECT COUNT(*) FROM {t('raw_transactions')})) AS rate""",
      lambda r: (r.rate or 0) <= MAX_DEADLETTER_RATE,
      lambda r: f"rate={ (r.rate or 0):.2%} (threshold {MAX_DEADLETTER_RATE:.0%})")


# ---- Report ----
print("\n  DATA QUALITY REPORT")
print("  " + "-" * 56)
passed = 0
for ok, name, detail in results:
    flag = "PASS" if ok else "FAIL"
    print(f"  [{flag}] {name:<32} {detail}")
    passed += ok
print("  " + "-" * 56)
print(f"  {passed}/{len(results)} checks passed\n")

# Non-zero exit on any failure -> orchestrator can halt the pipeline
sys.exit(0 if passed == len(results) else 1)
