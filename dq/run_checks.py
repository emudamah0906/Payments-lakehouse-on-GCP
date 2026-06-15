"""Data quality and reconciliation checks for the payments dataset.

Declares a set of expectations about the BigQuery tables and validates them.
The process exits non-zero if any check fails, so it can be used as a gate in an
orchestrator (for example, an Airflow task that halts the DAG on failure).

Checks:
    1. Volume         - staging is non-empty
    2. Not-null       - key columns are never null in staging
    3. Validity       - amounts are positive; currencies are in the allowed set
    4. Uniqueness     - transaction_id is unique in staging
    5. Reconciliation - raw_count == staging_count + rejects_count
    6. Dead-letter    - reject rate is within the acceptable threshold

Usage:
    GOOGLE_CLOUD_PROJECT=<project> python run_checks.py
"""
import os
import sys

from google.cloud import bigquery

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
DS = "payments"
ALLOWED_CURRENCIES = ("CAD", "USD", "EUR")
MAX_DEADLETTER_RATE = 0.10

client = bigquery.Client(project=PROJECT)
results = []


def check(name, sql, predicate, detail_fn):
    row = list(client.query(sql))[0]
    ok = predicate(row)
    results.append((ok, name, detail_fn(row)))


def t(name):
    return f"`{PROJECT}.{DS}.{name}`"


check("staging_non_empty",
      f"SELECT COUNT(*) AS n FROM {t('staging_transactions')}",
      lambda r: r.n > 0,
      lambda r: f"{r.n} rows")

check("no_null_keys",
      f"""SELECT COUNTIF(transaction_id IS NULL OR customer_id IS NULL
                         OR amount IS NULL) AS bad
          FROM {t('staging_transactions')}""",
      lambda r: r.bad == 0,
      lambda r: f"{r.bad} null-key rows")

check("amounts_positive",
      f"SELECT COUNTIF(amount <= 0) AS bad FROM {t('staging_transactions')}",
      lambda r: r.bad == 0,
      lambda r: f"{r.bad} non-positive amounts")

check("currencies_valid",
      f"""SELECT COUNTIF(currency NOT IN {ALLOWED_CURRENCIES}) AS bad
          FROM {t('staging_transactions')}""",
      lambda r: r.bad == 0,
      lambda r: f"{r.bad} invalid-currency rows")

check("transaction_id_unique",
      f"""SELECT COUNT(*) - COUNT(DISTINCT transaction_id) AS dupes
          FROM {t('staging_transactions')}""",
      lambda r: r.dupes == 0,
      lambda r: f"{r.dupes} duplicate ids")

check("reconciliation_raw_vs_outputs",
      f"""SELECT
            (SELECT COUNT(*) FROM {t('raw_transactions')})     AS raw_n,
            (SELECT COUNT(*) FROM {t('staging_transactions')}) AS stg_n,
            (SELECT COUNT(*) FROM {t('rejects')})              AS rej_n""",
      lambda r: r.raw_n == r.stg_n + r.rej_n,
      lambda r: f"raw={r.raw_n}, staging={r.stg_n}, rejects={r.rej_n}")

check("deadletter_rate_ok",
      f"""SELECT SAFE_DIVIDE(
            (SELECT COUNT(*) FROM {t('rejects')}),
            (SELECT COUNT(*) FROM {t('raw_transactions')})) AS rate""",
      lambda r: (r.rate or 0) <= MAX_DEADLETTER_RATE,
      lambda r: f"rate={(r.rate or 0):.2%} (threshold {MAX_DEADLETTER_RATE:.0%})")


print("\n  DATA QUALITY REPORT")
print("  " + "-" * 56)
passed = 0
for ok, name, detail in results:
    flag = "PASS" if ok else "FAIL"
    print(f"  [{flag}] {name:<32} {detail}")
    passed += ok
print("  " + "-" * 56)
print(f"  {passed}/{len(results)} checks passed\n")

sys.exit(0 if passed == len(results) else 1)
