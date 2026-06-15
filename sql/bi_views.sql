-- Reporting views for BI tools (Looker Studio, Tableau).
-- Dashboards read from these stable views rather than the underlying tables, so
-- the storage layer can change without breaking reporting.

-- Daily merchant performance, derived from the partitioned/clustered mart.
CREATE OR REPLACE VIEW `payments.v_merchant_daily` AS
SELECT
  txn_date,
  merchant,
  txn_count,
  approved_count,
  SAFE_DIVIDE(approved_count, txn_count) AS approval_rate,
  total_amount,
  approved_amount,
  avg_amount
FROM `payments.mart_daily_merchant`;

-- Headline KPIs for scorecards.
CREATE OR REPLACE VIEW `payments.v_kpis` AS
SELECT
  COUNT(*)                                                      AS total_transactions,
  COUNT(DISTINCT customer_id)                                  AS unique_customers,
  ROUND(SUM(amount), 2)                                        AS gross_amount,
  ROUND(AVG(amount), 2)                                        AS avg_ticket,
  ROUND(SAFE_DIVIDE(COUNTIF(status = 'approved'), COUNT(*)), 4) AS approval_rate
FROM `payments.staging_transactions`;

-- Pipeline data-quality summary.
CREATE OR REPLACE VIEW `payments.v_data_quality` AS
SELECT
  (SELECT COUNT(*) FROM `payments.raw_transactions`)     AS raw_rows,
  (SELECT COUNT(*) FROM `payments.staging_transactions`) AS clean_rows,
  (SELECT COUNT(*) FROM `payments.rejects`)              AS rejected_rows,
  ROUND(SAFE_DIVIDE(
    (SELECT COUNT(*) FROM `payments.rejects`),
    (SELECT COUNT(*) FROM `payments.raw_transactions`)), 4) AS deadletter_rate;
