-- Milestone 6: BI layer — curated views for Looker Studio / Tableau.
-- Dashboards should read from stable, well-named views (not raw tables), so you
-- can refactor underlying tables without breaking the dashboard.

-- 1) Daily merchant performance (from the partitioned/clustered mart)
CREATE OR REPLACE VIEW `payments.v_merchant_daily` AS
SELECT
  txn_date,
  merchant,
  txn_count,
  approved_count,
  SAFE_DIVIDE(approved_count, txn_count)        AS approval_rate,
  total_amount,
  approved_amount,
  avg_amount
FROM `payments.mart_daily_merchant`;

-- 2) Headline KPIs (single-row, for scorecards)
CREATE OR REPLACE VIEW `payments.v_kpis` AS
SELECT
  COUNT(*)                                       AS total_transactions,
  COUNT(DISTINCT customer_id)                    AS unique_customers,
  ROUND(SUM(amount), 2)                          AS gross_amount,
  ROUND(AVG(amount), 2)                          AS avg_ticket,
  ROUND(SAFE_DIVIDE(COUNTIF(status = 'approved'), COUNT(*)), 4) AS approval_rate
FROM `payments.staging_transactions`;

-- 3) Data-quality summary (so the dashboard can show pipeline health)
CREATE OR REPLACE VIEW `payments.v_data_quality` AS
SELECT
  (SELECT COUNT(*) FROM `payments.raw_transactions`)     AS raw_rows,
  (SELECT COUNT(*) FROM `payments.staging_transactions`) AS clean_rows,
  (SELECT COUNT(*) FROM `payments.rejects`)              AS rejected_rows,
  ROUND(SAFE_DIVIDE(
    (SELECT COUNT(*) FROM `payments.rejects`),
    (SELECT COUNT(*) FROM `payments.raw_transactions`)), 4) AS deadletter_rate;
