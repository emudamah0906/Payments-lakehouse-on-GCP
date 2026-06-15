-- Mart: daily revenue & approval metrics per merchant.
--
-- Two BigQuery optimizations interviewers always probe:
--   PARTITION BY txn_date  -> queries that filter on a date scan only those
--                            partitions (less data scanned = cheaper + faster).
--   CLUSTER BY merchant    -> rows are sorted/co-located by merchant, so filters
--                            and GROUP BY on merchant skip irrelevant blocks.
--
-- This is the "raw -> staging -> mart" gold layer, modeled for BI (Looker Studio).

CREATE OR REPLACE TABLE `payments.mart_daily_merchant`
PARTITION BY txn_date
CLUSTER BY merchant AS
SELECT
  DATE(event_time)                       AS txn_date,
  merchant,
  COUNT(*)                               AS txn_count,
  COUNTIF(status = 'approved')           AS approved_count,
  ROUND(SUM(amount), 2)                  AS total_amount,
  ROUND(AVG(amount), 2)                  AS avg_amount,
  ROUND(SUM(IF(status = 'approved', amount, 0)), 2) AS approved_amount
FROM `payments.staging_transactions`
GROUP BY txn_date, merchant;
