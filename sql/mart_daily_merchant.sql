-- Gold mart: daily revenue and approval metrics per merchant.
--
-- Partitioned by txn_date so date-range queries scan only the relevant
-- partitions, and clustered by merchant so filters and aggregations on merchant
-- read fewer blocks. Both reduce bytes scanned (cost) and improve performance.

CREATE OR REPLACE TABLE `payments.mart_daily_merchant`
PARTITION BY txn_date
CLUSTER BY merchant AS
SELECT
  DATE(event_time)                                  AS txn_date,
  merchant,
  COUNT(*)                                          AS txn_count,
  COUNTIF(status = 'approved')                      AS approved_count,
  ROUND(SUM(amount), 2)                             AS total_amount,
  ROUND(AVG(amount), 2)                             AS avg_amount,
  ROUND(SUM(IF(status = 'approved', amount, 0)), 2) AS approved_amount
FROM `payments.staging_transactions`
GROUP BY txn_date, merchant;
