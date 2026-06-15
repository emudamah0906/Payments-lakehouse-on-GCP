# Dashboard (Looker Studio)

The BI layer is built on the reporting views created by `sql/bi_views.sql`.
Dashboards connect to the views rather than the underlying tables.

## Build steps
1. Open https://lookerstudio.google.com and sign in.
2. **Create → Report**, then add a **BigQuery** data source.
3. Select project → dataset `payments` → view `v_merchant_daily`.
4. Add components:
   - **Scorecards** (data source `v_kpis`): `total_transactions`, `gross_amount`,
     `avg_ticket`, `approval_rate`.
   - **Time series**: dimension `txn_date`, metric `total_amount`, breakdown by `merchant`.
   - **Bar chart**: dimension `merchant`, metric `total_amount` (sorted descending).
   - **Table**: `merchant`, `txn_count`, `approval_rate`, `total_amount`.
   - **Data-quality scorecard** (data source `v_data_quality`): `deadletter_rate`.
5. Add a **date range control** bound to `txn_date`.
6. Share the report and record the link.

## Tableau alternative
Connect Tableau to Google BigQuery and select `payments.v_merchant_daily`. Because
the BI layer depends on views, the reporting tool is interchangeable.
