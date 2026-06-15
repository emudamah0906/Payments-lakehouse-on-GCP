# Milestone 6 — Build the Looker Studio dashboard (5 minutes)

Looker Studio is free and web-based. You'll connect it to the BigQuery views
we created and drop in a few charts. This gives you the JD's
"validating analytics and visualization layers using Looker Studio."

## Steps
1. Go to **https://lookerstudio.google.com** → sign in with the same Google
   account (maheshemuda0906@gmail.com).
2. **Create → Report**. When prompted to add data, choose the **BigQuery** connector.
3. Authorize, then pick:
   - Project: **My First Project**
   - Dataset: **payments**
   - Table/View: **v_merchant_daily**  → Add.
4. Build these (drag from the right panel):
   - **Scorecards (KPIs):** add a second data source = `v_kpis`. Add scorecards for
     `total_transactions`, `gross_amount`, `avg_ticket`, `approval_rate`.
   - **Time series chart:** dimension = `txn_date`, metric = `total_amount`. Breakdown by `merchant`.
   - **Bar chart:** dimension = `merchant`, metric = `total_amount` (sorted desc).
   - **Table:** `merchant`, `txn_count`, `approval_rate`, `total_amount`.
   - **Data-quality scorecard:** add source `v_data_quality`, show `deadletter_rate`.
5. Add a **date range control** wired to `txn_date` so viewers can filter.
6. **Share → anyone with the link can view**, and copy the URL into your portfolio.

## What to say in the interview
"I model a curated `v_merchant_daily` view on top of the partitioned mart, then
build the Looker Studio dashboard on the **view** — never the raw tables — so the
storage layer can change without breaking reporting. I also surface a
**data-quality scorecard** (dead-letter rate) so stakeholders trust the numbers."

## (Tableau alternative)
Same idea: Tableau → Connect → Google BigQuery → `payments.v_merchant_daily`.
The view layer means the BI tool is interchangeable.
