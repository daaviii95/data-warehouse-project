# Scenario Demos (1–5) — Aperture / ShopZada DWH

This guide maps each required demo scenario to **repo artifacts** (test files, SQL queries, and relevant pipeline behavior).

## Where the demo test files live

Scenario datasets are provided under:

- `data/Test/Scenario 1/`
- `data/Test/Scenario 2/`
- `data/Test/Scenario 3/`
- `data/Test/Scenario 4/`

> Note: the ingestion step searches `DATA_DIR` recursively and picks up matching patterns (e.g., `*order_data*`). For a clean demo of a single scenario, isolate the run by temporarily pointing `DATA_DIR` to the scenario folder or by temporarily moving other scenario folders out of `data/Test/`.

## Scenario 1 — New Daily Orders File (End-to-End Incremental Test)

**Goal**: Show incremental load increases the order fact count by the expected number of new orders and updates dashboard KPIs for the target date.

- **Test files**: `data/Test/Scenario 1/*`
  - Orders: `order_data_20240115_test.csv`
  - Order↔Merchant: `order_with_merchant_data_test.csv`
  - Line items: `line_item_data_prices_test.csv`, `line_item_data_products_test.csv`
  - Campaign txns: `transactional_campaign_data_test.csv`
- **Before/after evidence**:
  - `SELECT COUNT(*) FROM fact_orders;`
  - Dashboard KPI for the target date
  - Re-run the pipeline, then re-check counts and KPI

## Scenario 2 — New Customer + New Product (Dimension Creation)

**Goal**: Show the pipeline creates missing dimensions from fact data and successfully links new dimension surrogate keys into facts.

- **Test files**: `data/Test/Scenario 2/*`
- **What to show**:
  - Before: the new `user_id` not present in `dim_user`, new `product_id` not present in `dim_product`
  - After: new rows exist in `dim_user` / `dim_product` with valid surrogate keys
  - After: corresponding fact rows reference those surrogate keys

## Scenario 3 — Late & Missing Campaign Data (Unknown + Late-Arriving Update)

**Goal**: Show missing/unknown campaign codes map to the **Unknown** campaign initially, then later update to the real campaign when dimension data arrives.

- **Step 1 files**: `data/Test/Scenario 3/transactional_campaign_data_scenario3_step1.csv` (+ matching order files)
- **Step 2 files**: `data/Test/Scenario 3/campaign_data_scenario3_step2.csv`

**Business rule summary**:
- Missing or non-existent `campaign_id` → map to `dim_campaign.campaign_id = 'UNKNOWN'`
- After loading the missing campaign(s), the pipeline updates affected rows in `fact_campaign_transactions` from Unknown to the correct campaign surrogate keys.

## Scenario 4 — Data Quality Failure & Error Handling (Reject Tables)

**Goal**: Show valid records load to facts while invalid records are isolated into reject tables (with error reasons + raw JSON).

- **Test files**: `data/Test/Scenario 4/*`
- **Evidence**:
  - Valid rows present in facts
  - Invalid rows appear in:
    - `reject_fact_orders`
    - `reject_fact_line_items`
    - `reject_fact_campaign_transactions`

## Scenario 5 — Performance & Aggregation Consistency (SQL vs Dashboard)

**Goal**: Show dashboard KPI matches an equivalent SQL aggregation and measure query performance.

- **SQL file**: `sql/utilities/scenario5_verification_queries.sql`
- **Recommended KPI**: Monthly Sales Revenue (Option 1 in the SQL file)
- **Performance**: use `EXPLAIN ANALYZE` queries in the same file and report execution time.


