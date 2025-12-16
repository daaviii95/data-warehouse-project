# Scenario 1 Test Data Guide

## Overview

This document describes the test data files for **Scenario 1: New Daily Orders File (End-to-End Pipeline Test)** demonstration.

## Test Data Files

### 1. Order Data

**File**: `data/Test/order_data_scenario1.csv`

**Purpose**: Contains 5 new orders for a specific date (2024-01-20) to demonstrate incremental batch loading.

**Contents**:
- 5 orders with IDs: `scenario1-order-001` through `scenario1-order-005`
- All orders dated: **2024-01-20** (target date for dashboard KPI)
- Uses existing user_ids and staff_ids (should exist in dimensions)
- Total expected revenue: **$641.49** (sum of total_amount column)

**Expected Behavior**:
- When loaded, these orders should be added to `fact_orders`
- Dashboard should show updated daily sales for 2024-01-20
- Row count in `fact_orders` should increase by 5

---

### 2. Line Item Prices

**File**: `data/Test/line_item_data_prices_scenario1.csv`

**Purpose**: Contains price and quantity data for line items in the test orders.

**Contents**:
- 7 line items across 5 orders
- Prices and quantities for each line item
- Matches order_ids from order_data_scenario1.csv

**Expected Behavior**:
- When loaded, these should be added to `fact_line_items`
- Row count in `fact_line_items` should increase by 7

---

### 3. Line Item Products

**File**: `data/Test/line_item_data_products_scenario1.csv`

**Purpose**: Contains product information for line items.

**Contents**:
- 7 line items matching the prices file
- Uses existing product_ids (should exist in dimensions)
- Matches order_ids from order_data_scenario1.csv

**Expected Behavior**:
- When loaded, product information should be linked to line items
- Products should already exist in `dim_product`

---

### 4. Order with Merchant Data

**File**: `data/Test/order_with_merchant_data_scenario1.csv`

**Purpose**: Contains merchant and staff assignments for orders.

**Contents**:
- 5 rows matching the 5 orders
- Uses existing merchant_ids and staff_ids (should exist in dimensions)
- Matches order_ids from order_data_scenario1.csv

**Expected Behavior**:
- When loaded, merchant and staff information should be linked to orders
- Merchants and staff should already exist in dimensions

---

## Testing Steps

### Step 1: Show BEFORE State

1. **Record fact table row count**:
   ```sql
   -- Count orders before
   SELECT COUNT(*) AS order_count_before FROM fact_orders;
   
   -- Count line items before
   SELECT COUNT(*) AS line_item_count_before FROM fact_line_items;
   ```

2. **Record Dashboard KPI for target date (2024-01-20)**:
   - Open Power BI dashboard
   - Filter to date: **2024-01-20**
   - Record the following metrics:
     - Total Orders: _____
     - Total Revenue: $_____
     - Total Line Items: _____

3. **Show test CSV file contents**:
   - Display contents of all 4 test files
   - Note the expected totals:
     - 5 orders
     - 7 line items
     - Total revenue: $641.49

---

### Step 2: Run Full Workflow

1. **Copy test files to appropriate folders**:
   ```bash
   # Copy to Operations Department folder
   cp data/Test/order_data_scenario1.csv "data/Operations Department/"
   cp data/Test/line_item_data_prices_scenario1.csv "data/Operations Department/"
   cp data/Test/line_item_data_products_scenario1.csv "data/Operations Department/"
   cp data/Test/order_with_merchant_data_scenario1.csv "data/Operations Department/"
   ```

2. **Run ETL pipeline**:
   - Execute Airflow DAG: `shopzada_etl_pipeline`
   - Or run Python script: `python scripts/etl_pipeline_python.py`
   - Monitor logs for successful processing

3. **Verify ingestion**:
   - Check staging tables have new data
   - Verify transformation completed
   - Confirm load operations succeeded

---

### Step 3: Show AFTER State

1. **Verify fact table row count increased**:
   ```sql
   -- Count orders after
   SELECT COUNT(*) AS order_count_after FROM fact_orders;
   
   -- Count line items after
   SELECT COUNT(*) AS line_item_count_after FROM fact_line_items;
   
   -- Calculate difference
   SELECT 
       (SELECT COUNT(*) FROM fact_orders) - :order_count_before AS orders_added,
       (SELECT COUNT(*) FROM fact_line_items) - :line_item_count_before AS line_items_added;
   ```
   - Expected: **5 orders added**, **7 line items added**

2. **Verify specific orders loaded**:
   ```sql
   SELECT 
       order_id,
       user_id,
       transaction_date,
       total_amount
   FROM fact_orders
   WHERE order_id LIKE 'scenario1-%'
   ORDER BY order_id;
   ```
   - Should show all 5 test orders

3. **Verify dashboard updated**:
   - Refresh Power BI dashboard
   - Filter to date: **2024-01-20**
   - Verify updated metrics:
     - Total Orders: Should increase by 5
     - Total Revenue: Should increase by $641.49
     - Total Line Items: Should increase by 7

4. **Manual computation verification**:
   ```sql
   -- Calculate total revenue for scenario1 orders
   SELECT 
       SUM(fli.quantity * dp.price) AS calculated_total_revenue
   FROM fact_orders fo
   INNER JOIN fact_line_items fli ON fo.order_id = fli.order_id
   INNER JOIN dim_product dp ON fli.product_sk = dp.product_sk
   WHERE fo.order_id LIKE 'scenario1-%';
   ```
   - Should match sum of `total_amount` from order_data_scenario1.csv

---

## SQL Queries for Verification

### Before State
```sql
-- Record before state
SELECT 
    COUNT(*) AS fact_orders_count_before,
    (SELECT COUNT(*) FROM fact_line_items) AS fact_line_items_count_before
FROM fact_orders;

-- Dashboard KPI for 2024-01-20 (before)
SELECT 
    dd.date,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fli.quantity * dp.price) AS total_revenue,
    COUNT(fli.line_item_id) AS total_line_items
FROM fact_orders fo
LEFT JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
WHERE dd.date = '2024-01-20'
GROUP BY dd.date;
```

### After State
```sql
-- Verify new orders loaded
SELECT COUNT(*) AS new_orders
FROM fact_orders
WHERE order_id LIKE 'scenario1-%';

-- Verify new line items loaded
SELECT COUNT(*) AS new_line_items
FROM fact_line_items
WHERE order_id LIKE 'scenario1-%';

-- Dashboard KPI for 2024-01-20 (after)
SELECT 
    dd.date,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fli.quantity * dp.price) AS total_revenue,
    COUNT(fli.line_item_id) AS total_line_items
FROM fact_orders fo
LEFT JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
WHERE dd.date = '2024-01-20'
GROUP BY dd.date;
```

---

## Expected Results Summary

| Metric | Before | After | Difference |
|--------|--------|-------|------------|
| **fact_orders row count** | X | X + 5 | +5 |
| **fact_line_items row count** | Y | Y + 7 | +7 |
| **Dashboard: Total Orders (2024-01-20)** | A | A + 5 | +5 |
| **Dashboard: Total Revenue (2024-01-20)** | $B | $B + $641.49 | +$641.49 |
| **Dashboard: Total Line Items (2024-01-20)** | C | C + 7 | +7 |

---

## Explanation Template

**How this confirms ingestion → transformation → load → analytics:**

"This demonstration confirms the end-to-end data pipeline by showing that new CSV files are successfully ingested into staging tables, transformed to match the data warehouse schema, loaded into fact and dimension tables with proper foreign key relationships, and immediately reflected in the analytical dashboard. The before/after comparison validates that the incremental batch load correctly adds new records without duplicating existing data, and the dashboard KPI update demonstrates that the analytical layer automatically reflects changes in the underlying data warehouse, confirming the complete data flow from source files to business intelligence."

---

## Cleanup

After testing, you may want to remove test data:

```sql
-- Remove test line items first (foreign key constraint)
DELETE FROM fact_line_items 
WHERE order_id LIKE 'scenario1-%';

-- Remove test orders
DELETE FROM fact_orders 
WHERE order_id LIKE 'scenario1-%';
```

---

## Notes

- Ensure user_ids (USER16283, USER28531, etc.) exist in `dim_user`
- Ensure staff_ids (STAFF0048737, STAFF0012345, etc.) exist in `dim_staff`
- Ensure merchant_ids (MERCHANT29694, MERCHANT12345, etc.) exist in `dim_merchant`
- Ensure product_ids (PRODUCT16794, PRODUCT56387, etc.) exist in `dim_product`
- The date (2024-01-20) should exist in `dim_date` (populated by `02_populate_dim_date.sql`)
- File format: CSV with comma separators
- All test data uses `scenario1-` prefix for easy identification

