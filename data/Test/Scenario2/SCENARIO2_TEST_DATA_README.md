# Scenario 2 Test Data Guide

## Overview

This document describes the test data files for **Scenario 2: New Customer and New Product (Dimension Creation Test)** demonstration.

## Test Data Files

### 1. Order Data

**File**: `data/Test/order_data_scenario2.csv`

**Purpose**: Contains orders including one with a **NEW customer** that doesn't exist in `dim_user` yet.

**Contents**:
- 2 orders with IDs: `scenario2-order-001`, `scenario2-order-002`
- Order 001 uses: `NEW_USER_SCENARIO2` (NEW customer - doesn't exist in dim_user)
- Order 002 uses: `USER28531` (existing customer)
- All orders dated: **2024-01-21**

**Expected Behavior**:
- When loaded, `NEW_USER_SCENARIO2` should be **automatically created** in `dim_user`
- New customer should get a valid `user_sk` (surrogate key)
- Both orders should be loaded into `fact_orders` with correct foreign keys

---

### 2. Line Item Prices

**File**: `data/Test/line_item_data_prices_scenario2.csv`

**Purpose**: Contains price and quantity data for line items.

**Contents**:
- 5 line items across 2 orders
- Prices and quantities for each line item
- Matches order_ids from order_data_scenario2.csv

**Expected Behavior**:
- When loaded, these should be added to `fact_line_items`
- Row count in `fact_line_items` should increase by 5

---

### 3. Line Item Products

**File**: `data/Test/line_item_data_products_scenario2.csv`

**Purpose**: Contains product information including **NEW products** that don't exist in `dim_product` yet.

**Contents**:
- 5 line items matching the prices file
- Order 001 includes 2 NEW products:
  - `NEW_PRODUCT_SCENARIO2_ALPHA` (NEW product - doesn't exist in dim_product)
  - `NEW_PRODUCT_SCENARIO2_BETA` (NEW product - doesn't exist in dim_product)
- Order 001 also includes 1 existing product: `PRODUCT16794`
- Order 002 includes 2 existing products: `PRODUCT56387`, `PRODUCT26612`

**Expected Behavior**:
- When loaded, `NEW_PRODUCT_SCENARIO2_ALPHA` and `NEW_PRODUCT_SCENARIO2_BETA` should be **automatically created** in `dim_product`
- New products should get valid `product_sk` values (surrogate keys)
- All line items should be loaded into `fact_line_items` with correct foreign keys

---

### 4. Order with Merchant Data

**File**: `data/Test/order_with_merchant_data_scenario2.csv`

**Purpose**: Contains merchant and staff assignments for orders.

**Contents**:
- 2 rows matching the 2 orders
- Uses existing merchant_ids and staff_ids (should exist in dimensions)
- Matches order_ids from order_data_scenario2.csv

**Expected Behavior**:
- When loaded, merchant and staff information should be linked to orders
- Merchants and staff should already exist in dimensions

---

## Testing Steps

### Step 1: Show BEFORE State

1. **Verify new customer does NOT exist**:
   ```sql
   SELECT user_sk, user_id, name
   FROM dim_user
   WHERE user_id = 'NEW_USER_SCENARIO2';
   ```
   - **Expected**: No rows returned (customer doesn't exist)

2. **Verify new products do NOT exist**:
   ```sql
   SELECT product_sk, product_id, product_name
   FROM dim_product
   WHERE product_id IN ('NEW_PRODUCT_SCENARIO2_ALPHA', 'NEW_PRODUCT_SCENARIO2_BETA');
   ```
   - **Expected**: No rows returned (products don't exist)

3. **Show test CSV file contents**:
   - Display contents of all 4 test files
   - Highlight that `NEW_USER_SCENARIO2` and `NEW_PRODUCT_SCENARIO2_*` don't exist yet

---

### Step 2: Run Full Workflow

1. **Copy test files to appropriate folders**:
   ```bash
   # Copy to Operations Department folder
   cp data/Test/order_data_scenario2.csv "data/Operations Department/"
   cp data/Test/line_item_data_prices_scenario2.csv "data/Operations Department/"
   cp data/Test/line_item_data_products_scenario2.csv "data/Operations Department/"
   cp data/Test/order_with_merchant_data_scenario2.csv "data/Operations Department/"
   ```

2. **Run ETL pipeline**:
   - Execute Airflow DAG: `shopzada_etl_pipeline`
   - Or run Python script: `python scripts/etl_pipeline_python.py`
   - Monitor logs for:
     - "CREATING MISSING USERS FROM ORDER DATA (Scenario 2)"
     - "CREATING MISSING PRODUCTS FROM LINE ITEM DATA (Scenario 2)"
     - Dimension creation messages

3. **Verify dimension creation**:
   - Check logs for messages about creating new users/products
   - Verify no errors occurred during fact loading

---

### Step 3: Show AFTER State

1. **Verify new customer WAS created**:
   ```sql
   SELECT user_sk, user_id, name, email, country, city
   FROM dim_user
   WHERE user_id = 'NEW_USER_SCENARIO2';
   ```
   - **Expected**: 1 row returned
   - `user_sk` should be a valid integer (surrogate key)
   - `user_id` should be 'NEW_USER_SCENARIO2'
   - Other fields may be NULL (minimal dimension entry)

2. **Verify new products WERE created**:
   ```sql
   SELECT product_sk, product_id, product_name, product_type, price
   FROM dim_product
   WHERE product_id IN ('NEW_PRODUCT_SCENARIO2_ALPHA', 'NEW_PRODUCT_SCENARIO2_BETA')
   ORDER BY product_id;
   ```
   - **Expected**: 2 rows returned
   - Both should have valid `product_sk` values (surrogate keys)
   - Product names should match: "New Test Product Alpha", "New Test Product Beta"
   - Other fields may be NULL (minimal dimension entry)

3. **Verify fact rows reference new surrogate keys**:
   ```sql
   -- Check fact_orders references new customer correctly
   SELECT 
       fo.order_id,
       fo.user_sk,
       du.user_id,
       du.name AS customer_name
   FROM fact_orders fo
   INNER JOIN dim_user du ON fo.user_sk = du.user_sk
   WHERE fo.order_id = 'scenario2-order-001';
   ```
   - **Expected**: 1 row
   - `user_sk` should be valid (not NULL)
   - `user_id` should be 'NEW_USER_SCENARIO2'

4. **Verify fact_line_items reference new products correctly**:
   ```sql
   -- Check fact_line_items references new products correctly
   SELECT 
       fli.order_id,
       fli.product_sk,
       dp.product_id,
       dp.product_name
   FROM fact_line_items fli
   INNER JOIN dim_product dp ON fli.product_sk = dp.product_sk
   WHERE fli.order_id = 'scenario2-order-001'
     AND dp.product_id LIKE 'NEW_PRODUCT_SCENARIO2%'
   ORDER BY dp.product_id;
   ```
   - **Expected**: 2 rows (one for each new product)
   - Both should have valid `product_sk` values (not NULL)
   - Product IDs should match the new products

5. **Verify no NULL or broken foreign keys**:
   ```sql
   -- Check for NULL foreign keys in fact_orders
   SELECT COUNT(*) AS null_user_keys
   FROM fact_orders
   WHERE order_id LIKE 'scenario2-%' AND user_sk IS NULL;
   -- Expected: 0

   -- Check for NULL foreign keys in fact_line_items
   SELECT COUNT(*) AS null_product_keys
   FROM fact_line_items
   WHERE order_id LIKE 'scenario2-%' AND product_sk IS NULL;
   -- Expected: 0
   ```

6. **Verify dashboard shows new transaction**:
   - Open Power BI dashboard
   - Filter by customer: `NEW_USER_SCENARIO2`
   - Should show order `scenario2-order-001`
   - Filter by product: `NEW_PRODUCT_SCENARIO2_ALPHA` or `NEW_PRODUCT_SCENARIO2_BETA`
   - Should show line items from order `scenario2-order-001`

---

## SQL Queries for Verification

### Before State
```sql
-- Verify new customer does NOT exist
SELECT COUNT(*) AS customer_exists
FROM dim_user
WHERE user_id = 'NEW_USER_SCENARIO2';
-- Expected: 0

-- Verify new products do NOT exist
SELECT COUNT(*) AS products_exist
FROM dim_product
WHERE product_id IN ('NEW_PRODUCT_SCENARIO2_ALPHA', 'NEW_PRODUCT_SCENARIO2_BETA');
-- Expected: 0
```

### After State
```sql
-- Verify new customer WAS created with valid surrogate key
SELECT 
    user_sk,
    user_id,
    name,
    email,
    country,
    city
FROM dim_user
WHERE user_id = 'NEW_USER_SCENARIO2';
-- Expected: 1 row with valid user_sk

-- Verify new products WERE created with valid surrogate keys
SELECT 
    product_sk,
    product_id,
    product_name,
    product_type,
    price
FROM dim_product
WHERE product_id IN ('NEW_PRODUCT_SCENARIO2_ALPHA', 'NEW_PRODUCT_SCENARIO2_BETA')
ORDER BY product_id;
-- Expected: 2 rows, both with valid product_sk values

-- Verify fact_orders references new customer correctly
SELECT 
    fo.order_id,
    fo.user_sk,
    du.user_id,
    du.name AS customer_name
FROM fact_orders fo
INNER JOIN dim_user du ON fo.user_sk = du.user_sk
WHERE fo.order_id = 'scenario2-order-001';
-- Expected: 1 row, user_id = 'NEW_USER_SCENARIO2', user_sk is NOT NULL

-- Verify fact_line_items references new products correctly
SELECT 
    fli.order_id,
    fli.product_sk,
    dp.product_id,
    dp.product_name
FROM fact_line_items fli
INNER JOIN dim_product dp ON fli.product_sk = dp.product_sk
WHERE fli.order_id = 'scenario2-order-001'
  AND dp.product_id LIKE 'NEW_PRODUCT_SCENARIO2%'
ORDER BY dp.product_id;
-- Expected: 2 rows, both with valid product_sk values

-- Verify no broken foreign keys
SELECT 
    COUNT(*) AS orders_with_null_user_sk
FROM fact_orders
WHERE order_id LIKE 'scenario2-%' AND user_sk IS NULL;
-- Expected: 0

SELECT 
    COUNT(*) AS line_items_with_null_product_sk
FROM fact_line_items
WHERE order_id LIKE 'scenario2-%' AND product_sk IS NULL;
-- Expected: 0
```

---

## Expected Results Summary

| Component | Before | After | Status |
|-----------|--------|-------|--------|
| **dim_user: NEW_USER_SCENARIO2** | ❌ Does not exist | ✅ Exists with valid user_sk | ✅ Created |
| **dim_product: NEW_PRODUCT_SCENARIO2_ALPHA** | ❌ Does not exist | ✅ Exists with valid product_sk | ✅ Created |
| **dim_product: NEW_PRODUCT_SCENARIO2_BETA** | ❌ Does not exist | ✅ Exists with valid product_sk | ✅ Created |
| **fact_orders: scenario2-order-001** | ❌ Does not exist | ✅ Exists with valid user_sk | ✅ Loaded |
| **fact_line_items: new products** | ❌ Do not exist | ✅ Exist with valid product_sk | ✅ Loaded |
| **Foreign Keys** | N/A | ✅ All valid (no NULL) | ✅ Valid |
| **Dashboard: Filter by NEW_USER_SCENARIO2** | N/A | ✅ Shows order | ✅ Visible |
| **Dashboard: Filter by NEW_PRODUCT_SCENARIO2_*** | N/A | ✅ Shows line items | ✅ Visible |

---

## Explanation Template

**How the pipeline handles new dimension members:**

"The pipeline handles new dimension members through automatic dimension creation from fact data. Before loading facts, the system extracts unique user_ids from order data and product_ids from line item data, identifies which ones don't exist in the dimension tables, and creates minimal dimension entries with auto-generated surrogate keys. These new dimension members are then used when loading fact rows, ensuring all foreign key relationships are valid. The system uses upsert logic (ON CONFLICT DO UPDATE) to handle both new and existing dimension members, and surrogate keys are automatically generated by PostgreSQL's SERIAL type, guaranteeing unique identifiers for each dimension row."

---

## Cleanup

After testing, you may want to remove test data:

```sql
-- Remove test line items first (foreign key constraint)
DELETE FROM fact_line_items 
WHERE order_id LIKE 'scenario2-%';

-- Remove test orders
DELETE FROM fact_orders 
WHERE order_id LIKE 'scenario2-%';

-- Remove test dimensions (optional - keep for reference)
DELETE FROM dim_user 
WHERE user_id = 'NEW_USER_SCENARIO2';

DELETE FROM dim_product 
WHERE product_id IN ('NEW_PRODUCT_SCENARIO2_ALPHA', 'NEW_PRODUCT_SCENARIO2_BETA');
```

---

## Notes

- The system should automatically create missing dimensions before fact loading
- New dimensions are created with minimal data (NULL values for missing attributes)
- Surrogate keys are auto-generated by PostgreSQL
- Ensure staff_ids and merchant_ids exist in dimensions (they should already exist)
- The date (2024-01-21) should exist in `dim_date` (populated by `02_populate_dim_date.sql`)
- File format: CSV with comma separators
- All test data uses `scenario2-` prefix for easy identification
- New customer ID: `NEW_USER_SCENARIO2`
- New product IDs: `NEW_PRODUCT_SCENARIO2_ALPHA`, `NEW_PRODUCT_SCENARIO2_BETA`

