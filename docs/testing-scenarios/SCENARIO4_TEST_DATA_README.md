# Scenario 4 Test Data - Data Quality Failure & Error Handling

## Overview

This folder contains test data files designed to test Scenario 4 requirements:
- Valid records that should be successfully loaded
- Invalid records that should be rejected and stored in reject tables

## Test Files

### 1. `order_data_scenario4.csv`

**Valid Records (11 rows):**
- `scenario4-order-001` through `scenario4-order-002`, `scenario4-order-008`, `scenario4-order-010` through `scenario4-order-015`
- All have valid user_id, transaction_date, and staff_id

**Invalid Records (4 rows):**
- `scenario4-order-003`: **Unknown foreign key** - `INVALID_USER_999` (user_id not in dim_user)
- `scenario4-order-004`: **Invalid date format** - `2025-13-45` (invalid date)
- `scenario4-order-005`: **Unknown foreign key** - `INVALID_STAFF_999` (staff_id not in dim_staff)
- `scenario4-order-006`: **Missing required field** - Empty user_id (NULL)
- `scenario4-order-007`: **Invalid date format** - `invalid-date-format` (unparseable date)
- `scenario4-order-009`: **Missing required field** - Empty staff_id (NULL)

**Expected Results:**
- 11 valid orders → `fact_orders` table
- 6 invalid orders → `reject_fact_orders` table with error reasons

### 2. `line_item_data_prices_scenario4.csv`

**Valid Records (16 rows):**
- All rows with valid order_ids that exist in fact_orders

**Invalid Records (2 rows):**
- `INVALID_ORDER_999`: **Unknown foreign key** - Order not in fact_orders
- `scenario4-order-016`: **Unknown foreign key** - Order not in fact_orders (order was rejected)

**Expected Results:**
- Valid line items → `fact_line_items` table
- Invalid line items → `reject_fact_line_items` table

### 3. `line_item_data_products_scenario4.csv`

**Valid Records (15 rows):**
- All rows with valid product_ids that exist in dim_product

**Invalid Records (3 rows):**
- `scenario4-order-009`: **Unknown foreign key** - `INVALID_PRODUCT_999` (product_id not in dim_product)
- `scenario4-order-016`: **Missing required field** - Empty product_id (NULL)
- `INVALID_ORDER_999`: **Unknown foreign key** - Order not in fact_orders

**Expected Results:**
- Valid line items → `fact_line_items` table
- Invalid line items → `reject_fact_line_items` table

### 4. `transactional_campaign_data_scenario4.csv`

**Valid Records (13 rows):**
- All rows with valid campaign_ids and order_ids

**Invalid Records (2 rows):**
- `scenario4-order-003`: **Unknown foreign key** - `INVALID_CAMPAIGN_999` (campaign_id not in dim_campaign, and Unknown campaign may not be available)
- `INVALID_ORDER_999`: **Unknown foreign key** - Order not in fact_orders

**Expected Results:**
- Valid campaign transactions → `fact_campaign_transactions` table
- Invalid transactions → `reject_fact_campaign_transactions` table

## Test Scenarios Covered

### ✅ Invalid Date Format
- `order_data_scenario4.csv`: `2025-13-45`, `invalid-date-format`

### ✅ Missing Required IDs
- `order_data_scenario4.csv`: Empty `user_id`, empty `staff_id`
- `line_item_data_products_scenario4.csv`: Empty `product_id`

### ✅ Unknown Foreign Key Values
- `order_data_scenario4.csv`: `INVALID_USER_999`, `INVALID_STAFF_999`
- `line_item_data_products_scenario4.csv`: `INVALID_PRODUCT_999`
- `transactional_campaign_data_scenario4.csv`: `INVALID_CAMPAIGN_999`
- All files: `INVALID_ORDER_999` (order not in fact_orders)

## How to Use

1. **Copy test files to appropriate directories:**
   ```bash
   # Order data
   cp data/Test/order_data_scenario4.csv "data/Operations Department/"
   
   # Line item data
   cp data/Test/line_item_data_prices_scenario4.csv "data/Operations Department/"
   cp data/Test/line_item_data_products_scenario4.csv "data/Operations Department/"
   
   # Campaign data
   cp data/Test/transactional_campaign_data_scenario4.csv "data/Marketing Department/"
   ```

2. **Run the ETL Pipeline:**
   - Trigger `shopzada_etl_pipeline` DAG in Airflow
   - Monitor logs for error messages

3. **Verify Results:**

   **Check Fact Tables (should only have valid records):**
   ```sql
   SELECT COUNT(*) FROM fact_orders WHERE order_id LIKE 'scenario4-%';
   -- Expected: 11 valid orders
   
   SELECT COUNT(*) FROM fact_line_items WHERE order_id LIKE 'scenario4-%';
   -- Expected: Valid line items only
   
   SELECT COUNT(*) FROM fact_campaign_transactions WHERE order_id LIKE 'scenario4-%';
   -- Expected: Valid campaign transactions only
   ```

   **Check Reject Tables (should have invalid records with error reasons):**
   ```sql
   SELECT * FROM reject_fact_orders WHERE order_id LIKE 'scenario4-%' ORDER BY rejected_at;
   -- Expected: 6 rejected orders with error_reason
   
   SELECT error_reason, COUNT(*) as count 
   FROM reject_fact_orders 
   WHERE order_id LIKE 'scenario4-%'
   GROUP BY error_reason;
   -- Expected: Summary of error types
   
   SELECT * FROM reject_fact_line_items ORDER BY rejected_at;
   -- Expected: Invalid line items
   
   SELECT * FROM reject_fact_campaign_transactions ORDER BY rejected_at;
   -- Expected: Invalid campaign transactions
   ```

4. **Review Airflow Logs:**
   - Check `load_fact` task logs
   - Look for summary of skipped records
   - Verify error reasons are logged

## Expected Error Reasons

### reject_fact_orders
- `"user_id 'INVALID_USER_999' not found in dim_user"`
- `"invalid transaction_date format '2025-13-45'"`
- `"staff_id 'INVALID_STAFF_999' not found in dim_staff"`
- `"user_id '' not found in dim_user"` (empty user_id)
- `"invalid transaction_date format 'invalid-date-format'"`
- `"staff_id '' not found in dim_staff"` (empty staff_id)

### reject_fact_line_items
- `"order_id 'INVALID_ORDER_999' not found in fact_orders"`
- `"product_id 'INVALID_PRODUCT_999' not found in dim_product"`
- `"product_id is NULL"`

### reject_fact_campaign_transactions
- `"campaign_id 'INVALID_CAMPAIGN_999' not found in dim_campaign and Unknown campaign not available"`
- `"order_id 'INVALID_ORDER_999' not found in fact_orders"`

## Notes

- Valid records use existing IDs from the actual data (USER28531, PRODUCT16794, etc.)
- Invalid records use clearly marked invalid IDs (INVALID_*, empty values)
- All dates are within the dim_date range (2020-2025)
- Test files are designed to be processed alongside existing data

