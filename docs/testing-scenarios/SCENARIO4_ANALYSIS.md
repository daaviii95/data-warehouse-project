# Scenario 4 - Data Quality Failure & Error Handling Analysis

## Current System Capabilities

### ✅ What the System Currently Handles

1. **Invalid Date Format Detection**
   - ✅ Detects null/invalid transaction dates
   - ✅ Skips rows with unparseable dates
   - ✅ Logs skipped records (first 5 examples)
   - **Location**: `scripts/load_fact.py:230-256`

2. **Missing Required IDs Detection**
   - ✅ Detects missing user_id, merchant_id, staff_id, product_id
   - ✅ Skips rows with missing foreign keys
   - ✅ Logs skipped records with reasons
   - **Location**: `scripts/load_fact.py:212-226`

3. **Unknown Foreign Key Values**
   - ✅ Validates foreign keys exist in dimension tables
   - ✅ Skips rows with non-existent foreign keys
   - ✅ Logs which foreign key failed
   - **Location**: `scripts/load_fact.py:208-256`

4. **Error Logging**
   - ✅ Comprehensive logging of skipped records
   - ✅ Diagnostic counters for each error type
   - ✅ Summary logs showing total skipped records
   - **Location**: `scripts/load_fact.py:295-324`

5. **Valid Rows Successfully Loaded**
   - ✅ Only valid rows are inserted into fact tables
   - ✅ Uses ON CONFLICT for idempotent operations
   - ✅ No silent data corruption

### ❌ What's Missing for Scenario 4 Compliance

1. **Reject/Error Table**
   - ❌ No persistent storage for rejected records
   - ❌ Invalid rows are logged but not queryable
   - ❌ Cannot review rejected records after ETL completes

2. **Structured Error Tracking**
   - ❌ Errors are logged but not stored in database
   - ❌ No way to query error statistics
   - ❌ No audit trail of data quality issues

3. **Error Details Storage**
   - ❌ Missing specific error reason per record
   - ❌ No timestamp of when error occurred
   - ❌ No source file tracking for rejected records

## Required Implementation

### Step 1: Create Reject Tables Schema

Create tables to store rejected records:
- `reject_fact_orders` - Stores invalid order records
- `reject_fact_line_items` - Stores invalid line item records
- `reject_fact_campaign_transactions` - Stores invalid campaign transaction records

### Step 2: Modify Load Functions

Update `load_fact.py` to:
- Write invalid records to reject tables
- Include error reason for each rejected record
- Track source file and timestamp

### Step 3: Error Logging Enhancement

Ensure all errors are:
- Logged to Airflow logs (already done)
- Stored in reject tables (needs implementation)
- Queryable for reporting

## Data Quality Rules Implemented

1. **Referential Integrity**
   - All foreign keys must exist in dimension tables
   - Missing foreign keys → Reject

2. **Required Fields**
   - order_id, user_id, merchant_id, staff_id, product_id cannot be NULL
   - Missing required fields → Reject

3. **Date Validation**
   - transaction_date must be valid and parseable
   - transaction_date must exist in dim_date
   - Invalid dates → Reject

4. **Data Type Validation**
   - Numeric fields must be parseable
   - Invalid types → Reject

## Protection Mechanisms

1. **No Silent Failures**
   - All invalid records are logged
   - All invalid records will be stored in reject tables
   - ETL continues processing valid records

2. **Data Warehouse Integrity**
   - Only valid records enter fact tables
   - Foreign key constraints enforced
   - No orphaned records

3. **Audit Trail**
   - All rejected records tracked
   - Error reasons documented
   - Source files identified

## Implementation Status

### ✅ Completed

1. **Reject Table Schema Created**
   - `reject_fact_orders` - Stores invalid order records
   - `reject_fact_line_items` - Stores invalid line item records
   - `reject_fact_campaign_transactions` - Stores invalid campaign transaction records
   - **Location**: `sql/01_create_schema_from_physical_model.sql:157-195`
   - **Features**:
     - Stores error reason for each rejected record
     - Tracks source file and timestamp
     - Stores raw data as JSONB for full audit trail
     - Indexed for efficient querying

2. **Load Functions Updated**
   - `load_fact_orders()` - Writes rejected orders to `reject_fact_orders`
   - `load_fact_line_items()` - Writes rejected line items to `reject_fact_line_items`
   - `load_fact_campaign_transactions()` - Writes rejected transactions to `reject_fact_campaign_transactions`
   - **Location**: `scripts/load_fact.py`
   - **Features**:
     - All invalid records are captured with specific error reasons
     - Raw data preserved for debugging
     - Source file tracking included

3. **Error Reasons Captured**
   - Missing user_id/merchant_id/staff_id/product_id
   - Invalid foreign key references
   - Invalid date formats
   - Missing dates in dim_date
   - Missing orders in fact_orders (for line items/campaigns)
   - Exception errors during processing

## Testing Scenario 4

### Step 1: Create Test File with Invalid Data

Create a test CSV file with:
- **Valid records**: Normal order data with all required fields
- **Invalid records**:
  - Row with invalid date format (e.g., "2025-13-45")
  - Row with missing user_id
  - Row with non-existent product_id
  - Row with missing required fields

### Step 2: Run ETL Pipeline

1. Place test file in `data/Operations Department/`
2. Run the `shopzada_etl_pipeline` DAG
3. Monitor Airflow logs for error messages

### Step 3: Verify Results

**Check Fact Tables:**
```sql
SELECT COUNT(*) FROM fact_orders;
-- Should only contain valid rows
```

**Check Reject Tables:**
```sql
SELECT * FROM reject_fact_orders ORDER BY rejected_at DESC;
SELECT * FROM reject_fact_line_items ORDER BY rejected_at DESC;
SELECT * FROM reject_fact_campaign_transactions ORDER BY rejected_at DESC;
-- Should contain all invalid rows with error reasons
```

**Check Error Summary:**
```sql
SELECT error_reason, COUNT(*) as count 
FROM reject_fact_orders 
GROUP BY error_reason;
```

### Step 4: Verify Logs

Check Airflow task logs for:
- Summary of skipped records
- Error reasons logged
- Total valid vs invalid records

## Data Quality Rules Summary

1. **Referential Integrity**: All foreign keys must exist in dimension tables
2. **Required Fields**: Critical fields (order_id, user_id, etc.) cannot be NULL
3. **Date Validation**: Dates must be valid and exist in dim_date
4. **Data Type Validation**: Numeric fields must be parseable

## Protection Mechanisms

1. **No Silent Failures**: All invalid records logged and stored
2. **Data Warehouse Integrity**: Only valid records enter fact tables
3. **Audit Trail**: Complete history of rejected records with reasons
4. **Queryable Errors**: Reject tables allow analysis of data quality issues

## Reflection Points

1. **Data Quality Rules**: Documented in reject table error_reason field
2. **Bad Data Logging**: All invalid records stored in reject tables with full context
3. **Data Warehouse Protection**: Invalid data never enters fact tables, maintaining integrity
4. **Advantages**: 
   - Queryable error history
   - Source file tracking
   - Full audit trail
   - No data corruption
   - Easy error analysis and reporting

