# ETL Pipeline Testing Guide

## Overview

This guide covers how to test and validate the ETL pipeline, including Scenario 1 (end-to-end pipeline test) and Scenario 2 (new customer/product creation).

## Testing Options

### Option 1: Using Test Script (Recommended)

The `scripts/test_etl_pipeline.py` script provides comprehensive validation:

```bash
# Basic usage
python scripts/test_etl_pipeline.py /path/to/test_orders.csv

# With target date for KPI validation
python scripts/test_etl_pipeline.py /path/to/test_orders.csv --target-date 2024-01-15

# With Scenario 2 validation
python scripts/test_etl_pipeline.py /path/to/test_orders.csv \
  --validate-scenario2 \
  --new-user-id "NEW_USER_123" \
  --new-product-id "NEW_PRODUCT_456"
```

**Features:**
- ✅ Displays test file contents (Step 0)
- ✅ Shows before/after state (row counts, KPIs)
- ✅ Validates expected vs actual results
- ✅ Scenario 2 validation support
- ✅ Provides explanation summary

### Option 2: Using Main System with Metrics

Enable Scenario 1 metrics in the main ETL system:

```bash
# Set environment variables
export ENABLE_SCENARIO1_METRICS=true
export TARGET_DATE=2024-01-15

# Run pipeline
python scripts/etl_pipeline_python.py
```

**Features:**
- ✅ Before/after state tracking
- ✅ Dashboard KPI monitoring
- ✅ Pipeline summary explanation
- ❌ No test file display
- ❌ No validation logic

See [Scenario 1 Implementation](SCENARIO1_IMPLEMENTATION.md) for details.

## Test File Format

The test file should be a CSV file with order data, containing columns like:

- `order_id` - Unique order identifier
- `user_id` - Customer identifier
- `transaction_date` - Order date (YYYY-MM-DD format)
- `payment_method` - Payment type
- `total_amount` - Order total (optional, for validation)
- `estimated_arrival_days` - Estimated delivery days
- `staff_id` - Staff identifier (optional, will come from order_with_merchant_data)

### Example Test File

```csv
order_id,user_id,transaction_date,payment_method,total_amount,estimated_arrival_days
test-order-001,user-001,2024-01-15,Credit Card,100.50,5
test-order-002,user-002,2024-01-15,Debit Card,250.75,7
test-order-003,user-003,2024-01-15,Cash,75.25,3
```

## Scenario 1: End-to-End Pipeline Test

### Requirements

1. **Show BEFORE state:**
   - Fact table row count
   - Dashboard KPI for target date

2. **Run workflow** with test file

3. **Show AFTER state:**
   - Fact table row count (should increase)
   - Dashboard KPI (should be updated)

4. **Explain** how this confirms ingestion → transformation → load → analytics

### Using Test Script

```bash
python scripts/test_etl_pipeline.py test_orders.csv --target-date 2024-01-15
```

The script automatically:
- Shows test file contents
- Captures before state
- Runs the pipeline
- Captures after state
- Validates results
- Provides explanation

### Using Main System

```bash
ENABLE_SCENARIO1_METRICS=true TARGET_DATE=2024-01-15 python scripts/etl_pipeline_python.py
```

See [Scenario 1 Quick Start](SCENARIO1_QUICK_START.md) for details.

## Scenario 2: New Customer and Product Creation

### Requirements

1. **Prepare test data** with new customer and new product (not in dimension files)
2. **Run full workflow**
3. **Validate:**
   - New customer appears in `dim_user` with valid surrogate key
   - New product appears in `dim_product` with valid surrogate key
   - Fact row correctly references these keys (no null/broken FKs)
   - Transaction appears on dashboard when filtered

### Using Test Script

```bash
python scripts/test_etl_pipeline.py test_orders.csv \
  --validate-scenario2 \
  --new-user-id "NEW_USER_123" \
  --new-product-id "NEW_PRODUCT_456"
```

### Manual Validation

```sql
-- Check new customer exists
SELECT user_sk, user_id, name 
FROM dim_user 
WHERE user_id = 'NEW_USER_123';

-- Check new product exists
SELECT product_sk, product_id, product_name 
FROM dim_product 
WHERE product_id = 'NEW_PRODUCT_456';

-- Check fact row references them correctly
SELECT fo.order_id, fo.user_sk, li.product_sk
FROM fact_orders fo
JOIN fact_line_items li ON fo.order_id = li.order_id
WHERE fo.user_sk = (SELECT user_sk FROM dim_user WHERE user_id = 'NEW_USER_123')
  AND li.product_sk = (SELECT product_sk FROM dim_product WHERE product_id = 'NEW_PRODUCT_456');
```

See [Scenario 2 Analysis](SCENARIO2_ANALYSIS.md) for implementation details.

## Validation Criteria

### Success Criteria

1. **Row Count Match**: 
   - `fact_orders` row count increases by exactly the number of rows in test file
   - No duplicate rows are created

2. **Sales KPI Match**:
   - Daily sales KPI increases by the sum of `total_amount` from test file (if available)
   - Order count increases by the number of test orders

3. **Data Integrity**:
   - All new orders have valid dimension keys (user_sk, merchant_sk, staff_sk, date_sk)
   - All required fields are populated
   - No null or broken foreign keys

4. **Scenario 2** (if applicable):
   - New customers/products created in dimension tables
   - Fact rows correctly reference new dimension keys

### Failure Indicators

- Row count doesn't match expected
- Sales KPI doesn't match expected
- Missing dimension keys
- Duplicate orders
- Data quality issues
- New dimensions not created (Scenario 2)

## Troubleshooting

### Row Count Mismatch

- Check if test file was properly ingested
- Verify staging tables contain the test data
- Check for duplicate order_ids
- Ensure merge logic is working correctly
- Verify Scenario 2 support created missing dimensions

### Sales KPI Mismatch

- Verify `total_amount` column exists in test file
- Check if line items were loaded correctly
- Ensure date matching is correct
- Verify fact_line_items were created

### Missing Dimension Keys

- Verify dimension tables are populated
- Check if user_id, merchant_id, staff_id exist in dimensions
- Ensure date dimension covers the test date range
- **For Scenario 2**: Verify missing dimension creation ran before fact loading

### Test Orders Not Appearing

- Check if user_ids exist in `dim_user` (or were created by Scenario 2)
- Check if staff_ids exist in `dim_staff`
- Verify date exists in `dim_date`
- Check ingestion logs for errors
- Verify test file was copied to correct data directory

## Best Practices

1. **Use Small Test Files**: 5-10 rows for quick validation
2. **Use Unique Order IDs**: Prefix with `test-` to avoid conflicts
3. **Clean Up After Tests**: Remove test data after validation
4. **Test in Development First**: Run tests in dev/staging before production
5. **Document Test Scenarios**: Keep track of what each test validates

## Related Documentation

- [Scenario 1 Implementation](SCENARIO1_IMPLEMENTATION.md) - Main system metrics
- [Scenario 1 Quick Start](SCENARIO1_QUICK_START.md) - Quick reference
- [Scenario 2 Analysis](SCENARIO2_ANALYSIS.md) - Dimension creation details
- [ETL Metrics Execution Flow](ETL_METRICS_EXECUTION_FLOW.md) - When metrics run
- [Workflow Documentation](../core/WORKFLOW.md) - Complete ETL workflow
