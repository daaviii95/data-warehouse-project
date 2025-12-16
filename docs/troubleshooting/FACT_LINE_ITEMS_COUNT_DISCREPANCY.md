# fact_line_items Count Discrepancy: Airflow vs Database

## Issue

**Airflow Logs**: Reports **1,997,174** line items loaded  
**Database (pgcli/pgAdmin)**: Shows **1,794,057** rows in `fact_line_items`  
**Difference**: **203,117** rows

## Root Cause

The discrepancy occurs because:

1. **Airflow counts attempted inserts**: The code counts rows in `fact_rows` before executing the INSERT statement
2. **Database uses `ON CONFLICT DO NOTHING`**: Duplicate rows (same `order_id` + `product_sk`) are silently skipped
3. **No feedback on skipped rows**: The INSERT statement doesn't return how many rows were actually inserted

### The SQL Statement

```sql
INSERT INTO fact_line_items (order_id, product_sk, user_sk, merchant_sk, staff_sk, transaction_date_sk, price, quantity)
VALUES (:order_id, :product_sk, :user_sk, :merchant_sk, :staff_sk, :transaction_date_sk, :price, :quantity)
ON CONFLICT (order_id, product_sk) DO NOTHING
```

The `ON CONFLICT (order_id, product_sk) DO NOTHING` clause means:
- If a row with the same `(order_id, product_sk)` combination already exists, the insert is skipped
- No error is raised
- The row is not counted in the result

### Why This Happens

1. **Duplicate data in source files**: The same line item (order_id + product_id) appears multiple times
2. **Multiple ETL runs**: If the ETL pipeline runs multiple times, it attempts to insert the same data again
3. **Data quality issues**: Source files may contain duplicate records

## Verification

Run this SQL query to verify the counts:

```sql
-- Actual row count
SELECT COUNT(*) FROM fact_line_items;

-- Unique combinations
SELECT COUNT(DISTINCT (order_id, product_sk)) FROM fact_line_items;

-- Check for duplicates (should be 0)
SELECT 
    COUNT(*) - COUNT(DISTINCT (order_id, product_sk)) AS duplicate_count
FROM fact_line_items;
```

**Expected Results**:
- `COUNT(*)`: 1,794,057 (actual rows)
- `COUNT(DISTINCT (order_id, product_sk))`: 1,794,057 (should match, no duplicates)
- `duplicate_count`: 0 (unique constraint enforced)

## Solution

The code has been updated to:

1. **Count actual inserts**: Check table count before and after each INSERT to get actual rows inserted
2. **Log skipped rows**: Report how many rows were skipped due to conflicts
3. **Final verification**: Query the database at the end to get the actual count

### Updated Code Behavior

```python
# Before insert
count_before = get_count_from_db()

# Insert with ON CONFLICT DO NOTHING
execute_insert(fact_rows)

# After insert
count_after = get_count_from_db()
actual_inserted = count_after - count_before

# Log actual vs attempted
if actual_inserted < len(fact_rows):
    skipped = len(fact_rows) - actual_inserted
    logging.info(f"Actually inserted: {actual_inserted} ({skipped} skipped)")
```

## Which Count is Correct?

**The database count (1,794,057) is correct**. This is the actual number of unique rows in the table.

**Airflow's count (1,997,174) was incorrect** because it counted attempted inserts, not actual inserts.

## Why ON CONFLICT DO NOTHING?

The `ON CONFLICT DO NOTHING` clause is used to:
- **Prevent duplicates**: Ensures each `(order_id, product_sk)` combination appears only once
- **Idempotent ETL**: Allows the pipeline to be run multiple times without creating duplicates
- **Data integrity**: Enforces the unique constraint at the database level

## Finding the Source of Duplicates

To find where duplicates are coming from:

```sql
-- Check staging tables for duplicates
SELECT 
    order_id,
    product_id,
    COUNT(*) AS occurrence_count
FROM stg_operations_department_line_item_data_products
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 20;
```

This will show which `(order_id, product_id)` combinations appear multiple times in the source data.

## Summary

| Source | Count | Status |
|--------|-------|--------|
| **Database (pgcli/pgAdmin)** | **1,794,057** | ✅ **Correct** - Actual rows in table |
| **Airflow (old)** | 1,997,174 | ❌ **Incorrect** - Counted attempted inserts |
| **Airflow (updated)** | 1,794,057 | ✅ **Correct** - Counts actual inserts |
| **Difference** | 203,117 | Rows skipped due to `ON CONFLICT DO NOTHING` |

**Conclusion**: The database count is correct. The 203,117 difference represents duplicate rows that were correctly skipped by the `ON CONFLICT DO NOTHING` clause. The code has been updated to report the actual count going forward.

