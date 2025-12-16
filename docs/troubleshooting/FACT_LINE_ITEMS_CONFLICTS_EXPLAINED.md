# Understanding Conflicts in fact_line_items

## The Unique Constraint

The `fact_line_items` table has a **composite primary key**:

```sql
PRIMARY KEY (order_id, product_sk)
```

This means:
- **Each combination of `(order_id, product_sk)` must be unique**
- You cannot have two rows with the same `order_id` AND `product_sk`
- This enforces business logic: **One order cannot have the same product twice** (at least not in the same row)

## What is a Conflict?

A **conflict** occurs when the ETL pipeline tries to insert a row with a `(order_id, product_sk)` combination that **already exists** in the table.

### Example of a Conflict

**Scenario**: Trying to insert this row:
```sql
order_id = 'ORDER123'
product_sk = 42
price = 29.99
quantity = 2
```

But the table already contains:
```sql
order_id = 'ORDER123'
product_sk = 42
price = 29.99
quantity = 1
```

**Result**: The INSERT is **skipped** because `(ORDER123, 42)` already exists.

## Why Do Conflicts Happen?

### 1. **Duplicate Data in Source Files**

The same line item appears multiple times in the staging tables:

```sql
-- Example: Same order_id + product_id appears twice
stg_operations_department_line_item_data_products:
  Row 1: order_id='ORDER123', product_id='PROD42'
  Row 2: order_id='ORDER123', product_id='PROD42'  -- DUPLICATE!
```

**Why this happens**:
- Data entry errors
- Multiple exports of the same data
- Merged files with overlapping data
- System glitches creating duplicate records

### 2. **Multiple ETL Runs (Idempotency)**

If you run the ETL pipeline multiple times:
- **First run**: Inserts 1,997,174 rows
- **Second run**: Tries to insert the same 1,997,174 rows again
- **Result**: All 1,997,174 rows conflict (already exist)
- **Database**: Still has 1,997,174 rows (no duplicates created)

This is actually **good behavior** - it makes the ETL **idempotent** (safe to run multiple times).

### 3. **Data Merging Issues**

When merging `prices_df` and `products_df`:
- If the merge creates duplicate combinations
- Or if the same data is processed from multiple source files

## How ON CONFLICT DO NOTHING Works

```sql
INSERT INTO fact_line_items (...)
VALUES (...)
ON CONFLICT (order_id, product_sk) DO NOTHING
```

**Behavior**:
1. PostgreSQL checks if `(order_id, product_sk)` already exists
2. **If it exists**: Skip the insert (do nothing)
3. **If it doesn't exist**: Insert the row
4. **No error is raised** - the conflict is handled silently

**Why "DO NOTHING" instead of "DO UPDATE"?**
- We want to preserve the **first** value inserted
- Prevents accidental overwrites
- Maintains data integrity

## Analyzing Conflicts

### Query 1: Find Duplicates in Staging Tables

```sql
-- Find duplicate (order_id, product_id) in source data
SELECT 
    order_id,
    product_id,
    COUNT(*) AS occurrence_count,
    STRING_AGG(DISTINCT file_source, ', ') AS source_files
FROM stg_operations_department_line_item_data_products
WHERE product_id IS NOT NULL
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 20;
```

**What this shows**:
- Which `(order_id, product_id)` combinations appear multiple times
- How many times each appears
- Which source files contain the duplicates

### Query 2: Check for Conflicts During Insert

```sql
-- Find rows in staging that would conflict with existing fact_line_items
SELECT 
    li.order_id,
    li.product_id,
    dp.product_sk,
    COUNT(*) AS staging_occurrences,
    CASE 
        WHEN fli.order_id IS NOT NULL THEN 'Would Conflict'
        ELSE 'New Row'
    END AS status
FROM stg_operations_department_line_item_data_products li
INNER JOIN dim_product dp ON li.product_id = dp.product_id
LEFT JOIN fact_line_items fli ON li.order_id = fli.order_id AND dp.product_sk = fli.product_sk
GROUP BY li.order_id, li.product_id, dp.product_sk, fli.order_id
HAVING COUNT(*) > 1 OR fli.order_id IS NOT NULL
ORDER BY staging_occurrences DESC
LIMIT 20;
```

### Query 3: Count Conflicts by Source File

```sql
-- See which source files have the most duplicates
SELECT 
    file_source,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT (order_id, product_id)) AS unique_combinations,
    COUNT(*) - COUNT(DISTINCT (order_id, product_id)) AS duplicate_rows
FROM stg_operations_department_line_item_data_products
WHERE product_id IS NOT NULL
GROUP BY file_source
HAVING COUNT(*) - COUNT(DISTINCT (order_id, product_id)) > 0
ORDER BY duplicate_rows DESC;
```

### Query 4: Verify No Duplicates in fact_line_items

```sql
-- This should return 0 (no duplicates possible due to PRIMARY KEY)
SELECT 
    order_id,
    product_sk,
    COUNT(*) AS occurrence_count
FROM fact_line_items
GROUP BY order_id, product_sk
HAVING COUNT(*) > 1;
```

**Expected Result**: 0 rows (the PRIMARY KEY prevents duplicates)

## Understanding the 203,117 Difference

**Airflow attempted**: 1,997,174 rows  
**Database has**: 1,794,057 rows  
**Difference**: 203,117 rows

### What This Means

The 203,117 rows represent:
1. **Duplicate rows in source data**: Same `(order_id, product_id)` appears multiple times
2. **Rows already in database**: From previous ETL runs
3. **Data quality issues**: Invalid or duplicate records

### Breakdown Example

```
Total rows processed:        1,997,174
├─ New unique rows:          1,794,057  ✅ Inserted
├─ Duplicate (order_id, product_sk): 150,000  ❌ Skipped (conflict)
└─ Already in database:       53,117  ❌ Skipped (conflict)
────────────────────────────────────────────────
Total conflicts:               203,117
```

## Business Logic: Why This Constraint Exists

The `(order_id, product_sk)` primary key enforces:

> **"One order can only have one line item per product"**

### Valid Scenario
```
Order ORDER123:
  - Product A (product_sk=1): quantity=2, price=10.00
  - Product B (product_sk=2): quantity=1, price=5.00
```
✅ **Valid**: Different products in the same order

### Invalid Scenario (Would Conflict)
```
Order ORDER123:
  - Product A (product_sk=1): quantity=2, price=10.00
  - Product A (product_sk=1): quantity=1, price=10.00  ❌ CONFLICT!
```
❌ **Invalid**: Same product appears twice

**If you need multiple quantities of the same product**, you should:
- **Update the quantity** of the existing row, OR
- **Use a different key** (e.g., include `line_item_id` in the primary key)

## Handling Conflicts: Options

### Option 1: DO NOTHING (Current)
```sql
ON CONFLICT (order_id, product_sk) DO NOTHING
```
- **Pros**: Preserves first value, prevents overwrites
- **Cons**: Loses information about duplicates

### Option 2: DO UPDATE
```sql
ON CONFLICT (order_id, product_sk) DO UPDATE SET
    quantity = EXCLUDED.quantity,
    price = EXCLUDED.price
```
- **Pros**: Updates with latest values
- **Cons**: May overwrite correct data with incorrect data

### Option 3: Track Conflicts
```sql
-- Log conflicts to a separate table
INSERT INTO conflict_log (order_id, product_sk, conflict_reason, raw_data)
SELECT order_id, product_sk, 'Duplicate insert attempt', ...
WHERE EXISTS (
    SELECT 1 FROM fact_line_items 
    WHERE fact_line_items.order_id = ...
);
```
- **Pros**: Maintains audit trail
- **Cons**: More complex, requires additional table

## Recommendations

### 1. **Investigate Source Data Quality**

Run Query 1 above to find which source files have duplicates:
- If duplicates are in source files → Fix the source data
- If duplicates are from multiple ETL runs → This is expected behavior

### 2. **Monitor Conflict Rates**

Add logging to track:
- How many conflicts occur per ETL run
- Which source files cause the most conflicts
- Whether conflicts are increasing over time

### 3. **Consider Business Requirements**

Ask: **"Should one order be able to have the same product multiple times?"**

- **If NO**: Current constraint is correct
- **If YES**: Consider changing the primary key to include `line_item_id`

## Summary

| Aspect | Explanation |
|--------|-------------|
| **Constraint** | `PRIMARY KEY (order_id, product_sk)` - Each combination must be unique |
| **Conflict** | Attempting to insert a row with an existing `(order_id, product_sk)` |
| **Behavior** | `ON CONFLICT DO NOTHING` - Silently skips duplicate inserts |
| **Why It Happens** | Duplicate source data, multiple ETL runs, data quality issues |
| **Impact** | 203,117 rows skipped (11.2% of attempted inserts) |
| **Is This Bad?** | **No** - It prevents duplicates and maintains data integrity |
| **Action Needed** | Investigate source data quality, but conflicts are expected behavior |

## Quick Diagnostic Queries

```sql
-- 1. Check actual vs unique count (should match)
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT (order_id, product_sk)) AS unique_combinations,
    COUNT(*) - COUNT(DISTINCT (order_id, product_sk)) AS duplicates
FROM fact_line_items;

-- 2. Find most common conflicts in staging
SELECT 
    order_id,
    product_id,
    COUNT(*) AS times_seen
FROM stg_operations_department_line_item_data_products
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
ORDER BY times_seen DESC
LIMIT 10;

-- 3. Check if conflicts are from multiple files
SELECT 
    file_source,
    COUNT(*) AS rows,
    COUNT(DISTINCT (order_id, product_id)) AS unique_combos
FROM stg_operations_department_line_item_data_products
GROUP BY file_source
ORDER BY rows DESC;
```

