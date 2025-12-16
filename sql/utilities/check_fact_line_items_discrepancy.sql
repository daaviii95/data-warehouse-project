-- Check fact_line_items Count Discrepancy
-- Airflow reports 1,997,174 but database shows 1,794,057
-- This query helps identify why

-- ============================================================================
-- 1. Actual row count in fact_line_items
-- ============================================================================
SELECT 
    'Actual rows in fact_line_items' AS metric,
    COUNT(*) AS count
FROM fact_line_items;

-- ============================================================================
-- 2. Count of unique (order_id, product_sk) combinations
-- ============================================================================
SELECT 
    'Unique (order_id, product_sk) combinations' AS metric,
    COUNT(DISTINCT (order_id, product_sk)) AS count
FROM fact_line_items;

-- ============================================================================
-- 3. Check for duplicate (order_id, product_sk) pairs
--    (Should be 0 if ON CONFLICT is working correctly)
-- ============================================================================
SELECT 
    'Duplicate (order_id, product_sk) pairs' AS metric,
    COUNT(*) - COUNT(DISTINCT (order_id, product_sk)) AS duplicate_count
FROM fact_line_items;

-- ============================================================================
-- 4. Find duplicate (order_id, product_sk) pairs (if any exist)
-- ============================================================================
SELECT 
    order_id,
    product_sk,
    COUNT(*) AS occurrence_count
FROM fact_line_items
GROUP BY order_id, product_sk
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 20;

-- ============================================================================
-- 5. Count rows in staging tables (source of truth)
-- ============================================================================
SELECT 
    'Rows in stg_operations_department_line_item_data_prices' AS metric,
    COUNT(*) AS count
FROM stg_operations_department_line_item_data_prices;

SELECT 
    'Rows in stg_operations_department_line_item_data_products' AS metric,
    COUNT(*) AS count
FROM stg_operations_department_line_item_data_products;

-- ============================================================================
-- 6. Check for line items that were skipped due to missing orders
-- ============================================================================
SELECT 
    'Line items in staging with orders NOT in fact_orders' AS metric,
    COUNT(DISTINCT li.order_id) AS count
FROM stg_operations_department_line_item_data_prices li
LEFT JOIN fact_orders fo ON li.order_id = fo.order_id
WHERE fo.order_id IS NULL;

-- ============================================================================
-- 7. Check for line items that were skipped due to missing products
-- ============================================================================
SELECT 
    'Line items in staging with products NOT in dim_product' AS metric,
    COUNT(*) AS count
FROM stg_operations_department_line_item_data_products li
LEFT JOIN dim_product dp ON li.product_id = dp.product_id
WHERE dp.product_id IS NULL
AND li.product_id IS NOT NULL;

-- ============================================================================
-- 8. Summary: Calculate expected vs actual
-- ============================================================================
WITH staging_counts AS (
    SELECT 
        (SELECT COUNT(*) FROM stg_operations_department_line_item_data_prices) AS prices_count,
        (SELECT COUNT(*) FROM stg_operations_department_line_item_data_products) AS products_count
),
fact_counts AS (
    SELECT 
        (SELECT COUNT(*) FROM fact_line_items) AS actual_count,
        (SELECT COUNT(DISTINCT (order_id, product_sk)) FROM fact_line_items) AS unique_count
),
rejected_counts AS (
    SELECT 
        (SELECT COUNT(*) FROM reject_fact_line_items) AS rejected_count
)
SELECT 
    sc.prices_count AS "Staging Prices Count",
    sc.products_count AS "Staging Products Count",
    fc.actual_count AS "Actual fact_line_items Count",
    fc.unique_count AS "Unique (order_id, product_sk) Count",
    COALESCE(rc.rejected_count, 0) AS "Rejected Count",
    (sc.prices_count - fc.actual_count) AS "Difference (Staging - Fact)",
    CASE 
        WHEN fc.actual_count = fc.unique_count THEN '✅ No duplicates'
        ELSE '⚠️ Duplicates found'
    END AS "Duplicate Status"
FROM staging_counts sc
CROSS JOIN fact_counts fc
CROSS JOIN rejected_counts rc;

-- ============================================================================
-- 9. Check if Airflow is counting attempted inserts vs actual inserts
--    (This is likely the issue - ON CONFLICT DO NOTHING skips duplicates)
-- ============================================================================
-- The difference: 1,997,174 (Airflow) - 1,794,057 (Database) = 203,117
-- This suggests 203,117 rows were attempted but skipped due to ON CONFLICT

SELECT 
    'Estimated skipped rows (Airflow count - DB count)' AS metric,
    1997174 - 1794057 AS estimated_skipped
;

-- ============================================================================
-- 10. Verify the unique constraint is working
-- ============================================================================
-- Check if there are any violations of the (order_id, product_sk) unique constraint
-- This should return 0 if the constraint is properly enforced

SELECT 
    'Rows that would violate unique constraint' AS metric,
    COUNT(*) AS count
FROM (
    SELECT order_id, product_sk, COUNT(*) as cnt
    FROM fact_line_items
    GROUP BY order_id, product_sk
    HAVING COUNT(*) > 1
) duplicates;

