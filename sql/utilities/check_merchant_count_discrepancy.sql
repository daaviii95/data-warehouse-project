-- Check Merchant Count Discrepancy
-- This query helps identify why Power BI shows different merchant count than source file

-- ============================================================================
-- 1. Count merchants in dim_merchant (all merchants loaded)
-- ============================================================================
SELECT 
    'Total Merchants in dim_merchant' AS metric,
    COUNT(*) AS count
FROM dim_merchant;

-- ============================================================================
-- 2. Count distinct merchants in vw_merchant_performance (only merchants with orders)
-- ============================================================================
SELECT 
    'Distinct Merchants in vw_merchant_performance' AS metric,
    COUNT(DISTINCT merchant_id) AS count
FROM vw_merchant_performance;

-- ============================================================================
-- 3. Count merchants that have NO orders (won't appear in view)
-- ============================================================================
SELECT 
    'Merchants with NO orders' AS metric,
    COUNT(*) AS count
FROM dim_merchant dm
LEFT JOIN fact_orders fo ON dm.merchant_sk = fo.merchant_sk
WHERE fo.order_id IS NULL;

-- ============================================================================
-- 4. List merchants with no orders (sample of 10)
-- ============================================================================
SELECT 
    dm.merchant_id,
    dm.name AS merchant_name,
    dm.city,
    dm.country
FROM dim_merchant dm
LEFT JOIN fact_orders fo ON dm.merchant_sk = fo.merchant_sk
WHERE fo.order_id IS NULL
LIMIT 10;

-- ============================================================================
-- 5. Check for duplicate merchant_ids in dim_merchant (should be 0)
-- ============================================================================
SELECT 
    'Duplicate merchant_ids in dim_merchant' AS metric,
    COUNT(*) - COUNT(DISTINCT merchant_id) AS duplicate_count
FROM dim_merchant;

-- ============================================================================
-- 6. Check if Power BI might be counting something else
--    (e.g., counting merchant_id occurrences in fact_orders instead of distinct)
-- ============================================================================
SELECT 
    'Total merchant_id occurrences in fact_orders' AS metric,
    COUNT(merchant_sk) AS count
FROM fact_orders;

SELECT 
    'Distinct merchants in fact_orders' AS metric,
    COUNT(DISTINCT merchant_sk) AS count
FROM fact_orders;

-- ============================================================================
-- 7. Summary: Compare all counts side by side
-- ============================================================================
WITH counts AS (
    SELECT 
        (SELECT COUNT(*) FROM dim_merchant) AS total_in_dim,
        (SELECT COUNT(DISTINCT merchant_id) FROM vw_merchant_performance) AS distinct_in_view,
        (SELECT COUNT(DISTINCT merchant_sk) FROM fact_orders) AS distinct_in_facts,
        (SELECT COUNT(*) FROM dim_merchant dm LEFT JOIN fact_orders fo ON dm.merchant_sk = fo.merchant_sk WHERE fo.order_id IS NULL) AS merchants_without_orders
)
SELECT 
    total_in_dim AS "Total in dim_merchant",
    distinct_in_view AS "Distinct in vw_merchant_performance",
    distinct_in_facts AS "Distinct in fact_orders",
    merchants_without_orders AS "Merchants with NO orders",
    (total_in_dim - distinct_in_view) AS "Difference (should match merchants_without_orders)",
    CASE 
        WHEN distinct_in_view = distinct_in_facts THEN '✅ View matches fact_orders'
        ELSE '⚠️ View count differs from fact_orders'
    END AS validation
FROM counts;

-- ============================================================================
-- 8. If Power BI is using a different measure, check what it might be counting
-- ============================================================================
-- Power BI might be using:
-- - COUNT('vw_merchant_performance'[merchant_id]) instead of DISTINCTCOUNT
-- - This would count rows, not distinct merchants
-- - If there are multiple rows per merchant (shouldn't be, but check)

SELECT 
    'Merchants with multiple rows in view (should be 0)' AS metric,
    COUNT(*) AS count
FROM (
    SELECT merchant_id, COUNT(*) as row_count
    FROM vw_merchant_performance
    GROUP BY merchant_id
    HAVING COUNT(*) > 1
) duplicates;


