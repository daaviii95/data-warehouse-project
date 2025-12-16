-- Analyze Conflicts in fact_line_items
-- This query helps understand why conflicts occur and where they come from

-- ============================================================================
-- 1. Verify the unique constraint is working (should return 0)
-- ============================================================================
SELECT 
    'Duplicate rows in fact_line_items (should be 0)' AS check_name,
    COUNT(*) AS duplicate_count
FROM (
    SELECT order_id, product_sk, COUNT(*) as cnt
    FROM fact_line_items
    GROUP BY order_id, product_sk
    HAVING COUNT(*) > 1
) duplicates;

-- ============================================================================
-- 2. Find duplicate (order_id, product_id) in staging tables
--    These are the source of conflicts
-- ============================================================================
SELECT 
    'Top 20 duplicate (order_id, product_id) in staging' AS analysis,
    order_id,
    product_id,
    COUNT(*) AS occurrence_count,
    STRING_AGG(DISTINCT file_source, ', ' ORDER BY file_source) AS source_files,
    MIN(loaded_at) AS first_seen,
    MAX(loaded_at) AS last_seen
FROM stg_operations_department_line_item_data_products
WHERE product_id IS NOT NULL
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 20;

-- ============================================================================
-- 3. Count total duplicates in staging
-- ============================================================================
WITH staging_stats AS (
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(DISTINCT (order_id, product_id)) AS unique_combinations
    FROM stg_operations_department_line_item_data_products
    WHERE product_id IS NOT NULL
)
SELECT 
    'Staging table duplicate analysis' AS analysis,
    total_rows,
    unique_combinations,
    total_rows - unique_combinations AS duplicate_rows,
    ROUND((total_rows - unique_combinations)::NUMERIC / total_rows * 100, 2) AS duplicate_percentage
FROM staging_stats;

-- ============================================================================
-- 4. Check which source files have the most duplicates
-- ============================================================================
SELECT 
    'Duplicate analysis by source file' AS analysis,
    file_source,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT (order_id, product_id)) AS unique_combinations,
    COUNT(*) - COUNT(DISTINCT (order_id, product_id)) AS duplicate_rows,
    ROUND(
        (COUNT(*) - COUNT(DISTINCT (order_id, product_id)))::NUMERIC / COUNT(*) * 100, 
        2
    ) AS duplicate_percentage
FROM stg_operations_department_line_item_data_products
WHERE product_id IS NOT NULL
GROUP BY file_source
HAVING COUNT(*) - COUNT(DISTINCT (order_id, product_id)) > 0
ORDER BY duplicate_rows DESC;

-- ============================================================================
-- 5. Check if conflicts are from rows already in fact_line_items
--    (Multiple ETL runs scenario)
-- ============================================================================
SELECT 
    'Rows in staging that would conflict with existing fact_line_items' AS analysis,
    COUNT(*) AS would_conflict_count
FROM stg_operations_department_line_item_data_products li
INNER JOIN dim_product dp ON li.product_id = dp.product_id
INNER JOIN fact_line_items fli ON li.order_id = fli.order_id AND dp.product_sk = fli.product_sk
WHERE li.product_id IS NOT NULL;

-- ============================================================================
-- 6. Compare staging counts vs fact counts
-- ============================================================================
WITH staging_counts AS (
    SELECT 
        COUNT(DISTINCT (order_id, product_id)) AS unique_staging_combos
    FROM stg_operations_department_line_item_data_products
    WHERE product_id IS NOT NULL
),
fact_counts AS (
    SELECT 
        COUNT(*) AS fact_rows,
        COUNT(DISTINCT (order_id, product_sk)) AS unique_fact_combos
    FROM fact_line_items
)
SELECT 
    'Staging vs Fact comparison' AS analysis,
    sc.unique_staging_combos AS "Unique in Staging",
    fc.fact_rows AS "Rows in Fact Table",
    fc.unique_fact_combos AS "Unique in Fact",
    sc.unique_staging_combos - fc.fact_rows AS "Difference",
    CASE 
        WHEN sc.unique_staging_combos > fc.fact_rows THEN 
            'More in staging (some rows not loaded or conflicted)'
        WHEN sc.unique_staging_combos = fc.fact_rows THEN 
            'Match (all unique staging rows loaded)'
        ELSE 
            'More in fact (unexpected - check for issues)'
    END AS status
FROM staging_counts sc
CROSS JOIN fact_counts fc;

-- ============================================================================
-- 7. Sample of conflicting rows (showing what gets skipped)
-- ============================================================================
SELECT 
    'Sample of rows that would conflict' AS analysis,
    li.order_id,
    li.product_id,
    dp.product_sk,
    li.file_source,
    CASE 
        WHEN fli.order_id IS NOT NULL THEN 'Already in fact_line_items'
        ELSE 'Duplicate in staging'
    END AS conflict_reason
FROM stg_operations_department_line_item_data_products li
INNER JOIN dim_product dp ON li.product_id = dp.product_id
LEFT JOIN fact_line_items fli ON li.order_id = fli.order_id AND dp.product_sk = fli.product_sk
WHERE li.product_id IS NOT NULL
  AND (
      -- Either already in fact table
      fli.order_id IS NOT NULL
      OR
      -- Or duplicate in staging
      (li.order_id, li.product_id) IN (
          SELECT order_id, product_id
          FROM stg_operations_department_line_item_data_products
          WHERE product_id IS NOT NULL
          GROUP BY order_id, product_id
          HAVING COUNT(*) > 1
      )
  )
LIMIT 20;

-- ============================================================================
-- 8. Summary: Conflict breakdown
-- ============================================================================
WITH conflict_analysis AS (
    SELECT 
        -- Total unique combinations in staging
        (SELECT COUNT(DISTINCT (order_id, product_id))
         FROM stg_operations_department_line_item_data_products
         WHERE product_id IS NOT NULL) AS unique_staging,
        
        -- Rows in fact table
        (SELECT COUNT(*) FROM fact_line_items) AS fact_rows,
        
        -- Rows that would conflict (already in fact)
        (SELECT COUNT(*)
         FROM stg_operations_department_line_item_data_products li
         INNER JOIN dim_product dp ON li.product_id = dp.product_id
         INNER JOIN fact_line_items fli ON li.order_id = fli.order_id AND dp.product_sk = fli.product_sk
         WHERE li.product_id IS NOT NULL) AS would_conflict_existing,
        
        -- Duplicates within staging
        (SELECT COUNT(*) - COUNT(DISTINCT (order_id, product_id))
         FROM stg_operations_department_line_item_data_products
         WHERE product_id IS NOT NULL) AS duplicates_in_staging
)
SELECT 
    'Conflict Summary' AS analysis,
    unique_staging AS "Unique Combinations in Staging",
    fact_rows AS "Rows in fact_line_items",
    would_conflict_existing AS "Would Conflict (Already in Fact)",
    duplicates_in_staging AS "Duplicates in Staging",
    (would_conflict_existing + duplicates_in_staging) AS "Total Conflicts",
    ROUND(
        (would_conflict_existing + duplicates_in_staging)::NUMERIC / 
        NULLIF(unique_staging, 0) * 100, 
        2
    ) AS "Conflict Percentage"
FROM conflict_analysis;

