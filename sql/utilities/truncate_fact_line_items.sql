-- Truncate fact_line_items to allow re-loading
-- Use this if you want to reload all line items from staging tables
-- WARNING: This will delete ALL data in fact_line_items!

-- Check current count before truncating
SELECT 
    'Current row count' AS status,
    COUNT(*) AS count
FROM fact_line_items;

-- Uncomment the line below to actually truncate:
-- TRUNCATE TABLE fact_line_items;

-- Note: After truncating, you can re-run the load_fact task to reload data

