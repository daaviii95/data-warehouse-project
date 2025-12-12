-- Check Revenue for January 2020
-- This query helps identify discrepancies in revenue calculations

-- Method 1: Direct from fact tables (most accurate)
SELECT 
    'Direct from Facts' AS source,
    SUM(fli.quantity * dp.price) AS total_revenue,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fli.order_id) AS orders_with_line_items
FROM fact_orders fo
LEFT JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
WHERE dd.year = 2020 
  AND dd.month = 1;

-- Method 2: From vw_sales_by_time (daily aggregation, then sum)
SELECT 
    'vw_sales_by_time (Sum of Daily)' AS source,
    SUM(total_revenue) AS total_revenue,
    SUM(total_orders) AS total_orders
FROM vw_sales_by_time
WHERE year = 2020 
  AND month = 1;

-- Method 3: From vw_sales_by_time (monthly aggregation if exists)
SELECT 
    'vw_sales_by_time (Monthly)' AS source,
    SUM(total_revenue) AS total_revenue,
    SUM(total_orders) AS total_orders
FROM (
    SELECT 
        year,
        month,
        SUM(total_revenue) AS total_revenue,
        SUM(total_orders) AS total_orders
    FROM vw_sales_by_time
    WHERE year = 2020 AND month = 1
    GROUP BY year, month
) monthly;

-- Method 4: Check for NULL values that might cause discrepancies
SELECT 
    'Data Quality Check' AS source,
    COUNT(*) AS total_days_in_jan_2020,
    COUNT(CASE WHEN total_revenue IS NULL THEN 1 END) AS days_with_null_revenue,
    COUNT(CASE WHEN total_revenue = 0 THEN 1 END) AS days_with_zero_revenue,
    MIN(total_revenue) AS min_daily_revenue,
    MAX(total_revenue) AS max_daily_revenue,
    AVG(total_revenue) AS avg_daily_revenue
FROM vw_sales_by_time
WHERE year = 2020 AND month = 1;

-- Method 5: Check if there are orders without line items
SELECT 
    'Orders without Line Items' AS source,
    COUNT(DISTINCT fo.order_id) AS orders_without_line_items,
    COUNT(DISTINCT fo.order_id) * 0 AS potential_missing_revenue
FROM fact_orders fo
LEFT JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
WHERE dd.year = 2020 
  AND dd.month = 1
  AND fli.order_id IS NULL;

