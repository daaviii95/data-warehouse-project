-- Scenario 5: Performance & Aggregation Consistency Verification
-- This file contains SQL queries to verify dashboard metrics match SQL calculations

-- ============================================================================
-- OPTION 1: Monthly Sales Revenue (RECOMMENDED)
-- ============================================================================

-- Query 1: Total Revenue (matches Power BI KPI card)
SELECT 
    'Total Revenue (View)' AS metric,
    SUM(total_revenue) AS value,
    NOW() AS query_time
FROM vw_sales_by_time;

-- Query 2: Monthly Revenue Breakdown (matches Power BI column chart)
SELECT 
    year,
    month,
    month_name,
    quarter,
    SUM(total_revenue) AS monthly_revenue,
    SUM(total_orders) AS total_orders,
    SUM(total_items_sold) AS total_items_sold,
    AVG(avg_order_value) AS avg_order_value,
    COUNT(DISTINCT unique_customers) AS unique_customers
FROM vw_sales_by_time
GROUP BY year, month, month_name, quarter
ORDER BY year, month;

-- Query 3: Direct SQL calculation (bypassing view) for verification
SELECT 
    'Total Revenue (Direct from Facts)' AS metric,
    SUM(fli.quantity * dp.price) AS value,
    NOW() AS query_time
FROM fact_line_items fli
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk;

-- Query 4: Monthly Revenue from Fact Tables (for comparison)
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    SUM(fli.quantity * dp.price) AS monthly_revenue,
    COUNT(DISTINCT fo.order_id) AS total_orders
FROM fact_orders fo
LEFT JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- Query 5: Consistency Verification (View vs Direct)
WITH view_total AS (
    SELECT SUM(total_revenue) AS total_revenue
    FROM vw_sales_by_time
),
direct_total AS (
    SELECT SUM(fli.quantity * dp.price) AS total_revenue
    FROM fact_line_items fli
    LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
)
SELECT 
    vt.total_revenue AS view_total,
    dt.total_revenue AS direct_total,
    ABS(vt.total_revenue - dt.total_revenue) AS difference,
    ROUND(
        ABS(vt.total_revenue - dt.total_revenue) / NULLIF(vt.total_revenue, 0) * 100, 
        4
    ) AS percent_difference,
    CASE 
        WHEN ABS(vt.total_revenue - dt.total_revenue) < 0.01 THEN '✅ MATCH'
        WHEN ABS(vt.total_revenue - dt.total_revenue) / NULLIF(vt.total_revenue, 0) * 100 < 0.01 
            THEN '✅ MATCH (within 0.01%)'
        ELSE '❌ MISMATCH'
    END AS status
FROM view_total vt
CROSS JOIN direct_total dt;

-- Query 6: Performance Measurement (EXPLAIN ANALYZE)
-- Run this separately to see execution time
EXPLAIN ANALYZE
SELECT 
    year,
    month,
    month_name,
    SUM(total_revenue) AS monthly_revenue
FROM vw_sales_by_time
GROUP BY year, month, month_name
ORDER BY year, month;

-- ============================================================================
-- OPTION 2: Top 10 Campaigns by Order Volume
-- ============================================================================

-- Query 1: Top Campaigns (matches Power BI)
SELECT 
    campaign_name,
    discount,
    total_orders,
    unique_customers,
    total_revenue,
    avg_order_value,
    campaign_avail_rate_pct
FROM vw_campaign_performance
ORDER BY total_orders DESC
LIMIT 10;

-- Query 2: Verification - Total Campaign Orders
SELECT 
    'Total Campaign Orders (View)' AS metric,
    SUM(total_orders) AS value
FROM vw_campaign_performance;

-- Query 3: Verification - Direct from Fact Tables
SELECT 
    'Total Campaign Orders (Direct)' AS metric,
    COUNT(DISTINCT fct.order_id) AS value
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
WHERE dc.campaign_name != 'No Campaign';

-- ============================================================================
-- OPTION 3: Customer Segment Revenue Distribution
-- ============================================================================

-- Query 1: Revenue by Segment (matches Power BI)
SELECT 
    customer_segment,
    SUM(total_revenue) AS segment_revenue,
    SUM(total_orders) AS segment_orders,
    COUNT(DISTINCT unique_customers) AS segment_customers,
    AVG(avg_order_value) AS segment_avg_order_value
FROM vw_customer_segment_revenue
GROUP BY customer_segment
ORDER BY segment_revenue DESC;

-- Query 2: Verification - Direct from Fact Tables
SELECT 
    du.user_type AS customer_segment,
    SUM(fli.quantity * dp.price) AS segment_revenue,
    COUNT(DISTINCT fo.order_id) AS segment_orders
FROM fact_orders fo
LEFT JOIN dim_user du ON fo.user_sk = du.user_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
GROUP BY du.user_type
ORDER BY segment_revenue DESC;

-- ============================================================================
-- OPTION 4: Top 10 Merchants by Revenue
-- ============================================================================

-- Query 1: Top Merchants (matches Power BI)
SELECT 
    merchant_name,
    merchant_city,
    merchant_state,
    total_orders,
    total_revenue,
    avg_order_value,
    avg_delay_days,
    delay_rate_pct
FROM vw_merchant_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 2: Verification - Total Merchant Revenue
SELECT 
    'Total Merchant Revenue (View)' AS metric,
    SUM(total_revenue) AS value
FROM vw_merchant_performance;

-- Query 3: Verification - Direct from Fact Tables
SELECT 
    'Total Merchant Revenue (Direct)' AS metric,
    SUM(fli.quantity * dp.price) AS value
FROM fact_orders fo
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk;

-- ============================================================================
-- PERFORMANCE BENCHMARKING
-- ============================================================================

-- Run EXPLAIN ANALYZE on each view to measure performance
-- Look for "Execution Time" in the output

-- vw_sales_by_time performance
EXPLAIN ANALYZE
SELECT 
    year,
    month,
    SUM(total_revenue) AS monthly_revenue
FROM vw_sales_by_time
GROUP BY year, month
ORDER BY year, month;

-- vw_campaign_performance performance
EXPLAIN ANALYZE
SELECT 
    campaign_name,
    total_orders,
    total_revenue
FROM vw_campaign_performance
ORDER BY total_orders DESC
LIMIT 10;

-- vw_customer_segment_revenue performance
EXPLAIN ANALYZE
SELECT 
    customer_segment,
    SUM(total_revenue) AS segment_revenue
FROM vw_customer_segment_revenue
GROUP BY customer_segment
ORDER BY segment_revenue DESC;

-- vw_merchant_performance performance
EXPLAIN ANALYZE
SELECT 
    merchant_name,
    total_revenue
FROM vw_merchant_performance
ORDER BY total_revenue DESC
LIMIT 10;

