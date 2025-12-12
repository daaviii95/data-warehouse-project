-- Analytical Views for Presentation Layer
-- These views are optimized for BI tools (Power BI, Tableau, Looker Studio)
-- Based on the three business questions:
-- 1. What kinds of campaigns drive the highest order volume?
-- 2. How do merchant performance metrics affect sales?
-- 3. What customer segments contribute most to revenue?

-- ============================================================================
-- VIEW 1: Campaign Performance Analysis
-- ============================================================================
-- Answers: What kinds of campaigns drive the highest order volume?
-- Note: Excludes "No Campaign" entries - only shows actual marketing campaigns
CREATE OR REPLACE VIEW vw_campaign_performance AS
SELECT 
    dc.campaign_id,
    dc.campaign_name,
    dc.campaign_description,
    dc.discount,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.user_sk) AS unique_customers,
    SUM(fli.quantity) AS total_quantity_sold,
    SUM(fli.quantity * dp.price) AS total_revenue,
    AVG(fli.quantity * dp.price) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN fct.availed = 1 THEN fo.order_id END) AS orders_with_campaign,
    ROUND(
        COUNT(DISTINCT CASE WHEN fct.availed = 1 THEN fo.order_id END)::NUMERIC / 
        NULLIF(COUNT(DISTINCT fo.order_id), 0) * 100, 
        2
    ) AS campaign_avail_rate_pct
FROM fact_orders fo
INNER JOIN fact_campaign_transactions fct ON fo.order_id = fct.order_id
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
GROUP BY 
    dc.campaign_id,
    dc.campaign_name,
    dc.campaign_description,
    dc.discount
ORDER BY total_orders DESC, total_revenue DESC;

-- ============================================================================
-- VIEW 2: Merchant Performance Metrics
-- ============================================================================
-- Answers: How do merchant performance metrics affect sales?
CREATE OR REPLACE VIEW vw_merchant_performance AS
WITH merchant_delays AS (
    -- Calculate delay metrics from fact_orders only (before JOIN with line_items)
    SELECT 
        fo.merchant_sk,
        AVG(fo.delay_days) AS avg_delay_days,
        COUNT(DISTINCT CASE WHEN fo.delay_days > 0 THEN fo.order_id END) AS delayed_orders
    FROM fact_orders fo
    WHERE fo.delay_days IS NOT NULL
    GROUP BY fo.merchant_sk
)
SELECT 
    dm.merchant_id,
    dm.name AS merchant_name,
    dm.city AS merchant_city,
    dm.state AS merchant_state,
    dm.country AS merchant_country,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.user_sk) AS unique_customers,
    SUM(fli.quantity) AS total_items_sold,
    SUM(fli.quantity * dp.price) AS total_revenue,
    AVG(fli.quantity * dp.price) AS avg_order_value,
    md.avg_delay_days,
    COALESCE(md.delayed_orders, 0) AS delayed_orders,
    COUNT(DISTINCT ds.staff_id) AS staff_count,
    ROUND(
        COALESCE(md.delayed_orders, 0)::NUMERIC / 
        NULLIF(COUNT(DISTINCT fo.order_id), 0) * 100, 
        2
    ) AS delay_rate_pct
FROM fact_orders fo
LEFT JOIN dim_merchant dm ON fo.merchant_sk = dm.merchant_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
LEFT JOIN dim_staff ds ON fo.staff_sk = ds.staff_sk
LEFT JOIN merchant_delays md ON fo.merchant_sk = md.merchant_sk
GROUP BY 
    dm.merchant_id,
    dm.name,
    dm.city,
    dm.state,
    dm.country,
    md.avg_delay_days,
    md.delayed_orders
ORDER BY total_revenue DESC;

-- ============================================================================
-- VIEW 3: Customer Segment Revenue Analysis
-- ============================================================================
-- Answers: What customer segments contribute most to revenue?
CREATE OR REPLACE VIEW vw_customer_segment_revenue AS
SELECT 
    du.user_type AS customer_segment,
    du.country AS customer_country,
    du.city AS customer_city,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.user_sk) AS unique_customers,
    SUM(fli.quantity) AS total_items_purchased,
    SUM(fli.quantity * dp.price) AS total_revenue,
    AVG(fli.quantity * dp.price) AS avg_order_value,
    MIN(dd.date) AS first_order_date,
    MAX(dd.date) AS last_order_date,
    COUNT(DISTINCT dm.merchant_id) AS merchants_purchased_from,
    COUNT(DISTINCT dp.product_id) AS unique_products_purchased
FROM fact_orders fo
LEFT JOIN dim_user du ON fo.user_sk = du.user_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
LEFT JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN dim_merchant dm ON fo.merchant_sk = dm.merchant_sk
GROUP BY 
    du.user_type,
    du.country,
    du.city
ORDER BY total_revenue DESC;

-- ============================================================================
-- VIEW 4: Revenue by Segment and Time (for trend analysis)
-- ============================================================================
-- Combines customer segment and time dimensions for trend analysis
CREATE OR REPLACE VIEW vw_segment_revenue_by_time AS
SELECT 
    dd.date,
    dd.year,
    dd.month,
    dd.month_name,
    dd.quarter,
    du.user_type AS customer_segment,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.user_sk) AS unique_customers,
    SUM(fli.quantity) AS total_items_sold,
    SUM(fli.quantity * dp.price) AS total_revenue,
    AVG(fli.quantity * dp.price) AS avg_order_value
FROM fact_orders fo
LEFT JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN dim_user du ON fo.user_sk = du.user_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
GROUP BY 
    dd.date,
    dd.year,
    dd.month,
    dd.month_name,
    dd.quarter,
    du.user_type
ORDER BY dd.date, du.user_type;

-- ============================================================================
-- VIEW 5: Sales by Time Period
-- ============================================================================
-- Additional analytical view for time-based analysis
CREATE OR REPLACE VIEW vw_sales_by_time AS
SELECT 
    dd.date,
    dd.year,
    dd.month,
    dd.month_name,
    dd.quarter,
    dd.day,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.user_sk) AS unique_customers,
    SUM(fli.quantity) AS total_items_sold,
    SUM(fli.quantity * dp.price) AS total_revenue,
    AVG(fli.quantity * dp.price) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN fo.delay_days > 0 THEN fo.order_id END) AS delayed_orders,
    AVG(fo.delay_days) AS avg_delay_days
FROM fact_orders fo
LEFT JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
GROUP BY 
    dd.date,
    dd.year,
    dd.month,
    dd.month_name,
    dd.quarter,
    dd.day
ORDER BY dd.date DESC;

-- ============================================================================
-- VIEW 5: Product Performance Analysis
-- ============================================================================
-- Additional view for product-level insights
CREATE OR REPLACE VIEW vw_product_performance AS
SELECT 
    dp.product_id,
    dp.product_name,
    dp.product_type,
    dp.price,
    COUNT(DISTINCT fli.order_id) AS total_orders,
    COUNT(DISTINCT fo.user_sk) AS unique_customers,
    SUM(fli.quantity) AS total_quantity_sold,
    SUM(fli.quantity * dp.price) AS total_revenue,
    AVG(fli.quantity) AS avg_quantity_per_order,
    COUNT(DISTINCT dm.merchant_id) AS merchants_selling
FROM fact_line_items fli
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
LEFT JOIN fact_orders fo ON fli.order_id = fo.order_id
LEFT JOIN dim_merchant dm ON fo.merchant_sk = dm.merchant_sk
GROUP BY 
    dp.product_id,
    dp.product_name,
    dp.product_type,
    dp.price
ORDER BY total_revenue DESC;

-- ============================================================================
-- VIEW 6: Staff Performance Metrics
-- ============================================================================
-- Additional view for staff productivity analysis
CREATE OR REPLACE VIEW vw_staff_performance AS
SELECT 
    ds.staff_id,
    ds.name AS staff_name,
    ds.job_level AS staff_job_level,
    ds.city AS staff_city,
    ds.state AS staff_state,
    COUNT(DISTINCT fo.order_id) AS total_orders_processed,
    COUNT(DISTINCT fo.user_sk) AS unique_customers_served,
    SUM(fli.quantity) AS total_items_processed,
    SUM(fli.quantity * dp.price) AS total_revenue_generated,
    AVG(fli.quantity * dp.price) AS avg_order_value,
    COUNT(DISTINCT dm.merchant_id) AS merchants_worked_with
FROM fact_orders fo
LEFT JOIN dim_staff ds ON fo.staff_sk = ds.staff_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
LEFT JOIN dim_merchant dm ON fo.merchant_sk = dm.merchant_sk
GROUP BY 
    ds.staff_id,
    ds.name,
    ds.job_level,
    ds.city,
    ds.state
ORDER BY total_revenue_generated DESC;

-- ============================================================================
-- Grant permissions for BI tools
-- ============================================================================
-- Note: Adjust user/role names based on your BI tool connection requirements
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO bi_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO powerbi_user;

