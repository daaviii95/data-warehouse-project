-- Create Analytical Views for Business Intelligence
-- These views support the three main business questions:
-- 1. What kinds of campaigns drive the highest order volume?
-- 2. How do merchant performance metrics affect sales?
-- 3. What customer segments contribute most to revenue?

-- View 1: Campaign Performance Analysis
DROP VIEW IF EXISTS vw_campaign_performance CASCADE;

CREATE VIEW vw_campaign_performance AS
SELECT 
    dc.campaign_id,
    dc.campaign_name,
    dc.discount,
    COUNT(DISTINCT fct.order_id) AS total_orders,
    COUNT(DISTINCT fct.user_sk) AS unique_customers,
    SUM(fct.availed) AS times_availed,
    COUNT(*) - SUM(fct.availed) AS times_not_availed,
    ROUND(100.0 * SUM(fct.availed) / COUNT(*), 2) AS availed_percentage,
    SUM(fli.price * fli.quantity) AS total_revenue,
    AVG(fli.price * fli.quantity) AS avg_order_value
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
LEFT JOIN fact_line_items fli 
    ON fct.order_id = fli.order_id
    AND fct.user_sk = fli.user_sk
    AND fct.merchant_sk = fli.merchant_sk
    AND fct.transaction_date_sk = fli.transaction_date_sk
GROUP BY dc.campaign_id, dc.campaign_name, dc.discount
ORDER BY total_orders DESC, total_revenue DESC;

-- View 2: Merchant Performance Metrics
DROP VIEW IF EXISTS vw_merchant_performance CASCADE;

CREATE VIEW vw_merchant_performance AS
SELECT 
    dm.merchant_id,
    dm.name AS merchant_name,
    dm.city,
    dm.state,
    dm.country,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.user_sk) AS unique_customers,
    COUNT(DISTINCT fli.product_sk) AS unique_products_sold,
    SUM(fli.price * fli.quantity) AS total_revenue,
    AVG(fli.price * fli.quantity) AS avg_order_value,
    AVG(fo.estimated_arrival_days) AS avg_estimated_arrival_days,
    AVG(fo.delay_days) AS avg_delay_days,
    SUM(CASE WHEN fo.delay_days > 0 THEN 1 ELSE 0 END) AS delayed_orders,
    COUNT(*) AS total_line_items
FROM fact_orders fo
INNER JOIN dim_merchant dm ON fo.merchant_sk = dm.merchant_sk
LEFT JOIN fact_line_items fli 
    ON fo.order_id = fli.order_id
    AND fo.user_sk = fli.user_sk
    AND fo.merchant_sk = fli.merchant_sk
    AND fo.staff_sk = fli.staff_sk
    AND fo.transaction_date_sk = fli.transaction_date_sk
GROUP BY dm.merchant_id, dm.name, dm.city, dm.state, dm.country
ORDER BY total_revenue DESC;

-- View 3: Customer Segment Revenue Analysis
DROP VIEW IF EXISTS vw_customer_segment_revenue CASCADE;

CREATE VIEW vw_customer_segment_revenue AS
SELECT 
    du.user_id,
    du.name AS customer_name,
    du.user_type,
    du.city AS customer_city,
    du.state AS customer_state,
    du.country AS customer_country,
    duj.job_title,
    duj.job_level,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fli.price * fli.quantity) AS total_revenue,
    AVG(fli.price * fli.quantity) AS avg_order_value,
    SUM(fli.quantity) AS total_items_purchased,
    MIN(dd.date) AS first_purchase_date,
    MAX(dd.date) AS last_purchase_date,
    COUNT(DISTINCT fli.product_sk) AS unique_products_purchased,
    COUNT(DISTINCT fo.merchant_sk) AS merchants_purchased_from
FROM fact_orders fo
INNER JOIN dim_user du ON fo.user_sk = du.user_sk
LEFT JOIN dim_user_job duj ON du.user_id = duj.user_id
INNER JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN fact_line_items fli 
    ON fo.order_id = fli.order_id
    AND fo.user_sk = fli.user_sk
    AND fo.merchant_sk = fli.merchant_sk
    AND fo.staff_sk = fli.staff_sk
    AND fo.transaction_date_sk = fli.transaction_date_sk
GROUP BY du.user_id, du.name, du.user_type, du.city, du.state, du.country, duj.job_title, duj.job_level
ORDER BY total_revenue DESC;

-- View 4: Time-based Sales Analysis
DROP VIEW IF EXISTS vw_sales_by_time CASCADE;

CREATE VIEW vw_sales_by_time AS
SELECT 
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.user_sk) AS unique_customers,
    SUM(fli.price * fli.quantity) AS total_revenue,
    AVG(fli.price * fli.quantity) AS avg_order_value,
    SUM(fli.quantity) AS total_items_sold
FROM fact_orders fo
INNER JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
LEFT JOIN fact_line_items fli 
    ON fo.order_id = fli.order_id
    AND fo.user_sk = fli.user_sk
    AND fo.merchant_sk = fli.merchant_sk
    AND fo.staff_sk = fli.staff_sk
    AND fo.transaction_date_sk = fli.transaction_date_sk
GROUP BY dd.year, dd.quarter, dd.month, dd.month_name
ORDER BY dd.year DESC, dd.month DESC;

-- View 5: Product Performance Analysis
DROP VIEW IF EXISTS vw_product_performance CASCADE;

CREATE VIEW vw_product_performance AS
SELECT 
    dp.product_id,
    dp.product_name,
    dp.product_type,
    dp.price AS base_price,
    COUNT(DISTINCT fli.order_id) AS total_orders,
    SUM(fli.quantity) AS total_quantity_sold,
    SUM(fli.price * fli.quantity) AS total_revenue,
    AVG(fli.price) AS avg_selling_price,
    COUNT(DISTINCT fli.user_sk) AS unique_customers,
    COUNT(DISTINCT fli.merchant_sk) AS merchants_selling
FROM fact_line_items fli
INNER JOIN dim_product dp ON fli.product_sk = dp.product_sk
GROUP BY dp.product_id, dp.product_name, dp.product_type, dp.price
ORDER BY total_revenue DESC;

-- View 6: Staff Performance Analysis
DROP VIEW IF EXISTS vw_staff_performance CASCADE;

CREATE VIEW vw_staff_performance AS
SELECT 
    ds.staff_id,
    ds.name AS staff_name,
    ds.job_level,
    ds.city,
    ds.state,
    COUNT(DISTINCT fo.order_id) AS orders_processed,
    COUNT(DISTINCT fo.user_sk) AS unique_customers_served,
    SUM(fli.price * fli.quantity) AS total_revenue_processed,
    AVG(fo.estimated_arrival_days) AS avg_estimated_arrival,
    AVG(fo.delay_days) AS avg_delay_days
FROM fact_orders fo
INNER JOIN dim_staff ds ON fo.staff_sk = ds.staff_sk
LEFT JOIN fact_line_items fli 
    ON fo.order_id = fli.order_id
    AND fo.user_sk = fli.user_sk
    AND fo.merchant_sk = fli.merchant_sk
    AND fo.staff_sk = fli.staff_sk
    AND fo.transaction_date_sk = fli.transaction_date_sk
GROUP BY ds.staff_id, ds.name, ds.job_level, ds.city, ds.state
ORDER BY total_revenue_processed DESC;

