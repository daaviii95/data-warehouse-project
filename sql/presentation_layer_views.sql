-- ============================================================================
-- Presentation Layer Views
-- ============================================================================
-- Business Intelligence views for reporting and analytics
-- ============================================================================

-- Sales Summary by Date
CREATE OR REPLACE VIEW vw_sales_summary_daily AS
SELECT
    d.date_actual,
    d.year_number,
    d.month_number,
    d.month_name,
    d.quarter_name,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    COUNT(fs.sales_key) AS total_line_items,
    SUM(fs.quantity) AS total_quantity,
    SUM(fs.line_total) AS total_revenue,
    SUM(fs.discount_amount) AS total_discounts,
    SUM(fs.line_total - COALESCE(fs.cost_amount, 0)) AS total_profit,
    AVG(fs.line_total) AS avg_line_total
FROM fact_sales fs
JOIN dim_date d ON fs.order_date_key = d.date_key
GROUP BY d.date_actual, d.year_number, d.month_number, d.month_name, d.quarter_name
ORDER BY d.date_actual DESC;

-- Sales by Product Category
CREATE OR REPLACE VIEW vw_sales_by_product_category AS
SELECT
    p.product_category,
    p.product_subcategory,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    COUNT(fs.sales_key) AS total_line_items,
    SUM(fs.quantity) AS total_quantity_sold,
    SUM(fs.line_total) AS total_revenue,
    AVG(fs.unit_price) AS avg_unit_price,
    SUM(fs.line_total - COALESCE(fs.cost_amount, 0)) AS total_profit
FROM fact_sales fs
JOIN dim_product p ON fs.product_key = p.product_key
WHERE p.product_category IS NOT NULL
GROUP BY p.product_category, p.product_subcategory
ORDER BY total_revenue DESC;

-- Sales by Customer
CREATE OR REPLACE VIEW vw_sales_by_customer AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.job_category,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    SUM(fs.quantity) AS total_items_purchased,
    SUM(fs.line_total) AS total_revenue,
    AVG(fs.line_total) AS avg_order_value,
    MAX(d.date_actual) AS last_order_date
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_key = c.customer_key
JOIN dim_date d ON fs.order_date_key = d.date_key
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.job_category
ORDER BY total_revenue DESC;

-- Sales by Merchant
CREATE OR REPLACE VIEW vw_sales_by_merchant AS
SELECT
    m.merchant_id,
    m.merchant_name,
    m.merchant_category,
    m.country,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    COUNT(fs.sales_key) AS total_line_items,
    SUM(fs.quantity) AS total_quantity,
    SUM(fs.line_total) AS total_revenue
FROM fact_sales fs
JOIN dim_merchant m ON fs.merchant_key = m.merchant_key
GROUP BY m.merchant_id, m.merchant_name, m.merchant_category, m.country
ORDER BY total_revenue DESC;

-- Campaign Performance
CREATE OR REPLACE VIEW vw_campaign_performance AS
SELECT
    c.campaign_id,
    c.campaign_name,
    c.campaign_type,
    d.year_number,
    d.month_name,
    COUNT(fcp.campaign_performance_key) AS total_transactions,
    SUM(fcp.revenue_amount) AS total_revenue,
    SUM(fcp.discount_amount) AS total_discounts,
    SUM(fcp.net_revenue_amount) AS net_revenue,
    c.budget,
    (SUM(fcp.net_revenue_amount) / NULLIF(c.budget, 0)) * 100 AS roi_percent
FROM fact_campaign_performance fcp
JOIN dim_campaign c ON fcp.campaign_key = c.campaign_key
JOIN dim_date d ON fcp.transaction_date_key = d.date_key
GROUP BY c.campaign_id, c.campaign_name, c.campaign_type, c.budget, d.year_number, d.month_name
ORDER BY net_revenue DESC;

-- Order Delays Analysis
CREATE OR REPLACE VIEW vw_order_delays AS
SELECT
    d.date_actual,
    d.month_name,
    COUNT(CASE WHEN fs.is_delayed THEN 1 END) AS delayed_orders,
    COUNT(fs.order_id) AS total_orders,
    ROUND(
        (COUNT(CASE WHEN fs.is_delayed THEN 1 END)::NUMERIC / COUNT(fs.order_id)) * 100, 
        2
    ) AS delay_percentage,
    AVG(fs.delay_days) AS avg_delay_days,
    AVG(fs.estimated_arrival_days) AS avg_estimated_arrival
FROM fact_sales fs
JOIN dim_date d ON fs.order_date_key = d.date_key
WHERE fs.estimated_arrival_days IS NOT NULL
GROUP BY d.date_actual, d.month_name
HAVING COUNT(fs.order_id) > 0
ORDER BY d.date_actual DESC;

-- Monthly Sales Trend
CREATE OR REPLACE VIEW vw_monthly_sales_trend AS
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    COUNT(fs.sales_key) AS total_line_items,
    SUM(fs.line_total) AS total_revenue,
    SUM(fs.discount_amount) AS total_discounts,
    SUM(fs.line_total - COALESCE(fs.cost_amount, 0)) AS total_profit,
    LAG(SUM(fs.line_total)) OVER (ORDER BY d.year_number, d.month_number) AS prev_month_revenue,
    SUM(fs.line_total) - LAG(SUM(fs.line_total)) OVER (ORDER BY d.year_number, d.month_number) AS revenue_change
FROM fact_sales fs
JOIN dim_date d ON fs.order_date_key = d.date_key
GROUP BY d.year_number, d.month_number, d.month_name
ORDER BY d.year_number, d.month_number;

COMMENT ON VIEW vw_sales_summary_daily IS 'Daily sales summary with key metrics';
COMMENT ON VIEW vw_sales_by_product_category IS 'Sales performance by product category';
COMMENT ON VIEW vw_sales_by_customer IS 'Customer purchase behavior and value';
COMMENT ON VIEW vw_sales_by_merchant IS 'Sales performance by merchant';
COMMENT ON VIEW vw_campaign_performance IS 'Marketing campaign ROI and performance';
COMMENT ON VIEW vw_order_delays IS 'Order delivery delay analysis';
COMMENT ON VIEW vw_monthly_sales_trend IS 'Monthly sales trends with period-over-period comparison';

