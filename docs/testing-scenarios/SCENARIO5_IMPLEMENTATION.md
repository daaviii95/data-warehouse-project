# Scenario 5 - Performance & Aggregation Consistency

## Overview

Demonstrate that your analytical layer is both consistent and reasonably performant by:
1. Showing a KPI/report in your BI tool (Power BI)
2. Running an equivalent SQL query directly on the warehouse
3. Verifying the totals/metrics match (within rounding differences)
4. Recording execution times
5. Explaining why cross-checking is important

---

## Recommended KPIs to Test

Based on your analytical views, here are good options:

### Option 1: Monthly Sales Revenue (Recommended)
- **KPI**: Total Revenue by Month
- **View**: `vw_sales_by_time`
- **Why**: Simple aggregation, easy to verify, common business metric

### Option 2: Top 10 Campaigns by Order Volume
- **KPI**: Total Orders by Campaign
- **View**: `vw_campaign_performance`
- **Why**: Business Question 1, clear ranking metric

### Option 3: Customer Segment Revenue Distribution
- **KPI**: Total Revenue by Customer Segment (user_type)
- **View**: `vw_customer_segment_revenue`
- **Why**: Business Question 3, segment analysis

### Option 4: Top 10 Merchants by Revenue
- **KPI**: Total Revenue by Merchant
- **View**: `vw_merchant_performance`
- **Why**: Business Question 2, merchant ranking

---

## Step-by-Step Implementation Guide

### Step 1: Choose Your KPI

**Recommended**: **Monthly Sales Revenue** (Option 1)
- Simple to verify
- Clear aggregation
- Common business metric
- Easy to show in both Power BI and SQL

### Step 2: Create Power BI Visualization

#### 2.1 Connect to Database
- Use existing Power BI connection to PostgreSQL
- Connect to `vw_sales_by_time` view

#### 2.2 Create KPI Card
1. **Insert** → **Visualizations** → **Card**
2. **Fields**: 
   - Drag `total_revenue` to **Fields**
   - Format as **Currency** ($)
3. **Title**: "Total Revenue"

#### 2.3 Create Monthly Revenue Chart
1. **Insert** → **Visualizations** → **Column Chart**
2. **Axis**: `month_name` (or `date`)
3. **Values**: `total_revenue`
4. **Title**: "Monthly Sales Revenue"

#### 2.4 Record Dashboard Metrics
- **Total Revenue**: Note the value
- **Monthly breakdown**: Note values for each month
- **Execution Time**: Note how long the dashboard takes to refresh

### Step 3: Create Equivalent SQL Query

#### 3.1 Direct SQL Query (Matching Power BI)

**Query 1: Total Revenue (KPI Card Equivalent)**
```sql
-- Total Revenue (matches Power BI KPI card)
SELECT 
    SUM(total_revenue) AS total_revenue
FROM vw_sales_by_time;
```

**Query 2: Monthly Revenue Breakdown (Chart Equivalent)**
```sql
-- Monthly Revenue Breakdown (matches Power BI column chart)
SELECT 
    year,
    month,
    month_name,
    SUM(total_revenue) AS monthly_revenue,
    COUNT(DISTINCT order_id) AS total_orders
FROM vw_sales_by_time
GROUP BY year, month, month_name
ORDER BY year, month;
```

**Query 3: Detailed Monthly Breakdown (Alternative)**
```sql
-- More detailed monthly breakdown
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
```

#### 3.2 Alternative: Query Directly from Fact Tables (Without View)

**Direct Fact Table Query (for comparison)**
```sql
-- Total Revenue from fact tables (bypassing view)
SELECT 
    SUM(fli.quantity * dp.price) AS total_revenue
FROM fact_line_items fli
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk;
```

**Monthly Revenue from Fact Tables**
```sql
-- Monthly Revenue from fact tables
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
```

### Step 4: Verify Consistency

#### 4.1 Compare Totals

**Power BI Total Revenue** vs **SQL Total Revenue**
- Should match exactly (or within 0.01% for rounding)

**Power BI Monthly Values** vs **SQL Monthly Values**
- Each month should match
- Small rounding differences (< 0.01) are acceptable

#### 4.2 SQL Verification Query

```sql
-- Comprehensive verification query
WITH power_bi_total AS (
    -- This simulates what Power BI calculates
    SELECT SUM(total_revenue) AS total_revenue
    FROM vw_sales_by_time
),
sql_direct_total AS (
    -- This is the direct SQL calculation
    SELECT SUM(fli.quantity * dp.price) AS total_revenue
    FROM fact_line_items fli
    LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
)
SELECT 
    pbi.total_revenue AS power_bi_total,
    sqld.total_revenue AS sql_direct_total,
    ABS(pbi.total_revenue - sqld.total_revenue) AS difference,
    ROUND(
        ABS(pbi.total_revenue - sqld.total_revenue) / NULLIF(pbi.total_revenue, 0) * 100, 
        4
    ) AS percent_difference
FROM power_bi_total pbi
CROSS JOIN sql_direct_total sqld;
```

**Expected Result:**
- `difference` should be 0 or very small (< 0.01)
- `percent_difference` should be 0.0000% or < 0.01%

### Step 5: Performance Testing

#### 5.1 Measure SQL Query Execution Time

**Using PostgreSQL `EXPLAIN ANALYZE`:**
```sql
-- Measure execution time for view query
EXPLAIN ANALYZE
SELECT 
    year,
    month,
    month_name,
    SUM(total_revenue) AS monthly_revenue
FROM vw_sales_by_time
GROUP BY year, month, month_name
ORDER BY year, month;
```

**Look for:**
- **Execution Time**: Should be < 5 seconds for typical dataset
- **Planning Time**: Should be < 100ms
- **Total Time**: Planning + Execution

#### 5.2 Measure Power BI Refresh Time

1. **Data** view → **Refresh** button
2. Note the time shown in Power BI status bar
3. Or use **View** → **Performance Analyzer** for detailed timing

#### 5.3 Performance Benchmarks

**Acceptable Performance:**
- **SQL Query**: < 5 seconds for aggregated queries
- **Power BI Refresh**: < 10 seconds for view-based queries
- **Dashboard Load**: < 3 seconds after data refresh

**If Performance is Poor:**
- Check if indexes exist on fact tables
- Consider materializing views for large datasets
- Optimize Power BI query (use DirectQuery vs Import mode)

### Step 6: Document Results

Create a comparison table:

| Metric | Power BI Value | SQL Query Value | Difference | Match? |
|--------|---------------|-----------------|------------|--------|
| Total Revenue | $X,XXX,XXX.XX | $X,XXX,XXX.XX | $X.XX | ✅ Yes |
| Jan 2023 Revenue | $XXX,XXX.XX | $XXX,XXX.XX | $X.XX | ✅ Yes |
| Feb 2023 Revenue | $XXX,XXX.XX | $XXX,XXX.XX | $X.XX | ✅ Yes |
| ... | ... | ... | ... | ... |

**Performance Metrics:**
- SQL Query Execution Time: X.XX seconds
- Power BI Refresh Time: X.XX seconds
- Dashboard Load Time: X.XX seconds
- **Assessment**: ✅ Acceptable / ⚠️ Needs Optimization

---

## Alternative KPIs with SQL Queries

### Option 2: Top 10 Campaigns by Order Volume

**Power BI:**
- Use `vw_campaign_performance` view
- Sort by `total_orders` DESC
- Show top 10

**SQL Equivalent:**
```sql
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
```

**Verification:**
```sql
-- Verify total orders across all campaigns
SELECT 
    SUM(total_orders) AS total_campaign_orders
FROM vw_campaign_performance;

-- Compare with fact table
SELECT 
    COUNT(DISTINCT fct.order_id) AS total_campaign_orders
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
WHERE dc.campaign_name != 'No Campaign';
```

### Option 3: Customer Segment Revenue

**Power BI:**
- Use `vw_customer_segment_revenue` view
- Group by `customer_segment` (user_type)
- Sum `total_revenue`

**SQL Equivalent:**
```sql
SELECT 
    customer_segment,
    SUM(total_revenue) AS segment_revenue,
    SUM(total_orders) AS segment_orders,
    COUNT(DISTINCT unique_customers) AS segment_customers,
    AVG(avg_order_value) AS segment_avg_order_value
FROM vw_customer_segment_revenue
GROUP BY customer_segment
ORDER BY segment_revenue DESC;
```

**Verification:**
```sql
-- Verify segment totals match
SELECT 
    du.user_type AS customer_segment,
    SUM(fli.quantity * dp.price) AS segment_revenue
FROM fact_orders fo
LEFT JOIN dim_user du ON fo.user_sk = du.user_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
GROUP BY du.user_type
ORDER BY segment_revenue DESC;
```

### Option 4: Top 10 Merchants by Revenue

**Power BI:**
- Use `vw_merchant_performance` view
- Sort by `total_revenue` DESC
- Show top 10

**SQL Equivalent:**
```sql
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
```

---

## Why Cross-Checking SQL vs Dashboard is Important

### Sample Explanation (2-3 sentences):

**Option 1 (Concise):**
"Cross-checking SQL queries against dashboard visualizations ensures data accuracy and validates that BI tool aggregations match the underlying data warehouse calculations. This verification process catches potential errors in view definitions, DAX measures, or data transformation logic, preventing incorrect business decisions based on faulty metrics. Additionally, it helps identify performance bottlenecks and ensures the analytical layer provides reliable, consistent results across different access methods."

**Option 2 (Technical):**
"Validating dashboard metrics against direct SQL queries is critical for data integrity because it confirms that the analytical views, DAX measures, and BI tool aggregations accurately reflect the source data. This cross-validation process detects discrepancies that could arise from incorrect joins, aggregation logic errors, or data type mismatches, ensuring stakeholders can trust the metrics for decision-making. It also serves as a performance benchmark, helping identify when views need optimization or when materialized views should be considered for large datasets."

**Option 3 (Business-focused):**
"Cross-checking SQL queries with dashboard results is essential for maintaining data quality and building trust in business intelligence reports. This verification process ensures that the KPIs and metrics displayed to stakeholders accurately represent the actual data in the warehouse, preventing costly decisions based on incorrect information. By regularly performing these consistency checks, we can quickly identify and resolve any issues with data transformations, view definitions, or BI tool configurations before they impact business operations."

---

## Screen Recording Checklist

For your video submission, make sure to show:

1. ✅ **Power BI Dashboard View**
   - Show the KPI card/chart
   - Display the values clearly
   - Note the execution/refresh time

2. ✅ **SQL Query Execution**
   - Open database client (pgAdmin, DBeaver, or psql)
   - Execute the equivalent SQL query
   - Show the results

3. ✅ **Side-by-Side Comparison**
   - Split screen or switch between Power BI and SQL results
   - Highlight matching values
   - Note any small rounding differences (if any)

4. ✅ **Performance Metrics**
   - Show SQL query execution time (from EXPLAIN ANALYZE or query tool)
   - Show Power BI refresh time
   - Comment on whether performance is acceptable

5. ✅ **Spoken Explanation**
   - Explain why cross-checking is important (use one of the sample explanations above)
   - Mention specific benefits (accuracy, trust, error detection)

---

## Quick Reference: SQL Queries for Each View

### vw_sales_by_time
```sql
-- Total Revenue
SELECT SUM(total_revenue) FROM vw_sales_by_time;

-- Monthly Revenue
SELECT year, month, month_name, SUM(total_revenue) AS monthly_revenue
FROM vw_sales_by_time
GROUP BY year, month, month_name
ORDER BY year, month;
```

### vw_campaign_performance
```sql
-- Top Campaigns
SELECT campaign_name, total_orders, total_revenue
FROM vw_campaign_performance
ORDER BY total_orders DESC
LIMIT 10;
```

### vw_customer_segment_revenue
```sql
-- Revenue by Segment
SELECT customer_segment, SUM(total_revenue) AS segment_revenue
FROM vw_customer_segment_revenue
GROUP BY customer_segment
ORDER BY segment_revenue DESC;
```

### vw_merchant_performance
```sql
-- Top Merchants
SELECT merchant_name, total_revenue, total_orders
FROM vw_merchant_performance
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## Troubleshooting

### Values Don't Match

**Check:**
1. Are you using the same date filters in both?
2. Are NULL values handled the same way?
3. Are aggregations using SUM vs COUNT correctly?
4. Check for rounding differences (acceptable if < 0.01%)

**Common Issues:**
- Power BI might filter out NULLs automatically
- SQL might include NULLs in calculations
- Date filters might be applied differently

### Performance is Slow

**Solutions:**
1. Check if indexes exist on fact tables
2. Use `EXPLAIN ANALYZE` to identify slow operations
3. Consider materializing views
4. Use DirectQuery mode in Power BI for real-time data
5. Limit date ranges in queries

---

## Example Script for Testing

Save this as `scenario5_verification.sql`:

```sql
-- Scenario 5: Performance & Aggregation Consistency Verification
-- KPI: Monthly Sales Revenue

-- Step 1: Total Revenue from View (Power BI equivalent)
SELECT 
    'Total Revenue (View)' AS metric,
    SUM(total_revenue) AS value,
    NOW() AS query_time
FROM vw_sales_by_time;

-- Step 2: Monthly Breakdown (Power BI chart equivalent)
SELECT 
    year,
    month,
    month_name,
    SUM(total_revenue) AS monthly_revenue,
    SUM(total_orders) AS total_orders,
    COUNT(*) AS record_count
FROM vw_sales_by_time
GROUP BY year, month, month_name
ORDER BY year, month;

-- Step 3: Direct SQL (bypassing view) for verification
SELECT 
    'Total Revenue (Direct)' AS metric,
    SUM(fli.quantity * dp.price) AS value,
    NOW() AS query_time
FROM fact_line_items fli
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk;

-- Step 4: Performance measurement
EXPLAIN ANALYZE
SELECT 
    year,
    month,
    month_name,
    SUM(total_revenue) AS monthly_revenue
FROM vw_sales_by_time
GROUP BY year, month, month_name
ORDER BY year, month;
```

---

## Next Steps

1. ✅ Choose your KPI (recommended: Monthly Sales Revenue)
2. ✅ Create Power BI visualization
3. ✅ Write equivalent SQL query
4. ✅ Run verification queries
5. ✅ Measure performance
6. ✅ Record screen capture
7. ✅ Prepare explanation

Good luck with Scenario 5! 🚀

