# ShopZada Business Intelligence - Key Questions

This document outlines the three main business questions addressed by the data warehouse and the analytical views that support them.

## Question 1: What kinds of campaigns drive the highest order volume?

### Business Context
Marketing teams need to understand which campaigns are most effective at driving orders and revenue.

### Analytical View: `vw_campaign_performance`

**Key Metrics:**
- Total orders per campaign
- Unique customers reached
- Campaign availed rate (percentage)
- Total revenue generated
- Average order value

**SQL Query Example:**
```sql
SELECT 
    campaign_name,
    discount,
    total_orders,
    unique_customers,
    campaign_avail_rate_pct,
    total_revenue,
    avg_order_value
FROM vw_campaign_performance
ORDER BY total_orders DESC, total_revenue DESC
LIMIT 10;
```

**Insights Provided:**
- Which campaigns have the highest adoption rates
- Correlation between discount levels and order volume
- Campaign effectiveness by customer segment
- Revenue impact of different campaign types

**Dashboard Visualization:**
- Bar chart: Top 10 campaigns by order volume
- Scatter plot: Discount % vs. Total Revenue
- Line chart: Campaign performance over time
- Pie chart: Campaign availed vs. not availed

---

## Question 2: How do merchant performance metrics affect sales?

### Business Context
Operations and business development teams need to understand merchant performance to optimize partnerships and identify high-performing vendors.

### Analytical View: `vw_merchant_performance`

**Key Metrics:**
- Total orders processed
- Unique customers served
- Unique products sold
- Total revenue generated
- Average order value
- Average estimated arrival days
- Average delay days
- Delayed orders count

**SQL Query Example:**
```sql
SELECT 
    merchant_name,
    merchant_city,
    merchant_state,
    total_orders,
    total_revenue,
    avg_order_value,
    avg_delay_days,
    delayed_orders,
    ROUND(100.0 * delayed_orders / total_orders, 2) AS delay_percentage
FROM vw_merchant_performance
ORDER BY total_revenue DESC
LIMIT 20;
```

**Insights Provided:**
- Top-performing merchants by revenue
- Geographic distribution of merchant performance
- Delivery performance (on-time vs. delayed)
- Product diversity by merchant
- Customer retention by merchant

**Dashboard Visualization:**
- Map: Merchant locations with revenue bubbles
- Bar chart: Top merchants by revenue
- Scatter plot: Order volume vs. Delay days
- Heatmap: Merchant performance by region
- Gauge chart: On-time delivery percentage

---

## Question 3: What customer segments contribute most to revenue?

### Business Context
Marketing and sales teams need to identify high-value customer segments for targeted marketing and retention strategies.

### Analytical View: `vw_customer_segment_revenue`

**Key Metrics:**
- Total orders per customer
- Total revenue per customer
- Average order value
- Total items purchased
- First and last purchase dates
- Unique products purchased
- Merchants purchased from
- Customer demographics (user_type, job_title, job_level, location)

**SQL Query Example:**
```sql
-- Top customers by revenue
SELECT 
    customer_segment,
    customer_country,
    customer_city,
    total_orders,
    total_revenue,
    avg_order_value,
    unique_products_purchased
FROM vw_customer_segment_revenue
ORDER BY total_revenue DESC
LIMIT 50;

-- Revenue by user type
SELECT 
    customer_segment,
    SUM(total_revenue) AS segment_revenue,
    SUM(total_orders) AS segment_orders,
    SUM(unique_customers) AS segment_customers
FROM vw_customer_segment_revenue
GROUP BY customer_segment
ORDER BY segment_revenue DESC;

-- Note: job_title/job_level live in separate tables and are not included in vw_customer_segment_revenue.
-- If you need job-level analysis, join dim_user_job to dim_user and then to facts.
```

**Insights Provided:**
- High-value customer identification
- Customer lifetime value (CLV) analysis
- Segment-based revenue distribution
- Geographic revenue patterns
- Product preferences by segment
- Purchase frequency analysis

**Dashboard Visualization:**
- Treemap: Revenue by customer segment
- Bar chart: Top 20 customers by revenue
- Pie chart: Revenue distribution by user_type
- Map: Revenue by customer location
- Funnel chart: Customer segments by revenue contribution
- Cohort analysis: Customer retention over time

---

## Additional Analytical Views

### vw_sales_by_time
**Purpose**: Time-based sales analysis for trend identification

**Key Metrics:**
- Sales by year, quarter, month
- Order volume trends
- Revenue trends
- Customer acquisition trends

**Use Cases:**
- Seasonal pattern analysis
- Year-over-year growth
- Monthly performance tracking

### vw_product_performance
**Purpose**: Product-level performance analysis

**Key Metrics:**
- Total orders per product
- Quantity sold
- Revenue generated
- Average selling price
- Customer reach

**Use Cases:**
- Best-selling products
- Product pricing optimization
- Inventory planning

### vw_staff_performance
**Purpose**: Staff productivity and performance tracking

**Key Metrics:**
- Orders processed
- Customers served
- Revenue processed
- Delivery performance

**Use Cases:**
- Performance reviews
- Resource allocation
- Training needs identification

---

## Dashboard Recommendations

### Executive Dashboard
- **KPIs**: Total revenue, order volume, customer count
- **Charts**: Revenue trends, top campaigns, top merchants
- **Filters**: Date range, region, product category

### Marketing Dashboard
- **Focus**: Campaign performance, customer segments
- **Charts**: Campaign ROI, customer acquisition, segment revenue
- **Filters**: Campaign type, customer segment, time period

### Operations Dashboard
- **Focus**: Merchant performance, delivery metrics
- **Charts**: Merchant rankings, delivery performance, order volume
- **Filters**: Merchant, region, time period

### Sales Dashboard
- **Focus**: Revenue, customer value, product performance
- **Charts**: Revenue by segment, top customers, product sales
- **Filters**: Customer segment, product category, time period

---

## Implementation Notes

All analytical views are optimized for:
- **Fast Query Performance**: Pre-aggregated metrics
- **BI Tool Compatibility**: Standard SQL compatible with Tableau, Power BI, Looker Studio
- **Real-time Updates**: Views refresh when underlying data changes
- **Scalability**: Efficient joins and aggregations

Views can be materialized for even better performance if needed:
```sql
CREATE MATERIALIZED VIEW vw_campaign_performance_mv AS
SELECT * FROM vw_campaign_performance;

CREATE INDEX ON vw_campaign_performance_mv (campaign_id);
```

