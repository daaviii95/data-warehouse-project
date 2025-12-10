# Power BI Dashboard - Complete Guide

**Complete step-by-step guide to create professional Power BI dashboards for ShopZada Data Warehouse**

This guide combines setup, connection, design, and best practices into one comprehensive resource.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Step 1: Setup & Connection](#step-1-setup--connection)
3. [Step 2: Create Analytical Views](#step-2-create-analytical-views)
4. [Step 3: Connect Power BI to PostgreSQL](#step-3-connect-power-bi-to-postgresql)
5. [Step 4: Create Dashboard Pages](#step-4-create-dashboard-pages)
6. [Step 5: Create DAX Measures](#step-5-create-dax-measures)
7. [Step 6: Formatting & Styling](#step-6-formatting--styling)
8. [Step 7: Advanced Features](#step-7-advanced-features)
9. [Troubleshooting](#troubleshooting)
10. [Quick Reference](#quick-reference)

---

## Prerequisites

### Required Software

- [ ] **Power BI Desktop** installed (free download from [Microsoft](https://powerbi.microsoft.com/desktop/))
- [ ] **PostgreSQL ODBC Driver** (usually comes with Power BI, but may need to install separately)
- [ ] **Docker Desktop** running (for local database)

### System Requirements

- Docker containers running (`shopzada-db` and Airflow)
- ETL pipeline completed successfully
- Analytical views created in database
- At least 4GB RAM (8GB recommended for Docker)

### Verify Prerequisites

```bash
# Check Docker containers are running
docker ps | grep shopzada-db

# Test database connection
psql -h localhost -p 5432 -U postgres -d shopzada
# Password: postgres
```

---

## Step 1: Setup & Connection

### 1.1 Verify Database is Running

```bash
# Check if container is running
docker ps | grep shopzada-db

# Test connection
psql -h localhost -p 5432 -U postgres -d shopzada
```

### 1.2 Connection Details

You'll need these details for Power BI connection:

```
Server: localhost
Port: 5432
Database: shopzada
Username: postgres
Password: postgres
Data Connectivity mode: Import (recommended)
```

---

## Step 2: Create Analytical Views

The analytical views are SQL views that aggregate data for easy visualization. You need to create them **once** before connecting Power BI.

### Option A: Run via Airflow (Recommended)

1. Go to Airflow UI: `http://localhost:8080`
2. Find the `shopzada_analytical_views` DAG
3. Toggle it ON (if not already enabled)
4. Click **Trigger DAG** to run manually
5. Wait ~10-30 seconds for completion

### Option B: Run SQL Manually

**Using Docker:**
```bash
# From your local machine
docker exec -i shopzada-db psql -U postgres -d shopzada < sql/13_create_analytical_views.sql
```

**Using PowerShell (Windows):**
```powershell
Get-Content sql/13_create_analytical_views.sql | docker exec -i shopzada-db psql -U postgres -d shopzada
```

**Using psql directly:**
```bash
psql -h localhost -p 5432 -U postgres -d shopzada -f sql/13_create_analytical_views.sql
```

### Verify Views Created

```sql
-- Check if views exist
SELECT table_name 
FROM information_schema.views 
WHERE table_schema = 'public' 
AND table_name LIKE 'vw_%'
ORDER BY table_name;

-- Should show 6 views:
-- vw_campaign_performance
-- vw_customer_segment_revenue
-- vw_merchant_performance
-- vw_product_performance
-- vw_sales_by_time
-- vw_staff_performance
```

---

## Step 3: Connect Power BI to PostgreSQL

### 3.1 Open Power BI Desktop

1. Launch **Power BI Desktop**
2. Click **Get Data** → **More...**

### 3.2 Select PostgreSQL Database

1. In the **Get Data** dialog:
   - Select **Database** category
   - Choose **PostgreSQL database**
   - Click **Connect**

### 3.3 Enter Connection Details

Fill in the connection information:

```
Server: localhost
Port: 5432
Database: shopzada
Data Connectivity mode: Import (recommended for small-medium datasets)
    OR DirectQuery (for real-time data, requires stable connection)
```

**Advanced Options** (optional):
- Uncheck "Use SSL" (for local development)
- Command timeout: 600 seconds (for large queries)

### 3.4 Authentication

1. Select **Database** authentication
2. Enter credentials:
   - **Username**: `postgres`
   - **Password**: `postgres`
3. Click **OK**

### 3.5 Select Tables/Views

In the **Navigator** window, select the following analytical views:

**Primary Views (Required for Business Questions):**
- ✅ `vw_campaign_performance` - Campaign analysis
- ✅ `vw_merchant_performance` - Merchant analysis  
- ✅ `vw_customer_segment_revenue` - Customer segment analysis

**Additional Views (Optional but Recommended):**
- ✅ `vw_sales_by_time` - Time-based trends
- ✅ `vw_product_performance` - Product insights
- ✅ `vw_staff_performance` - Staff productivity

**Dimension Tables (Optional, for detailed analysis):**
- `dim_campaign`
- `dim_merchant`
- `dim_user`
- `dim_product`
- `dim_date`
- `dim_staff`

Click **Load** to import the data.

**Note**: If you encounter shared memory errors, see [Troubleshooting - Shared Memory Issues](#shared-memory-issues) below.

### 3.6 Review Data Types

1. Go to **Data** view (left sidebar)
2. Check that numeric fields are correctly typed:
   - `total_revenue`, `avg_order_value` → Decimal Number
   - `total_orders`, `quantity` → Whole Number
   - `date` → Date
   - `campaign_avail_rate_pct`, `delay_rate_pct` → Decimal Number (Percentage)

---

## Step 4: Create Dashboard Pages

Create **4 main dashboard pages** with a navigation sidebar:

1. **Sales and Order Processing Performance** (Executive Summary)
2. **Marketing Campaign Performance** (Question 1)
3. **Merchant Performance** (Question 2)
4. **Customer Segment Analysis** (Question 3)

### Navigation Sidebar Setup

1. **Insert** → **Buttons** → **Blank**
2. Create 4 buttons for navigation:
   - "Sales and Order Processing"
   - "Marketing Campaign"
   - "Merchant"
   - "Customer"

3. **Format each button:**
   - Background color: Dark blue (#1E3A5F or similar)
   - Text color: White
   - Font size: 14pt
   - Padding: 10px

4. **Add action to each button:**
   - Right-click button → **Format** → **Action**
   - Set **Type**: Page navigation
   - Select target page

5. **Arrange vertically** on the left side of each page

---

### Page 1: Sales and Order Processing Performance

**Purpose**: Executive summary dashboard showing overall business performance

#### Layout Structure

```
┌─────────────────────────────────────────────────────────┐
│  [Navigation Sidebar]  │  Main Dashboard Area           │
│                        │                                │
│  • Sales & Order       │  [Title: Sales and Order       │
│  • Marketing Campaign  │     Processing Performance]    │
│  • Merchant            │                                │
│  • Customer            │  [KPI Cards Row]               │
│                        │                                │
│                        │  [Filters: Year, Month]        │
│                        │                                │
│                        │  [Charts Area]                 │
└─────────────────────────────────────────────────────────┘
```

#### KPI Cards (Top Row)

Create 5 KPI cards using the **Card** visual:

**1. Total Revenue**
- **Visual**: Card
- **Value**: Create measure `Total Revenue = SUM('vw_sales_by_time'[total_revenue])`
- **Format**: Currency ($)
- **Title**: "Total Revenue"
- **Trend Axis**: `vw_sales_by_time[date]` (optional, for KPI visual)

**2. Total Orders**
- **Visual**: Card
- **Value**: Create measure `Total Orders = SUM('vw_sales_by_time'[total_orders])`
- **Format**: Whole number
- **Title**: "Total Orders"
- **Trend Axis**: `vw_sales_by_time[date]` (optional)

**3. Average Order Value**
- **Visual**: Card
- **Value**: Create measure `Average Order Value = AVERAGE('vw_sales_by_time'[avg_order_value])`
- **Format**: Currency ($)
- **Title**: "Average Order Value"
- **Trend Axis**: `vw_sales_by_time[date]` (optional)

**4. Late Delivery Rate**
- **Visual**: Card
- **Value**: Create measure:
  ```DAX
  Late Delivery Rate = 
  DIVIDE(
      SUM('vw_merchant_performance'[delayed_orders]),
      SUM('vw_merchant_performance'[total_orders]),
      0
  ) * 100
  ```
- **Format**: Percentage (1 decimal)
- **Title**: "Late Delivery Rate"
- **Trend Axis**: `vw_sales_by_time[date]` (optional)

**5. Total Late Orders**
- **Visual**: Card
- **Value**: Create measure `Total Late Orders = SUM('vw_merchant_performance'[delayed_orders])`
- **Format**: Whole number
- **Title**: "Total Late Orders"
- **Trend Axis**: `vw_sales_by_time[date]` (optional)

**Formatting:**
- Arrange horizontally across the top
- Size: Medium (consistent width)
- Background: Light gray or white
- Border: Subtle border
- Font: Bold for values (24-28pt), regular for titles (12pt)

#### Filters/Slicers (Top Right)

**1. Year Slicer**
- **Visual**: Slicer
- **Field**: `vw_sales_by_time[year]`
- **Style**: Buttons
- **Position**: Top right, below KPI cards

**2. Month Name Slicer**
- **Visual**: Slicer
- **Field**: `vw_sales_by_time[month_name]`
- **Style**: Buttons (2 rows)
- **Position**: Next to Year slicer

#### Charts (Main Area)

**Chart 1: Revenue by Product Type (Horizontal Bar Chart)**
- **Visual**: Bar chart (horizontal)
- **Y-axis**: `vw_product_performance[product_type]`
- **X-axis**: `SUM('vw_product_performance'[total_revenue])`
- **Title**: "Revenue by Product Type"
- **Position**: Left side, below filters
- **Format**: 
  - Sort: Descending by revenue
  - Data labels: On
  - Color: Gradient blue

**Chart 2: Late Orders by Day of Month (Vertical Bar Chart)**
- **Visual**: Column chart
- **X-axis**: `vw_sales_by_time[day]`
- **Y-axis**: `SUM('vw_sales_by_time'[delayed_orders])` ⚠️ **Use vw_sales_by_time, NOT vw_merchant_performance**
- **Title**: "Late Orders by Day of Month"
- **Position**: Bottom left
- **Format**:
  - Color: Red gradient (higher = darker)
  - Data labels: On
- **Note**: `vw_sales_by_time` now includes `delayed_orders` aggregated by day, so this will show the correct distribution across days of the month

**Chart 3: Revenue Trend Over Time (Line Chart)**
- **Visual**: Line chart
- **X-axis**: `vw_sales_by_time[date]`
- **Y-axis**: `SUM('vw_sales_by_time'[total_revenue])`
- **Title**: "Total Revenue Trend"
- **Position**: Bottom right
- **Format**:
  - Line color: Blue
  - Markers: On
  - Data labels: On (optional)

---

### Page 2: Marketing Campaign Performance

**Question**: What kinds of campaigns drive the highest order volume?

#### KPI Cards

1. **Total Campaign Orders**
   - Measure: `SUM('vw_campaign_performance'[total_orders])`
   - Format: Whole number

2. **Campaign Revenue**
   - Measure: `SUM('vw_campaign_performance'[total_revenue])`
   - Format: Currency ($)

3. **Average Campaign Avail Rate**
   - Measure: `AVERAGE('vw_campaign_performance'[campaign_avail_rate_pct])`
   - Format: Percentage

4. **Top Campaign Discount**
   - Measure: `MAX('vw_campaign_performance'[discount])`
   - Format: Percentage

#### Filters

- **Campaign Name** (Slicer)
- **Discount Range** (Slicer with min/max)

#### Charts

**1. All Campaigns by Order Volume (Horizontal Bar Chart)**
- **Visual**: Bar chart (horizontal)
- **Y-axis**: `vw_campaign_performance[campaign_name]`
- **X-axis**: `SUM('vw_campaign_performance'[total_orders])`
- **Color**: `discount` (gradient)
- **Sort**: Descending by `total_orders`
- **Title**: "Campaigns by Order Volume"
- **Note**: Shows all 10 marketing campaigns (excludes "No Campaign" entries)

**2. Campaign Revenue vs. Discount (Scatter Plot)**
- **Visual**: Scatter chart
- **X-axis**: `vw_campaign_performance[discount]`
- **Y-axis**: `SUM('vw_campaign_performance'[total_revenue])`
- **Size**: `SUM('vw_campaign_performance'[total_orders])` (bubble size represents order volume)
- **Legend/Color**: `vw_campaign_performance[campaign_name]` (colors data points by campaign, appears as legend)
- **Title**: "Campaign Revenue vs. Discount"
- **Note**: Each bubble represents a campaign. The legend shows different colors for each campaign name.

**3. Campaign Performance Table**
- **Visual**: Table
- **Columns**: 
  - `campaign_name`
  - `discount`
  - `total_orders`
  - `total_revenue`
  - `campaign_avail_rate_pct`
- **Conditional formatting**: For `campaign_avail_rate_pct` (green = high, red = low)
- **Sort**: By `total_orders` (descending)
- **Title**: "Campaign Performance Summary"

**4. Revenue Distribution by Campaign (Pie Chart)**
- **Visual**: Pie chart
- **Values**: `SUM('vw_campaign_performance'[total_revenue])`
- **Legend**: `vw_campaign_performance[campaign_name]`
- **Show**: All campaigns (10 total)
- **Title**: "Revenue Distribution by Campaign"
- **Note**: Only shows actual marketing campaigns (excludes "No Campaign")

---

### Page 3: Merchant Performance

**Question**: How do merchant performance metrics affect sales?

#### KPI Cards

1. **Total Merchant Revenue**
   - Measure: `SUM('vw_merchant_performance'[total_revenue])`
   - Format: Currency ($)

2. **Average Delay Days**
   - Measure: `AVERAGE('vw_merchant_performance'[avg_delay_days])`
   - Format: 1 decimal

3. **Overall Delay Rate**
   - Measure: 
     ```DAX
     Overall Delay Rate = 
     DIVIDE(
         SUM('vw_merchant_performance'[delayed_orders]), 
         SUM('vw_merchant_performance'[total_orders]), 
         0
     ) * 100
     ```
   - Format: Percentage

4. **Total Merchants**
   - Measure: `DISTINCTCOUNT('vw_merchant_performance'[merchant_id])`
   - Format: Whole number

#### Filters

- **Merchant Name** (Slicer)
- **Country/State** (Slicer)
- **Delay Threshold** (Slicer - see options below)

**Delay Threshold Filter Options:**

**Option 1: Range Slider (Recommended)**
- **Visual**: Slicer (Between)
- **Field**: `vw_merchant_performance[avg_delay_days]`
- **Type**: Between
- **Min**: 0
- **Max**: 10
- **Default**: 0 to 10 (show all)
- **Use case**: Filter merchants by average delay days (e.g., show only merchants with 0-4 days average delay)

**Option 2: Button Slicer (Categorical)**
- **Visual**: Slicer (Buttons)
- **Field**: Create a calculated column or use `delay_rate_pct` instead
- **Categories**:
  - "Good Performance" (< 3 days average delay)
  - "Average" (3-4 days)
  - "Above Average" (4-5 days)
  - "Needs Attention" (5+ days)

**Option 3: Delay Rate Percentage (Alternative)**
- **Visual**: Slicer (Between)
- **Field**: `vw_merchant_performance[delay_rate_pct]`
- **Type**: Between
- **Min**: 0
- **Max**: 100
- **Use case**: Filter by percentage of orders that are delayed (e.g., show merchants with < 20% delay rate)

**Recommended**: Use **Option 1** (Range Slider) with `avg_delay_days` for the most flexibility.

**Data Distribution:**
- Most merchants: 3-6 days average delay
- Median: ~4.5 days
- Range: 2.73 - 6.16 days
- Individual order delays: 0-9 days

#### Charts

**1. Top 20 Merchants by Revenue (Horizontal Bar Chart)**
- **Visual**: Bar chart (horizontal)
- **Y-axis**: `vw_merchant_performance[merchant_name]`
- **X-axis**: `SUM('vw_merchant_performance'[total_revenue])`
- **Color**: `delay_rate_pct` (conditional: green = low delay, red = high delay)
- **Sort**: Descending by revenue
- **Title**: "Top 20 Merchants by Revenue"

**2. Order Volume vs. Delay Days (Scatter Plot)**
- **Visual**: Scatter chart
- **X-axis**: `SUM('vw_merchant_performance'[total_orders])`
- **Y-axis**: `AVERAGE('vw_merchant_performance'[avg_delay_days])`
- **Size**: `SUM('vw_merchant_performance'[total_revenue])` (bubble size represents revenue)
- **Legend/Color**: `vw_merchant_performance[merchant_country]` (colors data points by country, appears as legend)
- **Title**: "Order Volume vs. Delay Days"
- **Note**: Each bubble represents a merchant. The legend shows different colors for each country.

**3. Merchant Performance Matrix**
- **Visual**: Matrix
- **Rows**: `vw_merchant_performance[merchant_name]`
- **Values**: 
  - `total_orders`
  - `total_revenue`
  - `avg_order_value`
  - `avg_delay_days`
  - `delay_rate_pct`
- **Conditional formatting**: For delay metrics (green = good, red = bad)
- **Title**: "Merchant Performance Matrix"

**4. Geographic Revenue Distribution (Map - if available)**
- **Visual**: Map
- **Location**: `merchant_city`, `merchant_state`
- **Size**: `SUM('vw_merchant_performance'[total_revenue])`
- **Color**: `AVERAGE('vw_merchant_performance'[avg_delay_days])`
- **Title**: "Geographic Revenue Distribution"

---

### Page 4: Customer Segment Analysis

**Question**: What customer segments contribute most to revenue?

#### KPI Cards

1. **Total Customer Revenue**
   - Measure: `SUM('vw_customer_segment_revenue'[total_revenue])`
   - Format: Currency ($)

2. **Unique Customers**
   - Measure: `SUM('vw_customer_segment_revenue'[unique_customers])`
   - Format: Whole number

3. **Average Order Value**
   - Measure: `AVERAGE('vw_customer_segment_revenue'[avg_order_value])`
   - Format: Currency ($)

4. **Top Segment Revenue**
   - Measure: `MAX('vw_customer_segment_revenue'[total_revenue])`
   - Format: Currency ($)

#### Filters

- **Customer Segment** (Slicer)
- **Country/City** (Slicer)
- **Date Range** (Date slicer)

#### Charts

**1. Revenue by Customer Segment (Treemap)**
- **Visual**: Treemap
- **Values**: `SUM('vw_customer_segment_revenue'[total_revenue])`
- **Category**: `vw_customer_segment_revenue[customer_segment]`
- **Color saturation**: `AVERAGE('vw_customer_segment_revenue'[avg_order_value])`
- **Title**: "Revenue by Customer Segment"

**2. Segment Revenue Distribution (Pie Chart)**
- **Visual**: Pie chart
- **Values**: `SUM('vw_customer_segment_revenue'[total_revenue])`
- **Legend**: `vw_customer_segment_revenue[customer_segment]`
- **Title**: "Segment Revenue Distribution"

**3. Segment Metrics Table**
- **Visual**: Table
- **Columns**: 
  - `customer_segment`
  - `unique_customers`
  - `total_orders`
  - `total_revenue`
  - `avg_order_value`
  - `unique_products_purchased`
- **Sort**: By `total_revenue` (descending)
- **Title**: "Segment Metrics Summary"

**4. Revenue Trend by Segment (Line Chart)**
- **Visual**: Line chart
- **X-axis**: `vw_sales_by_time[date]` (if time dimension available)
- **Y-axis**: `SUM('vw_customer_segment_revenue'[total_revenue])`
- **Legend**: `vw_customer_segment_revenue[customer_segment]`
- **Title**: "Revenue Trend by Segment"

---

## Step 5: Create DAX Measures

DAX (Data Analysis Expressions) measures are calculations that enhance your dashboards. Create these in the **Data** view.

### How to Create a Measure

1. Go to **Data** view (left sidebar)
2. Right-click on a table (e.g., `vw_sales_by_time`)
3. Select **New measure**
4. Enter the DAX formula
5. Press Enter

### Essential Measures

**Total Revenue (can be used across all pages)**
```DAX
Total Revenue = SUM('vw_sales_by_time'[total_revenue])
```

**Total Orders**
```DAX
Total Orders = SUM('vw_sales_by_time'[total_orders])
```

**Average Order Value**
```DAX
Avg Order Value = AVERAGE('vw_sales_by_time'[avg_order_value])
```

**Late Delivery Rate**
```DAX
Late Delivery Rate = 
DIVIDE(
    SUM('vw_merchant_performance'[delayed_orders]),
    SUM('vw_merchant_performance'[total_orders]),
    0
) * 100
```

**Campaign Conversion Rate**
```DAX
Campaign Conversion Rate = 
DIVIDE(
    SUM('vw_campaign_performance'[orders_with_campaign]),
    SUM('vw_campaign_performance'[total_orders]),
    0
) * 100
```

**Revenue Growth (Month-over-Month)**
```DAX
Revenue Growth MoM = 
VAR CurrentMonth = SUM('vw_sales_by_time'[total_revenue])
VAR PreviousMonth = CALCULATE(
    SUM('vw_sales_by_time'[total_revenue]),
    DATEADD('vw_sales_by_time'[date], -1, MONTH)
)
RETURN CurrentMonth - PreviousMonth
```

**Revenue Growth Percentage**
```DAX
Revenue Growth % = 
VAR CurrentRevenue = SUM('vw_sales_by_time'[total_revenue])
VAR PreviousRevenue = CALCULATE(
    SUM('vw_sales_by_time'[total_revenue]),
    DATEADD('vw_sales_by_time'[date], -1, MONTH)
)
RETURN
IF(
    PreviousRevenue = 0, 
    0, 
    (CurrentRevenue - PreviousRevenue) / PreviousRevenue
)
```

### Using Measures in Visuals

- Drag measures into **Values** field of visuals
- Measures automatically aggregate based on context (filters, slicers)
- Format measures: Right-click measure → **Format** → Choose format (Currency, Percentage, etc.)

---

## Step 6: Formatting & Styling

### Theme Setup

1. **View** → **Themes** → **Customize current theme**
2. **Color palette:**
   - Primary: Dark blue (#1E3A5F)
   - Secondary: Light blue (#4A90E2)
   - Accent: Orange/Red for alerts (#E74C3C)
   - Background: Light gray (#F5F5F5)
   - Text: Dark gray (#333333)

### Visual Formatting Standards

**1. Titles:**
- Font: Segoe UI, 14pt, Bold
- Color: Dark gray (#333333)
- Position: Top of visual

**2. KPI Cards:**
- Background: White or light gray
- Border: 1px, light gray
- Value font: 24-28pt, Bold
- Label font: 12pt, Regular

**3. Charts:**
- Gridlines: Light gray, subtle
- Data labels: On (where appropriate)
- Legend: Top or right
- Colors: Use theme colors consistently

**4. Slicers:**
- Style: Buttons or dropdown
- Background: White
- Selected: Theme primary color

### Page Layout

1. **Arrange elements** in a grid layout
2. **Align** visuals consistently
3. **Spacing**: Use consistent padding (10-15px)
4. **Size**: Make important visuals larger

### Data Labels

1. **Enable data labels** on key charts
2. **Format**: Currency for revenue, whole numbers for counts
3. **Position**: Inside or outside (avoid overlap)

### Conditional Formatting

1. **KPI Cards**: Use color coding (green = good, red = needs attention)
2. **Tables**: Apply conditional formatting to metrics
3. **Charts**: Use color gradients to show performance

---

## Step 7: Advanced Features

### Cross-Filtering

1. Select all visuals on a page
2. **Format** → **Edit interactions**
3. Enable cross-filtering between visuals
4. Set appropriate filter directions

### Tooltips

1. Select a visual
2. **Format** → **Tooltip**
3. Create tooltip pages with additional context:
   - Detailed metrics
   - Related information
   - Drill-down options

### Bookmarks

1. **View** → **Bookmarks** pane
2. Set up different views of your dashboard
3. Create buttons to navigate between bookmarks

### Drill-Through Pages

1. Create a detail page
2. Set up drill-through filters
3. Right-click on a visual → **Drill through** → Select detail page

---

## Troubleshooting

### Connection Issues

**Problem**: "Cannot connect to PostgreSQL"

**Solutions**:
1. Verify Docker is running: `docker ps`
2. Verify port 5432 is exposed: `docker port shopzada-db`
3. Test connection: `psql -h localhost -p 5432 -U postgres -d shopzada`
4. In Power BI, uncheck "Use SSL" (for local development)
5. Check firewall settings

**Problem**: "SSL connection required"

**Solution**: 
- In Power BI connection dialog, uncheck "Use SSL" (for local development)
- Or configure SSL in PostgreSQL

### Data Refresh Issues

**Problem**: Data not updating

**Solutions**:
1. Use DirectQuery mode for real-time data
2. Or refresh manually: **Home** → **Refresh**
3. Or set up scheduled refresh in Power BI Service

### Performance Issues

**Problem**: Slow queries

**Solutions**:
1. Use **Import** mode instead of DirectQuery
2. Create materialized views in PostgreSQL
3. Add indexes on frequently filtered columns
4. Limit date ranges in filters

### Shared Memory Issues

**Problem**: "could not resize shared memory segment" / "No space left on device"

This error occurs when PostgreSQL container doesn't have enough shared memory when Power BI imports multiple views.

**Solution 1: Recreate Container with Increased Shared Memory** (Recommended)

The `docker-compose.yml` has been updated with `shm_size: 1gb`. Recreate the container:

```bash
# Stop and remove the container
cd infra
docker compose stop shopzada-db
docker compose rm -f shopzada-db

# Start with new configuration
docker compose up -d shopzada-db

# Wait for it to be healthy (about 10 seconds)
docker compose ps shopzada-db
```

**Solution 2: Import Views One at a Time** (Quick Workaround)

Instead of selecting all views at once:

1. **First connection**: Select only `vw_campaign_performance` → Click **Load**
2. **Second connection**: Click **Get Data** again → Select only `vw_merchant_performance` → Click **Load**
3. **Repeat** for each view individually

This reduces memory pressure during import.

**Solution 3: Use DirectQuery Mode** (For Large Datasets)

Instead of Import mode, use DirectQuery:

1. In connection dialog, select **DirectQuery** instead of **Import**
2. This queries the database live instead of loading all data into Power BI
3. Trade-off: Slightly slower visuals, but uses less memory

**Solution 4: Check Docker Desktop Resources**

If using Docker Desktop on Windows:

1. Open **Docker Desktop** → **Settings** → **Resources**
2. Ensure **Memory** is at least 4GB (8GB recommended)
3. Click **Apply & Restart**

### Views Not Showing Data

**Problem**: Views exist but return no rows

**Solutions**:
1. Verify ETL pipeline completed: Check Airflow logs
2. Check if fact tables have data:
   ```sql
   SELECT COUNT(*) FROM fact_orders;
   SELECT COUNT(*) FROM fact_line_items;
   ```
3. Re-run analytical views creation: Run `shopzada_analytical_views` DAG or execute `sql/13_create_analytical_views.sql`

### KPI Visual Issues

**Problem**: KPI shows nothing or incorrect values

**Solutions**:
1. **Use Card visual instead of KPI visual** for simple metrics
2. **Create DAX measures** instead of using auto-sum on aggregated fields
3. **Check Trend Axis**: For KPI visuals, use `vw_sales_by_time[date]` as trend axis
4. **Format the measure**: Right-click measure → Format → Choose appropriate format

**Common Issue**: Using `SUM('vw_campaign_performance'[avg_order_value])` is incorrect because `avg_order_value` is already an average. Use `AVERAGE('vw_campaign_performance'[avg_order_value])` instead.

---

## Quick Reference

### Field Mappings by View

**vw_sales_by_time**
- Time fields: `date`, `year`, `month`, `month_name`, `quarter`, `day`
- Metrics: `total_orders`, `total_revenue`, `avg_order_value`, `unique_customers`

**vw_campaign_performance**
- Dimensions: `campaign_id`, `campaign_name`, `campaign_description`, `discount`
- Metrics: `total_orders`, `total_revenue`, `campaign_avail_rate_pct`, `avg_order_value`, `unique_customers`
- **Note**: ⚠️ **This view ONLY includes actual marketing campaigns** - "No Campaign" entries are excluded for fair comparison between campaigns
- **Total**: 10 campaigns, 124,887 orders (only campaign orders)

**vw_merchant_performance**
- Dimensions: `merchant_id`, `merchant_name`, `merchant_city`, `merchant_state`, `merchant_country`
- Metrics: `total_orders`, `total_revenue`, `avg_order_value`, `avg_delay_days`, `delay_rate_pct`, `delayed_orders`
- **Delay Metrics**:
  - `avg_delay_days`: Average delay per merchant (range: 2.73 - 6.16 days, median: ~4.5 days)
  - `delay_rate_pct`: Percentage of orders that are delayed (0-100%)
  - `delayed_orders`: Count of orders with delay > 0
- **For Delay Threshold Filter**: Use `avg_delay_days` with range 0-10 days (most merchants are 3-6 days)

**vw_customer_segment_revenue**
- Dimensions: `customer_segment`, `customer_country`, `customer_city`
- Metrics: `total_orders`, `total_revenue`, `avg_order_value`, `unique_customers`, `unique_products_purchased`

**vw_product_performance**
- Dimensions: `product_id`, `product_name`, `product_type`
- Metrics: `total_orders`, `total_revenue`, `total_quantity_sold`

**vw_staff_performance**
- Dimensions: `staff_id`, `staff_name`, `staff_job_level`, `staff_city`, `staff_state`
- Metrics: `total_orders_processed`, `total_revenue_generated`, `avg_order_value`

### Common DAX Patterns

**Sum of a metric:**
```DAX
Total Revenue = SUM('vw_sales_by_time'[total_revenue])
```

**Average of a metric:**
```DAX
Avg Order Value = AVERAGE('vw_sales_by_time'[avg_order_value])
```

**Percentage calculation:**
```DAX
Delay Rate = 
DIVIDE(
    SUM('vw_merchant_performance'[delayed_orders]),
    SUM('vw_merchant_performance'[total_orders]),
    0
) * 100
```

**Time-based comparison:**
```DAX
Previous Month Revenue = 
CALCULATE(
    SUM('vw_sales_by_time'[total_revenue]),
    DATEADD('vw_sales_by_time'[date], -1, MONTH)
)
```

### Visual Type Recommendations

| Question | Best Visual Types |
|----------|------------------|
| Compare values | Bar chart, Column chart |
| Show trends | Line chart |
| Show distribution | Pie chart, Treemap |
| Show relationships | Scatter plot |
| Show details | Table, Matrix |
| Show KPIs | Card, KPI visual |
| Filter data | Slicer |

---

## Tips for Professional Dashboards

1. **Consistency**: Use same colors, fonts, and styles across all pages
2. **Hierarchy**: Make important metrics larger and more prominent
3. **Whitespace**: Don't overcrowd - leave breathing room
4. **Labels**: Always include clear titles and axis labels
5. **Filters**: Position filters prominently and make them easy to use
6. **Performance**: Use Import mode for better performance
7. **Mobile**: Consider mobile layout if needed (View → Mobile layout)
8. **Accessibility**: Use high contrast colors and clear labels

---

## Next Steps

1. ✅ Create all 4 dashboard pages
2. ✅ Add navigation sidebar to each page
3. ✅ Build KPI cards with measures
4. ✅ Add filters/slicers
5. ✅ Create charts for each business question
6. ✅ Apply consistent formatting
7. ✅ Test interactivity and cross-filtering
8. ✅ Add tooltips for additional context
9. ✅ Review with stakeholders
10. ✅ Publish to Power BI Service (optional)

---

## Resources

- [Power BI Documentation](https://docs.microsoft.com/power-bi/)
- [DAX Function Reference](https://docs.microsoft.com/dax/)
- [PostgreSQL Power BI Connector](https://docs.microsoft.com/power-bi/connect-data/desktop-connect-postgresql)

---

**You now have a complete guide to create professional dashboards that answer your three business questions!** 🎉

For production use, consider:
- Setting up a dedicated BI database user with read-only permissions
- Using Power BI Gateway for secure connections
- Implementing row-level security if needed
- Creating materialized views for better performance

