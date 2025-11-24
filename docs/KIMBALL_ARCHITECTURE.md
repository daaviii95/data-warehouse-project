# ShopZada Kimball Dimensional Model Architecture

## Overview

This document describes the Kimball dimensional modeling approach implemented for ShopZada's data warehouse. The architecture follows the **Star Schema** design pattern with conformed dimensions and fact tables.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    SOURCE DATA LAYER                         │
│  (CSV, JSON, Excel, Parquet, Pickle, HTML files)            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    STAGING LAYER                             │
│  (stg_* tables in Postgres + Parquet exports)               │
│  - Raw data with light cleaning                             │
│  - One table per source file                                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              DIMENSIONAL MODEL (KIMBALL)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Dimensions  │  │    Facts     │  │    Date      │      │
│  │  (SCD Type 1)│  │  (Star Schema)│  │  (Conformed) │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                 PRESENTATION LAYER                           │
│  (Business Intelligence Views)                               │
│  - Pre-aggregated reports                                    │
│  - Analytics-ready views                                    │
└─────────────────────────────────────────────────────────────┘
```

## Dimension Tables

### dim_date (Conformed Dimension)
- **Purpose**: Shared date dimension for all fact tables
- **Grain**: One row per day
- **Coverage**: 2020-2025
- **Key Attributes**: date_key, date_actual, year, quarter, month, day_of_week, is_weekend

### dim_product
- **Purpose**: Product master data
- **Grain**: One row per product
- **Source**: Business Department (product_list.xlsx)
- **SCD Type**: Type 1 (overwrite on change)
- **Key Attributes**: product_id, product_name, category, subcategory, brand, unit_price, cost_price

### dim_customer
- **Purpose**: Customer master data
- **Grain**: One row per customer
- **Source**: Customer Management Department (user_data.json, user_job.csv, user_credit_card.pickle)
- **SCD Type**: Type 1
- **Key Attributes**: customer_id, name, email, job_title, credit_card_type

### dim_merchant
- **Purpose**: Merchant/vendor information
- **Grain**: One row per merchant
- **Source**: Enterprise Department (merchant_data.html)
- **SCD Type**: Type 1
- **Key Attributes**: merchant_id, merchant_name, type, category, country, city

### dim_staff
- **Purpose**: Staff/employee information
- **Grain**: One row per staff member
- **Source**: Enterprise Department (staff_data.html)
- **SCD Type**: Type 1
- **Key Attributes**: staff_id, staff_name, department, position, hire_date

### dim_campaign
- **Purpose**: Marketing campaign information
- **Grain**: One row per campaign
- **Source**: Marketing Department (campaign_data.csv)
- **SCD Type**: Type 1
- **Key Attributes**: campaign_id, campaign_name, type, start_date, end_date, budget

## Fact Tables

### fact_sales
- **Grain**: One row per line item
- **Source**: Operations Department (order data, line item data)
- **Measures**:
  - quantity
  - unit_price
  - discount_amount
  - discount_percent
  - line_total
  - cost_amount
  - profit_amount
- **Foreign Keys**: 
  - order_date_key → dim_date
  - customer_key → dim_customer
  - product_key → dim_product
  - merchant_key → dim_merchant
  - staff_key → dim_staff
  - campaign_key → dim_campaign
- **Degenerate Dimensions**: order_id, line_item_id

### fact_campaign_performance
- **Grain**: One row per campaign transaction
- **Source**: Marketing Department (transactional_campaign_data.csv)
- **Measures**:
  - transactions_count
  - revenue_amount
  - discount_amount
  - net_revenue_amount
- **Foreign Keys**:
  - campaign_key → dim_campaign
  - transaction_date_key → dim_date
  - customer_key → dim_customer

## ETL Process

### Step 1: Schema Creation
```sql
-- Create all dimension and fact tables
\i sql/kimball_schema.sql
```

### Step 2: Populate Date Dimension
```sql
-- Generate date dimension for 2020-2025
\i sql/populate_dim_date.sql
```

### Step 3: Load Dimensions
```bash
# Load all dimension tables from staging
python scripts/etl_dimensions.py
```

**Process**:
- Read from staging tables (`stg_*`)
- Transform and standardize data
- Apply SCD Type 1 (upsert on conflict)
- Log ETL runs to `etl_log` table

### Step 4: Load Facts
```bash
# Load all fact tables from staging
python scripts/etl_facts.py
```

**Process**:
- Read from staging tables
- Lookup dimension keys (surrogate keys)
- Calculate measures
- Insert into fact tables
- Handle missing dimension keys gracefully

### Step 5: Create Presentation Views
```sql
-- Create BI views for reporting
\i sql/presentation_layer_views.sql
```

## Presentation Layer Views

### vw_sales_summary_daily
Daily aggregated sales metrics: orders, revenue, discounts, profit

### vw_sales_by_product_category
Sales performance by product category and subcategory

### vw_sales_by_customer
Customer purchase behavior and lifetime value

### vw_sales_by_merchant
Sales performance by merchant/vendor

### vw_campaign_performance
Marketing campaign ROI and performance metrics

### vw_order_delays
Order delivery delay analysis

### vw_monthly_sales_trend
Monthly sales trends with period-over-period comparison

## Running the ETL Pipeline

### Option 1: Using Airflow DAG
1. Start Airflow services
2. Trigger `shopzada_etl_pipeline` DAG
3. Monitor progress in Airflow UI

### Option 2: Manual Execution
```bash
# 1. Create schema
psql -U postgres -d shopzada -f sql/kimball_schema.sql

# 2. Populate date dimension
psql -U postgres -d shopzada -f sql/populate_dim_date.sql

# 3. Load dimensions
python scripts/etl_dimensions.py

# 4. Load facts
python scripts/etl_facts.py

# 5. Create presentation views
psql -U postgres -d shopzada -f sql/presentation_layer_views.sql
```

## Data Quality & Validation

### ETL Logging
All ETL runs are logged to `etl_log` table with:
- Rows processed/inserted/updated/failed
- Execution time
- Status and error messages

### Validation Queries
```sql
-- Check dimension row counts
SELECT 'dim_product' AS table_name, COUNT(*) AS row_count FROM dim_product
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_merchant', COUNT(*) FROM dim_merchant
UNION ALL
SELECT 'dim_staff', COUNT(*) FROM dim_staff
UNION ALL
SELECT 'dim_campaign', COUNT(*) FROM dim_campaign;

-- Check fact row counts
SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM fact_sales
UNION ALL
SELECT 'fact_campaign_performance', COUNT(*) FROM fact_campaign_performance;

-- Check for orphaned fact records (missing dimension keys)
SELECT COUNT(*) AS orphaned_sales
FROM fact_sales fs
LEFT JOIN dim_customer c ON fs.customer_key = c.customer_key
WHERE fs.customer_key IS NOT NULL AND c.customer_key IS NULL;
```

## Business Value

### Analytical Capabilities
- **Sales Analysis**: Revenue trends, product performance, customer segmentation
- **Campaign ROI**: Marketing effectiveness and budget optimization
- **Operational Metrics**: Order delays, merchant performance
- **Customer Insights**: Purchase behavior, lifetime value

### Reporting Use Cases
1. **Executive Dashboard**: Monthly revenue, profit, growth trends
2. **Product Management**: Category performance, pricing analysis
3. **Marketing**: Campaign ROI, customer acquisition costs
4. **Operations**: Delivery performance, merchant relationships
5. **Customer Service**: Customer value, purchase patterns

## Maintenance

### Incremental Loading
- Dimensions: Upsert on conflict (SCD Type 1)
- Facts: Append-only (immutable historical records)
- Date Dimension: Static (pre-populated)

### Performance Optimization
- Indexes on all foreign keys
- Indexes on date columns
- Partitioning (future enhancement for large fact tables)

### Data Refresh
Run the ETL pipeline after staging layer updates:
```bash
# Reset and re-run complete pipeline
./scripts/reset_and_reingest.ps1  # PowerShell
# Then trigger shopzada_etl_pipeline DAG
```

