# ShopZada ETL Pipeline - Quick Start Guide

## Complete Workflow

### 1. Data Ingestion (Staging Layer)
```powershell
# Option A: Using Docker directly
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest

# Option B: Using Airflow DAG
# Trigger: shopzada_ingestion DAG
```

**Result**: All source files loaded into `stg_*` tables + Parquet exports

### 2. ETL to Dimensional Model
```powershell
# Option A: Using Airflow DAG (Recommended)
# Trigger: shopzada_etl_pipeline DAG

# Option B: Manual execution
# Step 1: Create schema
Get-Content sql/kimball_schema.sql | docker exec -i shopzada-db psql -U postgres -d shopzada

# Step 2: Populate date dimension
Get-Content sql/populate_dim_date.sql | docker exec -i shopzada-db psql -U postgres -d shopzada

# Step 3: Load dimensions
docker exec -it shopzada-airflow-scheduler python /opt/airflow/repo/scripts/etl_dimensions.py

# Step 4: Load facts
docker exec -it shopzada-airflow-scheduler python /opt/airflow/repo/scripts/etl_facts.py

# Step 5: Create presentation views
Get-Content sql/presentation_layer_views.sql | docker exec -i shopzada-db psql -U postgres -d shopzada
```

**Result**: 
- Dimension tables populated
- Fact tables populated
- Presentation views created

### 3. Query the Data Warehouse

```sql
-- Example: Daily sales summary
SELECT * FROM vw_sales_summary_daily 
ORDER BY date_actual DESC 
LIMIT 30;

-- Example: Top products by revenue
SELECT * FROM vw_sales_by_product_category 
ORDER BY total_revenue DESC 
LIMIT 10;

-- Example: Campaign ROI
SELECT * FROM vw_campaign_performance 
ORDER BY roi_percent DESC;
```

## Complete Reset & Rebuild

```powershell
# 1. Reset staging layer
.\scripts\reset_and_reingest.ps1

# 2. Run ETL pipeline (via Airflow or manual)
# Trigger shopzada_etl_pipeline DAG
```

## Verification Queries

```sql
-- Check staging tables
SELECT COUNT(*) FROM information_schema.tables 
WHERE table_name LIKE 'stg_%';

-- Check dimensions
SELECT 
    'dim_product' AS table_name, COUNT(*) AS row_count FROM dim_product
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_merchant', COUNT(*) FROM dim_merchant
UNION ALL SELECT 'dim_staff', COUNT(*) FROM dim_staff
UNION ALL SELECT 'dim_campaign', COUNT(*) FROM dim_campaign
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date;

-- Check facts
SELECT 
    'fact_sales' AS table_name, COUNT(*) AS row_count FROM fact_sales
UNION ALL SELECT 'fact_campaign_performance', COUNT(*) FROM fact_campaign_performance;

-- Check ETL logs
SELECT etl_name, status, rows_inserted, duration_seconds, created_at
FROM etl_log
ORDER BY created_at DESC
LIMIT 10;
```

## Common Issues

### Missing Dimension Keys
If fact table has NULL dimension keys, check:
1. Dimension tables are loaded first
2. Source data has matching IDs
3. ETL script handles missing keys gracefully

### Performance
For large datasets:
- Fact tables are append-only (no updates)
- Use indexes on foreign keys
- Consider partitioning for very large fact tables

### Data Quality
- Check `etl_log` table for failed ETL runs
- Validate row counts match expectations
- Review presentation views for data quality issues

