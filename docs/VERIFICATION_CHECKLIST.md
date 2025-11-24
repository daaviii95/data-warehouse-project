# ShopZada Data Warehouse - Verification Checklist

## Step-by-Step Verification Guide

### 1. Start Services

```powershell
# Start database
docker compose -f docker/docker-compose.yml up -d shopzada-db

# Start Airflow (optional, for DAG orchestration)
docker compose -f docker/docker-compose.yml up -d shopzada-airflow-webserver shopzada-airflow-scheduler
```

**Verify:**
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected output should show:
- `shopzada-db` - Up (healthy)
- `shopzada-airflow-webserver` - Up
- `shopzada-airflow-scheduler` - Up

---

### 2. Run Ingestion

**Option A: Direct Docker**
```powershell
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest
```

**Option B: Via Airflow**
1. Open http://localhost:8080
2. Login: `admin` / `admin`
3. Trigger `shopzada_ingestion` DAG

**Verify Ingestion:**
```powershell
# Check staging tables count
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT COUNT(*) as staging_table_count 
FROM information_schema.tables 
WHERE table_name LIKE 'stg_%';
"

# Check ingestion log
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT file_path, status, rows_ingested, ts 
FROM ingestion_log 
ORDER BY ts DESC 
LIMIT 10;
"
```

**Expected:**
- Should see ~24 staging tables (one per source file)
- All ingestion_log entries should show `status = 'success'`
- Total rows ingested should match your data files

---

### 3. Verify Staging Data

```powershell
# List all staging tables
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT table_name, 
       (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public' AND table_name LIKE 'stg_%'
ORDER BY table_name;
"

# Check row counts for key tables
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT 
    'stg_business_department_product_list_xlsx' as table_name,
    COUNT(*) as row_count 
FROM stg_business_department_product_list_xlsx
UNION ALL
SELECT 'stg_customer_management_department_user_data_json', COUNT(*) 
FROM stg_customer_management_department_user_data_json
UNION ALL
SELECT 'stg_operations_department_order_data_20211001_20220101_csv', COUNT(*) 
FROM stg_operations_department_order_data_20211001_20220101_csv;
"
```

**Expected:**
- Each staging table should have data
- Row counts should match source file sizes

---

### 4. Run ETL Pipeline (Dimensional Model)

**Option A: Via Airflow (Recommended)**
1. Open http://localhost:8080
2. Trigger `shopzada_etl_pipeline` DAG
3. Monitor progress

**Option B: Manual Execution**
```powershell
# 1. Create schema
Get-Content sql/kimball_schema.sql | docker exec -i shopzada-db psql -U postgres -d shopzada

# 2. Populate date dimension
Get-Content sql/populate_dim_date.sql | docker exec -i shopzada-db psql -U postgres -d shopzada

# 3. Load dimensions
docker exec -it shopzada-airflow-scheduler python /opt/airflow/repo/scripts/etl_dimensions.py

# 4. Load facts
docker exec -it shopzada-airflow-scheduler python /opt/airflow/repo/scripts/etl_facts.py

# 5. Create presentation views
Get-Content sql/presentation_layer_views.sql | docker exec -i shopzada-db psql -U postgres -d shopzada
```

**Verify ETL:**
```powershell
# Check dimensions
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT 
    'dim_product' as table_name, COUNT(*) as row_count FROM dim_product
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_merchant', COUNT(*) FROM dim_merchant
UNION ALL SELECT 'dim_staff', COUNT(*) FROM dim_staff
UNION ALL SELECT 'dim_campaign', COUNT(*) FROM dim_campaign
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date;
"

# Check facts
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT 
    'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales
UNION ALL SELECT 'fact_campaign_performance', COUNT(*) FROM fact_campaign_performance;
"

# Check ETL logs
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT etl_name, status, rows_inserted, duration_seconds, created_at
FROM etl_log
ORDER BY created_at DESC
LIMIT 10;
"
```

**Expected:**
- All 6 dimension tables should have data
- `dim_date` should have ~2,190 rows (2020-2025)
- Fact tables should have significant row counts
- ETL logs should show `status = 'success'`

---

### 5. Test Presentation Layer Views

```powershell
# Test a presentation view
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT * FROM vw_sales_summary_daily 
ORDER BY date_actual DESC 
LIMIT 10;
"

# Test product category view
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT * FROM vw_sales_by_product_category 
ORDER BY total_revenue DESC 
LIMIT 5;
"
```

**Expected:**
- Views should return data
- Aggregations should be correct

---

### 6. Quick Health Check Commands

```powershell
# All-in-one verification
.\scripts\verify_setup.ps1

# Or manual checks:
# Check services
docker ps --filter "name=shopzada"

# Check database
docker exec shopzada-db psql -U postgres -d shopzada -c "SELECT current_database(), version();"

# Check Airflow
curl http://localhost:8080/health
```

---

## Common Issues & Solutions

### Issue: No staging tables
**Solution:** Run ingestion
```powershell
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest
```

### Issue: Database connection failed
**Solution:** Check if database is running
```powershell
docker compose -f docker/docker-compose.yml up -d shopzada-db
# Wait a few seconds, then retry
```

### Issue: Dimensional model missing
**Solution:** Run ETL pipeline
```powershell
# Via Airflow: Trigger shopzada_etl_pipeline DAG
# Or manually: Follow step 4 above
```

### Issue: Airflow not accessible
**Solution:** Check if services are running
```powershell
docker compose -f docker/docker-compose.yml ps
# If not running:
docker compose -f docker/docker-compose.yml up -d shopzada-airflow-webserver shopzada-airflow-scheduler
```

---

## Success Criteria

✅ **Staging Layer:**
- 20+ staging tables (`stg_*`)
- All ingestion_log entries show `success`
- Row counts match source files

✅ **Dimensional Model:**
- 6 dimension tables populated
- 2 fact tables populated
- ETL logs show successful runs

✅ **Presentation Layer:**
- 7+ BI views created
- Views return data when queried

✅ **Services:**
- All Docker containers running
- Database accessible
- Airflow UI accessible at http://localhost:8080

