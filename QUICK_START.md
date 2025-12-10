# ShopZada Data Warehouse - Quick Start Guide

## 🎯 What You Need To Do (Step-by-Step)

### Step 1: Start the Database
```powershell
docker compose -f docker/docker-compose.yml up -d shopzada-db
```

**Wait 5-10 seconds for database to be ready.**

---

### Step 2: Run Ingestion (Load Raw Data → Database)

```powershell
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest
```

**What this does:**
- Reads all files from `data/` folder (CSV, JSON, Excel, Parquet, Pickle, HTML)
- Loads them into Postgres staging tables (`stg_*`)
- Creates `ingestion_log` table to track what was loaded

**Expected output:**
- Should see "Processing..." messages for each file
- Should see "Finished ingesting..." for each file
- Should see "Processed X files from /data"

---

### Step 3: Verify Ingestion Worked

```powershell
# Check how many staging tables were created
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT COUNT(*) as total_staging_tables 
FROM information_schema.tables 
WHERE table_name LIKE 'stg_%';
"
```

**Expected:** Should show ~24 tables (one per source file)

```powershell
# Check ingestion status
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT status, COUNT(*) as count 
FROM ingestion_log 
GROUP BY status;
"
```

**Expected:** Should show mostly `success` status

---

### Step 4: Run ETL Pipeline (Staging → Dimensional Model)

**Option A: Using Airflow (Recommended)**

1. Start Airflow:
   ```powershell
   docker compose -f docker/docker-compose.yml up -d shopzada-airflow-webserver shopzada-airflow-scheduler
   ```

2. Open browser: http://localhost:8080
   - Login: `admin` / `admin`

3. Find and trigger `shopzada_etl_pipeline` DAG
   - Click the play button ▶️
   - Monitor the progress

**Option B: Manual Execution**

```powershell
# 1. Create dimensional model schema
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

---

### Step 5: Verify Everything Works

**Run the verification script:**
```powershell
.\scripts\verify_setup.ps1
```

**Or check manually:**
```powershell
# Check dimensions
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT 'dim_product' as table_name, COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;
"
```

---

## 📋 Complete Workflow Summary

```
1. Start Database
   ↓
2. Run Ingestion (Raw Files → Postgres stg_* tables)
   ↓
3. Verify Staging Tables Exist
   ↓
4. Run ETL Pipeline (stg_* → dim_* + fact_*)
   ↓
5. Verify Dimensional Model
   ↓
6. Query Presentation Views
```

---

## 🔧 Troubleshooting

### Problem: Ingestion shows "failed" status
**Solution:** Check the error message in ingestion_log:
```powershell
docker exec shopzada-db psql -U postgres -d shopzada -c "
SELECT file_path, status, message 
FROM ingestion_log 
WHERE status = 'failed' 
LIMIT 5;
"
```

### Problem: No staging tables
**Solution:** Re-run ingestion:
```powershell
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest
```

### Problem: Database connection error
**Solution:** Make sure database is running:
```powershell
docker compose -f docker/docker-compose.yml up -d shopzada-db
# Wait a few seconds, then retry
```

---

## ✅ Success Checklist

After completing all steps, you should have:

- [ ] Database running (`shopzada-db` container)
- [ ] ~24 staging tables (`stg_*`) with data
- [ ] Ingestion log showing successful loads
- [ ] 6 dimension tables (`dim_*`) populated
- [ ] 2 fact tables (`fact_*`) populated
- [ ] 7+ presentation views created
- [ ] Can query views and get results

---

## 🎓 Next Steps

Once everything is verified:
1. Explore the data using SQL queries
2. Use the presentation views for analytics
3. Connect BI tools (Tableau, Power BI, etc.) to the database
4. Build dashboards using the dimensional model

