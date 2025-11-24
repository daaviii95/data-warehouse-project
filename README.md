ShopZada is a rapidly growing e-commerce platform that has expanded globally, now 
handling over half a million orders and two million line items from diverse product 
categories. Despite this growth, their data remains fragmented across multiple departments:
Business, Customer Management, Enterprise, Marketing, and Operations.

**Groupname**, was tasked to design, implement, and operationalize a complete Data Warehouse 
solution that integrates these datasets, delivers analytical insights, and supports data-driven decision-making.

Chosen Data Warehouse Methodology: **Kimball Dimensional Modeling (Star Schema)**

## Architecture Overview

```
Source Data → Staging Layer → Dimensional Model → Presentation Layer
```

- **Staging Layer**: Raw data ingestion into `stg_*` tables + Parquet exports
- **Dimensional Model**: Kimball star schema with fact and dimension tables
- **Presentation Layer**: Business Intelligence views for analytics


## Quick Verification

**Run the verification script to check if everything is working:**
```powershell
# PowerShell
.\scripts\verify_setup.ps1

# Bash/Linux
chmod +x scripts/verify_setup.sh
./scripts/verify_setup.sh
```

This checks:
- ✅ Docker services are running
- ✅ Database connection
- ✅ Staging tables exist
- ✅ Ingestion logs
- ✅ Dimensional model (if ETL has run)
- ✅ Airflow accessibility

## Dockerized Staging Layer

- Build and run the ShopZada images:
  ```powershell
  docker compose -f docker/docker-compose.yml up --build shopzada-ingest
  ```
- Custom images:
  - `shopzada-db:latest` – Postgres 15 with the `shopzada` warehouse database.
  - `shopzada-ingest:latest` – runs `scripts/ingest.py` against the mounted `data/` folder.
- Data sources must live in `./data`; the compose file mounts it read-only into each container.

## Airflow Orchestration

- DAGs live in `airflow/dags`. Start the full stack (Postgres + Airflow scheduler/webserver) and open the UI on http://localhost:8080:
  ```powershell
  docker compose -f docker/docker-compose.yml up --build shopzada-airflow-webserver shopzada-airflow-scheduler
  ```
  The first run will also start `shopzada-airflow-init` to apply migrations and provision the default admin user (`admin` / `admin`).

### Main Ingestion DAG (`shopzada_ingestion`)
- **Purpose**: Raw data ingestion to database
- **Workflow**: **Raw → Ingest → Database**
  1. **Validate**: Checks data directory is mounted
  2. **Ingestion**: Calls `scripts/ingest.py` to load all source files into Postgres staging tables (`stg_*`)
  3. **Snapshot**: Records row counts for monitoring and validation

### Optional: Department-Specific Parquet Export DAGs
If you need Parquet exports (e.g., for analytics tools), separate DAGs are available:

- **`shopzada_business_dept_parquet`** - Business Department
- **`shopzada_customer_dept_parquet`** - Customer Management Department
- **`shopzada_enterprise_dept_parquet`** - Enterprise Department
- **`shopzada_marketing_dept_parquet`** - Marketing Department
- **`shopzada_operations_dept_parquet`** - Operations Department
- **`shopzada_all_depts_parquet`** - All departments in parallel

**Note**: These are optional and separate from the main ingestion pipeline. The main flow is **Raw → Ingest → Database**.

- All staging data is stored in Postgres `stg_*` tables for downstream Kimball dimensional modeling.

## Kimball Dimensional Model (ETL Pipeline)

### Schema Creation
The Kimball star schema includes:
- **Dimension Tables**: `dim_date`, `dim_product`, `dim_customer`, `dim_merchant`, `dim_staff`, `dim_campaign`
- **Fact Tables**: `fact_sales` (grain: line item), `fact_campaign_performance`
- **Presentation Views**: Pre-aggregated BI views for reporting

### Running ETL Pipeline

**Using Airflow:**
1. Trigger `shopzada_etl_pipeline` DAG in Airflow UI
2. Pipeline executes: Schema → Date Dimension → Dimensions → Facts → Validation

**Manual Execution:**
```powershell
# 1. Create schema
Get-Content sql/kimball_schema.sql | docker exec -i shopzada-db psql -U postgres -d shopzada

# 2. Populate date dimension
Get-Content sql/populate_dim_date.sql | docker exec -i shopzada-db psql -U postgres -d shopzada

# 3. Load dimensions
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest python /opt/airflow/repo/scripts/etl_dimensions.py

# 4. Load facts
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest python /opt/airflow/repo/scripts/etl_facts.py

# 5. Create presentation views
Get-Content sql/presentation_layer_views.sql | docker exec -i shopzada-db psql -U postgres -d shopzada
```

### Presentation Layer Views
- `vw_sales_summary_daily` - Daily sales metrics
- `vw_sales_by_product_category` - Product performance
- `vw_sales_by_customer` - Customer analytics
- `vw_campaign_performance` - Marketing ROI
- `vw_order_delays` - Delivery analysis
- `vw_monthly_sales_trend` - Trend analysis

See `docs/KIMBALL_ARCHITECTURE.md` for detailed architecture documentation.

## Reset and Re-ingest

To clear all staging data and re-run ingestion:

### Option 1: Using Reset Script (Recommended)
```powershell
# PowerShell (Windows)
.\scripts\reset_and_reingest.ps1

# Bash (Linux/Mac/WSL)
chmod +x scripts/reset_and_reingest.sh
./scripts/reset_and_reingest.sh
```

### Option 2: Manual Steps

**PowerShell:**
```powershell
# 1. Clear staging tables
Get-Content sql/reset.sql | docker exec -i shopzada-db psql -U postgres -d shopzada

# 2. Clear Parquet exports (optional, if using Parquet export DAGs)
# Remove-Item -Path "data/staging_parquet/*.parquet" -Force -ErrorAction SilentlyContinue

# 3. Re-run ingestion
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest
```

**Bash/Linux:**
```bash
# 1. Clear staging tables
docker exec -i shopzada-db psql -U postgres -d shopzada < sql/reset.sql

# 2. Clear Parquet exports (optional)
rm -f data/staging_parquet/*.parquet

# 3. Re-run ingestion
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest
```

### Option 3: Using Airflow DAG
1. Ensure services are running:
   ```powershell
   docker compose -f docker/docker-compose.yml up -d shopzada-db shopzada-airflow-webserver shopzada-airflow-scheduler
   ```
2. Clear the database manually:
   ```powershell
   # PowerShell
   Get-Content sql/reset.sql | docker exec -i shopzada-db psql -U postgres -d shopzada
   
   # Bash
   docker exec -i shopzada-db psql -U postgres -d shopzada < sql/reset.sql
   ```
3. Open Airflow UI at http://localhost:8080
4. Find the `shopzada_ingestion` DAG and click "Trigger DAG"
5. The DAG will re-ingest and export to Parquet

### Verify Reset
```powershell
# Check staging tables are cleared (should return 0)
docker exec -it shopzada-db psql -U postgres -d shopzada -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'stg_%';"
```

## Troubleshooting

### Network Timeout Errors

If you encounter `TLS handshake timeout` when pulling Docker images:

1. **Pre-pull base images manually:**
   ```powershell
   docker pull python:3.11-slim
   docker pull apache/airflow:2.10.2-python3.11
   docker pull postgres:15
   ```

2. **Then build:**
   ```powershell
   docker compose -f docker/docker-compose.yml build
   ```

3. **If errors persist**, check your network/firewall settings or try building with `--no-cache` flag.

### Old Dockerfile References

If you see errors about `airflow.Dockerfile` or `postgres.Dockerfile`:
- Ensure you have the latest `docker/Dockerfile` (multi-stage build)
- Delete any old `docker/airflow.Dockerfile` or `docker/postgres.Dockerfile` files
- See `docker/TROUBLESHOOTING.md` for detailed solutions.



