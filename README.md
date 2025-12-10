# ShopZada Data Warehouse Solution

A complete end-to-end Data Warehouse solution implementing the Kimball methodology with Apache Airflow orchestration.

## 🏗️ Architecture

- **Methodology**: Kimball Star Schema
- **Orchestration**: Apache Airflow 2.10.2
- **Database**: PostgreSQL 15
- **Data Processing**: Python 3.11, Pandas, PyArrow
- **Containerization**: Docker & Docker Compose

## 📁 Project Structure

```
DWHFinalTest/
├── infra/                       # Infrastructure as Code
│   ├── Dockerfile              # Airflow container definition
│   └── docker-compose.yml      # Multi-container orchestration
├── scripts/                     # Python ETL scripts
│   ├── etl_pipeline_python.py  # Main Python ETL pipeline
│   └── data_quality.py         # Data quality validation
├── sql/                         # SQL transformation scripts
│   ├── 01_create_schema_from_physical_model.sql
│   ├── 02_populate_dim_date.sql
│   ├── 03_load_dim_campaign.sql
│   ├── 04_load_dim_product.sql
│   ├── 05_load_dim_user.sql
│   ├── 06_load_dim_staff.sql
│   ├── 07_load_dim_merchant.sql
│   ├── 08_load_dim_user_job.sql
│   ├── 09_load_dim_credit_card.sql
│   ├── 10_load_fact_orders.sql
│   ├── 11_load_fact_line_items.sql
│   ├── 12_load_fact_campaign_transactions.sql
│   └── 13_create_analytical_views.sql
├── workflows/                   # Airflow DAG definitions
│   ├── shopzada_etl_pipeline.py
│   └── shopzada_data_quality_dag.py
├── docs/                        # Technical documentation
│   ├── ARCHITECTURE.md
│   ├── DATA_MODEL.md
│   ├── WORKFLOW.md
│   ├── BUSINESS_QUESTIONS.md
│   └── SCRIPTS_USAGE.md
├── dashboard/                   # Dashboard documentation and guides
│   ├── DASHBOARD_DESIGN_GUIDE.md
│   ├── DASHBOARD_QUICK_START.md
│   └── POWER_BI_SETUP_GUIDE.md
├── data/                        # Source data files (not in repo)
│   ├── Business Department/
│   ├── Customer Management Department/
│   ├── Enterprise Department/
│   ├── Marketing Department/
│   └── Operations Department/
├── airflow/                     # Airflow runtime files (logs, plugins)
│   ├── logs/
│   └── plugins/
├── physicalmodel.txt           # Physical data model
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- At least 4GB RAM available
- Ports 5432 and 8080 available

### Installation

1. **Clone/Navigate to the project directory**
   ```bash
   cd DWHFinalTest
   ```

2. **Start the Docker containers**
   ```bash
   cd infra
   docker compose up -d
   ```

3. **Wait for services to initialize** (2-3 minutes)
   - Airflow database migration
   - Airflow user creation (admin/admin)
   - PostgreSQL database setup
   - Airflow connection to ShopZada database automatically created

4. **Access Airflow Web UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

5. **Trigger the ETL Pipeline**
   - In Airflow UI, find the `shopzada_etl_pipeline` DAG
   - Toggle the DAG ON (if it's paused)
   - Click the play button (▶) to trigger the DAG manually
   - Monitor progress in the Graph View or Tree View

### Verify Setup

1. **Check Airflow Connection**
   - Go to Admin → Connections
   - Verify `shopzada_postgres` connection exists

2. **Check Database Tables**
   ```bash
   docker exec -it shopzada-db psql -U postgres -d shopzada
   ```
   Then run:
   ```sql
   \dt                    -- List all tables
   \dv                    -- List all views
   SELECT COUNT(*) FROM dim_user;
   SELECT COUNT(*) FROM fact_orders;
   ```

## 📊 Data Warehouse Schema

### Dimension Tables
- `dim_date` - Time dimension (2020-2025)
- `dim_campaign` - Marketing campaigns
- `dim_product` - Product catalog
- `dim_user` - Customer information
- `dim_staff` - Staff/employee data
- `dim_merchant` - Merchant/vendor information
- `dim_user_job` - User job information (outrigger)
- `dim_credit_card` - Credit card information (outrigger)

### Fact Tables
- `fact_orders` - Order transactions
- `fact_line_items` - Order line items with product details
- `fact_campaign_transactions` - Campaign usage tracking

### Analytical Views
- `vw_campaign_performance` - Campaign effectiveness metrics
- `vw_merchant_performance` - Merchant performance analysis
- `vw_customer_segment_revenue` - Customer segment revenue analysis
- `vw_sales_by_time` - Time-based sales trends
- `vw_product_performance` - Product performance metrics
- `vw_staff_performance` - Staff productivity metrics

## 🔄 ETL Pipeline

The ETL pipeline consists of 4 main stages:

1. **Schema Creation** → Create all dimension and fact table structures
2. **Date Dimension** → Populate time dimension (2020-2025)
3. **Python ETL** → Direct file reading, transformation, and loading into dimensional model
   - Loads dimensions: campaign, product, user, staff, merchant, user_job, credit_card
   - Loads facts: orders, line_items, campaign_transactions
4. **Data Quality** → Validate data integrity (referential integrity, nulls, duplicates, etc.)

## 📈 Business Questions Addressed

1. **What kinds of campaigns drive the highest order volume?**
   - View: `vw_campaign_performance`
   - Metrics: Order volume, revenue, availed rate

2. **How do merchant performance metrics affect sales?**
   - View: `vw_merchant_performance`
   - Metrics: Revenue, delivery performance, customer reach

3. **What customer segments contribute most to revenue?**
   - View: `vw_customer_segment_revenue`
   - Metrics: Revenue by segment, customer lifetime value

## 🛠️ Development

### Running Individual Scripts

```bash
# Python ETL pipeline
docker exec -it airflow-scheduler python /opt/airflow/repo/scripts/etl_pipeline_python.py

# Data quality checks
docker exec -it airflow-scheduler python /opt/airflow/repo/scripts/data_quality.py
```

### Accessing Databases

**ShopZada Database:**
```bash
docker exec -it shopzada-db psql -U postgres -d shopzada
```

**Airflow Metadata Database:**
```bash
docker exec -it airflow-db psql -U airflow -d airflow
```

### Viewing Logs

```bash
# Airflow scheduler logs
docker logs airflow-scheduler

# Airflow webserver logs
docker logs airflow-webserver

# ShopZada database logs
docker logs shopzada-db
```

## 📝 Configuration

### Environment Variables

Edit `infra/docker-compose.yml` to modify:
- Database credentials
- Data directory paths
- Airflow configuration

### Airflow Connection

The `shopzada_postgres` connection is automatically created during initialization. To manually create:

1. Airflow UI → Admin → Connections
2. Add new connection:
   - Conn Id: `shopzada_postgres`
   - Conn Type: `Postgres`
   - Host: `shopzada-db`
   - Schema: `shopzada`
   - Login: `postgres`
   - Password: `postgres`
   - Port: `5432`

## 🧪 Data Quality Checks

The data quality module validates:
- ✅ Referential integrity (foreign keys)
- ✅ Null value constraints
- ✅ Duplicate detection
- ✅ Data type validation
- ✅ Range validation (e.g., prices > 0)

Run quality checks:
```bash
docker exec -it airflow-scheduler python /opt/airflow/repo/scripts/data_quality.py
```

## 📚 Documentation

- [Architecture Documentation](docs/ARCHITECTURE.md) - System architecture and design decisions
- [Data Model Documentation](docs/DATA_MODEL.md) - Star schema design and table structures
- [Workflow Documentation](docs/WORKFLOW.md) - ETL pipeline workflow and task dependencies
- [Business Questions](docs/BUSINESS_QUESTIONS.md) - Key business questions and analytical views
- [Scripts Usage](docs/SCRIPTS_USAGE.md) - Which scripts are used and which can be deleted

**Dashboard Documentation:**
- [Dashboard Design Guide](dashboard/DASHBOARD_DESIGN_GUIDE.md) - Complete guide for creating Power BI dashboards
- [Dashboard Quick Start](dashboard/DASHBOARD_QUICK_START.md) - Quick reference for dashboard creation
- [Power BI Setup Guide](dashboard/POWER_BI_SETUP_GUIDE.md) - Connect Power BI and create dashboards

## 🐛 Troubleshooting

### Containers won't start
- Check Docker Desktop is running
- Verify ports 5432 and 8080 are not in use
- Check logs: `docker compose logs`

### Airflow DAG not appearing
- Wait 30-60 seconds for DAG refresh
- Check scheduler logs: `docker logs airflow-scheduler`
- Verify DAG files are in `airflow/dags/`
- Check for Python syntax errors in DAG files
- Ensure all required Python packages are installed (check requirements.txt)

### Database connection errors
- Verify `shopzada-db` container is healthy
- Check connection string in Airflow UI
- Ensure database is initialized

### ETL pipeline failures
- Check file paths in `/opt/airflow/data`
- Verify file formats are supported (CSV, Parquet, JSON, Excel, Pickle, HTML)
- Review Airflow task logs for specific errors
- Check for data quality issues (null values, type mismatches)

## 🧹 Cleanup

```bash
# Stop containers
cd infra
docker compose down

# Remove volumes (⚠️ deletes all data)
docker compose down -v
```

## 📄 License

This project is for educational purposes as part of CSELEC1C Data Warehouse course.

## 👥 Authors

- Data Engineering Team
- Course: CSELEC1C - Data Warehouse

---

**Note**: This is a complete Data Warehouse solution demonstrating end-to-end ETL pipeline implementation with Kimball methodology, Airflow orchestration, and business intelligence capabilities.

