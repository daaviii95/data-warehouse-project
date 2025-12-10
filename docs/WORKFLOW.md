# ShopZada ETL Workflow Documentation

## Workflow Overview

The ShopZada ETL pipeline is orchestrated by Apache Airflow and follows a direct ETL approach: Python scripts read raw files and load data directly into the dimensional model (no staging layer).

## Workflow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW ORCHESTRATION                        │
│                    (Apache Airflow 2.7.3)                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 1: Create Schema                                          │
│  ────────────────────────────────────────────────────────────   │
│  • Create all dimension and fact tables                         │
│  • Define primary keys, foreign keys, constraints               │
│  • Source: sql/01_create_schema_from_physical_model.sql         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 2: Populate Date Dimension                                │
│  ────────────────────────────────────────────────────────────   │
│  • Generate date dimension (2020-01-01 to 2025-12-31)           │
│  • Populate time attributes (year, month, quarter, etc.)        │
│  • Source: sql/02_populate_dim_date.sql                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 3: Python ETL Pipeline                                    │
│  ────────────────────────────────────────────────────────────   │
│  • Read raw files directly from /opt/airflow/data               │
│  • Detect file types (CSV, Parquet, JSON, Excel, Pickle, HTML)  │
│  • Transform data using Pandas                                  │
│  • Load dimensions: campaign, product, user, staff, merchant    │
│  • Load outriggers: user_job, credit_card                       │
│  • Load facts: orders, line_items, campaign_transactions        │
│  • Source: scripts/etl_pipeline_python.py                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 4: Data Quality Checks                                    │
│  ────────────────────────────────────────────────────────────   │
│  • Referential integrity validation                             │
│  • Null value checks                                            │
│  • Duplicate detection                                          │
│  • Data type and range validation                               │
│  • Record count verification                                    │
│  • Source: scripts/data_quality.py                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BI TOOLS / REPORTING                         │
│  (Power BI, Tableau, Looker Studio)                             │
│  • Connect to PostgreSQL database                               │
│  • Use analytical views (vw_*) for pre-aggregated data          │
│  • Build dashboards for business questions                      │
└─────────────────────────────────────────────────────────────────┘
```

## Task Dependencies

### Sequential Dependencies
1. **Create Schema** → **Populate Date Dimension**: Schema must exist before populating
2. **Populate Date Dimension** → **Python ETL**: Date dimension needed for fact tables
3. **Python ETL** → **Data Quality**: Validate after ETL completion

### ETL Internal Flow (within Python ETL task)
The Python ETL pipeline (`etl_pipeline_python.py`) loads data in this order:
1. **Dimensions** (can load in any order):
   - `dim_campaign` (from campaign_data.csv)
   - `dim_product` (from product_list.xlsx)
   - `dim_user` (from user_data.json)
   - `dim_staff` (from staff_data.html)
   - `dim_merchant` (from merchant_data.html)
2. **Outriggers** (depend on dim_user):
   - `dim_user_job` (from user_job.csv)
   - `dim_credit_card` (from user_credit_card.pickle)
3. **Facts** (depend on all dimensions):
   - `fact_orders` (from order_data + order_with_merchant_data + order_delays)
   - `fact_line_items` (from line_item_data_prices + line_item_data_products)
   - `fact_campaign_transactions` (from transactional_campaign_data.csv)

## Data Flow Details

### Stage 1: Schema Creation
- **Input**: Physical model definition (`physicalmodel.txt`)
- **Process**: 
  - Create all dimension and fact table structures
  - Define primary keys, foreign keys, and constraints
  - Set up indexes for performance
- **Output**: Empty dimension and fact tables in PostgreSQL

### Stage 2: Date Dimension Population
- **Input**: Date range (2020-2025)
- **Process**:
  - Generate all dates in range
  - Calculate time attributes (year, month, quarter, day of week, etc.)
  - Populate date dimension table
- **Output**: Populated `dim_date` table

### Stage 3: ETL Pipeline (Python-based)
- **Input**: Raw files from `/opt/airflow/data` directory
- **Process**:
  - **File Discovery**: Recursively find files matching patterns
  - **File Loading**: Detect format and load (CSV, Parquet, JSON, Excel, Pickle, HTML)
  - **Data Cleaning**:
    - Convert NaN to NULL
    - Parse discount values (1%, 1pct, etc.)
    - Format product types (underscores to spaces, title case)
    - Extract numeric values from strings (e.g., "3days" → 3)
  - **Dimension Loading**: Load all dimension tables with surrogate keys
  - **Fact Loading**: 
    - Join data from multiple sources
    - Lookup surrogate keys from dimensions
    - Calculate metrics (delays, quantities, etc.)
- **Output**: Populated dimension and fact tables

### Stage 4: Quality Assurance
- **Input**: Dimension and fact tables
- **Process**:
  - Referential integrity checks (foreign keys)
  - Null value validation
  - Duplicate detection
  - Data type and range validation
  - Record count verification
- **Output**: Quality report and alerts

### Stage 5: BI Integration
- **Input**: Dimension and fact tables
- **Process**:
  - Connect BI tools to PostgreSQL
  - Query analytical views (created via SQL scripts)
  - Build dashboards for business questions
- **Output**: Business intelligence dashboards

## Scheduling

- **Main ETL Pipeline**: Daily execution (`@daily`)
- **Data Quality Monitoring**: Hourly execution (`@hourly`) via separate DAG

## Error Handling

- **Retry Logic**: All tasks have 1 retry with 5-minute delay
- **Failure Notifications**: Email on failure (configurable)
- **Logging**: Comprehensive logging at each stage
- **Idempotent Operations**: All ETL operations use `ON CONFLICT` clauses for safe re-runs

## Performance Optimization

- **Direct File Reading**: Files read directly into memory (no staging layer overhead)
- **Pandas Operations**: Efficient in-memory transformations using Pandas
- **Batch Inserts**: Data inserted in batches using SQLAlchemy transactions
- **Indexing**: Strategic indexes on foreign keys and date columns
- **Connection Pooling**: SQLAlchemy connection pooling for database operations
- **Idempotent Operations**: `ON CONFLICT` clauses allow safe re-runs without duplicates

## File Format Support

The ETL pipeline supports multiple file formats:

- **CSV**: Tab-separated and comma-separated, with automatic index column detection
- **Parquet**: Columnar format for efficient reading
- **JSON**: Nested JSON structures
- **Excel**: .xlsx and .xls files
- **Pickle**: Python serialized objects
- **HTML**: HTML tables extracted using pandas.read_html

## Data Quality Features

- **Automatic Data Cleaning**:
  - NaN/None/N/A values converted to NULL
  - Discount parsing (handles 1%, 1pct, 1percent, 10%%, etc.)
  - Product type formatting (underscores to spaces, title case)
  - Numeric extraction from strings (e.g., "3days" → 3, "1pieces" → 1)
- **Column Mapping**: Automatic column name normalization and mapping
- **Error Handling**: Graceful handling of missing files, malformed data, and type mismatches
