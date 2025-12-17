# ShopZada ETL Workflow Documentation

## Workflow Overview

The ShopZada ETL pipeline is orchestrated by Apache Airflow and follows a **staging-based ETL approach**: Data is first ingested into staging tables, then extracted, transformed, and loaded into the dimensional model.

## Workflow Structure

```
Schema → Staging → Date Dimension → Ingest → Extract → Transform → Load Dim → Create Missing Dims → Load Fact → Views
```

**Detailed Flow:**
1. **Schema**: Create all dimension and fact tables from physical model
2. **Staging**: Create staging tables for raw data
3. **Date Dimension**: Populate date dimension (prerequisite for facts)
4. **Ingest**: Load raw data per department into staging tables (parallel execution, incremental loading)
5. **Extract**: Extract data from staging tables into pandas DataFrames
6. **Transform**: Clean and transform extracted data
7. **Load Dim**: Load transformed data into dimension tables (from separate dimension files)
8. **Create Missing Dimensions** (Scenario 2): Extract dimensions from fact data and create missing entries
9. **Load Fact**: Load transformed data into fact tables with dimension key lookups
   - **Scenario 1 Support**: Track before/after states and dashboard KPIs (if enabled)
   - **Scenario 4 Support**: Invalid data captured in reject tables
10. **Views**: Create analytical views for BI tools

## Workflow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW ORCHESTRATION                        │
│                    (Apache Airflow 2.10.2)                      │
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
│  TASK 2: Create Staging Tables                                  │
│  ────────────────────────────────────────────────────────────   │
│  • Create staging tables for each department                    │
│  • Naming: stg_{department}_{data_type}                         │
│  • Source: sql/00_create_staging_tables.sql                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 3: Populate Date Dimension                                │
│  ────────────────────────────────────────────────────────────   │
│  • Generate date dimension (2020-01-01 to 2025-12-31)           │
│  • Populate time attributes (year, month, quarter, etc.)        │
│  • Source: sql/02_populate_dim_date.sql                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 4: Ingest Data Per Department → Staging                   │
│  ────────────────────────────────────────────────────────────   │
│  • Marketing: campaign data, transactional campaign data        │
│  • Operations: order data, line items, delays                   │
│  • Business: product list                                       │
│  • Customer Management: user data, jobs, credit cards           │
│  • Enterprise: merchant data, staff data, order-merchant maps   │
│  • Source: scripts/ingest.py                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 5: Extract                                                │
│  ────────────────────────────────────────────────────────────   │
│  • Read data from staging tables                                │
│  • Load into pandas DataFrames                                  │
│  • Extract all department data                                  │
│  • Source: scripts/extract.py                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 6: Transform                                              │
│  ────────────────────────────────────────────────────────────   │
│  • Clean and format data                                        │
│  • Normalize values (product types, discounts, etc.)            │
│  • Format names, addresses, phone numbers                       │
│  • Convert data types                                           │
│  • Source: scripts/transform.py                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 7: Load Dimensions                                        │
│  ────────────────────────────────────────────────────────────   │
│  • Load into dim_campaign                                       │
│  • Load into dim_product                                        │
│  • Load into dim_user                                           │
│  • Load into dim_staff                                          │
│  • Load into dim_merchant                                       │
│  • Load into dim_user_job                                       │
│  • Load into dim_credit_card                                    │
│  • Source: scripts/load_dim.py                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 8: Create Missing Dimensions (Scenario 2)                 │
│  ────────────────────────────────────────────────────────────   │
│  • Extract unique user_ids from order data                      │
│  • Extract unique product_ids from line item data               │
│  • Create minimal dim_user entries for missing users            │
│  • Create minimal dim_product entries for missing products      │
│  • Source: scripts/load_dim.py (create_missing_* functions)     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 9: Load Facts                                             │
│  ────────────────────────────────────────────────────────────   │
│  • Load into fact_orders with dimension key lookups             │
│  • Load into fact_line_items with dimension key lookups         │
│  • Load into fact_campaign_transactions with dimension keys     │
│  • Scenario 1: Track before/after states (if enabled)           │
│  • Source: scripts/load_fact.py                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TASK 10: Create Analytical Views                               │
│  ────────────────────────────────────────────────────────────   │
│  • vw_campaign_performance                                      │
│  • vw_merchant_performance                                      │
│  • vw_customer_segment_revenue                                  │
│  • vw_sales_by_time                                             │
│  • vw_product_performance                                       │
│  • vw_staff_performance                                         │
│  • vw_segment_revenue_by_time                                   │
│  • Source: sql/03_create_analytical_views.sql                   │
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
1. **Create Schema** → **Create Staging Tables**: Schema must exist first
2. **Create Staging Tables** → **Populate Date Dimension**: Tables must exist
3. **Populate Date Dimension** → **Ingest Data**: Date dimension needed for fact tables
4. **Ingest Data** → **Extract**: All staging data must be loaded before extraction
5. **Extract** → **Transform**: Extracted data needed for transformation
6. **Transform** → **Load Dimensions**: Transformed data needed for dimensions
7. **Load Dimensions** → **Create Missing Dimensions**: Load dimensions from files first
8. **Create Missing Dimensions** → **Load Facts**: All dimensions (including new ones) must exist
9. **Load Facts** → **Create Analytical Views**: Views created after all data is loaded

## Detailed Stage Descriptions

### Stage 1: Schema Creation
- **Input**: Physical model definition (`docs/LOGICAL-PHYSICALMODEL.txt`)
- **Process**: 
  - Create all dimension and fact table structures
  - Define primary keys, foreign keys, and constraints
  - Set up indexes for performance
- **Output**: Empty dimension and fact tables in PostgreSQL

### Stage 2: Staging Tables Creation
- **Input**: Staging table definitions
- **Process**:
  - Create staging tables for each department
  - Add indexes for performance
  - Tables follow naming: `stg_{department}_{data_type}`
- **Output**: Empty staging tables ready for data ingestion

### Stage 3: Date Dimension Population
- **Input**: Date range (2020-2025)
- **Process**:
  - Generate all dates in range
  - Calculate time attributes (year, month, quarter, day of week, etc.)
  - Populate date dimension table
- **Output**: Populated `dim_date` table

### Stage 4: Data Ingestion (Per Department)
- **Input**: Raw files from `/opt/airflow/data` directory
- **Process**:
  - **Marketing Department**: Load campaign data and transactional campaign data
  - **Operations Department**: Load order data, line item prices, line item products, order delays
  - **Business Department**: Load product list
  - **Customer Management Department**: Load user data, user jobs, credit cards
  - **Enterprise Department**: Load merchant data, staff data, order-merchant mappings
  - Files are loaded directly into staging tables with minimal transformation
- **Output**: Populated staging tables

### Stage 5: Extract
- **Input**: Staging tables
- **Process**:
  - Read data from staging tables into pandas DataFrames
  - Extract all department data (campaigns, orders, products, users, merchants, staff)
  - Load into memory for transformation
- **Output**: Extracted DataFrames ready for transformation

### Stage 6: Transform
- **Input**: Extracted DataFrames from staging
- **Process**:
  - Clean data (remove nulls, format strings)
  - Normalize values (product types, discounts, etc.)
  - Format names, addresses, phone numbers
  - Convert data types
- **Output**: Transformed DataFrames ready for loading

### Stage 7: Load Dimensions
- **Input**: Transformed data from staging
- **Process**:
  - Extract and transform dimension data
  - Load campaigns, products, users, staff, merchants
  - Load outriggers: user_job, credit_card
  - Handle duplicates with `ON CONFLICT` clauses
- **Output**: Populated dimension tables

### Stage 8: Create Missing Dimensions (Scenario 2)

- **Input**: Transformed fact-shaped data (orders + line items)
- **Process**:
  - Create missing `dim_user` rows from order data
  - Create missing `dim_product` rows from line item data
- **Output**: Dimensions are complete enough for fact loading (no broken FKs)

### Stage 9: Load Facts
- **Input**: Transformed data from staging, populated dimension tables
- **Process**:
  - **Scenario 2 Support**: Create missing dimensions from fact data
    - Extract unique user_ids from order data
    - Extract unique product_ids from line item data
    - Create minimal dimension entries for missing users/products
  - Extract and transform fact data
  - Join data from multiple sources
  - Lookup dimension surrogate keys
  - Calculate metrics (delays, quantities, etc.)
  - Process in chunks to prevent OOM errors
  - **Scenario 1 Support**: Track before/after states if enabled (via `ENABLE_SCENARIO1_METRICS`)
  - **Scenario 4 Support**: Invalid data captured in reject tables (reject_fact_orders, reject_fact_line_items, reject_fact_campaign_transactions)
  - **Scenario 3 Support**: Unknown campaign handling for late-arriving campaign data
- **Output**: Populated fact tables and reject tables (if invalid data found)

### Stage 10: Create Analytical Views
- **Input**: Populated dimension and fact tables
- **Process**:
  - Create SQL views for common analytical queries
  - Pre-aggregate data for performance
  - Optimize for BI tool consumption
- **Output**: Analytical views (vw_*)

## Scheduling

- **Main ETL Pipeline**: Daily execution (`@daily`) - includes data quality checks as part of the pipeline
- **Analytical Views**: Separate DAG for independent execution when needed

## Error Handling

- **Retry Logic**: All tasks have 1 retry with 5-minute delay
- **Failure Notifications**: Email on failure (configurable)
- **Logging**: Comprehensive logging at each stage
- **Idempotent Operations**: All ETL operations use `ON CONFLICT` clauses for safe re-runs

## Performance Optimization

- **Staging Layer**: Allows parallel ingestion and transformation
- **Incremental Loading**: Only processes new/changed files (see [Incremental Loading](INCREMENTAL_LOADING.md))
- **Bulk Inserts**: Data inserted in batches using SQLAlchemy transactions
- **Pre-loaded Mappings**: Dimension keys loaded once for all fact rows
- **Indexing**: Strategic indexes on foreign keys and date columns
- **Connection Pooling**: SQLAlchemy connection pooling for database operations
- **Chunked Processing**: Fact tables processed in chunks to prevent OOM errors

## Scenario Support

### Scenario 1: End-to-End Pipeline Test

Enable before/after state tracking and dashboard KPI monitoring:

```bash
export ENABLE_SCENARIO1_METRICS=true
export TARGET_DATE=2024-01-15
```

See `docs/core/SCENARIOS.md` for demo-ready Scenario 1–5 datasets and verification steps.

### Scenario 2: New Customer and Product Creation

Automatically creates missing dimensions from fact data:
- Extracts unique user_ids from order data
- Extracts unique product_ids from line item data
- Creates minimal dimension entries before fact loading

### Scenario 3: Late & Missing Campaign Data

Handles optional or late-arriving campaign data:
- Unknown campaign placeholder for missing campaign_id values
- Updates campaign transactions when new campaigns arrive

### Scenario 4: Data Quality Failure & Error Handling

Ensures invalid data doesn't corrupt the warehouse:
- Reject tables capture invalid records with error reasons
- Raw data preserved in JSONB format for debugging
- Valid data proceeds while invalid data is logged

### Scenario 5: Performance & Aggregation Consistency

Verifies analytical layer consistency:
- KPI verification queries compare BI tool results with direct SQL
- Performance testing ensures views are optimized

## File Format Support

The ingestion pipeline supports multiple file formats:
- **CSV**: Tab-separated and comma-separated, with automatic separator detection
- **Parquet**: Columnar format for efficient reading
- **JSON**: Nested JSON structures
- **Excel**: .xlsx and .xls files
- **Pickle**: Python serialized objects
- **HTML**: HTML tables extracted using pandas.read_html

## Benefits of New Workflow

1. **Separation of Concerns**: Each stage has a clear responsibility
2. **Modularity**: Easy to modify individual stages without affecting others
3. **Debugging**: Can inspect staging tables to debug data issues
4. **Reusability**: Staging tables can be reused for different transformations
5. **Scalability**: Can process departments in parallel if needed
6. **Data Lineage**: Clear traceability from source files → staging → dimensions/facts

