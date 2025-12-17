# ShopZada Data Warehouse Architecture (Aperture)

## Overview

ShopZada's Data Warehouse solution follows the **Kimball Methodology** with a multi-layered architecture designed to integrate heterogeneous datasets from multiple departments and deliver analytical insights.

## Architecture Layers

### 1. Data Sources Layer
- **Business Department**: Product catalogs (Excel)
- **Customer Management Department**: User data (JSON), user jobs (CSV), credit cards (Pickle)
- **Enterprise Department**: Merchant data (HTML), staff data (HTML), order data with merchant info (Parquet, CSV)
- **Marketing Department**: Campaign data (CSV), transactional campaign data (CSV)
- **Operations Department**: Order data (Parquet, Pickle, CSV, Excel, JSON, HTML), line item data (CSV, Parquet), order delays (HTML)

### 2. Data Warehouse Layer (DWH)
- **Methodology**: Kimball Star Schema
- **Structure**:
  - **Dimension Tables**: Descriptive attributes (slowly changing dimensions)
  - **Fact Tables**: Business events and metrics

#### Dimension Tables:
- `dim_date`: Time dimension (2020-2025)
- `dim_campaign`: Marketing campaigns
- `dim_product`: Product catalog
- `dim_user`: Customer information
- `dim_staff`: Staff/employee data
- `dim_merchant`: Merchant/vendor information
- `dim_user_job`: User job information (outrigger)
- `dim_credit_card`: Credit card information (outrigger)

#### Fact Tables:
- `fact_orders`: Order transactions
- `fact_line_items`: Order line items with product details
- `fact_campaign_transactions`: Campaign usage tracking

### 3. Analytical Layer (Presentation)
- **SQL Views**: Pre-aggregated analytical views
- **Business Intelligence**: Ready for Tableau, Power BI, or Looker Studio

## Technology Stack

- **Orchestration**: Apache Airflow 2.10.2
- **Database**: PostgreSQL 15
- **Data Processing**: Python 3.11, Pandas, PyArrow
- **Containerization**: Docker & Docker Compose
- **ETL Framework**: Custom Python scripts + SQL transformations

## Data Flow

```
Data Sources → Staging Tables → Extract → Transform → Load Dimensions → Create Missing Dims → Load Facts → Analytical Views → BI Tools
```

**Current Implementation:**
- **Staging-Based ETL**: Data is first ingested into staging tables, then extracted, transformed, and loaded
- **Incremental Loading**: Only processes new/changed files (see [Incremental Loading](INCREMENTAL_LOADING.md))
- **Scenario 2 Support**: Automatically creates missing dimensions from fact data
- **Scenario 1 Support**: Optional before/after state tracking and dashboard KPI monitoring
- **File Support**: CSV (tab/comma-separated), Parquet, JSON, Excel, Pickle, HTML

## Key Design Decisions

1. **Kimball Methodology**: Chosen for business-user-friendly star schemas and rapid development
2. **Surrogate Keys**: All dimensions use auto-incrementing surrogate keys (SK) for SCD handling
3. **Date Dimension**: Pre-populated time dimension for consistent time-based analysis
4. **Staging Layer**: Data first loaded into staging tables for separation of concerns and debugging
5. **Incremental Loading**: Tracks processed files to avoid duplicate processing
6. **Late-Arriving Dimensions** (Scenario 2): Creates missing dimensions from fact data automatically
7. **Pipeline Metrics** (Scenario 1): Optional before/after state tracking for validation
8. **Data Quality**: Reject tables capture invalid data while valid data proceeds (Scenario 4)
9. **Idempotent Operations**: All ETL operations use `ON CONFLICT` clauses for safe re-runs

## Scalability Considerations

- **Horizontal Scaling**: Airflow supports distributed execution
- **Partitioning**: Fact tables can be partitioned by date
- **Indexing**: Strategic indexes on foreign keys and date columns
- **Chunked Processing**: Large datasets processed in chunks to prevent OOM errors
- **Incremental Loading**: Only processes new/changed files for better performance
- **Connection Pooling**: SQLAlchemy connection pooling for efficient database operations

## Scenario support

### Scenario 1: End-to-End Pipeline Test
- **Purpose**: Validate incremental batch loading and end-to-end data flow
- **Features**: Before/after state tracking, dashboard KPI monitoring, pipeline summary
- **Enable**: Set `ENABLE_SCENARIO1_METRICS=true` environment variable
- **Demo guide**: `docs/core/SCENARIOS.md` (Scenario 1)

### Scenario 2: New Customer and Product Creation
- **Purpose**: Handle late-arriving dimensions (customers/products appearing only in order data)
- **Features**: Automatic dimension creation from fact data before fact loading
- **Implementation**: `create_missing_users_from_orders()` and `create_missing_products_from_line_items()`

### Scenario 3: Late & Missing Campaign Data
- **Purpose**: Handle optional or late-arriving campaign data
- **Features**: Unknown campaign placeholder for missing campaign_id values
- **Implementation**: `ensure_unknown_campaign()` and `update_campaign_transactions_for_new_campaigns()`

### Scenario 4: Data Quality Failure & Error Handling
- **Purpose**: Ensure invalid data doesn't corrupt the warehouse
- **Features**: Reject tables capture invalid records with error reasons and raw data
- **Implementation**: `write_to_reject_table()` function and reject table schemas

### Scenario 5: Performance & Aggregation Consistency
- **Purpose**: Verify analytical layer consistency and performance
- **Features**: KPI verification queries and performance testing

