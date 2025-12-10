# ShopZada Data Warehouse Architecture

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

### 4. Analytical Layer (Presentation)
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
Data Sources → Python ETL Pipeline → Dimensional Model → Analytical Views → BI Tools
```

**Current Implementation:**
- **Direct ETL**: Python scripts (`etl_pipeline_python.py`) read raw files directly and load into dimensional model
- **No Staging Layer**: Data is transformed in-memory using Pandas and loaded directly into dimension/fact tables
- **File Support**: CSV (tab/comma-separated), Parquet, JSON, Excel, Pickle, HTML

## Key Design Decisions

1. **Kimball Methodology**: Chosen for business-user-friendly star schemas and rapid development
2. **Surrogate Keys**: All dimensions use auto-incrementing surrogate keys (SK) for SCD handling
3. **Date Dimension**: Pre-populated time dimension for consistent time-based analysis
4. **Direct ETL**: Python-based ETL reads files directly and loads into dimensional model (no staging layer)
5. **Data Quality**: Automated validation checks after ETL completion
6. **Idempotent Operations**: All ETL operations use `ON CONFLICT` clauses for safe re-runs

## Scalability Considerations

- **Horizontal Scaling**: Airflow supports distributed execution
- **Partitioning**: Fact tables can be partitioned by date
- **Indexing**: Strategic indexes on foreign keys and date columns
- **Chunked Processing**: Large CSV files processed in chunks

