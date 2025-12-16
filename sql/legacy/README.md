# Legacy SQL Scripts

## Overview

This directory contains **legacy SQL scripts** that are **not used** in the current ETL pipeline. These scripts were designed for a staging table approach that has been replaced by the Python-based ETL pipeline.

## Status: ❌ NOT USED

These scripts are kept for **reference only**. They are not executed by any Airflow DAGs.

## Files in This Directory

### Dimension Loading Scripts (Staging Table Approach)
- `03_load_dim_campaign.sql` - Loads campaigns from staging tables
- `04_load_dim_product.sql` - Loads products from staging tables
- `05_load_dim_user.sql` - Loads users from staging tables
- `06_load_dim_staff.sql` - Loads staff from staging tables
- `07_load_dim_merchant.sql` - Loads merchants from staging tables
- `08_load_dim_user_job.sql` - Loads user jobs from staging tables
- `09_load_dim_credit_card.sql` - Loads credit cards from staging tables

### Fact Loading Scripts (Staging Table Approach)
- `10_load_fact_orders.sql` - Loads orders from staging tables
- `11_load_fact_line_items.sql` - Loads line items from staging tables
- `12_load_fact_campaign_transactions.sql` - Loads campaign transactions from staging tables

## Why These Are Not Used

### Current Approach: Direct File Loading (Python ETL)
```
Source Files → Python ETL (etl_pipeline_python.py) → Dimension/Fact Tables
```

The current ETL pipeline loads data **directly from source files** into dimension/fact tables using Python, bypassing staging tables entirely.

### Legacy Approach: Staging Tables (SQL Scripts)
```
Source Files → Staging Tables → SQL Scripts → Dimension/Fact Tables
```

These SQL scripts were designed to:
1. Read from staging tables (e.g., `stg_marketing_department_campaign_data%`)
2. Dynamically discover and combine data from multiple staging tables
3. Load data into dimension/fact tables using SQL

**Problem**: The Python ETL pipeline doesn't create staging tables, so these SQL scripts can't work with the current setup.

## When Would You Use These?

These scripts would be useful if you:
- Want to switch to a staging table approach
- Need to load data from pre-existing staging tables
- Want to reference SQL-based ETL patterns
- Are implementing a hybrid approach (some Python, some SQL)

## Active SQL Scripts

The **active SQL scripts** used in the current pipeline are in the parent `sql/` directory:
- `01_create_schema_from_physical_model.sql` - Creates all tables
- `02_populate_dim_date.sql` - Populates date dimension
- `03_create_analytical_views.sql` - Creates analytical views (now active, not legacy)

## Related Documentation

- See `docs/SQL_SCRIPTS_STATUS.md` for detailed status of all SQL scripts
- See `docs/core/WORKFLOW.md` for the current ETL workflow
- See `scripts/etl_pipeline_python.py` for the active ETL implementation

