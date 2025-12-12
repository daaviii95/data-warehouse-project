# SQL Utility Scripts

## Overview

This directory contains **utility and diagnostic SQL scripts** that are not part of the main ETL pipeline but are useful for maintenance, debugging, and data analysis.

## Files in This Directory

### `00_clean_database_complete.sql`
- **Purpose**: Complete database cleanup script
- **Use Case**: Resets the entire database (drops all tables, views, and data)
- **When to Use**: 
  - Starting fresh with a clean database
  - Testing ETL pipeline from scratch
  - Development/debugging scenarios
- **⚠️ Warning**: This script **deletes all data** - use with caution!

### `00_drop_staging_tables.sql`
- **Purpose**: Drop all staging tables to allow schema updates
- **Use Case**: When staging table schema has changed and needs to be recreated
- **When to Use**: 
  - After updating staging table definitions in `sql/00_create_staging_tables.sql`
  - When getting column mismatch errors due to old schema
  - Before running `create_staging_tables` task to ensure fresh schema
- **⚠️ Warning**: This script **deletes all staging data** - staging tables will be recreated by the next ETL run

### `check_jan_2020_revenue.sql`
- **Purpose**: Diagnostic script to verify revenue calculations for January 2020
- **Use Case**: Troubleshooting revenue discrepancies
- **What it does**: 
  - Compares revenue from different calculation methods
  - Checks for NULL values and data quality issues
  - Identifies orders without line items
- **When to Use**: When investigating revenue calculation differences

## Usage

These scripts are **not executed automatically** by Airflow. Run them manually when needed:

```bash
# Connect to database
docker exec -it shopzada-db psql -U postgres -d shopzada

# Run utility script
\i /opt/airflow/repo/sql/utilities/00_clean_database_complete.sql
```

## Adding New Utility Scripts

When adding new utility scripts:
1. Place them in this `utilities/` directory
2. Use descriptive names (e.g., `check_*.sql`, `fix_*.sql`, `analyze_*.sql`)
3. Add documentation in this README
4. Include clear warnings if scripts modify or delete data

