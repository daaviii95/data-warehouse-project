# Analytical Views DAG - Quick Reference

## Overview

The analytical views have been separated into their own DAG (`shopzada_analytical_views`) to allow quick regeneration without re-running the entire ETL pipeline.

## Why Separate DAG?

**Problem**: Regenerating analytical views required running the entire ETL pipeline, which takes a long time.

**Solution**: Created a separate DAG that only refreshes the analytical views, taking only seconds to complete.

## Usage

### In Airflow UI

1. Go to Airflow UI: `http://localhost:8080`
2. Find the DAG: `shopzada_analytical_views`
3. Toggle it ON (if not already enabled)
4. Click "Trigger DAG" to run it manually
5. Wait ~10-30 seconds for completion

### When to Use

Run `shopzada_analytical_views` DAG when you need to:
- ✅ Refresh views after updating view definitions in `sql/13_create_analytical_views.sql`
- ✅ Regenerate views after data corrections
- ✅ Update views after schema changes
- ✅ Quick refresh before dashboard updates
- ❌ **NOT needed** after regular ETL runs (views are automatically created)

### DAG Details

- **DAG ID**: `shopzada_analytical_views`
- **Schedule**: Manual trigger only (`schedule_interval=None`)
- **Duration**: ~10-30 seconds
- **Dependencies**: None (can run independently)
- **Idempotent**: Safe to run multiple times

## Views Created

The DAG creates/refreshes these analytical views:

1. `vw_campaign_performance` - Campaign analysis
2. `vw_merchant_performance` - Merchant metrics
3. `vw_customer_segment_revenue` - Customer segments
4. `vw_sales_by_time` - Time-based trends
5. `vw_product_performance` - Product insights
6. `vw_staff_performance` - Staff productivity

## Main ETL Pipeline

The main ETL pipeline (`shopzada_etl_pipeline`) now:
- Creates schema
- Populates date dimension
- Runs Python ETL (loads all data)
- Runs data quality checks
- **Does NOT** create analytical views (use separate DAG)

## Workflow Comparison

### Before
```
Full ETL → Data Quality → Analytical Views
(30+ minutes)              (10 seconds)
```

### After
```
Full ETL → Data Quality
(30+ minutes)

Analytical Views (separate DAG)
(10 seconds, run independently)
```

## Troubleshooting

### Views not updating?
- Check if DAG completed successfully
- Verify SQL file `sql/13_create_analytical_views.sql` is correct
- Check Airflow logs for errors

### Can't find the DAG?
- Ensure file is in `workflows/` directory
- Restart Airflow scheduler: `docker compose restart airflow-scheduler`
- Check DAG is enabled in Airflow UI

### Views showing old data?
- Run the main ETL pipeline first to refresh underlying data
- Then run analytical views DAG to refresh views

## Files Modified

1. **`workflows/shopzada_analytical_views.py`** - New separate DAG
2. **`workflows/shopzada_etl_pipeline.py`** - Removed analytical views task
3. **`docs/DATA_QUALITY_REPORT.md`** - Updated with fixes

## Data Quality Fixes Applied

1. **delay_days NULL handling**: Fixed to use `NULL` instead of `-1` when delay data is missing
2. **Orders without line items**: Documented as expected behavior (2.69% of orders)

