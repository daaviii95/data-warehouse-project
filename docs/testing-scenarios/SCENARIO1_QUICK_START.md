# Scenario 1 Quick Start Guide

## Quick Enable

### For Airflow DAG Run

```python
# In Airflow UI, set these environment variables for the load_fact task:
ENABLE_SCENARIO1_METRICS=true
TARGET_DATE=2024-01-15  # Optional: specific date for KPI tracking
```

### For Standalone Script

```bash
ENABLE_SCENARIO1_METRICS=true TARGET_DATE=2024-01-15 python scripts/etl_pipeline_python.py
```

### For Python Script

```python
import os
os.environ['ENABLE_SCENARIO1_METRICS'] = 'true'
os.environ['TARGET_DATE'] = '2024-01-15'  # Optional

# Then run your ETL pipeline
```

## What You'll See

When enabled, the pipeline will automatically:

1. **Before Loading**: Show current row counts and dashboard KPIs
2. **After Loading**: Show updated row counts and KPIs with changes
3. **Summary**: Explain how this confirms ingestion → transformation → load → analytics

## Example

```bash
# Enable metrics
export ENABLE_SCENARIO1_METRICS=true
export TARGET_DATE=2024-01-15

# Run pipeline
python scripts/etl_pipeline_python.py
```

Output will include:
- Before state (row counts, KPIs)
- ETL execution logs
- After state (row counts, KPIs, changes)
- Pipeline summary explanation

## Default: Disabled

By default, metrics are **disabled** to avoid performance impact on production runs.

Enable only when:
- Demonstrating Scenario 1
- Validating incremental loads
- Debugging data issues
- Testing pipeline changes
