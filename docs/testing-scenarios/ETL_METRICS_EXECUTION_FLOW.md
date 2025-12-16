# ETL Metrics Execution Flow

## When `etl_metrics.py` Functions Are Called

### Execution Timeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE EXECUTION                       │
└─────────────────────────────────────────────────────────────────┘

1. [START] Pipeline begins
   │
   ├─ Check if ENABLE_SCENARIO1_METRICS=true
   │  │
   │  ├─ YES → Call etl_metrics.log_before_state()
   │  │         └─ Records: fact_orders count, fact_line_items count, dashboard KPI
   │  │
   │  └─ NO  → Skip metrics (normal production run)
   │
2. [DIMENSIONS] Load dimension tables
   │  ├─ load_dim_campaign()
   │  ├─ load_dim_product()
   │  ├─ load_dim_user()
   │  ├─ load_dim_staff()
   │  ├─ load_dim_merchant()
   │  └─ ... (other dimensions)
   │
3. [SCENARIO 2] Create missing dimensions from fact data
   │  ├─ create_missing_users_from_orders()
   │  └─ create_missing_products_from_line_items()
   │
4. [FACTS] Load fact tables
   │  ├─ load_fact_orders()
   │  ├─ load_fact_line_items()
   │  └─ load_fact_campaign_transactions()
   │
5. [END] Pipeline completes
   │
   ├─ Check if ENABLE_SCENARIO1_METRICS=true AND before_state exists
   │  │
   │  ├─ YES → Call etl_metrics.log_after_state()
   │  │         └─ Records: updated counts, KPIs, calculates changes
   │  │         └─ Call etl_metrics.log_pipeline_summary()
   │  │             └─ Provides explanation of pipeline confirmation
   │  │
   │  └─ NO  → Skip metrics
   │
└─────────────────────────────────────────────────────────────────┘
```

## Detailed Execution Points

### In Airflow Workflow (`workflows/shopzada_etl_pipeline.py`)

**Function**: `run_load_fact()`

**Execution Order**:

1. **BEFORE State** (Line 384):
   ```python
   if ENABLE_SCENARIO1_METRICS:
       before_state = etl_metrics.log_before_state(target_date=TARGET_DATE)
   ```
   - **When**: Right at the start of `run_load_fact()` task
   - **Before**: Any fact loading happens
   - **Records**: Current state of fact tables and dashboard

2. **AFTER State** (Line 420-421):
   ```python
   if ENABLE_SCENARIO1_METRICS and before_state is not None:
       after_state = etl_metrics.log_after_state(before_state, target_date=TARGET_DATE)
       etl_metrics.log_pipeline_summary(before_state, after_state, target_date=TARGET_DATE)
   ```
   - **When**: After all fact tables are loaded
   - **After**: `load_fact_orders()`, `load_fact_line_items()`, `load_fact_campaign_transactions()` complete
   - **Records**: Updated state and calculates changes

### In Standalone Script (`scripts/etl_pipeline_python.py`)

**Function**: `main()`

**Execution Order**:

1. **BEFORE State** (Line 1419):
   ```python
   if ENABLE_SCENARIO1_METRICS:
       before_state = etl_metrics.log_before_state(target_date=TARGET_DATE)
   ```
   - **When**: Right after schema check, before any dimension loading
   - **Before**: All ETL operations
   - **Records**: Initial state of warehouse

2. **AFTER State** (Line 1489-1490):
   ```python
   if ENABLE_SCENARIO1_METRICS and before_state is not None:
       after_state = etl_metrics.log_after_state(before_state, target_date=TARGET_DATE)
       etl_metrics.log_pipeline_summary(before_state, after_state, target_date=TARGET_DATE)
   ```
   - **When**: After all fact tables are loaded
   - **After**: All ETL operations complete
   - **Records**: Final state and summary

## Exact Timing

### Airflow DAG Execution

```
Task: load_fact
├─ [0ms] Check ENABLE_SCENARIO1_METRICS
├─ [0-100ms] etl_metrics.log_before_state() ← HERE (if enabled)
├─ [100ms-5min] Extract and transform data
├─ [5min-10min] Create missing dimensions (Scenario 2)
├─ [10min-30min] Load fact tables
└─ [30min] etl_metrics.log_after_state() ← HERE (if enabled)
   └─ [30min] etl_metrics.log_pipeline_summary() ← HERE (if enabled)
```

### Standalone Script Execution

```
main()
├─ [0ms] Check ENABLE_SCENARIO1_METRICS
├─ [0-100ms] etl_metrics.log_before_state() ← HERE (if enabled)
├─ [100ms-5min] Load dimensions
├─ [5min-10min] Create missing dimensions (Scenario 2)
├─ [10min-30min] Load fact tables
└─ [30min] etl_metrics.log_after_state() ← HERE (if enabled)
   └─ [30min] etl_metrics.log_pipeline_summary() ← HERE (if enabled)
```

## Conditions for Execution

### Metrics Will Execute If:

1. ✅ `ENABLE_SCENARIO1_METRICS=true` environment variable is set
2. ✅ Database connection is available
3. ✅ Fact tables exist (for after state)

### Metrics Will NOT Execute If:

1. ❌ `ENABLE_SCENARIO1_METRICS` is not set or is `false` (default)
2. ❌ Database connection fails (gracefully handles error)
3. ❌ Fact tables don't exist (returns None, doesn't crash)

## What Gets Measured

### Before State
- `fact_orders` row count
- `fact_line_items` row count  
- Dashboard KPI (if `TARGET_DATE` provided):
  - Order count for target date
  - Total sales for target date

### After State
- Updated `fact_orders` row count
- Updated `fact_line_items` row count
- Updated dashboard KPI (if `TARGET_DATE` provided)
- **Calculated Changes**:
  - Rows added to fact_orders
  - Rows added to fact_line_items
  - Order count increase
  - Sales increase

### Summary
- Explanation of how pipeline confirms:
  - Ingestion → Transformation → Load → Analytics

## Performance Impact

- **Before State**: 2-3 database queries (~50-100ms)
- **After State**: 2-3 database queries (~50-100ms)
- **Total Overhead**: ~100-200ms when enabled
- **When Disabled**: 0ms overhead (checks are fast)

## Example Log Output

```
[2024-01-15 10:00:00] INFO: ============================================================
[2024-01-15 10:00:00] INFO: BEFORE STATE (Scenario 1)
[2024-01-15 10:00:00] INFO: ============================================================
[2024-01-15 10:00:01] INFO: Fact Orders Row Count (BEFORE): 810,000
[2024-01-15 10:00:01] INFO: Dashboard KPI for 2024-01-15 (BEFORE):
[2024-01-15 10:00:01] INFO:   - Order Count: 1,234
[2024-01-15 10:00:01] INFO:   - Total Sales: $45,678.90
[2024-01-15 10:00:01] INFO: ============================================================

[... ETL pipeline runs ...]

[2024-01-15 10:30:00] INFO: ============================================================
[2024-01-15 10:30:00] INFO: AFTER STATE (Scenario 1)
[2024-01-15 10:30:00] INFO: ============================================================
[2024-01-15 10:30:01] INFO: Fact Orders Row Count (AFTER): 810,010
[2024-01-15 10:30:01] INFO:   - Rows Added: 10
[2024-01-15 10:30:01] INFO: Dashboard KPI for 2024-01-15 (AFTER):
[2024-01-15 10:30:01] INFO:   - Order Count: 1,244
[2024-01-15 10:30:01] INFO:   - Total Sales: $46,913.46
[2024-01-15 10:30:01] INFO: Dashboard Update Summary:
[2024-01-15 10:30:01] INFO:   - Order Count: 1,234 → 1,244 (+10)
[2024-01-15 10:30:01] INFO:   - Total Sales: $45,678.90 → $46,913.46 (+$1,234.56)
[2024-01-15 10:30:01] INFO: ============================================================
```
