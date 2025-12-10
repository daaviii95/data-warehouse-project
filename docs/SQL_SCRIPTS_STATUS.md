# SQL Scripts Status and Usage

## Current Status

### ✅ **Active Scripts** (Used in ETL Pipeline)

1. **`01_create_schema_from_physical_model.sql`**
   - ✅ **USED** - Creates all dimension and fact tables
   - Called by: `shopzada_etl_pipeline` DAG → `create_schema` task

2. **`02_populate_dim_date.sql`**
   - ✅ **USED** - Populates date dimension
   - Called by: `shopzada_etl_pipeline` DAG → `populate_date_dimension` task

3. **`13_create_analytical_views.sql`**
   - ✅ **USED** - Creates analytical views for Power BI
   - Called by: `shopzada_analytical_views` DAG → `create_analytical_views` task

### ❌ **Legacy/Unused Scripts** (Not Used in Current Pipeline)

4. **`03_load_dim_campaign.sql`**
5. **`04_load_dim_product.sql`**
6. **`05_load_dim_user.sql`**
7. **`06_load_dim_staff.sql`**
8. **`07_load_dim_merchant.sql`**
9. **`08_load_dim_user_job.sql`**
10. **`09_load_dim_credit_card.sql`**
11. **`10_load_fact_orders.sql`**
12. **`11_load_fact_line_items.sql`**
13. **`12_load_fact_campaign_transactions.sql`**

**Status**: ❌ **NOT USED** - These scripts are designed for a **staging table approach** that is not used in the current Python-based ETL pipeline.

## Why They're Not Used

### Current Approach: Direct File Loading (Python ETL)

The active ETL pipeline (`scripts/etl_pipeline_python.py`) loads data **directly from source files** into dimension/fact tables:

```
Source Files → Python ETL → Dimension/Fact Tables
```

### Legacy Approach: Staging Tables (SQL Scripts)

The SQL load scripts (03-12) were designed for a **staging table approach**:

```
Source Files → Staging Tables → SQL Scripts → Dimension/Fact Tables
```

**Problem**: The Python ETL pipeline doesn't create staging tables, so these SQL scripts can't work with the current setup.

## What These SQL Scripts Do

The SQL load scripts:
1. Look for staging tables with patterns like:
   - `stg_marketing_department_campaign_data%`
   - `stg_operations_department_order_data%`
   - `stg_operations_department_line_item_data_prices%`
   - etc.

2. Dynamically discover and combine data from multiple staging tables

3. Load data into dimension/fact tables using SQL

## Recommendation

### Option 1: Keep as Reference (Recommended)

**Keep the scripts** but move them to an archive/reference folder:

```
sql/
├── active/                    # Currently used scripts
│   ├── 01_create_schema_from_physical_model.sql
│   ├── 02_populate_dim_date.sql
│   └── 13_create_analytical_views.sql
└── archive/                   # Legacy/unused scripts
    ├── 03_load_dim_campaign.sql
    ├── 04_load_dim_product.sql
    ├── ...
    └── 12_load_fact_campaign_transactions.sql
```

**Benefits**:
- Reference for alternative ETL approach
- Useful if you need to switch to staging table approach later
- Documentation of different ETL patterns

### Option 2: Delete (If Definitely Not Needed)

**Delete the scripts** if:
- You're certain you'll never use staging tables
- You want to reduce code clutter
- The Python ETL approach is permanent

**Risks**:
- Lose reference implementation
- Can't easily switch approaches later

### Option 3: Keep in Place with Documentation

**Keep scripts in place** but add clear documentation that they're legacy:

- Add header comments: `-- LEGACY: Not used in current Python ETL pipeline`
- Update README to explain they're for reference only

## Current ETL Flow

```
┌─────────────────────────────────────────────────┐
│  shopzada_etl_pipeline DAG                      │
├─────────────────────────────────────────────────┤
│  1. create_schema                               │
│     → sql/01_create_schema_from_physical_model  │
│                                                 │
│  2. populate_date_dimension                     │
│     → sql/02_populate_dim_date                 │
│                                                 │
│  3. run_python_etl                              │
│     → scripts/etl_pipeline_python.py          │
│     (Loads directly from files)                 │
│                                                 │
│  4. run_data_quality_checks                     │
│     → scripts/data_quality.py                   │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  shopzada_analytical_views DAG                  │
├─────────────────────────────────────────────────┤
│  1. create_analytical_views                     │
│     → sql/13_create_analytical_views            │
└─────────────────────────────────────────────────┘
```

## Decision Matrix

| Factor | Keep Scripts | Delete Scripts |
|--------|--------------|----------------|
| **Current Usage** | ❌ Not used | ❌ Not used |
| **Future Flexibility** | ✅ Can switch approaches | ❌ Locked into Python ETL |
| **Reference Value** | ✅ Good documentation | ❌ Lost knowledge |
| **Code Clutter** | ⚠️ Some clutter | ✅ Cleaner |
| **Maintenance** | ⚠️ Need to keep in sync | ✅ Less to maintain |

## Recommendation: **Keep as Reference**

**Suggested Action**: Move unused scripts to `sql/archive/` folder with a README explaining they're for reference only.

This provides:
- ✅ Clean active codebase
- ✅ Preserved reference implementation
- ✅ Flexibility for future changes
- ✅ Clear documentation of what's active vs legacy

