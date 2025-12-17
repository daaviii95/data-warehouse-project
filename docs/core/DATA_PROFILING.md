# ShopZada Data Warehouse - Data Profiling Report

## Overview

This document provides a comprehensive data inventory and profiling report for the ShopZada multi-departmental enterprise data warehouse. It details the structure, quality, and characteristics of all source datasets.

**Last Updated**: Based on ETL pipeline analysis and data quality checks

---

## Overall Data Landscape

- **Source Departments**: Marketing, Operations, Business, Customer Management, Enterprise
- **File Formats**: CSV, Parquet, JSON, Pickle, Excel (.xlsx), HTML
- **Total Datasets**: ~18 files, ranging from small lookup tables to large transactional datasets
- **Data Quality**: High completeness, no duplicates, minor inconsistencies in text formatting and naming conventions
- **Total Volume**: ~2.5M+ transactional records across all departments

---

## Department-by-Department Summary

### 1. Marketing Department

#### `campaign_data.csv`
- **Rows**: 10 campaigns
- **Columns**: 4 (campaign_id, campaign_name, campaign_description, discount)
- **Issues Identified**:
  - Discount field has inconsistent formats (`1%`, `1pct`, `10%%` typo)
  - **ETL Handling**: `parse_discount()` function normalizes all formats
- **Status**: Cleaned and loaded into `dim_campaign`

#### `transactional_campaign_data.csv`
- **Rows**: ~125,000 transactions
- **Columns**: 6 (transaction_date, campaign_id, order_id, estimated arrival, availed, [index])
- **Characteristics**:
  - 70% campaign uptake (`availed = 1`)
  - `estimated arrival` is textual; needs conversion to numeric
- **Status**: Loaded into `fact_campaign_transactions`

---

### 2. Operations Department

#### Line Item Data (6 files total)
- **Prices Files** (3): `line_item_data_prices1.csv`, `line_item_data_prices2.csv`, `line_item_data_prices3.parquet`
- **Products Files** (3): `line_item_data_products1.csv`, `line_item_data_products2.csv`, `line_item_data_products3.parquet`
- **Total Rows**: ~2M line items
- **Issues Identified**:
  - Quantity field has inconsistent suffixes (`pcs`, `pieces`, `PC`)
  - Prices vary across files (expected - different time periods)
  - **ETL Handling**: `extract_numeric()` function handles all quantity formats
- **Status**: Merged by row position and loaded into `fact_line_items`

#### Order Data (6 files across formats)
- **Files**:
  - `order_data_20200101-20200701.parquet`
  - `order_data_20200701-20211001.pickle`
  - `order_data_20211001-20220101.csv`
  - `order_data_20220101-20221201.xlsx`
  - `order_data_20221201-20230601.json`
  - `order_data_20230601-20240101.html`
- **Time Range**: 2020-01-01 to 2024-01-01
- **Issues Identified**:
  - Schema mostly consistent
  - Some files have extra `Unnamed: 0` index columns
  - **ETL Handling**: `clean_dataframe()` automatically removes index columns
- **Status**: Loaded into `fact_orders`

#### `order_delays.html`
- **Rows**: ~200,000 delay records
- **Columns**: 3 (order_id, delay in days)
- **Characteristics**:
  - `delay_days` as integer (0-9 days)
  - Can be joined with order/merchant data for operational analytics
- **Status**: Merged with order data in `fact_orders`

---

### 3. Business Department

#### `product_list.xlsx`
- **Rows**: 750 products
- **Columns**: 5 (product_id, product_name, product_type, price, [other])
- **Issues Identified**:
  - One missing `product_type` value
  - Inconsistent category naming:
    - `Cosmetic` vs `Cosmetics` (singular/plural variations)
    - `stationary` vs `stationery` vs `stationary and school supplies`
    - Other variations: `Electronic`/`Electronics`, `Toy`/`Toys`, etc.
  - **ETL Handling**: 
    - `format_product_type()` function normalizes variations
    - SQL script `14_fix_product_type_normalization.sql` fixes existing data
- **Status**: Loaded into `dim_product` with normalization

---

### 4. Customer Management Department

#### `user_job.csv`
- **Rows**: 5,000 user job records
- **Columns**: 5 (user_id, job_title, job_level, [index column])
- **Issues Identified**:
  - 1,629 missing `job_level` values (mostly for `"Student"` job titles)
  - Index column (`Unnamed: 0`) present
  - **ETL Handling**: Index columns automatically removed
- **Status**: Loaded into `dim_user_job`

#### `user_data.json`
- **Rows**: 5,000 users
- **Columns**: 11 (user_id, name, street, state, city, country, birthdate, gender, device_address, user_type, creation_date)
- **Characteristics**:
  - Clean, flat JSON structure
  - Balanced gender distribution
  - Three user types: `basic`, `verified`, `premium`
- **Status**: Loaded into `dim_user`

#### `user_credit_card.pickle`
- **Rows**: 5,000 credit card records
- **Columns**: 4 (user_id, name, credit_card_number, issuing_bank)
- **Characteristics**:
  - Clean data
  - Card numbers stored as integers (converted to strings in ETL)
  - 8 unique issuing banks
- **Status**: Loaded into `dim_credit_card`

---

### 5. Enterprise Department

#### `staff_data.html`
- **Rows**: 5,000 staff members
- **Columns**: 10 (staff_id, name, street, state, city, country, job_level, contact_number, creation_date, [index])
- **Characteristics**:
  - Global staff across 249 countries
  - `contact_number` formatting inconsistent
  - **ETL Handling**: `format_phone_number()` standardizes phone formats
- **Status**: Loaded into `dim_staff`

#### `merchant_data.html`
- **Rows**: 5,000 merchants
- **Columns**: 9 (merchant_id, name, street, state, city, country, contact_number, creation_date, [index])
- **Characteristics**:
  - Global merchants across 249 countries
  - `contact_number` needs normalization
  - **ETL Handling**: `format_phone_number()` standardizes phone formats
- **Status**: Loaded into `dim_merchant`

#### `order_with_merchant_data` (3 files)
- **Files**:
  - `order_with_merchant_data1.parquet`
  - `order_with_merchant_data2.parquet`
  - `order_with_merchant_data3.csv`
- **Total Rows**: 500,000+ order-merchant relationships
- **Columns**: 3-4 (order_id, merchant_id, staff_id, [index column])
- **Characteristics**:
  - Clean and referentially consistent
  - Some files have extra index columns (`Unnamed: 0`)
  - Used for joining orders with merchant/staff information
- **Status**: Merged with order data in `fact_orders`
- **Note**: These files are excluded from `dim_merchant` loading (they do not contain full merchant attributes)

---

## Common Data Quality Issues & ETL Solutions

### 1. Inconsistent Text Formats

**Issue**: Various formats for the same concept
- Discounts: `1%`, `1pct`, `10%%` (typo)
- Quantities: `pcs`, `pieces`, `PC`
- Phone numbers: Various formats

**ETL Solution**:
- `parse_discount()`: Extracts numeric value from any discount format
- `extract_numeric()`: Handles quantity suffixes (`pcs`, `pieces`, etc.)
- `format_phone_number()`: Standardizes phone number formats

### 2. Redundant Columns

**Issue**: `Unnamed: 0` index columns in many files

**ETL Solution**:
- `clean_dataframe()` automatically detects and removes index columns
- Checks for sequential numeric columns and unnamed columns

### 3. Category Inconsistencies

**Issue**: Product types and job levels with typos/variants
- `Cosmetic` vs `Cosmetics`
- `Electronic` vs `Electronics`
- `Toy` vs `Toys`
- `stationary` vs `stationary and school supplies`

**ETL Solution**:
- `format_product_type()`: Normalizes singular/plural variations and spelling (e.g., "stationary"/"stationery")
- Product type normalization runs automatically in `sql/03_create_analytical_views.sql` before views are created

### 4. Mixed File Formats

**Issue**: Same department uses multiple formats (CSV, Parquet, JSON, Pickle, Excel, HTML)

**ETL Solution**:
- `load_file()` function handles all formats automatically
- Detects format by extension and uses appropriate pandas reader

### 5. Missing Values

**Issue**: 
- 1,629 missing `job_level` values (mostly for "Student" job titles)
- One missing `product_type`

**ETL Solution**:
- NULL values are preserved (not filled with defaults)
- Data quality checks validate completeness

---

## Data Quality Assessment

### Strengths
- **High completeness**: Most fields have >99% completeness
- **No duplicates**: Referential integrity maintained
- **Strong relationships**: Order-merchant-staff relationships are consistent
- **Well-structured**: Clear schema across all files
- **Time-series coverage**: Complete coverage from 2020-2024

### Areas Requiring Attention
- **Text normalization**: Handled by ETL functions
- **Product type variations**: Fixed by normalization function
- **Missing job levels**: Expected for "Student" job titles (acceptable NULLs)

---

## Data Volume Summary

| Department | Files | Total Rows | Primary Use |
|------------|-------|------------|-------------|
| Marketing | 2 | ~125,010 | Campaign analysis |
| Operations | 13 | ~2,200,000+ | Transactional facts |
| Business | 1 | 750 | Product catalog |
| Customer Management | 3 | 15,000 | Customer dimensions |
| Enterprise | 5 | 510,000+ | Merchant/staff dimensions |
| **Total** | **24** | **~2,850,000+** | **Complete DWH** |

---

## ETL Data Flow

```
Source Files → clean_dataframe() → format_*() functions → Dimension/Fact Tables
```

### Data Cleaning Pipeline
1. **File Loading**: Format detection and reading
2. **Column Cleaning**: Remove index columns, normalize names
3. **Data Cleaning**: Handle NULLs, normalize strings
4. **Type Conversion**: Extract numerics, parse dates
5. **Formatting**: Standardize text (names, addresses, phone numbers)
6. **Normalization**: Product types, user types, etc.
7. **Database Loading**: Bulk inserts with conflict handling

---

## Notes for Data Analysts

### When Querying Views
- Use `vw_sales_by_time` for **date-filtered** queries
- Use `vw_merchant_performance` for **merchant-filtered** queries (without date filters)
- Product types are normalized - check for standard names (e.g., "Cosmetics" not "Cosmetic")

### Known Data Characteristics
- **Campaign Data**: Only includes actual campaigns (excludes "No Campaign" entries)
- **Delay Days**: Range 0-9 days, with -1 used for missing values
- **User Types**: `basic`, `verified`, `premium`
- **Product Types**: Standardized list (see normalization function)

---

## Data Profiling Queries

### Check Product Type Distribution
```sql
SELECT product_type, COUNT(*) 
FROM dim_product 
GROUP BY product_type 
ORDER BY COUNT(*) DESC;
```

### Check Missing Values
```sql
SELECT 
    'dim_user_job' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(job_level) AS non_null_job_level,
    COUNT(*) - COUNT(job_level) AS missing_job_level
FROM dim_user_job;
```

### Verify Data Completeness
```sql
SELECT 
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fli.order_id) AS orders_with_line_items,
    COUNT(DISTINCT fo.order_id) - COUNT(DISTINCT fli.order_id) AS orders_without_items
FROM fact_orders fo
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id;
```

---

**Document Version**: 1.0  
**Last Updated**: Based on ETL pipeline v2.0 (with optimizations)

---

## Related Documentation

- **Data Model**: See `docs/core/DATA_MODEL.md` for schema details
- **ETL Pipeline**: See `docs/core/WORKFLOW.md` for ETL process
- **Architecture**: See `docs/core/ARCHITECTURE.md` for system architecture
- **Incremental Loading**: See `docs/core/INCREMENTAL_LOADING.md` for file tracking
- **Business Questions**: See `docs/core/BUSINESS_QUESTIONS.md` for analytical use cases

