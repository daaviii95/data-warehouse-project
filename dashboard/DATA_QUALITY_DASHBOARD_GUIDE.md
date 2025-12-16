# Data Quality Dashboard Guide

## Overview

This guide helps you create a **Data Quality Monitoring Dashboard** in Power BI to track data quality metrics, completeness, and identify data issues in real-time.

---

## Purpose

The Data Quality Dashboard provides:
- **Real-time data quality metrics** across all dimensions and facts
- **Completeness tracking** (NULL value monitoring)
- **Referential integrity validation**
- **Data freshness indicators**
- **Anomaly detection** (unusual patterns, outliers)

---

## Data Source

Connect to the same PostgreSQL database as your main dashboards:
- **Connection**: PostgreSQL database (same as analytical views)
- **Views/Tables**: Use analytical views + direct table access for quality checks

---

## Recommended Dashboard Structure

### Page 1: Data Quality Overview

#### KPI Cards (Top Row)

**1. Overall Data Quality Score**
```DAX
Data Quality Score = 
VAR TotalChecks = 10  -- Total quality checks
VAR PassedChecks = 
    [CompletenessScore] + [ReferentialIntegrityScore] + [UniquenessScore] + ...
RETURN (PassedChecks / TotalChecks) * 100
```
- **Format**: Percentage (0-100%)
- **Color**: Green (>90%), Yellow (70-90%), Red (<70%)

**2. Total Records**
- **Measure**: `SUM('vw_sales_by_time'[total_orders])` (or similar)
- **Format**: Whole number with K/M suffixes

**3. Data Freshness**
- **Measure**: `MAX('vw_sales_by_time'[date])`
- **Format**: Date
- **Shows**: Last data update date

**4. Missing Values Count**
- **Measure**: Count of NULL values across critical fields
- **Format**: Whole number

#### Charts

**1. Completeness by Table (Bar Chart)**
- **X-axis**: Table names (dim_user, dim_product, fact_orders, etc.)
- **Y-axis**: Completeness percentage
- **Shows**: Which tables have missing data

**2. Data Quality Trends (Line Chart)**
- **X-axis**: Date (if tracking over time)
- **Y-axis**: Data Quality Score
- **Shows**: Quality trends over time

**3. Referential Integrity Status (Table)**
- **Columns**: 
  - Foreign Key
  - Orphaned Records Count
  - Status (Pass/Fail)

---

## Page 2: Dimension Quality

### Tables to Monitor

#### dim_user Quality
```DAX
User Completeness = 
DIVIDE(
    COUNTROWS(FILTER('dim_user', NOT(ISBLANK([name])) AND NOT(ISBLANK([user_id])))),
    COUNTROWS('dim_user'),
    0
) * 100
```

**Metrics to Track**:
- Total users: `COUNTROWS('dim_user')`
- Users with missing names: Count where `name IS NULL`
- Users with missing addresses: Count where `city IS NULL OR state IS NULL`
- Duplicate user_ids: Check for duplicates

#### dim_product Quality
```DAX
Product Type Completeness = 
DIVIDE(
    COUNTROWS(FILTER('dim_product', NOT(ISBLANK([product_type])))),
    COUNTROWS('dim_product'),
    0
) * 100
```

**Metrics to Track**:
- Total products
- Products with missing product_type
- Products with missing prices
- Product type distribution (should be normalized)

#### dim_merchant Quality
```DAX
Merchant Completeness = 
DIVIDE(
    COUNTROWS(FILTER('dim_merchant', 
        NOT(ISBLANK([name])) AND 
        NOT(ISBLANK([merchant_id])) AND
        NOT(ISBLANK([country]))
    )),
    COUNTROWS('dim_merchant'),
    0
) * 100
```

**Metrics to Track**:
- Merchants with NULL names (should be 0 after fix)
- Merchants with missing contact information
- Geographic distribution

---

## Page 3: Fact Table Quality

### fact_orders Quality

**Metrics**:
- Total orders: `COUNTROWS('fact_orders')`
- Orders without line items: Orders not in `fact_line_items`
- Orders with invalid dates: Orders where `transaction_date_sk` doesn't exist in `dim_date`
- Orders with missing dimension keys: Count NULL foreign keys

```DAX
Orders Without Line Items = 
VAR OrdersWithItems = DISTINCTCOUNT('fact_line_items'[order_id])
VAR TotalOrders = COUNTROWS('fact_orders')
RETURN TotalOrders - OrdersWithItems
```

### fact_line_items Quality

**Metrics**:
- Total line items
- Line items with missing products
- Line items with zero/negative quantities
- Line items with zero/negative prices

```DAX
Invalid Line Items = 
COUNTROWS(FILTER('fact_line_items', 
    [quantity] <= 0 OR 
    [price] <= 0 OR
    ISBLANK([product_sk])
))
```

---

## Page 4: Referential Integrity

### Foreign Key Validation

**Checks to Implement**:

1. **fact_orders → dim_user**
```DAX
Orphaned Orders (User) = 
COUNTROWS(
    FILTER('fact_orders',
        NOT(RELATED('dim_user'[user_sk]) IN VALUES('dim_user'[user_sk]))
    )
)
```

2. **fact_orders → dim_merchant**
```DAX
Orphaned Orders (Merchant) = 
COUNTROWS(
    FILTER('fact_orders',
        NOT(RELATED('dim_merchant'[merchant_sk]) IN VALUES('dim_merchant'[merchant_sk]))
    )
)
```

3. **fact_line_items → dim_product**
```DAX
Orphaned Line Items = 
COUNTROWS(
    FILTER('fact_line_items',
        NOT(RELATED('dim_product'[product_sk]) IN VALUES('dim_product'[product_sk]))
    )
)
```

### Visual: Referential Integrity Matrix
- **Visual**: Matrix
- **Rows**: Fact tables
- **Columns**: Dimension tables
- **Values**: Pass/Fail indicators

---

## Page 5: Data Anomalies

### Unusual Patterns Detection

**1. Revenue Anomalies**
- Orders with unusually high/low revenue
- Daily revenue spikes/drops
- Product price outliers

**2. Date Anomalies**
- Orders with future dates
- Orders with dates before 2020
- Missing transaction dates

**3. Quantity Anomalies**
- Line items with extremely high quantities
- Negative quantities (should be 0)

**4. Delay Anomalies**
- Orders with negative delay days
- Orders with delay > 30 days (unusual)

---

## Recommended Measures

### Core Quality Measures

```DAX
// Overall Completeness
Overall Completeness = 
AVERAGEX(
    {
        [User Completeness],
        [Product Completeness],
        [Merchant Completeness],
        [Order Completeness]
    },
    [Value]
)

// Data Freshness
Days Since Last Update = 
DATEDIFF(
    MAX('vw_sales_by_time'[date]),
    TODAY(),
    DAY
)

// Referential Integrity Score
Referential Integrity Score = 
VAR TotalFKs = 10  -- Total foreign key relationships
VAR ValidFKs = [Valid User FKs] + [Valid Merchant FKs] + [Valid Product FKs] + ...
RETURN (ValidFKs / TotalFKs) * 100
```

---

## Alerts and Thresholds

### Recommended Thresholds

| Metric | Good | Warning | Critical |
|--------|------|---------|---------|
| Completeness | >95% | 90-95% | <90% |
| Referential Integrity | 100% | 99-100% | <99% |
| Data Freshness | <1 day | 1-3 days | >3 days |
| Duplicate Rate | 0% | <1% | >1% |

### Visual Indicators
- **Green**: Within acceptable range
- **Yellow**: Needs attention
- **Red**: Critical issue

---

## Refresh Schedule

- **Recommended**: Refresh every hour or after each ETL run
- **Connection**: DirectQuery or Import (depending on data volume)
- **Automation**: Can be scheduled via Power BI Service

---

## Integration with ETL

The data quality checks in `scripts/data_quality.py` can be extended to write results to a `data_quality_metrics` table, which can then be visualized in this dashboard.

---

## Next Steps

1. **Connect to Database**: Use the same connection as your analytical dashboards
2. **Create Measures**: Start with the core quality measures above
3. **Build Visuals**: Use the recommended structure
4. **Set Alerts**: Configure Power BI alerts for critical thresholds
5. **Schedule Refresh**: Automate dashboard updates

---

**Related Documentation**:
- `docs/core/DATA_PROFILING.md` - Detailed data inventory
- `scripts/data_quality.py` - Automated quality checks
- `docs/core/DATA_MODEL.md` - Data model reference

