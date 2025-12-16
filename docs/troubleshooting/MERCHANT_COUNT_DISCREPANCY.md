# Merchant Count Discrepancy: Source File vs Power BI

## Issue

**Source File**: `merchant_data.html` contains **5,000 merchants**  
**Power BI**: Shows **4,812** for "Count of merchant_id" (or 4,812k if counting rows)

## Root Cause

The discrepancy occurs because **`vw_merchant_performance` only includes merchants that have orders**.

### Why This Happens

1. **Source File**: Contains all 5,000 merchants in `merchant_data.html`
2. **dim_merchant**: All 5,000 merchants are loaded into the dimension table
3. **vw_merchant_performance**: This view is based on `fact_orders` with a LEFT JOIN to `dim_merchant`
   - Only merchants that appear in `fact_orders` will be in the view
   - Merchants with **zero orders** are excluded from the view
4. **Power BI**: Counts merchants from `vw_merchant_performance`, so it only sees merchants with orders

### The Math

```
Total merchants in source file:        5,000
Merchants with orders (in view):      4,812
Merchants with NO orders (missing):     188
```

## Verification

Run this SQL query to verify:

```sql
-- Check the counts
SELECT 
    (SELECT COUNT(*) FROM dim_merchant) AS total_merchants,
    (SELECT COUNT(DISTINCT merchant_id) FROM vw_merchant_performance) AS merchants_in_view,
    (SELECT COUNT(*) FROM dim_merchant dm 
     LEFT JOIN fact_orders fo ON dm.merchant_sk = fo.merchant_sk 
     WHERE fo.order_id IS NULL) AS merchants_without_orders;
```

**Expected Result**:
- `total_merchants`: 5,000
- `merchants_in_view`: 4,812
- `merchants_without_orders`: 188

## Solutions

### Option 1: Count from dim_merchant (Recommended)

If you want to show **all merchants** (including those with no orders), create a measure that counts from `dim_merchant` directly:

**In Power BI:**
1. Go to **Data** view
2. Right-click on `dim_merchant` table
3. Select **New measure**
4. Enter:
   ```DAX
   Total Merchants (All) = DISTINCTCOUNT('dim_merchant'[merchant_id])
   ```
5. Use this measure instead of counting from `vw_merchant_performance`

**Result**: Will show **5,000** (all merchants)

---

### Option 2: Count Active Merchants Only

If you want to show only **merchants with orders** (current behavior), keep using:

```DAX
Total Merchants (Active) = DISTINCTCOUNT('vw_merchant_performance'[merchant_id])
```

**Result**: Will show **4,812** (merchants with at least one order)

**Note**: This is actually the correct count for "active merchants" - merchants that have processed orders.

---

### Option 3: Create a View That Includes All Merchants

If you need a view that includes all merchants (even with zero orders), create a new view:

```sql
CREATE OR REPLACE VIEW vw_all_merchants AS
SELECT 
    dm.merchant_id,
    dm.name AS merchant_name,
    dm.city AS merchant_city,
    dm.state AS merchant_state,
    dm.country AS merchant_country,
    COALESCE(COUNT(DISTINCT fo.order_id), 0) AS total_orders,
    COALESCE(SUM(fli.quantity * dp.price), 0) AS total_revenue,
    CASE 
        WHEN COUNT(DISTINCT fo.order_id) > 0 THEN 'Active'
        ELSE 'Inactive'
    END AS merchant_status
FROM dim_merchant dm
LEFT JOIN fact_orders fo ON dm.merchant_sk = fo.merchant_sk
LEFT JOIN fact_line_items fli ON fo.order_id = fli.order_id
LEFT JOIN dim_product dp ON fli.product_sk = dp.product_sk
GROUP BY 
    dm.merchant_id,
    dm.name,
    dm.city,
    dm.state,
    dm.country;
```

Then in Power BI, use:
```DAX
Total Merchants (All) = DISTINCTCOUNT('vw_all_merchants'[merchant_id])
```

---

## If Power BI Shows 4,812k (4,812,000)

If Power BI is showing **4,812,000** instead of **4,812**, this indicates a different problem:

### Problem: Counting Rows Instead of Distinct Values

**Wrong Measure** (counts all rows):
```DAX
Count of merchant_id = COUNT('vw_merchant_performance'[merchant_id])
```

**Correct Measure** (counts distinct merchants):
```DAX
Count of merchant_id = DISTINCTCOUNT('vw_merchant_performance'[merchant_id])
```

### How to Fix

1. Check your Power BI measure
2. If it uses `COUNT()`, change it to `DISTINCTCOUNT()`
3. `COUNT()` counts all rows (including duplicates)
4. `DISTINCTCOUNT()` counts unique values only

---

## Understanding the Views

### vw_merchant_performance
- **Purpose**: Shows merchant performance metrics
- **Source**: Based on `fact_orders` (only merchants with orders)
- **Use Case**: Analyzing active merchants, performance metrics
- **Merchant Count**: 4,812 (merchants with orders)

### dim_merchant
- **Purpose**: Complete merchant dimension table
- **Source**: All merchants from source files
- **Use Case**: Complete merchant list, including inactive merchants
- **Merchant Count**: 5,000 (all merchants)

---

## Recommended Approach

**For "Total Merchants" KPI:**

1. **If showing "Active Merchants"**: Use `vw_merchant_performance` → 4,812
2. **If showing "All Merchants"**: Use `dim_merchant` → 5,000

**Best Practice**: 
- Label the measure clearly: "Active Merchants" vs "Total Merchants"
- Use `DISTINCTCOUNT()` not `COUNT()`
- Consider showing both metrics side-by-side for context

---

## Quick Check Query

Run this to see the breakdown:

```sql
-- See the full breakdown
SELECT 
    'Total in dim_merchant' AS source,
    COUNT(*) AS count
FROM dim_merchant
UNION ALL
SELECT 
    'Distinct in vw_merchant_performance' AS source,
    COUNT(DISTINCT merchant_id) AS count
FROM vw_merchant_performance
UNION ALL
SELECT 
    'Merchants with NO orders' AS source,
    COUNT(*) AS count
FROM dim_merchant dm
LEFT JOIN fact_orders fo ON dm.merchant_sk = fo.merchant_sk
WHERE fo.order_id IS NULL;
```

---

## Summary

| Source | Count | Reason |
|--------|-------|--------|
| `merchant_data.html` | 5,000 | All merchants in source file |
| `dim_merchant` | 5,000 | All merchants loaded into dimension |
| `vw_merchant_performance` | 4,812 | Only merchants with orders |
| **Power BI (from view)** | **4,812** | ✅ **Correct** - counts active merchants |
| **Power BI (if 4,812k)** | **4,812,000** | ❌ **Wrong** - using COUNT() instead of DISTINCTCOUNT() |

**Conclusion**: The count of **4,812** is correct if you're counting active merchants (those with orders). If you need all 5,000 merchants, count from `dim_merchant` instead.


