# Power BI Day Exceeding 31 - Fix Guide

## Problem

When using `vw_sales_by_time[day]` in Power BI charts, days are exceeding 31 on the X-axis. This happens because Power BI is treating the `day` field as a numeric measure and aggregating it (summing) instead of using it as a categorical dimension.

## Root Cause

Power BI is likely:
1. **Treating `day` as a measure** instead of a dimension/category
2. **Aggregating day values** (SUM) when grouping data across multiple months
3. **Data type confusion** - Power BI may be interpreting the INTEGER field incorrectly

## Solution

### Option 1: Change Field Type in Power BI (Recommended)

1. **In Power BI Desktop:**
   - Go to **Data** view (left sidebar)
   - Find the `vw_sales_by_time` table
   - Select the `day` field
   - In the **Properties** pane, change:
     - **Data Category**: Set to "None" or "Whole Number"
     - **Summarization**: Change from "Sum" to **"Don't summarize"**
     - **Format**: Set to "Whole number"

2. **In the Visual:**
   - Remove `day` from the X-axis
   - Add it back to the X-axis
   - Power BI should now treat it as a category (1, 2, 3... 31) instead of summing

### Option 2: Create a Calculated Column

Create a calculated column in Power BI that explicitly formats day as text:

```DAX
Day of Month = FORMAT(vw_sales_by_time[day], "0")
```

Then use `Day of Month` instead of `day` in your charts.

### Option 3: Use Date Instead of Day

Instead of grouping by `day`, use the full `date` field and format it to show only the day:

1. Use `vw_sales_by_time[date]` on X-axis
2. Right-click the field → **Format**
3. Set format to show only day (e.g., "dd" format)
4. This ensures proper date handling

### Option 4: Create a Proper Day Dimension

Create a calculated table in Power BI:

```DAX
Day Dimension = 
SELECTCOLUMNS(
    GENERATESERIES(1, 31, 1),
    "Day", [Value],
    "Day Label", FORMAT([Value], "0")
)
```

Then create a relationship and use this dimension instead.

## Quick Fix Steps

**Fastest Solution:**

1. Open your Power BI report
2. Select the visual with the day axis issue
3. In the **Fields** pane, find `day` under `vw_sales_by_time`
4. Right-click `day` → **Don't summarize** (or change aggregation to "Don't summarize")
5. If it's in the X-axis, remove it and add it back
6. The chart should now show days 1-31 correctly

## Verification

After applying the fix:
- Days should range from 1 to 31 only
- No values should exceed 31
- Chart should show proper distribution across days of the month

## Prevention

To prevent this issue in future visuals:
- Always set numeric fields that represent categories (like day, month, year) to **"Don't summarize"**
- Use **Data Category** settings to help Power BI understand the field type
- Consider creating explicit dimension tables for time components
