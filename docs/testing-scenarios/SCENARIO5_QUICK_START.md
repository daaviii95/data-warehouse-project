# Scenario 5 - Quick Start Guide

## 🎯 Recommended Approach: Monthly Sales Revenue

This is the simplest and most reliable KPI to test for Scenario 5.

---

## Step 1: Power BI Setup (5 minutes)

### Create KPI Card
1. Open Power BI → Connect to `vw_sales_by_time`
2. **Insert** → **Card** visual
3. Drag `total_revenue` to **Fields**
4. Format as **Currency ($)**
5. **Title**: "Total Revenue"

### Create Monthly Chart
1. **Insert** → **Column Chart**
2. **Axis**: `month_name`
3. **Values**: `total_revenue`
4. **Title**: "Monthly Sales Revenue"

### Record Values
- **Total Revenue**: $___________
- **Refresh Time**: _____ seconds

---

## Step 2: SQL Verification (5 minutes)

### Run These Queries:

**Query 1: Total Revenue**
```sql
SELECT SUM(total_revenue) AS total_revenue
FROM vw_sales_by_time;
```

**Query 2: Monthly Breakdown**
```sql
SELECT 
    year,
    month,
    month_name,
    SUM(total_revenue) AS monthly_revenue
FROM vw_sales_by_time
GROUP BY year, month, month_name
ORDER BY year, month;
```

**Query 3: Performance Check**
```sql
EXPLAIN ANALYZE
SELECT 
    year,
    month,
    month_name,
    SUM(total_revenue) AS monthly_revenue
FROM vw_sales_by_time
GROUP BY year, month, month_name
ORDER BY year, month;
```

### Record Results:
- **SQL Total Revenue**: $___________
- **Match?**: ✅ Yes / ❌ No
- **Difference**: $___________
- **SQL Execution Time**: _____ seconds

---

## Step 3: Comparison Table

| Metric | Power BI | SQL Query | Difference | Match? |
|--------|----------|-----------|------------|--------|
| Total Revenue | $_______ | $_______ | $_______ | ✅/❌ |
| Jan 2023 | $_______ | $_______ | $_______ | ✅/❌ |
| Feb 2023 | $_______ | $_______ | $_______ | ✅/❌ |
| ... | ... | ... | ... | ... |

**Performance:**
- Power BI Refresh: _____ seconds
- SQL Query: _____ seconds
- **Assessment**: ✅ Acceptable

---

## Step 4: Explanation Template

**Why Cross-Checking is Important:**

"Cross-checking SQL queries against dashboard visualizations ensures data accuracy and validates that BI tool aggregations match the underlying data warehouse calculations. This verification process catches potential errors in view definitions, DAX measures, or data transformation logic, preventing incorrect business decisions based on faulty metrics. Additionally, it helps identify performance bottlenecks and ensures the analytical layer provides reliable, consistent results across different access methods."

---

## Screen Recording Checklist

- [ ] Power BI dashboard showing KPI and chart
- [ ] SQL query execution and results
- [ ] Side-by-side comparison of values
- [ ] Performance metrics (execution times)
- [ ] Spoken explanation of importance

---

## Quick SQL File

All queries are in: `sql/utilities/scenario5_verification_queries.sql`

Run the queries for **Option 1: Monthly Sales Revenue** (recommended).

---

## Troubleshooting

**Values don't match?**
- Check date filters are the same
- Verify NULL handling
- Acceptable difference: < 0.01%

**Performance slow?**
- Check indexes exist
- Use EXPLAIN ANALYZE to find bottlenecks
- Consider materialized views for large datasets

