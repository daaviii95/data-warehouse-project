# Scenario 3 Test Data Guide

## Overview

This document describes the test data files for **Scenario 3: Late & Missing Campaign Data** demonstration.

## Test Data Files

### 1. Order Data (Prerequisites)

**File**: `data/Test/order_data_scenario3.csv`

**Purpose**: Contains order data that must exist before campaign transactions can be loaded.

**Contents**:
- 10 orders with IDs: `scenario3-order-001` through `scenario3-order-010`
- Dates: 2024-01-15 to 2024-01-17
- Uses placeholder user_ids and staff_ids (ensure these exist in dimensions)

**Note**: These orders must be loaded BEFORE campaign transactions.

---

### 2. Transactional Campaign Data (Initial Load - Missing Campaigns)

**File**: `data/Test/transactional_campaign_data_scenario3.csv`

**Purpose**: Contains campaign transactions with campaign_ids that **don't exist** in `dim_campaign` yet.

**Contents**:
- 10 transactions referencing 3 campaigns: `LATE_CAMPAIGN_001`, `LATE_CAMPAIGN_002`, `LATE_CAMPAIGN_003`
- These campaign_ids do NOT exist in `dim_campaign` when first loaded
- Distribution:
  - `LATE_CAMPAIGN_001`: 4 transactions (orders 001, 002, 007, 010)
  - `LATE_CAMPAIGN_002`: 3 transactions (orders 003, 004, 008)
  - `LATE_CAMPAIGN_003`: 3 transactions (orders 005, 006, 009)

**Expected Behavior (Step 1)**:
- When loaded, these transactions should be linked to the **"Unknown"** campaign
- Dashboard should show **"Unknown Campaign"** for these transactions
- `fact_campaign_transactions` should have rows with `campaign_sk` pointing to Unknown campaign

---

### 3. Campaign Dimension Data (Late-Arriving)

**File**: `data/Test/campaign_data_scenario3.csv`

**Purpose**: Contains the campaign dimension data that **arrives after** the transactional data.

**Contents**:
- 3 campaigns matching the campaign_ids in transactional data:
  - `LATE_CAMPAIGN_001`: Spring Sale 2024 (15% discount)
  - `LATE_CAMPAIGN_002`: Summer Flash Sale (20% discount)
  - `LATE_CAMPAIGN_003`: Back to School (10% discount)

**Expected Behavior (Step 2)**:
- When loaded, these campaigns should be inserted into `dim_campaign`
- Existing `fact_campaign_transactions` rows should be **automatically updated** from Unknown to actual campaigns
- Dashboard should show the **actual campaign names** instead of "Unknown Campaign"

---

## Testing Steps

### Step 1: Initial State

1. **Check `dim_campaign` table**:
   ```sql
   SELECT campaign_id, campaign_name, discount
   FROM dim_campaign
   ORDER BY campaign_id;
   ```
   - Should show existing campaigns (or Unknown campaign if already created)
   - Should NOT have `LATE_CAMPAIGN_001`, `LATE_CAMPAIGN_002`, `LATE_CAMPAIGN_003`

2. **Show test files**:
   - Display contents of `transactional_campaign_data_scenario3.csv`
   - Display contents of `order_data_scenario3.csv`
   - Note that campaigns referenced don't exist yet

---

### Step 2: Load Orders First

1. Copy `order_data_scenario3.csv` to `data/Operations Department/` folder
2. Run ETL pipeline to load orders into `fact_orders`
3. **Verify**:
   ```sql
   SELECT COUNT(*) FROM fact_orders WHERE order_id LIKE 'scenario3-%';
   ```
   - Should return **10**

---

### Step 3: Load Campaign Transactions (Missing Campaigns)

1. Copy `transactional_campaign_data_scenario3.csv` to `data/Marketing Department/` folder
2. Run ETL pipeline to load campaign transactions
3. **Verify `dim_campaign`**:
   ```sql
   SELECT campaign_id, campaign_name, discount
   FROM dim_campaign
   WHERE campaign_id = 'UNKNOWN';
   ```
   - Should show **"Unknown Campaign"** entry

4. **Verify `fact_campaign_transactions`**:
   ```sql
   SELECT 
       fct.order_id,
       dc.campaign_id,
       dc.campaign_name,
       fct.availed
   FROM fact_campaign_transactions fct
   INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
   WHERE fct.order_id LIKE 'scenario3-%'
   ORDER BY fct.order_id;
   ```
   - All 10 rows should show `campaign_id = 'UNKNOWN'`
   - All 10 rows should show `campaign_name = 'Unknown Campaign'`

5. **Check Dashboard**: Should show **"Unknown Campaign"** for all 10 transactions

---

### Step 4: Load Campaign Dimension (Late-Arriving)

1. Copy `campaign_data_scenario3.csv` to `data/Marketing Department/` folder
2. Run ETL pipeline to load campaign dimensions
3. **Verify `dim_campaign`**:
   ```sql
   SELECT campaign_id, campaign_name, discount
   FROM dim_campaign
   WHERE campaign_id LIKE 'LATE_CAMPAIGN%'
   ORDER BY campaign_id;
   ```
   - Should show **3 new campaigns**:
     - LATE_CAMPAIGN_001: Spring Sale 2024
     - LATE_CAMPAIGN_002: Summer Flash Sale
     - LATE_CAMPAIGN_003: Back to School

4. **Verify `fact_campaign_transactions` Updated**:
   ```sql
   SELECT 
       fct.order_id,
       dc.campaign_id,
       dc.campaign_name,
       dc.discount,
       fct.availed
   FROM fact_campaign_transactions fct
   INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
   WHERE fct.order_id LIKE 'scenario3-%'
   ORDER BY dc.campaign_id, fct.order_id;
   ```
   - Rows should now show actual campaign_ids (LATE_CAMPAIGN_001, LATE_CAMPAIGN_002, LATE_CAMPAIGN_003)
   - Campaign names should be "Spring Sale 2024", "Summer Flash Sale", "Back to School"
   - **No rows should reference "Unknown Campaign" anymore**

5. **Verify Update Distribution**:
   ```sql
   SELECT 
       dc.campaign_id,
       dc.campaign_name,
       COUNT(*) as transaction_count
   FROM fact_campaign_transactions fct
   INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
   WHERE fct.order_id LIKE 'scenario3-%'
   GROUP BY dc.campaign_id, dc.campaign_name
   ORDER BY dc.campaign_id;
   ```
   - Expected distribution:
     - LATE_CAMPAIGN_001: **4 transactions**
     - LATE_CAMPAIGN_002: **3 transactions**
     - LATE_CAMPAIGN_003: **3 transactions**

6. **Check Dashboard**: Should now show **actual campaign names** instead of "Unknown Campaign"

---

## SQL Queries for Verification

### Before Campaign Load (Step 3)

```sql
-- Check Unknown campaign exists
SELECT * FROM dim_campaign WHERE campaign_id = 'UNKNOWN';

-- Check facts using Unknown campaign
SELECT 
    fct.order_id,
    dc.campaign_id,
    dc.campaign_name,
    fct.availed
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
WHERE dc.campaign_id = 'UNKNOWN'
  AND fct.order_id LIKE 'scenario3-%'
ORDER BY fct.order_id;
```

**Expected**: All 10 rows should show Unknown campaign

---

### After Campaign Load (Step 4)

```sql
-- Check new campaigns exist
SELECT campaign_id, campaign_name, discount
FROM dim_campaign
WHERE campaign_id LIKE 'LATE_CAMPAIGN%'
ORDER BY campaign_id;

-- Verify facts updated
SELECT 
    fct.order_id,
    dc.campaign_id,
    dc.campaign_name,
    fct.availed
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
WHERE fct.order_id LIKE 'scenario3-%'
ORDER BY dc.campaign_id, fct.order_id;

-- Verify no Unknown references remain
SELECT COUNT(*) as unknown_count
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
WHERE dc.campaign_id = 'UNKNOWN'
  AND fct.order_id LIKE 'scenario3-%';
```

**Expected**: `unknown_count` should be **0**

---

## Expected Results Summary

| Step | dim_campaign | fact_campaign_transactions | Dashboard |
|------|--------------|---------------------------|-----------|
| **Before** | Only existing campaigns | N/A | N/A |
| **After Step 3** | + Unknown Campaign | 10 rows with Unknown campaign_sk | Shows "Unknown Campaign" |
| **After Step 4** | + 3 new campaigns (LATE_CAMPAIGN_001-003) | 10 rows updated to actual campaigns | Shows actual campaign names |

---

## Business Rules Explanation

**Unknown Campaign**:
- Used when a `campaign_id` is referenced in transactional data but doesn't exist in `dim_campaign`
- Indicates **late-arriving dimension data** - the campaign exists but hasn't been loaded yet
- Fact rows are created with Unknown campaign_sk and **automatically updated** when actual campaign data arrives
- **Business Meaning**: "This order referenced a campaign, but we don't have campaign details yet"

**Not Applicable**:
- Used when an order has no campaign reference (NULL or empty `campaign_id`)
- Indicates **optional dimension** - the order simply didn't use a campaign
- These orders may not appear in `fact_campaign_transactions` at all
- **Business Meaning**: "This order did not use any campaign"

**Late-Arriving Dimensions**:
- Process: Facts reference Unknown campaign → Dimension loaded → Facts automatically updated
- Benefits: No data loss, automatic correction, historical accuracy

---

## Cleanup

After testing, you may want to remove test data:

```sql
-- Remove test campaign transactions
DELETE FROM fact_campaign_transactions 
WHERE order_id LIKE 'scenario3-%';

-- Remove test orders
DELETE FROM fact_orders 
WHERE order_id LIKE 'scenario3-%';

-- Remove test campaigns (optional - keep for reference)
DELETE FROM dim_campaign 
WHERE campaign_id LIKE 'LATE_CAMPAIGN%';
```

---

## Notes

- Ensure `user-001` through `user-010` exist in `dim_user` (or use existing user_ids)
- Ensure `staff-001`, `staff-002`, `staff-003` exist in `dim_staff` (or use existing staff_ids)
- The dates (2024-01-15 to 2024-01-17) should exist in `dim_date` (populated by `02_populate_dim_date.sql`)
- File format: CSV with comma separators
- Campaign IDs use `LATE_CAMPAIGN_` prefix to clearly indicate late-arriving data

