# Scenario 3 Test Data Guide

## Overview

This document describes the test data files created for Scenario 3: Late & Missing Campaign Data demonstration.

## Test Data Files

### 1. Transactional Campaign Data (Initial Load - Missing Campaigns)

**File**: `data/Marketing Department/transactional_campaign_data_scenario3.csv`

**Purpose**: Contains campaign transactions with campaign_ids that don't exist in `dim_campaign` yet.

**Contents**:
- 10 transactions
- 3 new campaign_ids: `NEW_CAMPAIGN_001`, `NEW_CAMPAIGN_002`, `NEW_CAMPAIGN_003`
- All transactions reference orders that will be created in `order_data_scenario3.csv`
- Mix of availed (1) and not availed (0) values

**Expected Behavior**:
- When loaded, these transactions should be linked to the "Unknown" campaign
- Dashboard should show "Unknown Campaign" for these transactions

### 2. Campaign Dimension Data (Late-Arriving)

**File**: `data/Marketing Department/campaign_data_scenario3.csv`

**Purpose**: Contains the campaign dimension data that arrives after the transactional data.

**Contents**:
- 3 campaigns matching the campaign_ids in transactional data:
  - `NEW_CAMPAIGN_001`: Spring Sale 2024 (15% discount)
  - `NEW_CAMPAIGN_002`: Summer Flash Sale (20% discount)
  - `NEW_CAMPAIGN_003`: Back to School (10% discount)

**Expected Behavior**:
- When loaded, these campaigns should be inserted into `dim_campaign`
- Existing `fact_campaign_transactions` rows should be updated from Unknown to actual campaigns
- Dashboard should show the actual campaign names instead of "Unknown Campaign"

### 3. Order Data (Prerequisites)

**File**: `data/Operations Department/order_data_scenario3.csv`

**Purpose**: Contains order data that must exist before campaign transactions can be loaded.

**Contents**:
- 10 orders matching the order_ids in transactional campaign data
- Orders use existing user_ids, staff_ids (should exist in dimensions)
- All orders dated 2024-01-15 to 2024-01-17

**Note**: Ensure these user_ids and staff_ids exist in `dim_user` and `dim_staff` before running the test.

## Testing Steps

### Step 1: Initial State
1. **Check `dim_campaign`**: Should only have existing campaigns (or Unknown campaign if already created)
2. **Show test files**: Display contents of `transactional_campaign_data_scenario3.csv` and `order_data_scenario3.csv`

### Step 2: Load Orders First
1. Copy `order_data_scenario3.csv` to Operations Department folder (if not already there)
2. Run ETL pipeline to load orders into `fact_orders`
3. Verify: `SELECT COUNT(*) FROM fact_orders WHERE order_id LIKE 'scenario3-%'` should return 10

### Step 3: Load Campaign Transactions (Missing Campaigns)
1. Copy `transactional_campaign_data_scenario3.csv` to Marketing Department folder
2. Run ETL pipeline to load campaign transactions
3. **Verify `dim_campaign`**: Should now have "Unknown Campaign" entry
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
   - All rows should show `campaign_id = 'UNKNOWN'` and `campaign_name = 'Unknown Campaign'`

5. **Check Dashboard**: Should show "Unknown Campaign" for these transactions

### Step 4: Load Campaign Dimension (Late-Arriving)
1. Copy `campaign_data_scenario3.csv` to Marketing Department folder
2. Run ETL pipeline to load campaign dimensions
3. **Verify `dim_campaign`**:
   ```sql
   SELECT campaign_id, campaign_name, discount
   FROM dim_campaign
   WHERE campaign_id LIKE 'NEW_CAMPAIGN%'
   ORDER BY campaign_id;
   ```
   - Should show 3 new campaigns

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
   ORDER BY fct.order_id;
   ```
   - Rows should now show actual campaign_ids (NEW_CAMPAIGN_001, NEW_CAMPAIGN_002, NEW_CAMPAIGN_003)
   - Campaign names should be "Spring Sale 2024", "Summer Flash Sale", "Back to School"
   - No rows should reference "Unknown Campaign" anymore

5. **Check Dashboard**: Should now show actual campaign names instead of "Unknown Campaign"

### Step 5: Verify Update Count
```sql
-- Count how many rows were updated
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

Expected distribution:
- NEW_CAMPAIGN_001: 4 transactions
- NEW_CAMPAIGN_002: 3 transactions
- NEW_CAMPAIGN_003: 3 transactions

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
  AND fct.order_id LIKE 'scenario3-%';
```

### After Campaign Load (Step 4)
```sql
-- Check new campaigns exist
SELECT campaign_id, campaign_name, discount
FROM dim_campaign
WHERE campaign_id LIKE 'NEW_CAMPAIGN%';

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
-- Should return 0
```

## Expected Results Summary

| Step | dim_campaign | fact_campaign_transactions | Dashboard |
|------|--------------|---------------------------|-----------|
| **Before** | Only existing campaigns | N/A | N/A |
| **After Step 3** | + Unknown Campaign | 10 rows with Unknown campaign_sk | Shows "Unknown Campaign" |
| **After Step 4** | + 3 new campaigns | 10 rows updated to actual campaigns | Shows actual campaign names |

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
WHERE campaign_id LIKE 'NEW_CAMPAIGN%';
```

## Notes

- Ensure `user-001` through `user-010` exist in `dim_user` (or use existing user_ids)
- Ensure `staff-001`, `staff-002`, `staff-003` exist in `dim_staff` (or use existing staff_ids)
- The dates (2024-01-15 to 2024-01-17) should exist in `dim_date` (populated by `02_populate_dim_date.sql`)
- File format matches existing data structure (CSV with appropriate separators)

