# Scenario 3 Implementation: Late & Missing Campaign Data

## Overview

Scenario 3 demonstrates how the system handles optional or late-arriving campaign data. The system now supports:

1. **Unknown Campaign Handling**: Orders with missing or non-existent campaign codes are mapped to an "Unknown" campaign entry
2. **Late-Arriving Campaigns**: When campaign dimension data arrives later, fact rows are automatically updated from "Unknown" to the actual campaign
3. **Business Rules**: Clear distinction between "Unknown" (data not yet available) and "Not Applicable" (no campaign used)

## Implementation Details

### 1. Unknown Campaign Creation

The system automatically creates an "Unknown" campaign entry in `dim_campaign`:

- **campaign_id**: `'UNKNOWN'`
- **campaign_name**: `'Unknown Campaign'`
- **campaign_description**: `'Placeholder for orders with missing or late-arriving campaign data'`
- **discount**: `0.0`

This entry is created before any fact loading occurs, ensuring it's always available.

**Function**: `load_dim.ensure_unknown_campaign()`

### 2. Fact Loading with Missing Campaigns

When loading `fact_campaign_transactions`, if a `campaign_id` doesn't exist in `dim_campaign`:

1. The system checks if the campaign exists in the dimension
2. If not found, it uses the "Unknown" campaign's `campaign_sk`
3. The fact row is inserted with the Unknown campaign reference
4. Logging indicates when Unknown campaign is used

**Modified Functions**:
- `load_fact.load_fact_campaign_transactions()` (in `load_fact.py`)
- `load_fact_campaign_transactions()` (in `etl_pipeline_python.py`)

### 3. Late-Arriving Campaign Updates

When campaign dimension data is loaded after facts have been created:

1. New campaigns are inserted into `dim_campaign` via `load_dim_campaign()`
2. The system automatically updates `fact_campaign_transactions` rows that:
   - Currently reference the "Unknown" campaign
   - Have a matching `campaign_id` in the transactional data
   - Now have an actual campaign entry in `dim_campaign`
3. Updated rows are changed from Unknown `campaign_sk` to the actual campaign's `campaign_sk`

**Function**: `load_dim.update_campaign_transactions_for_new_campaigns()`

## Business Rules

### Unknown vs Not Applicable

**Unknown Campaign**:
- Used when a `campaign_id` is referenced in transactional data but doesn't exist in `dim_campaign`
- Indicates **late-arriving dimension data** - the campaign exists but hasn't been loaded yet
- Fact rows are created with Unknown campaign_sk and updated when actual campaign data arrives
- **Business Meaning**: "This order referenced a campaign, but we don't have campaign details yet"

**Not Applicable**:
- Used when an order has no campaign reference (NULL or empty `campaign_id`)
- Indicates **optional dimension** - the order simply didn't use a campaign
- These orders may not appear in `fact_campaign_transactions` at all, or may have NULL campaign_sk
- **Business Meaning**: "This order did not use any campaign"

### Late-Arriving Dimensions

**Process**:
1. **Initial Load**: Facts reference Unknown campaign when campaign_id doesn't exist
2. **Dimension Load**: Campaign dimension file is loaded, creating actual campaign entries
3. **Automatic Update**: System updates fact rows from Unknown to actual campaign
4. **Dashboard Update**: Dashboard reflects the updated campaign assignments

**Benefits**:
- No data loss: Orders with missing campaigns are still captured
- Automatic correction: Facts are updated when dimension data arrives
- Historical accuracy: Final state shows correct campaign assignments

## Demonstration Steps

### Step 1: Initial State
- Show `dim_campaign` table (should have Unknown campaign)
- Show test CSV file with orders referencing non-existent campaign codes

### Step 2: Load Orders with Missing Campaigns
- Load transactional campaign data with campaign_ids that don't exist in `dim_campaign`
- Show `fact_campaign_transactions` - rows should reference Unknown campaign_sk
- Show dashboard - Unknown campaign should appear in breakdown

### Step 3: Load Campaign Dimension
- Load campaign dimension file with the previously missing campaigns
- Show `dim_campaign` - new campaigns should be added
- Show `fact_campaign_transactions` - rows should be updated from Unknown to actual campaigns
- Show dashboard - Unknown campaign count should decrease, actual campaigns should appear

## SQL Queries for Verification

### Check Unknown Campaign
```sql
SELECT * FROM dim_campaign WHERE campaign_id = 'UNKNOWN';
```

### Check Facts Using Unknown Campaign
```sql
SELECT 
    fct.*,
    dc.campaign_id,
    dc.campaign_name
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
WHERE dc.campaign_id = 'UNKNOWN';
```

### Check Campaign Distribution
```sql
SELECT 
    dc.campaign_id,
    dc.campaign_name,
    COUNT(*) as transaction_count
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
GROUP BY dc.campaign_id, dc.campaign_name
ORDER BY transaction_count DESC;
```

### Verify Updates After Campaign Load
```sql
-- Before campaign load: Count Unknown references
SELECT COUNT(*) as unknown_count
FROM fact_campaign_transactions fct
INNER JOIN dim_campaign dc ON fct.campaign_sk = dc.campaign_sk
WHERE dc.campaign_id = 'UNKNOWN';

-- After campaign load: Should see fewer Unknown references
-- (Run same query again to see decrease)
```

## Integration Points

### Main Pipeline (`etl_pipeline_python.py`)
- Unknown campaign is ensured before dimension loading
- Campaign transactions are updated after dimension loading

### Airflow Workflow (`shopzada_etl_pipeline.py`)
- Unknown campaign is ensured in `run_load_dim()` task
- Campaign transactions are updated in `run_load_dim()` task after campaigns are loaded

### Load Functions
- `load_dim.ensure_unknown_campaign()`: Creates/verifies Unknown campaign
- `load_dim.update_campaign_transactions_for_new_campaigns()`: Updates facts when campaigns arrive
- `load_fact.load_fact_campaign_transactions()`: Uses Unknown campaign for missing campaign_ids

## Testing

### Test Case 1: Missing Campaign on Initial Load
1. Ensure `dim_campaign` has only Unknown campaign
2. Load transactional campaign data with `campaign_id = 'NEW_CAMPAIGN_001'`
3. Verify: `fact_campaign_transactions` has rows with Unknown campaign_sk
4. Verify: Dashboard shows Unknown campaign

### Test Case 2: Late-Arriving Campaign
1. After Test Case 1, load campaign dimension with `campaign_id = 'NEW_CAMPAIGN_001'`
2. Verify: `dim_campaign` now has NEW_CAMPAIGN_001
3. Verify: `fact_campaign_transactions` rows updated from Unknown to NEW_CAMPAIGN_001
4. Verify: Dashboard shows NEW_CAMPAIGN_001 instead of Unknown

### Test Case 3: Multiple Missing Campaigns
1. Load transactional data with multiple missing campaign_ids
2. Load campaign dimension with all missing campaigns
3. Verify: All fact rows are updated correctly
4. Verify: Unknown campaign count decreases appropriately

## Logging

The system logs:
- Unknown campaign creation/verification
- When Unknown campaign is used for missing campaign_ids
- Number of fact rows updated when campaigns arrive
- Campaign mapping statistics

## Related Documentation

- [Scenario 2 Analysis](SCENARIO2_ANALYSIS.md) - Similar late-arriving dimension handling for users/products
- [Testing Guide](TESTING_GUIDE.md) - General testing procedures
- [Workflow Documentation](../core/WORKFLOW.md) - Complete ETL workflow
