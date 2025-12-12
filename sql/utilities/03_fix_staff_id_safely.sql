-- Safe Fix for staff_id in order_with_merchant_data staging table
-- This script preserves all existing data in other staging tables
-- Only affects stg_enterprise_department_order_with_merchant_data

-- Step 1: Add staff_id column if it doesn't exist (safe, no data loss)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_enterprise_department_order_with_merchant_data' 
        AND column_name = 'staff_id'
    ) THEN
        ALTER TABLE stg_enterprise_department_order_with_merchant_data 
        ADD COLUMN staff_id VARCHAR(50);
        
        RAISE NOTICE '✓ Added staff_id column to stg_enterprise_department_order_with_merchant_data';
    ELSE
        RAISE NOTICE '✓ Column staff_id already exists';
    END IF;
END $$;

-- Step 2: Show current row count (for reference)
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO row_count 
    FROM stg_enterprise_department_order_with_merchant_data;
    RAISE NOTICE 'Current rows in staging table: %', row_count;
END $$;

-- Step 3: Truncate to prepare for re-ingestion with staff_id
-- NOTE: This only clears order_with_merchant_data, all other staging tables remain intact
-- Uncomment the line below when ready to re-ingest:
-- TRUNCATE TABLE stg_enterprise_department_order_with_merchant_data;

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Column staff_id has been added';
    RAISE NOTICE '2. Uncomment TRUNCATE line above if you want to re-ingest';
    RAISE NOTICE '3. Re-run ingest_enterprise_department task to reload data with staff_id';
    RAISE NOTICE '========================================';
END $$;
