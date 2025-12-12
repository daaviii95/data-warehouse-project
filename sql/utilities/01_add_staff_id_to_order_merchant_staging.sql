-- Migration: Add staff_id column to stg_enterprise_department_order_with_merchant_data
-- This fixes the issue where staff_id was being dropped during ingestion
-- Run this script if you don't want to drop and recreate the staging table

-- Add staff_id column if it doesn't exist
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
        
        RAISE NOTICE 'Added staff_id column to stg_enterprise_department_order_with_merchant_data';
    ELSE
        RAISE NOTICE 'Column staff_id already exists in stg_enterprise_department_order_with_merchant_data';
    END IF;
END $$;
