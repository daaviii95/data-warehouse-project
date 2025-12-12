-- Safe re-ingestion: Truncate only order_with_merchant_data staging table
-- This preserves all other staging data while allowing re-ingestion with staff_id
-- Run this BEFORE re-running the ingest_enterprise_department task

-- Truncate only the order_with_merchant_data table (preserves all other staging data)
TRUNCATE TABLE stg_enterprise_department_order_with_merchant_data;

-- Note: After running this, re-run the ingest_enterprise_department task
-- to reload order_with_merchant_data files with staff_id populated
