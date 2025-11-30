-- Clean Database Script
-- Drops all staging, fact, and dimension tables to prepare for fresh ingestion

-- Drop all fact tables
DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS fact_line_items CASCADE;
DROP TABLE IF EXISTS fact_campaign_transactions CASCADE;

-- Drop all dimension tables
DROP TABLE IF EXISTS dim_campaign CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;
DROP TABLE IF EXISTS dim_staff CASCADE;
DROP TABLE IF EXISTS dim_merchant CASCADE;
DROP TABLE IF EXISTS dim_user_job CASCADE;
DROP TABLE IF EXISTS dim_credit_card CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- Drop ingestion log table
DROP TABLE IF EXISTS ingestion_log CASCADE;

-- Drop all staging tables dynamically
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'stg_%')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Dropped table: %', r.tablename;
    END LOOP;
END $$;

-- Verify cleanup
SELECT 
    'Remaining tables' as status,
    COUNT(*) as count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND (table_name LIKE 'stg_%' 
       OR table_name LIKE 'fact_%' 
       OR table_name LIKE 'dim_%'
       OR table_name = 'ingestion_log');

