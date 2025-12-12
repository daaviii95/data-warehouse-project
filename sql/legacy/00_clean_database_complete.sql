-- Complete Database Cleanup
-- Drops ALL tables, views, materialized views, and sequences
-- Use this to start completely fresh

-- Drop all materialized views first (they depend on tables)
DROP MATERIALIZED VIEW IF EXISTS mv_products CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_customers CASCADE;

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
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_user_job CASCADE;
DROP TABLE IF EXISTS dim_credit_card CASCADE;

-- Drop all staging tables (dynamic)
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'stg_%') 
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;

-- Drop ingestion log
DROP TABLE IF EXISTS ingestion_log CASCADE;

-- Drop all sequences (they're auto-dropped with tables, but just in case)
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') 
    LOOP
        EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r.sequence_name) || ' CASCADE';
    END LOOP;
END $$;

DO $$
BEGIN
    RAISE NOTICE 'Database cleaned. All tables, views, and sequences dropped.';
END $$;

