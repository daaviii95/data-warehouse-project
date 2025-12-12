-- Load Merchant Dimension
-- Kimball Methodology: SCD Type 1 (overwrite on conflict)
-- Dynamically discovers staging tables matching pattern: stg_enterprise_department_merchant_data%

DO $$
DECLARE
    sql_query TEXT;
    tbl TEXT;
    merchant_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find all merchant_data staging tables (any format/extension, including HTML sub-tables)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO merchant_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_enterprise_department_merchant_data%';
    
    -- Build dynamic SQL query
    IF array_length(merchant_tables, 1) > 0 THEN
        sql_query := 'INSERT INTO dim_merchant (merchant_id, name, street, state, city, country, contact_number, creation_date)
        SELECT DISTINCT
            merchant_id,
            name,
            street,
            state,
            city,
            country,
            contact_number,
            creation_date::TIMESTAMP
        FROM (';
        
        FOREACH tbl IN ARRAY merchant_tables
        LOOP
            sql_query := sql_query || format('
            SELECT 
                merchant_id,
                name,
                street,
                state,
                city,
                country,
                contact_number,
                creation_date
            FROM %I
            WHERE merchant_id IS NOT NULL', tbl);
            
            IF tbl != merchant_tables[array_length(merchant_tables, 1)] THEN
                sql_query := sql_query || ' UNION ALL';
            END IF;
        END LOOP;
        
        sql_query := sql_query || ') combined
        ON CONFLICT (merchant_id) DO UPDATE SET
            name = EXCLUDED.name,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            contact_number = EXCLUDED.contact_number,
            creation_date = EXCLUDED.creation_date';
        
        EXECUTE sql_query;
        
        RAISE NOTICE 'Loaded dim_merchant successfully from % table(s)', array_length(merchant_tables, 1);
    ELSE
        RAISE WARNING 'No staging tables found for dim_merchant. Pattern: stg_enterprise_department_merchant_data%%';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading dim_merchant: %', SQLERRM;
END $$;

