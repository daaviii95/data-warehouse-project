-- Load Credit Card Dimension (Outrigger)
-- Kimball Methodology: SCD Type 1 (overwrite on conflict)
-- Dynamically discovers staging tables matching pattern: stg_customer_management_department_user_credit_card%

DO $$
DECLARE
    sql_query TEXT;
    tbl TEXT;
    credit_card_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find all user_credit_card staging tables (any format/extension)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO credit_card_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_customer_management_department_user_credit_card%'
      AND t.table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Build dynamic SQL query
    IF array_length(credit_card_tables, 1) > 0 THEN
        sql_query := 'INSERT INTO dim_credit_card (user_id, name, credit_card_number, issuing_bank)
        SELECT DISTINCT
            cc.user_id,
            cc.name,
            cc.credit_card_number,
            cc.issuing_bank
        FROM (';
        
        FOREACH tbl IN ARRAY credit_card_tables
        LOOP
            sql_query := sql_query || format('
            SELECT 
                user_id,
                name,
                credit_card_number,
                issuing_bank
            FROM %I
            WHERE user_id IS NOT NULL', tbl);
            
            IF tbl != credit_card_tables[array_length(credit_card_tables, 1)] THEN
                sql_query := sql_query || ' UNION ALL';
            END IF;
        END LOOP;
        
        sql_query := sql_query || ') cc
        INNER JOIN dim_user du ON cc.user_id = du.user_id
        ON CONFLICT DO NOTHING';  -- User can have multiple credit cards
        
        EXECUTE sql_query;
        
        RAISE NOTICE 'Loaded dim_credit_card successfully from % table(s)', array_length(credit_card_tables, 1);
    ELSE
        RAISE WARNING 'No staging tables found for dim_credit_card. Pattern: stg_customer_management_department_user_credit_card%%';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading dim_credit_card: %', SQLERRM;
END $$;

