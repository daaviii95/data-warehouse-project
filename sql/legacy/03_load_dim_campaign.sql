-- Load Campaign Dimension
-- Kimball Methodology: SCD Type 1 (overwrite on conflict)
-- Dynamically discovers staging tables matching pattern: stg_marketing_department_campaign_data%

DO $$
DECLARE
    sql_query TEXT;
    tbl TEXT;
    campaign_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find all campaign_data staging tables (any format/extension)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO campaign_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_marketing_department_campaign_data%'
      AND t.table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Build dynamic SQL query
    IF array_length(campaign_tables, 1) > 0 THEN
        sql_query := 'INSERT INTO dim_campaign (campaign_id, campaign_name, campaign_description, discount)
        SELECT DISTINCT
            campaign_id,
            campaign_name,
            campaign_description,
            discount::DECIMAL(10,2)
        FROM (';
        
        FOREACH tbl IN ARRAY campaign_tables
        LOOP
            sql_query := sql_query || format('
            SELECT 
                campaign_id,
                campaign_name,
                campaign_description,
                discount
            FROM %I
            WHERE campaign_id IS NOT NULL', tbl);
            
            IF tbl != campaign_tables[array_length(campaign_tables, 1)] THEN
                sql_query := sql_query || ' UNION ALL';
            END IF;
        END LOOP;
        
        sql_query := sql_query || ') combined
        ON CONFLICT (campaign_id) DO UPDATE SET
            campaign_name = EXCLUDED.campaign_name,
            campaign_description = EXCLUDED.campaign_description,
            discount = EXCLUDED.discount';
        
        EXECUTE sql_query;
        
        RAISE NOTICE 'Loaded dim_campaign successfully from % table(s)', array_length(campaign_tables, 1);
    ELSE
        RAISE WARNING 'No staging tables found for dim_campaign. Pattern: stg_marketing_department_campaign_data%%';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading dim_campaign: %', SQLERRM;
END $$;

