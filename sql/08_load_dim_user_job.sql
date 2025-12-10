-- Load User Job Dimension (Outrigger)
-- Kimball Methodology: SCD Type 1 (overwrite on conflict)
-- Dynamically discovers staging tables matching pattern: stg_customer_management_department_user_job%

DO $$
DECLARE
    sql_query TEXT;
    tbl TEXT;
    user_job_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find all user_job staging tables (any format/extension)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO user_job_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_customer_management_department_user_job%'
      AND t.table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Build dynamic SQL query
    IF array_length(user_job_tables, 1) > 0 THEN
        sql_query := 'INSERT INTO dim_user_job (user_id, job_title, job_level)
        SELECT DISTINCT
            uj.user_id,
            uj.job_title,
            uj.job_level
        FROM (';
        
        FOREACH tbl IN ARRAY user_job_tables
        LOOP
            sql_query := sql_query || format('
            SELECT 
                user_id,
                job_title,
                job_level
            FROM %I
            WHERE user_id IS NOT NULL', tbl);
            
            IF tbl != user_job_tables[array_length(user_job_tables, 1)] THEN
                sql_query := sql_query || ' UNION ALL';
            END IF;
        END LOOP;
        
        sql_query := sql_query || ') uj
        INNER JOIN dim_user du ON uj.user_id = du.user_id
        ON CONFLICT DO NOTHING';  -- User job can have multiple records per user
        
        EXECUTE sql_query;
        
        RAISE NOTICE 'Loaded dim_user_job successfully from % table(s)', array_length(user_job_tables, 1);
    ELSE
        RAISE WARNING 'No staging tables found for dim_user_job. Pattern: stg_customer_management_department_user_job%%';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading dim_user_job: %', SQLERRM;
END $$;

