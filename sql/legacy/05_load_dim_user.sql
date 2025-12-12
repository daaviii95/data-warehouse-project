-- Load User Dimension
-- Kimball Methodology: SCD Type 1 (overwrite on conflict)
-- Dynamically discovers staging tables matching pattern: stg_customer_management_department_user_data%

DO $$
DECLARE
    sql_query TEXT;
    tbl TEXT;
    user_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find all user_data staging tables (any format/extension)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO user_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_customer_management_department_user_data%'
      AND t.table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Build dynamic SQL query
    IF array_length(user_tables, 1) > 0 THEN
        sql_query := 'INSERT INTO dim_user (user_id, name, street, state, city, country, birthdate, gender, device_address, user_type, creation_date)
        SELECT DISTINCT
            user_id,
            name,
            street,
            state,
            city,
            country,
            birthdate::DATE,
            gender,
            device_address,
            user_type,
            creation_date::TIMESTAMP
        FROM (';
        
        FOREACH tbl IN ARRAY user_tables
        LOOP
            sql_query := sql_query || format('
            SELECT 
                user_id,
                name,
                street,
                state,
                city,
                country,
                birthdate,
                gender,
                device_address,
                user_type,
                creation_date
            FROM %I
            WHERE user_id IS NOT NULL', tbl);
            
            IF tbl != user_tables[array_length(user_tables, 1)] THEN
                sql_query := sql_query || ' UNION ALL';
            END IF;
        END LOOP;
        
        sql_query := sql_query || ') combined
        ON CONFLICT (user_id) DO UPDATE SET
            name = EXCLUDED.name,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            birthdate = EXCLUDED.birthdate,
            gender = EXCLUDED.gender,
            device_address = EXCLUDED.device_address,
            user_type = EXCLUDED.user_type,
            creation_date = EXCLUDED.creation_date';
        
        EXECUTE sql_query;
        
        RAISE NOTICE 'Loaded dim_user successfully from % table(s)', array_length(user_tables, 1);
    ELSE
        RAISE WARNING 'No staging tables found for dim_user. Pattern: stg_customer_management_department_user_data%%';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading dim_user: %', SQLERRM;
END $$;

