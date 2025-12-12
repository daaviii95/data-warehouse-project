-- Load Staff Dimension
-- Kimball Methodology: SCD Type 1 (overwrite on conflict)
-- Dynamically discovers staging tables matching pattern: stg_enterprise_department_staff_data%

DO $$
DECLARE
    sql_query TEXT;
    tbl TEXT;
    staff_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find all staff_data staging tables (any format/extension, including HTML sub-tables)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO staff_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_enterprise_department_staff_data%';
    
    -- Build dynamic SQL query
    IF array_length(staff_tables, 1) > 0 THEN
        sql_query := 'INSERT INTO dim_staff (staff_id, name, street, state, city, country, job_level, contact_number, creation_date)
        SELECT DISTINCT
            staff_id,
            name,
            street,
            state,
            city,
            country,
            job_level,
            contact_number,
            creation_date::TIMESTAMP
        FROM (';
        
        FOREACH tbl IN ARRAY staff_tables
        LOOP
            sql_query := sql_query || format('
            SELECT 
                staff_id,
                name,
                street,
                state,
                city,
                country,
                job_level,
                contact_number,
                creation_date
            FROM %I
            WHERE staff_id IS NOT NULL', tbl);
            
            IF tbl != staff_tables[array_length(staff_tables, 1)] THEN
                sql_query := sql_query || ' UNION ALL';
            END IF;
        END LOOP;
        
        sql_query := sql_query || ') combined
        ON CONFLICT (staff_id) DO UPDATE SET
            name = EXCLUDED.name,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            job_level = EXCLUDED.job_level,
            contact_number = EXCLUDED.contact_number,
            creation_date = EXCLUDED.creation_date';
        
        EXECUTE sql_query;
        
        RAISE NOTICE 'Loaded dim_staff successfully from % table(s)', array_length(staff_tables, 1);
    ELSE
        RAISE WARNING 'No staging tables found for dim_staff. Pattern: stg_enterprise_department_staff_data%%';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading dim_staff: %', SQLERRM;
END $$;

