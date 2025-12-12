-- Load Product Dimension
-- Kimball Methodology: SCD Type 1 (overwrite on conflict)
-- Dynamically discovers staging tables matching pattern: stg_business_department_product_list%

DO $$
DECLARE
    sql_query TEXT;
    tbl TEXT;
    product_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find all product_list staging tables (any format/extension)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO product_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_business_department_product_list%'
      AND t.table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Build dynamic SQL query
    IF array_length(product_tables, 1) > 0 THEN
        sql_query := 'INSERT INTO dim_product (product_id, product_name, product_type, price)
        SELECT DISTINCT
            product_id,
            product_name,
            product_type,
            price::DECIMAL(10,2)
        FROM (';
        
        FOREACH tbl IN ARRAY product_tables
        LOOP
            sql_query := sql_query || format('
            SELECT 
                product_id,
                product_name,
                product_type,
                price
            FROM %I
            WHERE product_id IS NOT NULL', tbl);
            
            IF tbl != product_tables[array_length(product_tables, 1)] THEN
                sql_query := sql_query || ' UNION ALL';
            END IF;
        END LOOP;
        
        sql_query := sql_query || ') combined
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            product_type = EXCLUDED.product_type,
            price = EXCLUDED.price';
        
        EXECUTE sql_query;
        
        RAISE NOTICE 'Loaded dim_product successfully from % table(s)', array_length(product_tables, 1);
    ELSE
        RAISE WARNING 'No staging tables found for dim_product. Pattern: stg_business_department_product_list%%';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading dim_product: %', SQLERRM;
END $$;

