-- Load Fact Orders
-- Kimball Methodology: Transaction fact table
-- Combines order data from multiple staging sources
-- Dynamically discovers staging tables

-- Set lock timeout to prevent deadlocks
SET lock_timeout = '30s';

DO $$
DECLARE
    sql_query TEXT;
    order_data_query TEXT;
    merchant_data_query TEXT;
    delay_query TEXT;
    order_tbl TEXT;
    merchant_tbl TEXT;
    delay_tbl TEXT;
    order_tables TEXT[] := ARRAY[]::TEXT[];
    merchant_tables TEXT[] := ARRAY[]::TEXT[];
    delay_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find all order_data tables (any format/extension)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO order_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_operations_department_order_data%'
      AND t.table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Find all order_with_merchant_data tables
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO merchant_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_enterprise_department_order_with_merchant_data%'
      AND t.table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Find all order_delays tables (including HTML sub-tables)
    SELECT ARRAY_AGG(t.table_name ORDER BY t.table_name)
    INTO delay_tables
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
      AND t.table_name LIKE 'stg_operations_department_order_delays%';
    
    -- Build dynamic SQL query with separate CTEs
    IF array_length(order_tables, 1) > 0 OR array_length(merchant_tables, 1) > 0 THEN
        -- Build order_data CTE (has user_id, transaction_date, "estimated arrival")
        order_data_query := '';
        IF array_length(order_tables, 1) > 0 THEN
            order_data_query := 'order_data AS (';
            FOREACH order_tbl IN ARRAY order_tables
            LOOP
                order_data_query := order_data_query || format('
            SELECT 
                order_id::TEXT, 
                user_id, 
                transaction_date, 
                "estimated arrival"::INTEGER as estimated_arrival_days
            FROM %I
            WHERE order_id IS NOT NULL', order_tbl);
                
                IF order_tbl != order_tables[array_length(order_tables, 1)] THEN
                    order_data_query := order_data_query || ' UNION ALL';
                END IF;
            END LOOP;
            order_data_query := order_data_query || ')';
        ELSE
            order_data_query := 'order_data AS (SELECT NULL::TEXT as order_id, NULL::TEXT as user_id, NULL::TIMESTAMP as transaction_date, NULL::INTEGER as estimated_arrival_days WHERE FALSE)';
        END IF;
        
        -- Build order_with_merchant_data CTE (has merchant_id, staff_id)
        merchant_data_query := '';
        IF array_length(merchant_tables, 1) > 0 THEN
            merchant_data_query := ', order_merchant_data AS (';
            FOREACH merchant_tbl IN ARRAY merchant_tables
            LOOP
                merchant_data_query := merchant_data_query || format('
            SELECT 
                order_id::TEXT, 
                merchant_id, 
                staff_id
            FROM %I
            WHERE order_id IS NOT NULL', merchant_tbl);
                
                IF merchant_tbl != merchant_tables[array_length(merchant_tables, 1)] THEN
                    merchant_data_query := merchant_data_query || ' UNION ALL';
                END IF;
            END LOOP;
            merchant_data_query := merchant_data_query || ')';
        ELSE
            merchant_data_query := ', order_merchant_data AS (SELECT NULL::TEXT as order_id, NULL::TEXT as merchant_id, NULL::TEXT as staff_id WHERE FALSE)';
        END IF;
        
        sql_query := 'WITH ' || order_data_query || merchant_data_query;
        
        -- Build delay tables query dynamically
        delay_query := '';
        IF array_length(delay_tables, 1) > 0 THEN
            FOREACH delay_tbl IN ARRAY delay_tables
            LOOP
                IF delay_query != '' THEN
                    delay_query := delay_query || ' UNION ALL ';
                END IF;
                delay_query := delay_query || format('SELECT order_id::TEXT, "delay in days"::INTEGER as delay_days FROM %I', delay_tbl);
            END LOOP;
        ELSE
            delay_query := 'SELECT NULL::TEXT as order_id, NULL::INTEGER as delay_days WHERE FALSE';
        END IF;
        
        -- Complete the query - join order_data with order_with_merchant_data on order_id
        sql_query := sql_query || ',
        combined_orders AS (
            SELECT 
                od.order_id,
                od.user_id,
                om.merchant_id,
                om.staff_id,
                od.transaction_date,
                od.estimated_arrival_days
            FROM order_data od
            INNER JOIN order_merchant_data om ON od.order_id = om.order_id
            WHERE od.order_id IS NOT NULL
              AND od.user_id IS NOT NULL
              AND om.merchant_id IS NOT NULL
              AND om.staff_id IS NOT NULL
              AND od.transaction_date IS NOT NULL
        ),
        orders_with_delays AS (
            SELECT 
                co.*,
                COALESCE(od.delay_days, NULL::INTEGER) as delay_days
            FROM combined_orders co
            LEFT JOIN (' || delay_query || ') od ON co.order_id = od.order_id
        )
        INSERT INTO fact_orders (order_id, user_sk, merchant_sk, staff_sk, transaction_date_sk, estimated_arrival_days, delay_days)
        SELECT DISTINCT
            o.order_id,
            du.user_sk,
            dm.merchant_sk,
            ds.staff_sk,
            dd.date_sk,
            o.estimated_arrival_days,
            o.delay_days
        FROM orders_with_delays o
        INNER JOIN dim_user du ON o.user_id = du.user_id
        INNER JOIN dim_merchant dm ON o.merchant_id = dm.merchant_id
        INNER JOIN dim_staff ds ON o.staff_id = ds.staff_id
        INNER JOIN dim_date dd ON DATE(o.transaction_date) = dd.date
        WHERE o.order_id IS NOT NULL
        ON CONFLICT (order_id) DO NOTHING';
        
        EXECUTE sql_query;
        
        RAISE NOTICE 'Loaded fact_orders successfully';
    ELSE
        RAISE WARNING 'No staging tables found for fact_orders. Pattern: stg_operations_department_order_data%% or stg_enterprise_department_order_with_merchant_data%%';
    END IF;
EXCEPTION
    WHEN undefined_table THEN
        RAISE WARNING 'One or more staging tables do not exist. Skipping fact_orders load.';
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading fact_orders: %', SQLERRM;
END $$;

