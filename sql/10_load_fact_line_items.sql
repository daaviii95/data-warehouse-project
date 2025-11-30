-- Load Fact Line Items Table
-- Source: Operations Department - line_item_data files (prices + products)
-- DYNAMIC: Automatically discovers all matching staging tables

DROP TABLE IF EXISTS fact_line_items CASCADE;

CREATE TABLE fact_line_items (
    order_id TEXT NOT NULL,
    product_sk INTEGER NOT NULL,
    user_sk INTEGER NOT NULL,
    merchant_sk INTEGER NOT NULL,
    staff_sk INTEGER NOT NULL,
    transaction_date_sk INTEGER NOT NULL,
    price DECIMAL(10,2),
    quantity INTEGER,
    PRIMARY KEY (order_id, product_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk),
    FOREIGN KEY (staff_sk) REFERENCES dim_staff(staff_sk),
    FOREIGN KEY (transaction_date_sk) REFERENCES dim_date(date_sk)
);

-- Dynamically combine line item data from prices and products files
DO $$
DECLARE
    sql_query TEXT;
    prices_tbl TEXT;
    products_tbl TEXT;
    prices_tables TEXT[] := ARRAY[]::TEXT[];
    products_tables TEXT[] := ARRAY[]::TEXT[];
    err_msg TEXT;
BEGIN
    -- Find all line_item_data_prices tables (any number, any format)
    SELECT ARRAY_AGG(table_name ORDER BY table_name)
    INTO prices_tables
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name LIKE 'stg_operations_department_line_item_data_prices%'
      AND table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Find all line_item_data_products tables (any number, any format)
    SELECT ARRAY_AGG(table_name ORDER BY table_name)
    INTO products_tables
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name LIKE 'stg_operations_department_line_item_data_products%'
      AND table_name NOT LIKE '%_tbl%';  -- Exclude HTML sub-tables
    
    -- Build dynamic SQL query
    IF array_length(prices_tables, 1) > 0 OR array_length(products_tables, 1) > 0 THEN
        sql_query := 'WITH line_item_prices AS (';
        
        -- Add prices tables (only has order_id, price, quantity)
        IF array_length(prices_tables, 1) > 0 THEN
            FOREACH prices_tbl IN ARRAY prices_tables
            LOOP
                sql_query := sql_query || format('
            SELECT 
                order_id, price, quantity
            FROM %I
            WHERE order_id IS NOT NULL', prices_tbl);
                
                -- Add UNION ALL if not the last table
                IF prices_tbl != prices_tables[array_length(prices_tables, 1)] THEN
                    sql_query := sql_query || ' UNION ALL';
                END IF;
            END LOOP;
        ELSE
            -- If no prices tables, create empty CTE
            sql_query := sql_query || 'SELECT NULL::TEXT as order_id, NULL::DECIMAL as price, NULL::INTEGER as quantity WHERE FALSE';
        END IF;
        
        sql_query := sql_query || '
        ),
        line_item_products AS (';
        
        -- Add products tables (only has order_id, product_id, product_name)
        IF array_length(products_tables, 1) > 0 THEN
            FOREACH products_tbl IN ARRAY products_tables
            LOOP
                sql_query := sql_query || format('
            SELECT 
                order_id, product_id
            FROM %I
            WHERE order_id IS NOT NULL AND product_id IS NOT NULL', products_tbl);
                
                -- Add UNION ALL if not the last table
                IF products_tbl != products_tables[array_length(products_tables, 1)] THEN
                    sql_query := sql_query || ' UNION ALL';
                END IF;
            END LOOP;
        ELSE
            -- If no products tables, create empty CTE
            sql_query := sql_query || 'SELECT NULL::TEXT as order_id, NULL::TEXT as product_id WHERE FALSE';
        END IF;
        
        -- Complete the query - join prices and products, then get dimension keys from fact_orders
        sql_query := sql_query || '
        ),
        combined_line_items AS (
            SELECT 
                pr.order_id,
                pr.product_id,
                p.price,
                p.quantity
            FROM line_item_products pr
            INNER JOIN line_item_prices p ON pr.order_id = p.order_id
            WHERE pr.order_id IS NOT NULL
              AND pr.product_id IS NOT NULL
        )
        INSERT INTO fact_line_items (order_id, product_sk, user_sk, merchant_sk, staff_sk, transaction_date_sk, price, quantity)
        SELECT DISTINCT
            cli.order_id,
            dp.product_sk,
            fo.user_sk,
            fo.merchant_sk,
            fo.staff_sk,
            fo.transaction_date_sk,
            cli.price,
            cli.quantity
        FROM combined_line_items cli
        INNER JOIN dim_product dp ON cli.product_id = dp.product_id
        INNER JOIN fact_orders fo ON cli.order_id = fo.order_id
        WHERE cli.order_id IS NOT NULL
          AND cli.product_id IS NOT NULL
        ON CONFLICT (order_id, product_sk) DO NOTHING';
        
        EXECUTE sql_query;
        
        -- Log success (using simple message to avoid format string issues)
        RAISE NOTICE 'Loaded fact_line_items successfully';
    ELSE
        RAISE WARNING 'No staging tables found for fact_line_items. Pattern: stg_operations_department_line_item_data_prices%% or stg_operations_department_line_item_data_products%%';
    END IF;
EXCEPTION
    WHEN undefined_table THEN
        RAISE WARNING 'One or more staging tables do not exist. Skipping fact_line_items load.';
    WHEN OTHERS THEN
        err_msg := 'Error loading fact_line_items: ' || SQLERRM;
        RAISE WARNING '%', err_msg;
END $$;

CREATE INDEX idx_fact_line_items_order_id ON fact_line_items(order_id);
CREATE INDEX idx_fact_line_items_product_sk ON fact_line_items(product_sk);
CREATE INDEX idx_fact_line_items_user_sk ON fact_line_items(user_sk);
CREATE INDEX idx_fact_line_items_transaction_date_sk ON fact_line_items(transaction_date_sk);
