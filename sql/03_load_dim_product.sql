-- Load Product Dimension
-- Source: Business Department - product_list.xlsx

DROP TABLE IF EXISTS dim_product CASCADE;

CREATE TABLE dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price DECIMAL(10,2)
);

-- Load from staging table (check if exists first)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_business_department_product_list_xlsx'
    ) THEN
        INSERT INTO dim_product (product_id, product_name, product_type, price)
        SELECT DISTINCT ON (COALESCE(product_id, 'UNKNOWN'))
            COALESCE(product_id, 'UNKNOWN') AS product_id,
            product_name,
            product_type,
            price
        FROM stg_business_department_product_list_xlsx
        WHERE product_id IS NOT NULL
        ORDER BY COALESCE(product_id, 'UNKNOWN')
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            product_type = EXCLUDED.product_type,
            price = EXCLUDED.price;
    ELSE
        RAISE WARNING 'Staging table stg_business_department_product_list_xlsx does not exist. Skipping load.';
    END IF;
END $$;

CREATE INDEX idx_dim_product_product_id ON dim_product(product_id);

