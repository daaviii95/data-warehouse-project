-- Load Merchant Dimension
-- Source: Enterprise Department - merchant_data.html

DROP TABLE IF EXISTS dim_merchant CASCADE;

CREATE TABLE dim_merchant (
    merchant_sk SERIAL PRIMARY KEY,
    merchant_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP
);

-- Load from staging table (HTML may have multiple tables)
DO $$
BEGIN
    -- Try main table first
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_enterprise_department_merchant_data_html'
    ) THEN
        INSERT INTO dim_merchant (merchant_id, name, street, state, city, country, contact_number, creation_date)
        SELECT DISTINCT ON (COALESCE(merchant_id, 'UNKNOWN'))
            COALESCE(merchant_id, 'UNKNOWN') AS merchant_id,
            name,
            street,
            state,
            city,
            country,
            contact_number,
            creation_date::TIMESTAMP AS creation_date
        FROM stg_enterprise_department_merchant_data_html
        WHERE merchant_id IS NOT NULL
        ORDER BY COALESCE(merchant_id, 'UNKNOWN'),
                 CASE WHEN creation_date IS NOT NULL THEN creation_date::TIMESTAMP ELSE '1970-01-01'::TIMESTAMP END DESC
        ON CONFLICT (merchant_id) DO UPDATE SET
            name = EXCLUDED.name,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            contact_number = EXCLUDED.contact_number,
            creation_date = EXCLUDED.creation_date;
    END IF;

    -- If merchant_data has multiple tables, try alternative table names
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_enterprise_department_merchant_data_html_tbl1'
    ) THEN
        INSERT INTO dim_merchant (merchant_id, name, street, state, city, country, contact_number, creation_date)
        SELECT DISTINCT ON (COALESCE(merchant_id, 'UNKNOWN'))
            COALESCE(merchant_id, 'UNKNOWN') AS merchant_id,
            name,
            street,
            state,
            city,
            country,
            contact_number,
            creation_date::TIMESTAMP AS creation_date
        FROM stg_enterprise_department_merchant_data_html_tbl1
        WHERE merchant_id IS NOT NULL
        ORDER BY COALESCE(merchant_id, 'UNKNOWN'),
                 CASE WHEN creation_date IS NOT NULL THEN creation_date::TIMESTAMP ELSE '1970-01-01'::TIMESTAMP END DESC
        ON CONFLICT (merchant_id) DO NOTHING;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('stg_enterprise_department_merchant_data_html', 'stg_enterprise_department_merchant_data_html_tbl1')
    ) THEN
        RAISE WARNING 'Staging table stg_enterprise_department_merchant_data_html does not exist. Skipping load.';
    END IF;
END $$;

CREATE INDEX idx_dim_merchant_merchant_id ON dim_merchant(merchant_id);

