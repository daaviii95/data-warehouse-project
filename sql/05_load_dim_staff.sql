-- Load Staff Dimension
-- Source: Enterprise Department - staff_data.html

DROP TABLE IF EXISTS dim_staff CASCADE;

CREATE TABLE dim_staff (
    staff_sk SERIAL PRIMARY KEY,
    staff_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    job_level VARCHAR(50),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP
);

-- Load from staging table (HTML may have multiple tables, check for staff_id or similar)
DO $$
BEGIN
    -- Try main table first
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_enterprise_department_staff_data_html'
    ) THEN
        INSERT INTO dim_staff (staff_id, name, street, state, city, country, job_level, contact_number, creation_date)
        SELECT DISTINCT ON (COALESCE(staff_id, 'UNKNOWN'))
            COALESCE(staff_id, 'UNKNOWN') AS staff_id,
            name,
            street,
            state,
            city,
            country,
            job_level,
            contact_number,
            creation_date::TIMESTAMP AS creation_date
        FROM stg_enterprise_department_staff_data_html
        WHERE staff_id IS NOT NULL
        ORDER BY COALESCE(staff_id, 'UNKNOWN'),
                 CASE WHEN creation_date IS NOT NULL THEN creation_date::TIMESTAMP ELSE '1970-01-01'::TIMESTAMP END DESC
        ON CONFLICT (staff_id) DO UPDATE SET
            name = EXCLUDED.name,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            job_level = EXCLUDED.job_level,
            contact_number = EXCLUDED.contact_number,
            creation_date = EXCLUDED.creation_date;
    END IF;

    -- If staff_data has multiple tables, try alternative table names
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_enterprise_department_staff_data_html_tbl1'
    ) THEN
        INSERT INTO dim_staff (staff_id, name, street, state, city, country, job_level, contact_number, creation_date)
        SELECT DISTINCT ON (COALESCE(staff_id, 'UNKNOWN'))
            COALESCE(staff_id, 'UNKNOWN') AS staff_id,
            name,
            street,
            state,
            city,
            country,
            job_level,
            contact_number,
            creation_date::TIMESTAMP AS creation_date
        FROM stg_enterprise_department_staff_data_html_tbl1
        WHERE staff_id IS NOT NULL
        ORDER BY COALESCE(staff_id, 'UNKNOWN'),
                 CASE WHEN creation_date IS NOT NULL THEN creation_date::TIMESTAMP ELSE '1970-01-01'::TIMESTAMP END DESC
        ON CONFLICT (staff_id) DO NOTHING;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('stg_enterprise_department_staff_data_html', 'stg_enterprise_department_staff_data_html_tbl1')
    ) THEN
        RAISE WARNING 'Staging table stg_enterprise_department_staff_data_html does not exist. Skipping load.';
    END IF;
END $$;

CREATE INDEX idx_dim_staff_staff_id ON dim_staff(staff_id);

