-- Load User Dimension
-- Source: Customer Management Department - user_data.json

DROP TABLE IF EXISTS dim_user CASCADE;

CREATE TABLE dim_user (
    user_sk SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    birthdate DATE,
    gender VARCHAR(20),
    device_address VARCHAR(255),
    user_type VARCHAR(50),
    creation_date TIMESTAMP
);

-- Load from staging table (check if exists first)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_customer_management_department_user_data_json'
    ) THEN
        INSERT INTO dim_user (user_id, name, street, state, city, country, birthdate, gender, device_address, user_type, creation_date)
        SELECT DISTINCT ON (COALESCE(user_id, 'UNKNOWN'))
            COALESCE(user_id, 'UNKNOWN') AS user_id,
            name,
            street,
            state,
            city,
            country,
            birthdate::DATE AS birthdate,
            gender,
            device_address,
            user_type,
            creation_date::TIMESTAMP AS creation_date
        FROM stg_customer_management_department_user_data_json
        WHERE user_id IS NOT NULL
        ORDER BY COALESCE(user_id, 'UNKNOWN'), 
                 CASE WHEN creation_date IS NOT NULL THEN creation_date::TIMESTAMP ELSE '1970-01-01'::TIMESTAMP END DESC
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
            creation_date = EXCLUDED.creation_date;
    ELSE
        RAISE WARNING 'Staging table stg_customer_management_department_user_data_json does not exist. Skipping load.';
    END IF;
END $$;

CREATE INDEX idx_dim_user_user_id ON dim_user(user_id);

