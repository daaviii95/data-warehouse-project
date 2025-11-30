-- Load User Job Dimension
-- Source: Customer Management Department - user_job.csv

DROP TABLE IF EXISTS dim_user_job CASCADE;

CREATE TABLE dim_user_job (
    user_job_sk SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    job_title VARCHAR(255),
    job_level VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id)
);

-- Load from staging table (check if exists first)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_customer_management_department_user_job_csv'
    ) THEN
        INSERT INTO dim_user_job (user_id, job_title, job_level)
        SELECT DISTINCT
            user_id,
            job_title,
            job_level
        FROM stg_customer_management_department_user_job_csv
        WHERE user_id IS NOT NULL
          AND user_id IN (SELECT user_id FROM dim_user);
    ELSE
        RAISE WARNING 'Staging table stg_customer_management_department_user_job_csv does not exist. Skipping load.';
    END IF;
END $$;

CREATE INDEX idx_dim_user_job_user_id ON dim_user_job(user_id);

