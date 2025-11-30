-- Load Campaign Dimension
-- Source: Marketing Department - campaign_data.csv

DROP TABLE IF EXISTS dim_campaign CASCADE;

CREATE TABLE dim_campaign (
    campaign_sk SERIAL PRIMARY KEY,
    campaign_id VARCHAR(50) NOT NULL UNIQUE,
    campaign_name VARCHAR(255),
    campaign_description TEXT,
    discount DECIMAL(10,2)
);

-- Load from staging table (check if exists first)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_marketing_department_campaign_data_csv'
    ) THEN
        INSERT INTO dim_campaign (campaign_id, campaign_name, campaign_description, discount)
        SELECT DISTINCT ON (COALESCE(campaign_id, 'UNKNOWN'))
            COALESCE(campaign_id, 'UNKNOWN') AS campaign_id,
            campaign_name,
            campaign_description,
            discount
        FROM stg_marketing_department_campaign_data_csv
        WHERE campaign_id IS NOT NULL
        ORDER BY COALESCE(campaign_id, 'UNKNOWN')
        ON CONFLICT (campaign_id) DO UPDATE SET
            campaign_name = EXCLUDED.campaign_name,
            campaign_description = EXCLUDED.campaign_description,
            discount = EXCLUDED.discount;
    ELSE
        RAISE WARNING 'Staging table stg_marketing_department_campaign_data_csv does not exist. Skipping load.';
    END IF;
END $$;

CREATE INDEX idx_dim_campaign_campaign_id ON dim_campaign(campaign_id);

