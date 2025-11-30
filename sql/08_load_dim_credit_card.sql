-- Load Credit Card Dimension
-- Source: Customer Management Department - user_credit_card.pickle

DROP TABLE IF EXISTS dim_credit_card CASCADE;

CREATE TABLE dim_credit_card (
    credit_card_sk SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    credit_card_number VARCHAR(50),
    issuing_bank VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id)
);

-- Load from staging table (check if exists first)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_customer_management_department_user_credit_card_pickle'
    ) THEN
        INSERT INTO dim_credit_card (user_id, name, credit_card_number, issuing_bank)
        SELECT DISTINCT
            user_id,
            name,
            credit_card_number,
            issuing_bank
        FROM stg_customer_management_department_user_credit_card_pickle
        WHERE user_id IS NOT NULL
          AND user_id IN (SELECT user_id FROM dim_user);
    ELSE
        RAISE WARNING 'Staging table stg_customer_management_department_user_credit_card_pickle does not exist. Skipping load.';
    END IF;
END $$;

CREATE INDEX idx_dim_credit_card_user_id ON dim_credit_card(user_id);

