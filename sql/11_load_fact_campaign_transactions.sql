-- Load Fact Campaign Transactions Table
-- Source: Marketing Department - transactional_campaign_data.csv

DROP TABLE IF EXISTS fact_campaign_transactions CASCADE;

CREATE TABLE fact_campaign_transactions (
    order_id TEXT NOT NULL,
    campaign_sk INTEGER NOT NULL,
    user_sk INTEGER NOT NULL,
    merchant_sk INTEGER NOT NULL,
    transaction_date_sk INTEGER NOT NULL,
    availed INTEGER,
    PRIMARY KEY (order_id, campaign_sk),
    FOREIGN KEY (campaign_sk) REFERENCES dim_campaign(campaign_sk),
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk),
    FOREIGN KEY (transaction_date_sk) REFERENCES dim_date(date_sk)
);

-- Load from staging table (check if exists first)
-- Note: Requires fact_orders to be loaded first to get user_sk and merchant_sk from order_id
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_marketing_department_transactional_campaign_data_csv'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'fact_orders'
        ) THEN
            INSERT INTO fact_campaign_transactions (order_id, campaign_sk, user_sk, merchant_sk, transaction_date_sk, availed)
            SELECT DISTINCT
                tcd.order_id::TEXT,
                dc.campaign_sk,
                fo.user_sk,
                fo.merchant_sk,
                dd.date_sk,
                CASE 
                    WHEN tcd.availed::TEXT IN ('1', 'true', 'True', 'TRUE') OR tcd.availed = 1 THEN 1 
                    ELSE 0 
                END AS availed
            FROM stg_marketing_department_transactional_campaign_data_csv tcd
            INNER JOIN dim_campaign dc ON tcd.campaign_id = dc.campaign_id
            INNER JOIN dim_date dd ON DATE(tcd.transaction_date) = dd.date
            INNER JOIN fact_orders fo ON tcd.order_id::TEXT = fo.order_id
            WHERE tcd.order_id IS NOT NULL
              AND tcd.campaign_id IS NOT NULL
            ON CONFLICT (order_id, campaign_sk) DO NOTHING;
        ELSE
            RAISE WARNING 'fact_orders table does not exist. fact_campaign_transactions requires fact_orders to be loaded first. Skipping load.';
        END IF;
    ELSE
        RAISE WARNING 'Staging table stg_marketing_department_transactional_campaign_data_csv does not exist. Skipping load.';
    END IF;
END $$;

CREATE INDEX idx_fact_campaign_transactions_order_id ON fact_campaign_transactions(order_id);
CREATE INDEX idx_fact_campaign_transactions_campaign_sk ON fact_campaign_transactions(campaign_sk);
CREATE INDEX idx_fact_campaign_transactions_user_sk ON fact_campaign_transactions(user_sk);
CREATE INDEX idx_fact_campaign_transactions_transaction_date_sk ON fact_campaign_transactions(transaction_date_sk);

