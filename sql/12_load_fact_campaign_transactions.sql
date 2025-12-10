-- Load Fact Campaign Transactions
-- Kimball Methodology: Transaction fact table
-- Links campaigns to orders through transactional campaign data
-- Requires fact_orders to be loaded first

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'stg_marketing_department_transactional_campaign_data%'
        AND table_name NOT LIKE '%_tbl%'
        LIMIT 1
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'fact_orders'
        ) THEN
            -- Build dynamic query to handle multiple transactional_campaign_data tables
            EXECUTE format('
            INSERT INTO fact_campaign_transactions (order_id, campaign_sk, user_sk, merchant_sk, transaction_date_sk, availed)
            SELECT DISTINCT
                tcd.order_id::TEXT,
                dc.campaign_sk,
                fo.user_sk,
                fo.merchant_sk,
                fo.transaction_date_sk,
                CASE 
                    WHEN tcd.availed::TEXT IN (''1'', ''true'', ''True'', ''TRUE'') OR tcd.availed::INTEGER = 1 THEN 1 
                    WHEN tcd.availed::TEXT IN (''0'', ''false'', ''False'', ''FALSE'') OR tcd.availed::INTEGER = 0 THEN 0
                    ELSE NULL
                END AS availed
            FROM (
                SELECT order_id, campaign_id, transaction_date, "estimated arrival" as estimated_arrival, availed
                FROM %I
                WHERE order_id IS NOT NULL AND campaign_id IS NOT NULL
            ) tcd
            INNER JOIN dim_campaign dc ON tcd.campaign_id = dc.campaign_id
            INNER JOIN dim_date dd ON DATE(tcd.transaction_date) = dd.date
            INNER JOIN fact_orders fo ON tcd.order_id::TEXT = fo.order_id
                AND fo.transaction_date_sk = dd.date_sk
            WHERE tcd.order_id IS NOT NULL
              AND tcd.campaign_id IS NOT NULL
            ON CONFLICT (campaign_sk, user_sk, merchant_sk, transaction_date_sk) DO NOTHING',
                (SELECT table_name FROM information_schema.tables 
                 WHERE table_schema = 'public' 
                 AND table_name LIKE 'stg_marketing_department_transactional_campaign_data%'
                 AND table_name NOT LIKE '%_tbl%'
                 LIMIT 1)
            );
            
            RAISE NOTICE 'Loaded fact_campaign_transactions successfully';
        ELSE
            RAISE WARNING 'fact_orders table does not exist. fact_campaign_transactions requires fact_orders to be loaded first. Skipping load.';
        END IF;
    ELSE
        RAISE WARNING 'Staging table stg_marketing_department_transactional_campaign_data%% does not exist. Skipping load.';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error loading fact_campaign_transactions: %', SQLERRM;
END $$;

