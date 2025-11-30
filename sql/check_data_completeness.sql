-- Data Completeness Check Queries for pgcli
-- Run these queries to verify all data is present

-- ============================================================================
-- 1. STAGING TABLES SUMMARY
-- ============================================================================

-- Line Items - Prices
SELECT 
    'Line Items (Prices)' as category,
    COUNT(*) as total_records
FROM (
    SELECT 1 FROM stg_operations_department_line_item_data_prices1_csv
    UNION ALL
    SELECT 1 FROM stg_operations_department_line_item_data_prices2_csv
    UNION ALL
    SELECT 1 FROM stg_operations_department_line_item_data_prices3_parquet
) t;

-- Line Items - Products
SELECT 
    'Line Items (Products)' as category,
    COUNT(*) as total_records
FROM (
    SELECT 1 FROM stg_operations_department_line_item_data_products1_csv
    UNION ALL
    SELECT 1 FROM stg_operations_department_line_item_data_products2_csv
    UNION ALL
    SELECT 1 FROM stg_operations_department_line_item_data_products3_parquet
) t;

-- Orders - Operations Department
SELECT 
    'Orders (Operations)' as category,
    COUNT(*) as total_records
FROM (
    SELECT 1 FROM stg_operations_department_order_data_20200101_20200701_parquet
    UNION ALL
    SELECT 1 FROM stg_operations_department_order_data_20200701_20211001_pickle
    UNION ALL
    SELECT 1 FROM stg_operations_department_order_data_20211001_20220101_csv
    UNION ALL
    SELECT 1 FROM stg_operations_department_order_data_20220101_20221201_xlsx
    UNION ALL
    SELECT 1 FROM stg_operations_department_order_data_20221201_20230601_json
    UNION ALL
    SELECT 1 FROM stg_operations_department_order_data_20230601_20240101_html
) t;

-- Orders - Enterprise Department (Merchant Data)
SELECT 
    'Orders (Merchant)' as category,
    COUNT(*) as total_records
FROM (
    SELECT 1 FROM stg_enterprise_department_order_with_merchant_data1_parquet
    UNION ALL
    SELECT 1 FROM stg_enterprise_department_order_with_merchant_data2_parquet
    UNION ALL
    SELECT 1 FROM stg_enterprise_department_order_with_merchant_data3_csv
) t;

-- Campaign Transactions
SELECT 
    'Campaign Transactions' as category,
    COUNT(*) as total_records
FROM stg_marketing_department_transactional_campaign_data_csv;

-- Order Delays
SELECT 
    'Order Delays' as category,
    COUNT(*) as total_records
FROM stg_operations_department_order_delays_html;

-- ============================================================================
-- 2. FACT TABLES SUMMARY
-- ============================================================================

SELECT 
    'fact_orders' as table_name,
    COUNT(*) as record_count
FROM fact_orders
UNION ALL
SELECT 
    'fact_line_items',
    COUNT(*)
FROM fact_line_items
UNION ALL
SELECT 
    'fact_campaign_transactions',
    COUNT(*)
FROM fact_campaign_transactions
ORDER BY table_name;

-- ============================================================================
-- 3. DIMENSION TABLES SUMMARY
-- ============================================================================

SELECT 
    'dim_campaign' as table_name,
    COUNT(*) as record_count
FROM dim_campaign
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_user', COUNT(*) FROM dim_user
UNION ALL
SELECT 'dim_staff', COUNT(*) FROM dim_staff
UNION ALL
SELECT 'dim_merchant', COUNT(*) FROM dim_merchant
UNION ALL
SELECT 'dim_user_job', COUNT(*) FROM dim_user_job
UNION ALL
SELECT 'dim_credit_card', COUNT(*) FROM dim_credit_card
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
ORDER BY table_name;

-- ============================================================================
-- 4. QUICK SUMMARY (All in one query)
-- ============================================================================

SELECT 
    '=== STAGING TABLES ===' as section,
    '' as detail,
    '' as count
UNION ALL
SELECT 
    'Line Items (Prices)',
    '',
    (SELECT COUNT(*)::TEXT FROM (
        SELECT 1 FROM stg_operations_department_line_item_data_prices1_csv
        UNION ALL SELECT 1 FROM stg_operations_department_line_item_data_prices2_csv
        UNION ALL SELECT 1 FROM stg_operations_department_line_item_data_prices3_parquet
    ) t)
UNION ALL
SELECT 
    'Line Items (Products)',
    '',
    (SELECT COUNT(*)::TEXT FROM (
        SELECT 1 FROM stg_operations_department_line_item_data_products1_csv
        UNION ALL SELECT 1 FROM stg_operations_department_line_item_data_products2_csv
        UNION ALL SELECT 1 FROM stg_operations_department_line_item_data_products3_parquet
    ) t)
UNION ALL
SELECT 
    'Orders (Operations)',
    '',
    (SELECT COUNT(*)::TEXT FROM (
        SELECT 1 FROM stg_operations_department_order_data_20200101_20200701_parquet
        UNION ALL SELECT 1 FROM stg_operations_department_order_data_20200701_20211001_pickle
        UNION ALL SELECT 1 FROM stg_operations_department_order_data_20211001_20220101_csv
        UNION ALL SELECT 1 FROM stg_operations_department_order_data_20220101_20221201_xlsx
        UNION ALL SELECT 1 FROM stg_operations_department_order_data_20221201_20230601_json
        UNION ALL SELECT 1 FROM stg_operations_department_order_data_20230601_20240101_html
    ) t)
UNION ALL
SELECT 
    'Orders (Merchant)',
    '',
    (SELECT COUNT(*)::TEXT FROM (
        SELECT 1 FROM stg_enterprise_department_order_with_merchant_data1_parquet
        UNION ALL SELECT 1 FROM stg_enterprise_department_order_with_merchant_data2_parquet
        UNION ALL SELECT 1 FROM stg_enterprise_department_order_with_merchant_data3_csv
    ) t)
UNION ALL
SELECT 
    'Campaign Transactions',
    '',
    (SELECT COUNT(*)::TEXT FROM stg_marketing_department_transactional_campaign_data_csv)
UNION ALL
SELECT 
    '---',
    '---',
    '---'
UNION ALL
SELECT 
    'TOTAL STAGING',
    '',
    (SELECT (
        (SELECT COUNT(*) FROM (
            SELECT 1 FROM stg_operations_department_line_item_data_prices1_csv
            UNION ALL SELECT 1 FROM stg_operations_department_line_item_data_prices2_csv
            UNION ALL SELECT 1 FROM stg_operations_department_line_item_data_prices3_parquet
        ) t) +
        (SELECT COUNT(*) FROM (
            SELECT 1 FROM stg_operations_department_line_item_data_products1_csv
            UNION ALL SELECT 1 FROM stg_operations_department_line_item_data_products2_csv
            UNION ALL SELECT 1 FROM stg_operations_department_line_item_data_products3_parquet
        ) t) +
        (SELECT COUNT(*) FROM (
            SELECT 1 FROM stg_operations_department_order_data_20200101_20200701_parquet
            UNION ALL SELECT 1 FROM stg_operations_department_order_data_20200701_20211001_pickle
            UNION ALL SELECT 1 FROM stg_operations_department_order_data_20211001_20220101_csv
            UNION ALL SELECT 1 FROM stg_operations_department_order_data_20220101_20221201_xlsx
            UNION ALL SELECT 1 FROM stg_operations_department_order_data_20221201_20230601_json
            UNION ALL SELECT 1 FROM stg_operations_department_order_data_20230601_20240101_html
        ) t) +
        (SELECT COUNT(*) FROM (
            SELECT 1 FROM stg_enterprise_department_order_with_merchant_data1_parquet
            UNION ALL SELECT 1 FROM stg_enterprise_department_order_with_merchant_data2_parquet
            UNION ALL SELECT 1 FROM stg_enterprise_department_order_with_merchant_data3_csv
        ) t) +
        (SELECT COUNT(*) FROM stg_marketing_department_transactional_campaign_data_csv)
    )::TEXT)
UNION ALL
SELECT 
    '',
    '',
    ''
UNION ALL
SELECT 
    '=== FACT TABLES ===',
    '',
    ''
UNION ALL
SELECT 
    'fact_orders',
    '',
    (SELECT COUNT(*)::TEXT FROM fact_orders)
UNION ALL
SELECT 
    'fact_line_items',
    '',
    (SELECT COUNT(*)::TEXT FROM fact_line_items)
UNION ALL
SELECT 
    'fact_campaign_transactions',
    '',
    (SELECT COUNT(*)::TEXT FROM fact_campaign_transactions)
UNION ALL
SELECT 
    '---',
    '---',
    '---'
UNION ALL
SELECT 
    'TOTAL FACT',
    '',
    (SELECT (COUNT(*) FROM fact_orders) + (SELECT COUNT(*) FROM fact_line_items) + (SELECT COUNT(*) FROM fact_campaign_transactions))::TEXT;

-- ============================================================================
-- 5. INDIVIDUAL TABLE COUNTS (Detailed breakdown)
-- ============================================================================

-- All staging tables with counts
SELECT 
    table_name,
    (xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %I.%I', table_schema, table_name), false, true, '')))[1]::text::int AS row_count
FROM information_schema.tables
WHERE table_schema = 'public' 
  AND table_name LIKE 'stg_%'
ORDER BY table_name;

