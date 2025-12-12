-- Drop All Staging Tables
-- Use this script to drop all staging tables before recreating them
-- This ensures the schema matches the current definition

DROP TABLE IF EXISTS stg_marketing_department_campaign_data CASCADE;
DROP TABLE IF EXISTS stg_marketing_department_transactional_campaign_data CASCADE;
DROP TABLE IF EXISTS stg_operations_department_order_data CASCADE;
DROP TABLE IF EXISTS stg_operations_department_line_item_data_prices CASCADE;
DROP TABLE IF EXISTS stg_operations_department_line_item_data_products CASCADE;
DROP TABLE IF EXISTS stg_operations_department_order_delays CASCADE;
DROP TABLE IF EXISTS stg_business_department_product_list CASCADE;
DROP TABLE IF EXISTS stg_customer_management_department_user_data CASCADE;
DROP TABLE IF EXISTS stg_customer_management_department_user_job CASCADE;
DROP TABLE IF EXISTS stg_customer_management_department_user_credit_card CASCADE;
DROP TABLE IF EXISTS stg_enterprise_department_merchant_data CASCADE;
DROP TABLE IF EXISTS stg_enterprise_department_staff_data CASCADE;
DROP TABLE IF EXISTS stg_enterprise_department_order_with_merchant_data CASCADE;

