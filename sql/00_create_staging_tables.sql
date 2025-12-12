-- Create Staging Tables for Department Data
-- These tables store raw data loaded from source files before transformation
-- Data types match the physical model where applicable
-- Naming convention: stg_{department}_{data_type}

-- ============================================================================
-- MARKETING DEPARTMENT STAGING TABLES
-- ============================================================================

-- Campaign data staging
CREATE TABLE IF NOT EXISTS stg_marketing_department_campaign_data (
    campaign_id VARCHAR(50),
    campaign_name VARCHAR(255),
    campaign_description TEXT,
    discount DECIMAL(10,2),
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactional campaign data staging
CREATE TABLE IF NOT EXISTS stg_marketing_department_transactional_campaign_data (
    transaction_id VARCHAR(50),
    user_id VARCHAR(50),
    campaign_id VARCHAR(50),
    order_id VARCHAR(50),
    availed INTEGER,
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- OPERATIONS DEPARTMENT STAGING TABLES
-- ============================================================================

-- Order data staging (multiple files will be loaded here)
CREATE TABLE IF NOT EXISTS stg_operations_department_order_data (
    order_id VARCHAR(50),
    user_id VARCHAR(50),
    transaction_date DATE,
    payment_method VARCHAR(50),
    total_amount DECIMAL(10,2),
    estimated_arrival_days INTEGER,
    staff_id VARCHAR(50),
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Line item prices staging
CREATE TABLE IF NOT EXISTS stg_operations_department_line_item_data_prices (
    line_item_id VARCHAR(50),
    order_id VARCHAR(50),
    quantity INTEGER,
    discount DECIMAL(10,2),
    price DECIMAL(10,2),
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Line item products staging
CREATE TABLE IF NOT EXISTS stg_operations_department_line_item_data_products (
    line_item_id VARCHAR(50),
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order delays staging
CREATE TABLE IF NOT EXISTS stg_operations_department_order_delays (
    order_id VARCHAR(50),
    delay_days INTEGER,
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- BUSINESS DEPARTMENT STAGING TABLES
-- ============================================================================

-- Product list staging
CREATE TABLE IF NOT EXISTS stg_business_department_product_list (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price DECIMAL(10,2),
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- CUSTOMER MANAGEMENT DEPARTMENT STAGING TABLES
-- ============================================================================

-- User data staging
CREATE TABLE IF NOT EXISTS stg_customer_management_department_user_data (
    user_id VARCHAR(50),
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    birthdate DATE,
    gender VARCHAR(20),
    device_address VARCHAR(255),
    user_type VARCHAR(50),
    creation_date TIMESTAMP,
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User job staging
CREATE TABLE IF NOT EXISTS stg_customer_management_department_user_job (
    user_id VARCHAR(50),
    job_title VARCHAR(255),
    job_level VARCHAR(100),
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User credit card staging
CREATE TABLE IF NOT EXISTS stg_customer_management_department_user_credit_card (
    user_id VARCHAR(50),
    name VARCHAR(255),
    credit_card_number VARCHAR(50),
    issuing_bank VARCHAR(255),
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ENTERPRISE DEPARTMENT STAGING TABLES
-- ============================================================================

-- Merchant data staging
CREATE TABLE IF NOT EXISTS stg_enterprise_department_merchant_data (
    merchant_id VARCHAR(50),
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP,
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staff data staging
CREATE TABLE IF NOT EXISTS stg_enterprise_department_staff_data (
    staff_id VARCHAR(50),
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    job_level VARCHAR(50),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP,
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order with merchant data staging
CREATE TABLE IF NOT EXISTS stg_enterprise_department_order_with_merchant_data (
    order_id VARCHAR(50),
    merchant_id VARCHAR(50),
    staff_id VARCHAR(50),
    file_source VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_stg_campaign_campaign_id ON stg_marketing_department_campaign_data(campaign_id);
CREATE INDEX IF NOT EXISTS idx_stg_transactional_campaign_user_id ON stg_marketing_department_transactional_campaign_data(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_transactional_campaign_campaign_id ON stg_marketing_department_transactional_campaign_data(campaign_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_order_id ON stg_operations_department_order_data(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_user_id ON stg_operations_department_order_data(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_line_item_prices_line_item_id ON stg_operations_department_line_item_data_prices(line_item_id);
CREATE INDEX IF NOT EXISTS idx_stg_line_item_prices_order_id ON stg_operations_department_line_item_data_prices(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_line_item_products_line_item_id ON stg_operations_department_line_item_data_products(line_item_id);
CREATE INDEX IF NOT EXISTS idx_stg_line_item_products_order_id ON stg_operations_department_line_item_data_products(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_line_item_products_product_id ON stg_operations_department_line_item_data_products(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_delays_order_id ON stg_operations_department_order_delays(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_product_product_id ON stg_business_department_product_list(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_user_user_id ON stg_customer_management_department_user_data(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_user_job_user_id ON stg_customer_management_department_user_job(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_credit_card_user_id ON stg_customer_management_department_user_credit_card(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_merchant_merchant_id ON stg_enterprise_department_merchant_data(merchant_id);
CREATE INDEX IF NOT EXISTS idx_stg_staff_staff_id ON stg_enterprise_department_staff_data(staff_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_merchant_order_id ON stg_enterprise_department_order_with_merchant_data(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_merchant_merchant_id ON stg_enterprise_department_order_with_merchant_data(merchant_id);
