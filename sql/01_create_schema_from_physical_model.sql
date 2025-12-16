-- Create Schema Based on Logical Model (/docs/LOGICAL-PHYSICALMODEL.txt)
-- Kimball Star Schema Implementation
-- This script creates ALL dimension and fact tables exactly as specified

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- dim_campaign
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_sk SERIAL PRIMARY KEY,
    campaign_id VARCHAR(50),
    campaign_name VARCHAR(255),
    campaign_description TEXT,
    discount DECIMAL(10,2),
    UNIQUE(campaign_id)
);

-- dim_product
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price DECIMAL(10,2),
    UNIQUE(product_id)
);

-- dim_user
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE,
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

-- dim_staff
CREATE TABLE IF NOT EXISTS dim_staff (
    staff_sk SERIAL PRIMARY KEY,
    staff_id VARCHAR(50),
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    job_level VARCHAR(50),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP,
    UNIQUE(staff_id)
);

-- dim_merchant
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_sk SERIAL PRIMARY KEY,
    merchant_id VARCHAR(50),
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP,
    UNIQUE(merchant_id)
);

-- dim_date
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INTEGER PRIMARY KEY,
    date DATE UNIQUE,
    year INTEGER,
    month INTEGER,
    month_name VARCHAR(50),
    day INTEGER,
    quarter VARCHAR(2)
);

-- dim_user_job (Note: requires user_id to be UNIQUE in dim_user)
CREATE TABLE IF NOT EXISTS dim_user_job (
    user_job_sk SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    job_title VARCHAR(255),
    job_level VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id)
);

-- dim_credit_card (Note: requires user_id to be UNIQUE in dim_user)
CREATE TABLE IF NOT EXISTS dim_credit_card (
    credit_card_sk SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    name VARCHAR(255),
    credit_card_number VARCHAR(50),
    issuing_bank VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id)
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- fact_orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT NOT NULL,
    user_sk INTEGER NOT NULL,
    merchant_sk INTEGER NOT NULL,
    staff_sk INTEGER NOT NULL,
    transaction_date_sk INTEGER NOT NULL,
    estimated_arrival_days INTEGER,
    delay_days INTEGER,
    PRIMARY KEY (order_id),
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk),
    FOREIGN KEY (staff_sk) REFERENCES dim_staff(staff_sk),
    FOREIGN KEY (transaction_date_sk) REFERENCES dim_date(date_sk)
);

-- fact_line_items
CREATE TABLE IF NOT EXISTS fact_line_items (
    order_id TEXT NOT NULL,
    product_sk INTEGER NOT NULL,
    user_sk INTEGER NOT NULL,
    merchant_sk INTEGER NOT NULL,
    staff_sk INTEGER NOT NULL,
    transaction_date_sk INTEGER NOT NULL,
    price DECIMAL(10,2),
    quantity INTEGER,
    PRIMARY KEY (order_id, product_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk),
    FOREIGN KEY (staff_sk) REFERENCES dim_staff(staff_sk),
    FOREIGN KEY (transaction_date_sk) REFERENCES dim_date(date_sk)
);

-- fact_campaign_transactions
CREATE TABLE IF NOT EXISTS fact_campaign_transactions (
    order_id TEXT NOT NULL,
    campaign_sk INTEGER NOT NULL,
    user_sk INTEGER NOT NULL,
    merchant_sk INTEGER NOT NULL,
    transaction_date_sk INTEGER NOT NULL,
    availed INTEGER,
    PRIMARY KEY (campaign_sk, user_sk, merchant_sk, transaction_date_sk),
    FOREIGN KEY (campaign_sk) REFERENCES dim_campaign(campaign_sk),
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk),
    FOREIGN KEY (transaction_date_sk) REFERENCES dim_date(date_sk)
);

-- ============================================================================
-- REJECT TABLES FOR DATA QUALITY TRACKING (Scenario 4)
-- ============================================================================

-- Reject table for fact_orders
CREATE TABLE IF NOT EXISTS reject_fact_orders (
    reject_id SERIAL PRIMARY KEY,
    order_id TEXT,
    user_id TEXT,
    merchant_id TEXT,
    staff_id TEXT,
    transaction_date TEXT,
    error_reason TEXT NOT NULL,
    source_file TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB
);

-- Reject table for fact_line_items
CREATE TABLE IF NOT EXISTS reject_fact_line_items (
    reject_id SERIAL PRIMARY KEY,
    order_id TEXT,
    product_id TEXT,
    line_item_id TEXT,
    error_reason TEXT NOT NULL,
    source_file TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB
);

-- Reject table for fact_campaign_transactions
CREATE TABLE IF NOT EXISTS reject_fact_campaign_transactions (
    reject_id SERIAL PRIMARY KEY,
    order_id TEXT,
    user_id TEXT,
    campaign_id TEXT,
    transaction_date TEXT,
    error_reason TEXT NOT NULL,
    source_file TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB
);

-- Indexes for reject tables
CREATE INDEX IF NOT EXISTS idx_reject_fact_orders_order_id ON reject_fact_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_reject_fact_orders_rejected_at ON reject_fact_orders(rejected_at);
CREATE INDEX IF NOT EXISTS idx_reject_fact_line_items_order_id ON reject_fact_line_items(order_id);
CREATE INDEX IF NOT EXISTS idx_reject_fact_line_items_rejected_at ON reject_fact_line_items(rejected_at);
CREATE INDEX IF NOT EXISTS idx_reject_fact_campaign_transactions_order_id ON reject_fact_campaign_transactions(order_id);
CREATE INDEX IF NOT EXISTS idx_reject_fact_campaign_transactions_rejected_at ON reject_fact_campaign_transactions(rejected_at);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_dim_campaign_campaign_id ON dim_campaign(campaign_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_product_id ON dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_user_user_id ON dim_user(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_staff_staff_id ON dim_staff(staff_id);
CREATE INDEX IF NOT EXISTS idx_dim_merchant_merchant_id ON dim_merchant(merchant_id);
CREATE INDEX IF NOT EXISTS idx_dim_date_date ON dim_date(date);

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id ON fact_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_user_sk ON fact_orders(user_sk);
CREATE INDEX IF NOT EXISTS idx_fact_orders_transaction_date_sk ON fact_orders(transaction_date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_line_items_order_id ON fact_line_items(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_line_items_product_sk ON fact_line_items(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_campaign_transactions_order_id ON fact_campaign_transactions(order_id);

