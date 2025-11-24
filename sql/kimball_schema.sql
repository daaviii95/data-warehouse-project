-- ============================================================================
-- ShopZada Kimball Dimensional Model (Star Schema)
-- ============================================================================
-- This script creates the dimensional model tables for the data warehouse.
-- Architecture: Staging → Dimensional Model (Star Schema) → Presentation Layer
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Date Dimension (Conformed Dimension)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_number INTEGER NOT NULL,
    quarter_name VARCHAR(2) NOT NULL,
    year_number INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_date_actual ON dim_date(date_actual);
CREATE INDEX idx_dim_date_year_month ON dim_date(year_number, month_number);

-- Product Dimension
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(100),
    product_name VARCHAR(500),
    product_category VARCHAR(200),
    product_subcategory VARCHAR(200),
    brand VARCHAR(200),
    unit_price NUMERIC(10, 2),
    cost_price NUMERIC(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, source_system)
);

CREATE INDEX idx_dim_product_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(product_category);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(100),
    first_name VARCHAR(200),
    last_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(20),
    job_title VARCHAR(200),
    job_category VARCHAR(100),
    credit_card_type VARCHAR(50),
    credit_card_last4 VARCHAR(4),
    registration_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, source_system)
);

CREATE INDEX idx_dim_customer_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_email ON dim_customer(email);

-- Merchant Dimension
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_key SERIAL PRIMARY KEY,
    merchant_id VARCHAR(100),
    merchant_name VARCHAR(500),
    merchant_type VARCHAR(100),
    merchant_category VARCHAR(200),
    country VARCHAR(100),
    city VARCHAR(200),
    is_active BOOLEAN DEFAULT TRUE,
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(merchant_id, source_system)
);

CREATE INDEX idx_dim_merchant_merchant_id ON dim_merchant(merchant_id);

-- Staff Dimension
CREATE TABLE IF NOT EXISTS dim_staff (
    staff_key SERIAL PRIMARY KEY,
    staff_id VARCHAR(100),
    staff_name VARCHAR(500),
    department VARCHAR(100),
    position VARCHAR(200),
    hire_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(staff_id, source_system)
);

CREATE INDEX idx_dim_staff_staff_id ON dim_staff(staff_id);

-- Campaign Dimension
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id VARCHAR(100),
    campaign_name VARCHAR(500),
    campaign_type VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget NUMERIC(12, 2),
    is_active BOOLEAN DEFAULT TRUE,
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(campaign_id, source_system)
);

CREATE INDEX idx_dim_campaign_campaign_id ON dim_campaign(campaign_id);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Sales Fact Table (Grain: One row per line item)
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    line_item_id VARCHAR(100),
    order_date_key INTEGER REFERENCES dim_date(date_key),
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    merchant_key INTEGER REFERENCES dim_merchant(merchant_key),
    staff_key INTEGER REFERENCES dim_staff(staff_key),
    campaign_key INTEGER REFERENCES dim_campaign(campaign_key),
    
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    discount_amount NUMERIC(10, 2) DEFAULT 0,
    discount_percent NUMERIC(5, 2) DEFAULT 0,
    line_total NUMERIC(12, 2) NOT NULL,
    cost_amount NUMERIC(12, 2),
    profit_amount NUMERIC(12, 2),
    
    -- Order-level attributes
    order_total NUMERIC(12, 2),
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_method VARCHAR(50),
    estimated_arrival_days INTEGER,
    actual_arrival_date DATE,
    is_delayed BOOLEAN DEFAULT FALSE,
    delay_days INTEGER,
    
    -- Metadata
    source_system VARCHAR(50),
    source_file VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_line_total_positive CHECK (line_total >= 0)
);

CREATE INDEX idx_fact_sales_order_date ON fact_sales(order_date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_merchant ON fact_sales(merchant_key);
CREATE INDEX idx_fact_sales_order_id ON fact_sales(order_id);
CREATE INDEX idx_fact_sales_created_at ON fact_sales(created_at);

-- Campaign Performance Fact Table
CREATE TABLE IF NOT EXISTS fact_campaign_performance (
    campaign_performance_key BIGSERIAL PRIMARY KEY,
    campaign_key INTEGER REFERENCES dim_campaign(campaign_key),
    transaction_date_key INTEGER REFERENCES dim_date(date_key),
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    
    -- Measures
    transactions_count INTEGER DEFAULT 1,
    revenue_amount NUMERIC(12, 2),
    discount_amount NUMERIC(12, 2),
    net_revenue_amount NUMERIC(12, 2),
    
    -- Metadata
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_campaign_campaign ON fact_campaign_performance(campaign_key);
CREATE INDEX idx_fact_campaign_date ON fact_campaign_performance(transaction_date_key);
CREATE INDEX idx_fact_campaign_customer ON fact_campaign_performance(customer_key);

-- ============================================================================
-- STAGING TO DIMENSIONAL MODEL ETL LOG TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_log (
    etl_log_id SERIAL PRIMARY KEY,
    etl_name VARCHAR(200) NOT NULL,
    source_table VARCHAR(200),
    target_table VARCHAR(200),
    rows_processed BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    rows_failed BIGINT,
    status VARCHAR(50),
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_etl_log_status ON etl_log(status);
CREATE INDEX idx_etl_log_created_at ON etl_log(created_at);

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE dim_date IS 'Conformed date dimension for all fact tables';
COMMENT ON TABLE dim_product IS 'Product master data dimension';
COMMENT ON TABLE dim_customer IS 'Customer master data dimension';
COMMENT ON TABLE dim_merchant IS 'Merchant/vendor dimension';
COMMENT ON TABLE dim_staff IS 'Staff/employee dimension';
COMMENT ON TABLE dim_campaign IS 'Marketing campaign dimension';
COMMENT ON TABLE fact_sales IS 'Sales fact table - grain: one row per line item';
COMMENT ON TABLE fact_campaign_performance IS 'Campaign performance fact table';