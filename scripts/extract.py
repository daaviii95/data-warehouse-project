#!/usr/bin/env python3
"""
Extract Data from Staging Tables
Reads data from staging tables into pandas DataFrames for transformation
"""

import os
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from urllib.parse import quote_plus

# Configuration
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopzada")

# Build SQLAlchemy engine
password_quoted = quote_plus(DB_PASS)
engine_url = f"postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(engine_url, pool_size=5, max_overflow=10, future=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def extract_campaign_data():
    """Extract campaign data from staging table"""
    logging.info("Extracting campaign data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_marketing_department_campaign_data", conn)
    logging.info(f"Extracted {len(df)} campaign records")
    return df

def extract_transactional_campaign_data():
    """Extract transactional campaign data from staging table"""
    logging.info("Extracting transactional campaign data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_marketing_department_transactional_campaign_data", conn)
    logging.info(f"Extracted {len(df)} transactional campaign records")
    return df

def extract_order_data():
    """Extract order data from staging table"""
    logging.info("Extracting order data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_operations_department_order_data", conn)
    logging.info(f"Extracted {len(df)} order records")
    return df

def extract_line_item_prices():
    """Extract line item prices from staging table"""
    logging.info("Extracting line item prices from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_operations_department_line_item_data_prices", conn)
    logging.info(f"Extracted {len(df)} line item price records")
    return df

def extract_line_item_products():
    """Extract line item products from staging table"""
    logging.info("Extracting line item products from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_operations_department_line_item_data_products", conn)
    logging.info(f"Extracted {len(df)} line item product records")
    return df

def extract_order_delays():
    """Extract order delays from staging table"""
    logging.info("Extracting order delays from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_operations_department_order_delays", conn)
    logging.info(f"Extracted {len(df)} order delay records")
    return df

def extract_product_data():
    """Extract product data from staging table"""
    logging.info("Extracting product data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_business_department_product_list", conn)
    logging.info(f"Extracted {len(df)} product records")
    return df

def extract_user_data():
    """Extract user data from staging table"""
    logging.info("Extracting user data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_customer_management_department_user_data", conn)
    logging.info(f"Extracted {len(df)} user records")
    return df

def extract_user_job_data():
    """Extract user job data from staging table"""
    logging.info("Extracting user job data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_customer_management_department_user_job", conn)
    logging.info(f"Extracted {len(df)} user job records")
    return df

def extract_user_credit_card_data():
    """Extract user credit card data from staging table"""
    logging.info("Extracting user credit card data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_customer_management_department_user_credit_card", conn)
    logging.info(f"Extracted {len(df)} credit card records")
    return df

def extract_merchant_data():
    """Extract merchant data from staging table"""
    logging.info("Extracting merchant data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_enterprise_department_merchant_data", conn)
    logging.info(f"Extracted {len(df)} merchant records")
    return df

def extract_staff_data():
    """Extract staff data from staging table"""
    logging.info("Extracting staff data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_enterprise_department_staff_data", conn)
    logging.info(f"Extracted {len(df)} staff records")
    return df

def extract_order_merchant_data():
    """Extract order with merchant data from staging table"""
    logging.info("Extracting order with merchant data from staging...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg_enterprise_department_order_with_merchant_data", conn)
    logging.info(f"Extracted {len(df)} order merchant records")
    return df

