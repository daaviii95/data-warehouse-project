#!/usr/bin/env python3
"""
Load Dimension Tables
Loads transformed data into dimension tables
"""

import os
import sys
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from urllib.parse import quote_plus

# Import utilities from etl_pipeline_python
sys.path.insert(0, os.path.dirname(__file__))
from etl_pipeline_python import clean_value

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

def load_dim_campaign(df):
    """Load campaign dimension from transformed DataFrame"""
    if df is None or df.empty:
        logging.warning("No campaign data to load")
        return
    
    logging.info(f"Loading {len(df)} campaigns into dim_campaign...")
    
    with engine.begin() as conn:
        for _, row in df.iterrows():
            campaign_id = row.get('campaign_id')
            if pd.isna(campaign_id):
                continue
            
            conn.execute(text("""
                INSERT INTO dim_campaign (campaign_id, campaign_name, campaign_description, discount)
                VALUES (:campaign_id, :campaign_name, :campaign_description, :discount)
                ON CONFLICT (campaign_id) DO UPDATE SET
                    campaign_name = EXCLUDED.campaign_name,
                    campaign_description = EXCLUDED.campaign_description,
                    discount = EXCLUDED.discount
            """), {
                'campaign_id': str(campaign_id),
                'campaign_name': clean_value(row.get('campaign_name')),
                'campaign_description': clean_value(row.get('campaign_description')),
                'discount': row.get('discount')
            })
    
    logging.info(f"Loaded campaigns into dim_campaign")

def load_dim_product(df):
    """Load product dimension from transformed DataFrame"""
    if df is None or df.empty:
        logging.warning("No product data to load")
        return
    
    logging.info(f"Loading {len(df)} products into dim_product...")
    
    with engine.begin() as conn:
        for _, row in df.iterrows():
            product_id = row.get('product_id')
            if pd.isna(product_id):
                continue
            
            conn.execute(text("""
                INSERT INTO dim_product (product_id, product_name, product_type, price)
                VALUES (:product_id, :product_name, :product_type, :price)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_type = EXCLUDED.product_type,
                    price = EXCLUDED.price
            """), {
                'product_id': str(product_id),
                'product_name': clean_value(row.get('product_name')),
                'product_type': clean_value(row.get('product_type')),
                'price': float(row.get('price', 0)) if pd.notna(row.get('price')) else None
            })
    
    logging.info(f"Loaded products into dim_product")

def load_dim_user(df):
    """Load user dimension from transformed DataFrame"""
    if df is None or df.empty:
        logging.warning("No user data to load")
        return
    
    logging.info(f"Loading {len(df)} users into dim_user...")
    
    with engine.begin() as conn:
        for _, row in df.iterrows():
            user_id = row.get('user_id')
            if pd.isna(user_id):
                continue
            
            conn.execute(text("""
                INSERT INTO dim_user (user_id, name, street, state, city, country, birthdate, gender, device_address, user_type, creation_date)
                VALUES (:user_id, :name, :street, :state, :city, :country, :birthdate, :gender, :device_address, :user_type, :creation_date)
                ON CONFLICT (user_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    street = EXCLUDED.street,
                    state = EXCLUDED.state,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    birthdate = EXCLUDED.birthdate,
                    gender = EXCLUDED.gender,
                    device_address = EXCLUDED.device_address,
                    user_type = EXCLUDED.user_type,
                    creation_date = EXCLUDED.creation_date
            """), {
                'user_id': str(user_id),
                'name': clean_value(row.get('name')),
                'street': clean_value(row.get('street')),
                'state': clean_value(row.get('state')),
                'city': clean_value(row.get('city')),
                'country': clean_value(row.get('country')),
                'birthdate': pd.to_datetime(row.get('birthdate'), errors='coerce') if pd.notna(row.get('birthdate')) else None,
                'gender': clean_value(row.get('gender')),
                'device_address': clean_value(row.get('device_address')),
                'user_type': clean_value(row.get('user_type')),
                'creation_date': pd.to_datetime(row.get('creation_date'), errors='coerce') if pd.notna(row.get('creation_date')) else None
            })
    
    logging.info(f"Loaded users into dim_user")

def load_dim_staff(df):
    """Load staff dimension from transformed DataFrame"""
    if df is None or df.empty:
        logging.warning("No staff data to load")
        return
    
    logging.info(f"Loading {len(df)} staff into dim_staff...")
    
    with engine.begin() as conn:
        for _, row in df.iterrows():
            staff_id = row.get('staff_id')
            if pd.isna(staff_id):
                continue
            
            conn.execute(text("""
                INSERT INTO dim_staff (staff_id, name, street, state, city, country, job_level, contact_number, creation_date)
                VALUES (:staff_id, :name, :street, :state, :city, :country, :job_level, :contact_number, :creation_date)
                ON CONFLICT (staff_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    street = EXCLUDED.street,
                    state = EXCLUDED.state,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    job_level = EXCLUDED.job_level,
                    contact_number = EXCLUDED.contact_number,
                    creation_date = EXCLUDED.creation_date
            """), {
                'staff_id': str(staff_id),
                'name': clean_value(row.get('name')),
                'street': clean_value(row.get('street')),
                'state': clean_value(row.get('state')),
                'city': clean_value(row.get('city')),
                'country': clean_value(row.get('country')),
                'job_level': clean_value(row.get('job_level')),
                'contact_number': clean_value(row.get('contact_number')),
                'creation_date': pd.to_datetime(row.get('creation_date'), errors='coerce') if pd.notna(row.get('creation_date')) else None
            })
    
    logging.info(f"Loaded staff into dim_staff")

def load_dim_merchant(df):
    """Load merchant dimension from transformed DataFrame"""
    if df is None or df.empty:
        logging.warning("No merchant data to load")
        return
    
    logging.info(f"Loading {len(df)} merchants into dim_merchant...")
    
    with engine.begin() as conn:
        for _, row in df.iterrows():
            merchant_id = row.get('merchant_id')
            if pd.isna(merchant_id):
                continue
            
            conn.execute(text("""
                INSERT INTO dim_merchant (merchant_id, name, street, state, city, country, contact_number, creation_date)
                VALUES (:merchant_id, :name, :street, :state, :city, :country, :contact_number, :creation_date)
                ON CONFLICT (merchant_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    street = EXCLUDED.street,
                    state = EXCLUDED.state,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    contact_number = EXCLUDED.contact_number,
                    creation_date = EXCLUDED.creation_date
            """), {
                'merchant_id': str(merchant_id),
                'name': clean_value(row.get('name')),
                'street': clean_value(row.get('street')),
                'state': clean_value(row.get('state')),
                'city': clean_value(row.get('city')),
                'country': clean_value(row.get('country')),
                'contact_number': clean_value(row.get('contact_number')),
                'creation_date': pd.to_datetime(row.get('creation_date'), errors='coerce') if pd.notna(row.get('creation_date')) else None
            })
    
    logging.info(f"Loaded merchants into dim_merchant")

def load_dim_user_job(df):
    """Load user job dimension from transformed DataFrame"""
    if df is None or df.empty:
        logging.warning("No user job data to load")
        return
    
    logging.info(f"Loading {len(df)} user jobs into dim_user_job...")
    
    with engine.begin() as conn:
        for _, row in df.iterrows():
            user_id = row.get('user_id')
            if pd.isna(user_id):
                continue
            
            # Clean job_level - convert [null] strings to None
            job_level = row.get('job_level')
            if job_level is not None and not pd.isna(job_level):
                job_level_str = str(job_level).strip()
                if job_level_str.lower() in ['[null]', 'null', 'none', '']:
                    job_level = None
                else:
                    job_level = clean_value(job_level)
            else:
                job_level = None
            
            conn.execute(text("""
                INSERT INTO dim_user_job (user_id, job_title, job_level)
                VALUES (:user_id, :job_title, :job_level)
                ON CONFLICT DO NOTHING
            """), {
                'user_id': str(user_id),
                'job_title': clean_value(row.get('job_title')),
                'job_level': job_level
            })
    
    logging.info(f"Loaded user jobs into dim_user_job")

def load_dim_credit_card(df):
    """Load credit card dimension from transformed DataFrame"""
    if df is None or df.empty:
        logging.warning("No credit card data to load")
        return
    
    logging.info(f"Loading {len(df)} credit cards into dim_credit_card...")
    
    with engine.begin() as conn:
        for _, row in df.iterrows():
            user_id = row.get('user_id')
            if pd.isna(user_id):
                continue
            
            conn.execute(text("""
                INSERT INTO dim_credit_card (user_id, name, credit_card_number, issuing_bank)
                VALUES (:user_id, :name, :credit_card_number, :issuing_bank)
                ON CONFLICT DO NOTHING
            """), {
                'user_id': str(user_id),
                'name': clean_value(row.get('name')),
                'credit_card_number': clean_value(row.get('credit_card_number')),
                'issuing_bank': clean_value(row.get('issuing_bank'))
            })
    
    logging.info(f"Loaded credit cards into dim_credit_card")

