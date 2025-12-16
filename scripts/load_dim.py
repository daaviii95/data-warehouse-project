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

def create_missing_users_from_orders(order_df):
    """
    Create missing user dimension entries from order data (Scenario 2 support)
    
    Extracts unique user_id values from order data and creates minimal dim_user entries
    for any users that don't already exist in the dimension table.
    
    Args:
        order_df: DataFrame containing order data with user_id column
    
    Returns:
        int: Number of new user dimension entries created
    """
    if order_df is None or order_df.empty:
        logging.info("No order data provided for missing user extraction")
        return 0
    
    if 'user_id' not in order_df.columns:
        logging.warning("user_id column not found in order data")
        return 0
    
    logging.info("=" * 60)
    logging.info("CREATING MISSING USERS FROM ORDER DATA (Scenario 2)")
    logging.info("=" * 60)
    
    # Extract unique user_ids from order data
    unique_user_ids = order_df['user_id'].dropna().unique()
    unique_user_ids = [str(uid).strip() for uid in unique_user_ids if str(uid).strip()]
    
    if not unique_user_ids:
        logging.info("No valid user_ids found in order data")
        return 0
    
    logging.info(f"Found {len(unique_user_ids)} unique user_ids in order data")
    
    # Get existing user_ids from dim_user
    existing_user_ids = set()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT user_id FROM dim_user WHERE user_id IS NOT NULL"))
        existing_user_ids = {str(row[0]).strip() for row in result}
    
    logging.info(f"Found {len(existing_user_ids)} existing users in dim_user")
    
    # Find missing user_ids
    missing_user_ids = [uid for uid in unique_user_ids if uid not in existing_user_ids]
    
    if not missing_user_ids:
        logging.info("All users from order data already exist in dim_user")
        return 0
    
    logging.info(f"Creating {len(missing_user_ids)} missing user dimension entries...")
    
    # Create minimal dim_user entries for missing users
    # Use NULL/default values for attributes not available in order data
    created_count = 0
    with engine.begin() as conn:
        for user_id in missing_user_ids:
            try:
                conn.execute(text("""
                    INSERT INTO dim_user (user_id, name, street, state, city, country, birthdate, gender, device_address, user_type, creation_date)
                    VALUES (:user_id, :name, :street, :state, :city, :country, :birthdate, :gender, :device_address, :user_type, :creation_date)
                    ON CONFLICT (user_id) DO NOTHING
                """), {
                    'user_id': str(user_id),
                    'name': None,  # Not available in order data
                    'street': None,
                    'state': None,
                    'city': None,
                    'country': None,
                    'birthdate': None,
                    'gender': None,
                    'device_address': None,
                    'user_type': None,
                    'creation_date': None
                })
                created_count += 1
            except Exception as e:
                logging.warning(f"Error creating user dimension entry for user_id '{user_id}': {e}")
                continue
    
    logging.info(f"Created {created_count} new user dimension entries from order data")
    logging.info("=" * 60)
    
    return created_count

def create_missing_products_from_line_items(products_df):
    """
    Create missing product dimension entries from line item data (Scenario 2 support)
    
    Extracts unique product_id values from line item data and creates minimal dim_product entries
    for any products that don't already exist in the dimension table.
    
    Args:
        products_df: DataFrame containing line item product data with product_id column
    
    Returns:
        int: Number of new product dimension entries created
    """
    if products_df is None or products_df.empty:
        logging.info("No line item product data provided for missing product extraction")
        return 0
    
    if 'product_id' not in products_df.columns:
        logging.warning("product_id column not found in line item product data")
        return 0
    
    logging.info("=" * 60)
    logging.info("CREATING MISSING PRODUCTS FROM LINE ITEM DATA (Scenario 2)")
    logging.info("=" * 60)
    
    # Extract unique product_ids from line item data
    unique_product_ids = products_df['product_id'].dropna().unique()
    unique_product_ids = [str(pid).strip() for pid in unique_product_ids if str(pid).strip()]
    
    if not unique_product_ids:
        logging.info("No valid product_ids found in line item data")
        return 0
    
    logging.info(f"Found {len(unique_product_ids)} unique product_ids in line item data")
    
    # Get existing product_ids from dim_product
    existing_product_ids = set()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT product_id FROM dim_product WHERE product_id IS NOT NULL"))
        existing_product_ids = {str(row[0]).strip() for row in result}
    
    logging.info(f"Found {len(existing_product_ids)} existing products in dim_product")
    
    # Find missing product_ids
    missing_product_ids = [pid for pid in unique_product_ids if pid not in existing_product_ids]
    
    if not missing_product_ids:
        logging.info("All products from line item data already exist in dim_product")
        return 0
    
    logging.info(f"Creating {len(missing_product_ids)} missing product dimension entries...")
    
    # Create minimal dim_product entries for missing products
    # Use NULL/default values for attributes not available in line item data
    created_count = 0
    with engine.begin() as conn:
        for product_id in missing_product_ids:
            try:
                conn.execute(text("""
                    INSERT INTO dim_product (product_id, product_name, product_type, price)
                    VALUES (:product_id, :product_name, :product_type, :price)
                    ON CONFLICT (product_id) DO NOTHING
                """), {
                    'product_id': str(product_id),
                    'product_name': None,  # Not available in line item data
                    'product_type': None,
                    'price': None
                })
                created_count += 1
            except Exception as e:
                logging.warning(f"Error creating product dimension entry for product_id '{product_id}': {e}")
                continue
    
    logging.info(f"Created {created_count} new product dimension entries from line item data")
    logging.info("=" * 60)
    
    return created_count

def ensure_unknown_campaign():
    """
    Ensure Unknown campaign exists in dim_campaign (Scenario 3 support)
    
    Creates an "Unknown" campaign entry if it doesn't exist. This is used for
    orders with missing or late-arriving campaign data.
    
    Returns:
        int: campaign_sk of the Unknown campaign
    """
    logging.info("Ensuring Unknown campaign exists in dim_campaign...")
    
    with engine.begin() as conn:
        # Check if Unknown campaign already exists
        result = conn.execute(text("""
            SELECT campaign_sk FROM dim_campaign WHERE campaign_id = 'UNKNOWN'
        """))
        row = result.fetchone()
        
        if row:
            unknown_campaign_sk = row[0]
            logging.info(f"Unknown campaign already exists with campaign_sk: {unknown_campaign_sk}")
            return unknown_campaign_sk
        
        # Create Unknown campaign
        result = conn.execute(text("""
            INSERT INTO dim_campaign (campaign_id, campaign_name, campaign_description, discount)
            VALUES ('UNKNOWN', 'Unknown Campaign', 'Placeholder for orders with missing or late-arriving campaign data', 0.0)
            ON CONFLICT (campaign_id) DO NOTHING
            RETURNING campaign_sk
        """))
        row = result.fetchone()
        
        if row:
            unknown_campaign_sk = row[0]
            logging.info(f"Created Unknown campaign with campaign_sk: {unknown_campaign_sk}")
        else:
            # If ON CONFLICT triggered, fetch the existing one
            result = conn.execute(text("""
                SELECT campaign_sk FROM dim_campaign WHERE campaign_id = 'UNKNOWN'
            """))
            row = result.fetchone()
            unknown_campaign_sk = row[0] if row else None
            logging.info(f"Unknown campaign already exists with campaign_sk: {unknown_campaign_sk}")
    
    return unknown_campaign_sk

def update_campaign_transactions_for_new_campaigns():
    """
    Update fact_campaign_transactions when late-arriving campaigns are loaded (Scenario 3 support)
    
    This function updates fact_campaign_transactions rows that currently reference the Unknown campaign
    but now have actual campaign entries in dim_campaign. It matches based on campaign_id from the
    transactional data stored in staging tables or by matching order_id patterns.
    
    Returns:
        int: Number of fact rows updated
    """
    logging.info("=" * 60)
    logging.info("UPDATING CAMPAIGN TRANSACTIONS FOR LATE-ARRIVING CAMPAIGNS (Scenario 3)")
    logging.info("=" * 60)
    
    # Get Unknown campaign_sk
    unknown_campaign_sk = ensure_unknown_campaign()
    if not unknown_campaign_sk:
        logging.warning("Unknown campaign not found, cannot update campaign transactions")
        return 0
    
    # Get all campaigns except Unknown
    campaign_map = {}
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT campaign_sk, campaign_id 
            FROM dim_campaign 
            WHERE campaign_id != 'UNKNOWN'
        """))
        for row in result:
            campaign_map[str(row[1])] = row[0]
    
    if not campaign_map:
        logging.info("No campaigns found (except Unknown), nothing to update")
        return 0
    
    logging.info(f"Found {len(campaign_map)} campaigns to match against")
    
    updated_count = 0
    
    # Query staging table for campaign_id to order_id mappings
    staging_campaign_map = {}
    with engine.connect() as conn:
        # Check if staging table exists
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'stg_marketing_department_transactional_campaign_data'
        """))
        if result.fetchone():
            # Get campaign_id to order_id mappings from staging
            result = conn.execute(text("""
                SELECT DISTINCT order_id, campaign_id
                FROM stg_marketing_department_transactional_campaign_data
                WHERE campaign_id IS NOT NULL AND campaign_id != ''
            """))
            for row in result:
                order_id = str(row[0]).strip()
                campaign_id = str(row[1]).strip()
                if order_id and campaign_id:
                    if order_id not in staging_campaign_map:
                        staging_campaign_map[order_id] = []
                    staging_campaign_map[order_id].append(campaign_id)
    
    if not staging_campaign_map:
        logging.info("No staging campaign data found - cannot update without staging data")
        logging.info("This is expected if staging tables were truncated")
        return 0
    
    logging.info(f"Found {len(staging_campaign_map)} order_id to campaign_id mappings in staging")
    
    # Update fact_campaign_transactions
    with engine.begin() as conn:
        for order_id, campaign_ids in staging_campaign_map.items():
            campaign_id = campaign_ids[0] if campaign_ids else None
            if not campaign_id or campaign_id not in campaign_map:
                continue
            
            new_campaign_sk = campaign_map[campaign_id]
            
            result = conn.execute(text("""
                UPDATE fact_campaign_transactions
                SET campaign_sk = :new_campaign_sk
                WHERE order_id = :order_id
                  AND campaign_sk = :unknown_campaign_sk
            """), {
                'order_id': order_id,
                'new_campaign_sk': new_campaign_sk,
                'unknown_campaign_sk': unknown_campaign_sk
            })
            
            rows_updated = result.rowcount
            if rows_updated > 0:
                updated_count += rows_updated
                logging.debug(f"Updated {rows_updated} fact rows for order_id '{order_id}' to campaign '{campaign_id}'")
    
    logging.info(f"Updated {updated_count} fact_campaign_transactions rows from Unknown to actual campaigns")
    logging.info("=" * 60)
    
    return updated_count

