#!/usr/bin/env python3
"""
ETL Script: Load Dimension Tables from Staging Layer
Kimball Methodology - Dimension Loading with SCD Type 2 support
"""

import os
import logging
from pathlib import Path
from urllib.parse import quote_plus
from datetime import datetime

import pandas as pd
import sqlalchemy
from sqlalchemy import text, inspect

# CONFIG via env vars
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopzada")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Build SQLAlchemy engine
password_quoted = quote_plus(DB_PASS)
engine_url = f"postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
logging.info(f"Connecting to DB: {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}")
engine = sqlalchemy.create_engine(engine_url, pool_size=5, max_overflow=10, future=True)


def log_etl_run(engine, etl_name, source_table, target_table, rows_processed, 
                rows_inserted, rows_updated, rows_failed, status, error_msg, 
                started_at, completed_at):
    """Log ETL run to etl_log table."""
    duration = (completed_at - started_at).total_seconds() if completed_at and started_at else None
    
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_log (
                etl_name, source_table, target_table, rows_processed, rows_inserted,
                rows_updated, rows_failed, status, error_message, started_at, completed_at, duration_seconds
            ) VALUES (
                :etl_name, :source_table, :target_table, :rows_processed, :rows_inserted,
                :rows_updated, :rows_failed, :status, :error_msg, :started_at, :completed_at, :duration
            )
        """), {
            "etl_name": etl_name,
            "source_table": source_table,
            "target_table": target_table,
            "rows_processed": rows_processed,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_failed": rows_failed,
            "status": status,
            "error_msg": error_msg,
            "started_at": started_at,
            "completed_at": completed_at,
            "duration": int(duration) if duration else None
        })


def load_dim_product(engine):
    """Load Product Dimension from staging tables."""
    etl_name = "load_dim_product"
    started_at = datetime.now()
    source_table = "stg_business_department_product_list_xlsx"
    
    try:
        logging.info("Loading dim_product from staging...")
        
        # Read from staging
        df = pd.read_sql_table(source_table, con=engine)
        rows_processed = len(df)
        
        # Transform and standardize
        df_dim = pd.DataFrame()
        df_dim['product_id'] = df.get('product_id', df.get('id', df.index.astype(str)))
        df_dim['product_name'] = df.get('product_name', df.get('name', ''))
        df_dim['product_category'] = df.get('category', df.get('product_category', ''))
        df_dim['product_subcategory'] = df.get('subcategory', '')
        df_dim['brand'] = df.get('brand', '')
        df_dim['unit_price'] = pd.to_numeric(df.get('price', df.get('unit_price', 0)), errors='coerce').fillna(0)
        df_dim['cost_price'] = pd.to_numeric(df.get('cost', df.get('cost_price', 0)), errors='coerce').fillna(0)
        df_dim['is_active'] = df.get('is_active', True)
        df_dim['source_system'] = 'Business Department'
        
        # Remove duplicates
        df_dim = df_dim.drop_duplicates(subset=['product_id', 'source_system'])
        
        # Upsert to dimension (SCD Type 1 - overwrite)
        rows_inserted = 0
        rows_updated = 0
        
        with engine.begin() as conn:
            for _, row in df_dim.iterrows():
                result = conn.execute(text("""
                    INSERT INTO dim_product (
                        product_id, product_name, product_category, product_subcategory,
                        brand, unit_price, cost_price, is_active, source_system, updated_at
                    ) VALUES (
                        :product_id, :product_name, :product_category, :product_subcategory,
                        :brand, :unit_price, :cost_price, :is_active, :source_system, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (product_id, source_system) 
                    DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        product_category = EXCLUDED.product_category,
                        product_subcategory = EXCLUDED.product_subcategory,
                        brand = EXCLUDED.brand,
                        unit_price = EXCLUDED.unit_price,
                        cost_price = EXCLUDED.cost_price,
                        is_active = EXCLUDED.is_active,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING product_key
                """), row.to_dict())
                
                if result.rowcount > 0:
                    if result.fetchone()[0] is not None:
                        rows_updated += 1
                    else:
                        rows_inserted += 1
        
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, source_table, "dim_product", rows_processed,
                   rows_inserted, rows_updated, 0, "success", None, started_at, completed_at)
        
        logging.info(f"✓ Loaded dim_product: {rows_inserted} inserted, {rows_updated} updated")
        return True
        
    except Exception as e:
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, source_table, "dim_product", 0, 0, 0, 0,
                   "failed", str(e), started_at, completed_at)
        logging.error(f"✗ Failed to load dim_product: {e}")
        return False


def load_dim_customer(engine):
    """Load Customer Dimension from staging tables."""
    etl_name = "load_dim_customer"
    started_at = datetime.now()
    
    try:
        logging.info("Loading dim_customer from staging...")
        
        # Read from multiple staging tables
        df_user = pd.read_sql_table("stg_customer_management_department_user_data_json", con=engine)
        df_job = pd.read_sql_table("stg_customer_management_department_user_job_csv", con=engine)
        df_cc = pd.read_sql_table("stg_customer_management_department_user_credit_card_pickle", con=engine)
        
        rows_processed = len(df_user)
        
        # Merge customer data
        df_dim = df_user.copy()
        
        # Merge job data
        if 'user_id' in df_job.columns:
            df_dim = df_dim.merge(df_job, on='user_id', how='left', suffixes=('', '_job'))
        
        # Merge credit card data
        if 'user_id' in df_cc.columns:
            df_cc_agg = df_cc.groupby('user_id').agg({
                'credit_card_type': 'first',
                'credit_card_number': lambda x: str(x.iloc[0])[-4:] if len(str(x.iloc[0])) >= 4 else ''
            }).reset_index()
            df_dim = df_dim.merge(df_cc_agg, on='user_id', how='left')
        
        # Standardize columns
        df_dim_clean = pd.DataFrame()
        df_dim_clean['customer_id'] = df_dim.get('user_id', df_dim.get('id', df_dim.index.astype(str)))
        df_dim_clean['first_name'] = df_dim.get('first_name', df_dim.get('fname', ''))
        df_dim_clean['last_name'] = df_dim.get('last_name', df_dim.get('lname', ''))
        df_dim_clean['email'] = df_dim.get('email', '')
        df_dim_clean['phone'] = df_dim.get('phone', '')
        df_dim_clean['date_of_birth'] = pd.to_datetime(df_dim.get('date_of_birth', df_dim.get('dob')), errors='coerce')
        df_dim_clean['gender'] = df_dim.get('gender', '')
        df_dim_clean['job_title'] = df_dim.get('job_title', '')
        df_dim_clean['job_category'] = df_dim.get('job_category', '')
        df_dim_clean['credit_card_type'] = df_dim.get('credit_card_type', '')
        df_dim_clean['credit_card_last4'] = df_dim.get('credit_card_number', '')
        df_dim_clean['registration_date'] = pd.to_datetime(df_dim.get('registration_date', df_dim.get('created_at')), errors='coerce')
        df_dim_clean['is_active'] = True
        df_dim_clean['source_system'] = 'Customer Management Department'
        
        df_dim_clean = df_dim_clean.drop_duplicates(subset=['customer_id', 'source_system'])
        
        rows_inserted = 0
        rows_updated = 0
        
        with engine.begin() as conn:
            for _, row in df_dim_clean.iterrows():
                result = conn.execute(text("""
                    INSERT INTO dim_customer (
                        customer_id, first_name, last_name, email, phone, date_of_birth,
                        gender, job_title, job_category, credit_card_type, credit_card_last4,
                        registration_date, is_active, source_system, updated_at
                    ) VALUES (
                        :customer_id, :first_name, :last_name, :email, :phone, :date_of_birth,
                        :gender, :job_title, :job_category, :credit_card_type, :credit_card_last4,
                        :registration_date, :is_active, :source_system, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (customer_id, source_system)
                    DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        email = EXCLUDED.email,
                        phone = EXCLUDED.phone,
                        date_of_birth = EXCLUDED.date_of_birth,
                        gender = EXCLUDED.gender,
                        job_title = EXCLUDED.job_title,
                        job_category = EXCLUDED.job_category,
                        credit_card_type = EXCLUDED.credit_card_type,
                        credit_card_last4 = EXCLUDED.credit_card_last4,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING customer_key
                """), row.to_dict())
                
                if result.rowcount > 0:
                    rows_inserted += 1
        
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, "stg_customer_*", "dim_customer", rows_processed,
                   rows_inserted, rows_updated, 0, "success", None, started_at, completed_at)
        
        logging.info(f"✓ Loaded dim_customer: {rows_inserted} inserted, {rows_updated} updated")
        return True
        
    except Exception as e:
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, "stg_customer_*", "dim_customer", 0, 0, 0, 0,
                   "failed", str(e), started_at, completed_at)
        logging.error(f"✗ Failed to load dim_customer: {e}")
        return False


def load_dim_merchant(engine):
    """Load Merchant Dimension from staging tables."""
    etl_name = "load_dim_merchant"
    started_at = datetime.now()
    source_table = "stg_enterprise_department_merchant_data_html"
    
    try:
        logging.info("Loading dim_merchant from staging...")
        
        df = pd.read_sql_table(source_table, con=engine)
        rows_processed = len(df)
        
        df_dim = pd.DataFrame()
        df_dim['merchant_id'] = df.get('merchant_id', df.get('id', df.index.astype(str)))
        df_dim['merchant_name'] = df.get('merchant_name', df.get('name', ''))
        df_dim['merchant_type'] = df.get('merchant_type', df.get('type', ''))
        df_dim['merchant_category'] = df.get('category', '')
        df_dim['country'] = df.get('country', '')
        df_dim['city'] = df.get('city', '')
        df_dim['is_active'] = True
        df_dim['source_system'] = 'Enterprise Department'
        
        df_dim = df_dim.drop_duplicates(subset=['merchant_id', 'source_system'])
        
        rows_inserted = 0
        
        with engine.begin() as conn:
            for _, row in df_dim.iterrows():
                result = conn.execute(text("""
                    INSERT INTO dim_merchant (
                        merchant_id, merchant_name, merchant_type, merchant_category,
                        country, city, is_active, source_system, updated_at
                    ) VALUES (
                        :merchant_id, :merchant_name, :merchant_type, :merchant_category,
                        :country, :city, :is_active, :source_system, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (merchant_id, source_system)
                    DO UPDATE SET
                        merchant_name = EXCLUDED.merchant_name,
                        merchant_type = EXCLUDED.merchant_type,
                        merchant_category = EXCLUDED.merchant_category,
                        country = EXCLUDED.country,
                        city = EXCLUDED.city,
                        updated_at = CURRENT_TIMESTAMP
                """), row.to_dict())
                
                if result.rowcount > 0:
                    rows_inserted += 1
        
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, source_table, "dim_merchant", rows_processed,
                   rows_inserted, 0, 0, "success", None, started_at, completed_at)
        
        logging.info(f"✓ Loaded dim_merchant: {rows_inserted} inserted")
        return True
        
    except Exception as e:
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, source_table, "dim_merchant", 0, 0, 0, 0,
                   "failed", str(e), started_at, completed_at)
        logging.error(f"✗ Failed to load dim_merchant: {e}")
        return False


def load_dim_staff(engine):
    """Load Staff Dimension from staging tables."""
    etl_name = "load_dim_staff"
    started_at = datetime.now()
    source_table = "stg_enterprise_department_staff_data_html"
    
    try:
        logging.info("Loading dim_staff from staging...")
        
        df = pd.read_sql_table(source_table, con=engine)
        rows_processed = len(df)
        
        df_dim = pd.DataFrame()
        df_dim['staff_id'] = df.get('staff_id', df.get('id', df.index.astype(str)))
        df_dim['staff_name'] = df.get('staff_name', df.get('name', ''))
        df_dim['department'] = df.get('department', '')
        df_dim['position'] = df.get('position', df.get('job_title', ''))
        df_dim['hire_date'] = pd.to_datetime(df.get('hire_date', df.get('start_date')), errors='coerce')
        df_dim['is_active'] = True
        df_dim['source_system'] = 'Enterprise Department'
        
        df_dim = df_dim.drop_duplicates(subset=['staff_id', 'source_system'])
        
        rows_inserted = 0
        
        with engine.begin() as conn:
            for _, row in df_dim.iterrows():
                result = conn.execute(text("""
                    INSERT INTO dim_staff (
                        staff_id, staff_name, department, position, hire_date,
                        is_active, source_system, updated_at
                    ) VALUES (
                        :staff_id, :staff_name, :department, :position, :hire_date,
                        :is_active, :source_system, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (staff_id, source_system)
                    DO UPDATE SET
                        staff_name = EXCLUDED.staff_name,
                        department = EXCLUDED.department,
                        position = EXCLUDED.position,
                        hire_date = EXCLUDED.hire_date,
                        updated_at = CURRENT_TIMESTAMP
                """), row.to_dict())
                
                if result.rowcount > 0:
                    rows_inserted += 1
        
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, source_table, "dim_staff", rows_processed,
                   rows_inserted, 0, 0, "success", None, started_at, completed_at)
        
        logging.info(f"✓ Loaded dim_staff: {rows_inserted} inserted")
        return True
        
    except Exception as e:
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, source_table, "dim_staff", 0, 0, 0, 0,
                   "failed", str(e), started_at, completed_at)
        logging.error(f"✗ Failed to load dim_staff: {e}")
        return False


def load_dim_campaign(engine):
    """Load Campaign Dimension from staging tables."""
    etl_name = "load_dim_campaign"
    started_at = datetime.now()
    source_table = "stg_marketing_department_campaign_data_csv"
    
    try:
        logging.info("Loading dim_campaign from staging...")
        
        df = pd.read_sql_table(source_table, con=engine)
        rows_processed = len(df)
        
        df_dim = pd.DataFrame()
        df_dim['campaign_id'] = df.get('campaign_id', df.get('id', df.index.astype(str)))
        df_dim['campaign_name'] = df.get('campaign_name', df.get('name', ''))
        df_dim['campaign_type'] = df.get('campaign_type', df.get('type', ''))
        df_dim['start_date'] = pd.to_datetime(df.get('start_date', df.get('start')), errors='coerce')
        df_dim['end_date'] = pd.to_datetime(df.get('end_date', df.get('end')), errors='coerce')
        df_dim['budget'] = pd.to_numeric(df.get('budget', 0), errors='coerce').fillna(0)
        df_dim['is_active'] = True
        df_dim['source_system'] = 'Marketing Department'
        
        df_dim = df_dim.drop_duplicates(subset=['campaign_id', 'source_system'])
        
        rows_inserted = 0
        
        with engine.begin() as conn:
            for _, row in df_dim.iterrows():
                result = conn.execute(text("""
                    INSERT INTO dim_campaign (
                        campaign_id, campaign_name, campaign_type, start_date, end_date,
                        budget, is_active, source_system, updated_at
                    ) VALUES (
                        :campaign_id, :campaign_name, :campaign_type, :start_date, :end_date,
                        :budget, :is_active, :source_system, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (campaign_id, source_system)
                    DO UPDATE SET
                        campaign_name = EXCLUDED.campaign_name,
                        campaign_type = EXCLUDED.campaign_type,
                        start_date = EXCLUDED.start_date,
                        end_date = EXCLUDED.end_date,
                        budget = EXCLUDED.budget,
                        updated_at = CURRENT_TIMESTAMP
                """), row.to_dict())
                
                if result.rowcount > 0:
                    rows_inserted += 1
        
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, source_table, "dim_campaign", rows_processed,
                   rows_inserted, 0, 0, "success", None, started_at, completed_at)
        
        logging.info(f"✓ Loaded dim_campaign: {rows_inserted} inserted")
        return True
        
    except Exception as e:
        completed_at = datetime.now()
        log_etl_run(engine, etl_name, source_table, "dim_campaign", 0, 0, 0, 0,
                   "failed", str(e), started_at, completed_at)
        logging.error(f"✗ Failed to load dim_campaign: {e}")
        return False


def main():
    """Main ETL function to load all dimensions."""
    logging.info("=" * 60)
    logging.info("ShopZada ETL: Loading Dimension Tables")
    logging.info("=" * 60)
    
    results = {}
    results['dim_product'] = load_dim_product(engine)
    results['dim_customer'] = load_dim_customer(engine)
    results['dim_merchant'] = load_dim_merchant(engine)
    results['dim_staff'] = load_dim_staff(engine)
    results['dim_campaign'] = load_dim_campaign(engine)
    
    logging.info("=" * 60)
    logging.info("ETL Summary:")
    logging.info("=" * 60)
    
    successful = [k for k, v in results.items() if v]
    failed = [k for k, v in results.items() if not v]
    
    logging.info(f"✓ Successfully loaded: {len(successful)}/{len(results)} dimensions")
    for dim in successful:
        logging.info(f"  - {dim}")
    
    if failed:
        logging.warning(f"✗ Failed to load: {len(failed)} dimensions")
        for dim in failed:
            logging.warning(f"  - {dim}")
    
    logging.info("=" * 60)


if __name__ == "__main__":
    main()

