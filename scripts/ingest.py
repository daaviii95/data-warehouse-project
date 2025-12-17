#!/usr/bin/env python3
"""
Ingest Data Per Department into Staging Tables
Loads raw data files from each department into corresponding staging tables
Direct ingestion from source files (CSV, JSON, HTML, Excel, Pickle, Parquet, etc.)
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from urllib.parse import quote_plus
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

# Import shared utilities from etl_pipeline_python
sys.path.insert(0, os.path.dirname(__file__))
from etl_pipeline_python import (
    load_file, find_files, clean_dataframe,
    parse_discount, extract_numeric, clean_value,
    format_product_type, format_name, format_address,
    format_phone_number, format_gender, format_user_type,
    format_job_title, format_job_level, format_credit_card_number,
    format_issuing_bank, format_campaign_description, format_product_name,
    format_campaign_name
)

# Configuration
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopzada")
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
# Set to True to force full reload (ignore processed files tracking)
FORCE_FULL_RELOAD = os.getenv("FORCE_FULL_RELOAD", "false").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Build SQLAlchemy engine
password_quoted = quote_plus(DB_PASS)
engine_url = f"postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(engine_url, pool_size=5, max_overflow=10, future=True)

def _get_processed_files(table_name):
    """Get set of file paths that have already been processed for a staging table"""
    if FORCE_FULL_RELOAD:
        return set()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT DISTINCT file_source 
                FROM {table_name}
                WHERE file_source IS NOT NULL
            """))
            return {row[0] for row in result}
    except Exception as e:
        # Table might not exist or be empty, return empty set
        logging.debug(f"Could not get processed files for {table_name}: {e}")
        return set()

def _should_process_file(file_path, table_name, processed_files):
    """
    Determine if a file should be processed
    
    Args:
        file_path: Path to the file
        table_name: Staging table name
        processed_files: Set of already processed file paths
    
    Returns:
        tuple: (should_process: bool, reason: str)
    """
    if FORCE_FULL_RELOAD:
        return True, "FORCE_FULL_RELOAD is enabled"
    
    file_path_str = str(file_path)
    
    # Check if file has been processed
    if file_path_str in processed_files:
        # Check if file was modified since last ingestion
        try:
            file_mtime = os.path.getmtime(file_path)
            file_mtime_dt = datetime.fromtimestamp(file_mtime)
            
            # Get last ingestion time for this file
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT MAX(loaded_at) 
                    FROM {table_name}
                    WHERE file_source = :file_source
                """), {'file_source': file_path_str})
                row = result.fetchone()
                
                if row and row[0]:
                    last_loaded = row[0]
                    if isinstance(last_loaded, str):
                        last_loaded = datetime.fromisoformat(last_loaded.replace('Z', '+00:00'))
                    
                    # If file was modified after last load, re-process it
                    if file_mtime_dt > last_loaded.replace(tzinfo=None):
                        return True, f"File modified after last ingestion (modified: {file_mtime_dt}, last loaded: {last_loaded})"
                    else:
                        return False, f"File already processed and not modified (last loaded: {last_loaded})"
                else:
                    return False, "File already processed (no timestamp available)"
        except Exception as e:
            logging.warning(f"Could not check file modification time for {file_path}: {e}")
            # If we can't check, assume it needs processing (safer)
            return True, f"Could not verify file status, will process: {e}"
    
    return True, "File not yet processed"

def _check_staging_tables():
    """Check if staging tables exist and validate schema matches physical model"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'stg_marketing_department_campaign_data'
        """))
        if result.fetchone()[0] == 0:
            logging.error("Staging tables not created! Please run create_staging_tables task first.")
            raise RuntimeError("Staging tables do not exist")
        
        # Check if availed column is INTEGER (per PHYSICALMODEL.txt) or BOOLEAN (old schema)
        result = conn.execute(text("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'stg_marketing_department_transactional_campaign_data'
            AND column_name = 'availed'
        """))
        row = result.fetchone()
        if row and row[0].upper() != 'INTEGER':
            logging.warning(f"WARNING: stg_marketing_department_transactional_campaign_data.availed is {row[0]}, but should be INTEGER per PHYSICALMODEL.txt")
            logging.warning("Please drop and recreate staging tables using sql/00_create_staging_tables.sql")

def _format_dataframe_for_staging(df, table_name):
    """Format DataFrame data to match staging table data types before insertion"""
    if df is None or df.empty:
        return df
    
    df = df.copy()
    
    # Format based on table name
    if table_name == 'stg_marketing_department_campaign_data':
        # Drop transaction_date if it exists (not in staging table schema)
        if 'transaction_date' in df.columns:
            df = df.drop(columns=['transaction_date'], errors='ignore')
        if 'discount' in df.columns:
            df['discount'] = df['discount'].apply(parse_discount)
        if 'campaign_name' in df.columns:
            df['campaign_name'] = df['campaign_name'].apply(lambda x: format_campaign_name(clean_value(x)))
        if 'campaign_description' in df.columns:
            df['campaign_description'] = df['campaign_description'].apply(lambda x: format_campaign_description(clean_value(x)))
    
    elif table_name == 'stg_marketing_department_transactional_campaign_data':
        if 'availed' in df.columns:
            def convert_availed(val):
                if pd.isna(val) or val is None:
                    return None
                if isinstance(val, (int, float)):
                    return int(val)
                str_val = str(val).strip().lower()
                if str_val in ('1', 'true', 'yes'):
                    return 1
                return 0
            df['availed'] = df['availed'].apply(convert_availed)
    
    elif table_name == 'stg_operations_department_order_data':
        if 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce').dt.date
        if 'total_amount' in df.columns:
            df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
        if 'estimated_arrival_days' in df.columns:
            df['estimated_arrival_days'] = df['estimated_arrival_days'].apply(extract_numeric)
        elif 'estimated_arrival' in df.columns:
            df['estimated_arrival_days'] = df['estimated_arrival'].apply(extract_numeric)
            if 'estimated_arrival_days' in df.columns:
                df = df.drop(columns=['estimated_arrival'], errors='ignore')
    
    elif table_name == 'stg_operations_department_line_item_data_prices':
        if 'quantity' in df.columns:
            df['quantity'] = df['quantity'].apply(extract_numeric)
        if 'discount' in df.columns:
            df['discount'] = df['discount'].apply(parse_discount)
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    elif table_name == 'stg_operations_department_line_item_data_products':
        # Per PHYSICALMODEL.txt: fact_line_items requires product_sk, which comes from product_id
        # Ensure product_id is properly formatted and present
        if 'product_id' in df.columns:
            # Clean and format product_id (should be VARCHAR(50) per staging schema)
            df['product_id'] = df['product_id'].apply(lambda x: str(clean_value(x)) if pd.notna(x) else None)
        elif 'product' in df.columns:
            # Handle potential column name variation
            df['product_id'] = df['product'].apply(lambda x: str(clean_value(x)) if pd.notna(x) else None)
            df = df.drop(columns=['product'], errors='ignore')
        
        # Ensure order_id is properly formatted
        if 'order_id' in df.columns:
            df['order_id'] = df['order_id'].apply(lambda x: str(clean_value(x)) if pd.notna(x) else None)
        
        # Ensure line_item_id is properly formatted
        # Note: line_item_id may not be in source files - it will be obtained from prices table during fact loading
        # But staging table requires it, so generate a placeholder if missing
        if 'line_item_id' in df.columns:
            df['line_item_id'] = df['line_item_id'].apply(lambda x: str(clean_value(x)) if pd.notna(x) else None)
        else:
            # Generate synthetic line_item_id based on index (will be replaced during fact loading merge with prices)
            logging.debug(f"line_item_id not found in source, generating placeholder values")
            df['line_item_id'] = df.index.astype(str)
        
        # Log warning if product_id is missing (critical for fact_line_items per PHYSICALMODEL.txt)
        if 'product_id' not in df.columns or df['product_id'].isna().all():
            logging.warning(f"WARNING: product_id is missing or all NULL in {table_name}. This will cause fact_line_items loading to fail per PHYSICALMODEL.txt requirement.")
        else:
            null_count = df['product_id'].isna().sum()
            if null_count > 0:
                logging.warning(f"WARNING: {null_count} rows have NULL product_id in {table_name}. These will be skipped during fact_line_items loading.")
            else:
                logging.info(f"All {len(df)} rows have product_id populated in {table_name}")
    
    elif table_name == 'stg_operations_department_order_delays':
        if 'delay_days' in df.columns:
            df['delay_days'] = df['delay_days'].apply(extract_numeric)
        elif 'delay_in_days' in df.columns:
            df['delay_days'] = df['delay_in_days'].apply(extract_numeric)
            if 'delay_days' in df.columns:
                df = df.drop(columns=['delay_in_days'], errors='ignore')
    
    elif table_name == 'stg_business_department_product_list':
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
        if 'product_type' in df.columns:
            df['product_type'] = df['product_type'].apply(format_product_type)
        if 'product_name' in df.columns:
            df['product_name'] = df['product_name'].apply(lambda x: format_product_name(clean_value(x)))
    
    elif table_name == 'stg_customer_management_department_user_data':
        if 'birthdate' in df.columns:
            df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce').dt.date
        if 'creation_date' in df.columns:
            df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')
        if 'name' in df.columns:
            df['name'] = df['name'].apply(lambda x: format_name(clean_value(x)))
        if 'street' in df.columns:
            df['street'] = df['street'].apply(lambda x: format_address(clean_value(x), 'street'))
        if 'gender' in df.columns:
            df['gender'] = df['gender'].apply(format_gender)
        if 'user_type' in df.columns:
            df['user_type'] = df['user_type'].apply(format_user_type)
    
    elif table_name == 'stg_customer_management_department_user_job':
        if 'job_title' in df.columns:
            df['job_title'] = df['job_title'].apply(lambda x: format_job_title(clean_value(x)))
        if 'job_level' in df.columns:
            # Clean [null] strings and actual NULL values
            def clean_job_level(val):
                if pd.isna(val) or val is None:
                    return None
                val_str = str(val).strip()
                if val_str.lower() in ['[null]', 'null', 'none', '']:
                    return None
                return format_job_level(val_str)
            df['job_level'] = df['job_level'].apply(clean_job_level)
    
    elif table_name == 'stg_customer_management_department_user_credit_card':
        if 'name' in df.columns:
            df['name'] = df['name'].apply(lambda x: format_name(clean_value(x)))
        if 'credit_card_number' in df.columns:
            df['credit_card_number'] = df['credit_card_number'].apply(lambda x: format_credit_card_number(clean_value(x)))
        if 'issuing_bank' in df.columns:
            df['issuing_bank'] = df['issuing_bank'].apply(lambda x: format_issuing_bank(clean_value(x)))
    
    elif table_name == 'stg_enterprise_department_merchant_data':
        if 'name' in df.columns:
            df['name'] = df['name'].apply(lambda x: format_name(clean_value(x)))
        if 'street' in df.columns:
            df['street'] = df['street'].apply(lambda x: format_address(clean_value(x), 'street'))
        if 'contact_number' in df.columns:
            df['contact_number'] = df['contact_number'].apply(lambda x: format_phone_number(clean_value(x)))
        if 'creation_date' in df.columns:
            df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')
    
    elif table_name == 'stg_enterprise_department_staff_data':
        if 'name' in df.columns:
            df['name'] = df['name'].apply(lambda x: format_name(clean_value(x)))
        if 'street' in df.columns:
            df['street'] = df['street'].apply(lambda x: format_address(clean_value(x), 'street'))
        if 'contact_number' in df.columns:
            df['contact_number'] = df['contact_number'].apply(lambda x: format_phone_number(clean_value(x)))
        if 'creation_date' in df.columns:
            df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')
    
    elif table_name == 'stg_enterprise_department_order_with_merchant_data':
        # Per PHYSICALMODEL.txt: fact_orders requires staff_sk, which comes from staff_id
        # Ensure staff_id is properly formatted and present
        if 'staff_id' in df.columns:
            # Clean and format staff_id (should be VARCHAR(50) per staging schema)
            df['staff_id'] = df['staff_id'].apply(lambda x: str(clean_value(x)) if pd.notna(x) else None)
        elif 'staff' in df.columns:
            # Handle potential column name variation
            df['staff_id'] = df['staff'].apply(lambda x: str(clean_value(x)) if pd.notna(x) else None)
            df = df.drop(columns=['staff'], errors='ignore')
        
        # Ensure merchant_id is properly formatted
        if 'merchant_id' in df.columns:
            df['merchant_id'] = df['merchant_id'].apply(lambda x: str(clean_value(x)) if pd.notna(x) else None)
        
        # Ensure order_id is properly formatted
        if 'order_id' in df.columns:
            df['order_id'] = df['order_id'].apply(lambda x: str(clean_value(x)) if pd.notna(x) else None)
        
        # Log warning if staff_id is missing (critical for fact_orders per PHYSICALMODEL.txt)
        if 'staff_id' not in df.columns or df['staff_id'].isna().all():
            logging.warning(f"WARNING: staff_id is missing or all NULL in {table_name}. This will cause fact_orders loading to fail per PHYSICALMODEL.txt requirement.")
        else:
            null_count = df['staff_id'].isna().sum()
            if null_count > 0:
                logging.warning(f"WARNING: {null_count} rows have NULL staff_id in {table_name}. These will be skipped during fact_orders loading.")
            else:
                logging.info(f"All {len(df)} rows have staff_id populated in {table_name}")
    
    # Filter columns to match staging table schema
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = :table_name
            AND column_name NOT IN ('file_source', 'loaded_at')
        """), {'table_name': table_name})
        staging_info = {row[0]: row[1] for row in result}
        staging_columns = list(staging_info.keys())
    
    # Filter DataFrame to only include columns that exist in staging table
    available_columns = [col for col in df.columns if col in staging_columns]
    
    if not available_columns:
        logging.warning(f"No matching columns found for {table_name}. Available: {list(df.columns)}, Expected: {staging_columns}")
        return None
    
    # Select only matching columns
    filtered_df = df[available_columns].copy()
    
    # Explicitly remove transaction_date if it somehow still exists (shouldn't be in staging table)
    if 'transaction_date' in filtered_df.columns and 'transaction_date' not in staging_columns:
        filtered_df = filtered_df.drop(columns=['transaction_date'], errors='ignore')
        logging.debug(f"Removed transaction_date column from {table_name} (not in staging schema)")
    
    # Handle data type mismatches - convert data to match actual database schema
    # This is a workaround if staging tables have old schema
    # Note: Data should be formatted per PHYSICALMODEL.txt, but we handle mismatches here
    for col in filtered_df.columns:
        if col in staging_info:
            db_type = staging_info[col].upper()
            if db_type == 'BOOLEAN' and col == 'availed':
                # Convert integer to boolean if table has boolean type (old schema)
                logging.warning(f"Converting {col} from INTEGER to BOOLEAN to match database schema. Table should be recreated with INTEGER type per PHYSICALMODEL.txt")
                filtered_df[col] = filtered_df[col].apply(lambda x: bool(x) if pd.notna(x) else None)
            elif db_type in ('INTEGER', 'BIGINT', 'SMALLINT') and filtered_df[col].dtype == 'object':
                # Convert string to numeric for integer columns
                filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce').astype('Int64')
    
    # Log any dropped columns
    dropped_columns = [col for col in df.columns if col not in staging_columns and col not in ['file_source', 'loaded_at']]
    if dropped_columns:
        logging.info(f"Dropped columns not in staging table {table_name}: {dropped_columns}")
    
    # Final validation: ensure no unexpected columns
    final_columns = [col for col in filtered_df.columns if col not in staging_columns and col not in ['file_source', 'loaded_at']]
    if final_columns:
        logging.warning(f"WARNING: {table_name} still has unexpected columns after filtering: {final_columns}. These will cause insertion errors.")
        filtered_df = filtered_df[[col for col in filtered_df.columns if col in staging_columns or col in ['file_source', 'loaded_at']]]
    
    return filtered_df

def ingest_marketing_department():
    """Ingest Marketing Department data into staging tables"""
    _check_staging_tables()
    logging.info("=" * 60)
    logging.info("INGESTING: Marketing Department")
    if FORCE_FULL_RELOAD:
        logging.info("FORCE_FULL_RELOAD enabled - will process all files")
    logging.info("=" * 60)
    
    # Get processed files for tracking
    processed_campaign = _get_processed_files('stg_marketing_department_campaign_data')
    processed_transactional = _get_processed_files('stg_marketing_department_transactional_campaign_data')
    
    # Campaign data
    campaign_files = find_files("*campaign_data*", DATA_DIR)
    # Filter out transactional files
    campaign_files = [f for f in campaign_files if 'transactional' not in str(f).lower()]
    
    processed_count = 0
    skipped_count = 0
    for file_path in campaign_files:
        should_process, reason = _should_process_file(file_path, 'stg_marketing_department_campaign_data', processed_campaign)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading campaign data from: {file_path} ({reason})")
        # load_file will automatically prefer parquet if available and up to date
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            # Format and filter data for staging table
            df = _format_dataframe_for_staging(df, 'stg_marketing_department_campaign_data')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                # Insert into staging table
                df.to_sql('stg_marketing_department_campaign_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_marketing_department_campaign_data")
                processed_count += 1
    
    logging.info(f"Campaign data: {processed_count} files processed, {skipped_count} files skipped")
    
    # Transactional campaign data
    transactional_files = find_files("*transactional_campaign_data*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in transactional_files:
        should_process, reason = _should_process_file(file_path, 'stg_marketing_department_transactional_campaign_data', processed_transactional)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading transactional campaign data from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_marketing_department_transactional_campaign_data')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_marketing_department_transactional_campaign_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_marketing_department_transactional_campaign_data")
                processed_count += 1
    
    logging.info(f"Transactional campaign data: {processed_count} files processed, {skipped_count} files skipped")

def ingest_operations_department():
    """Ingest Operations Department data into staging tables"""
    _check_staging_tables()
    logging.info("=" * 60)
    logging.info("INGESTING: Operations Department")
    if FORCE_FULL_RELOAD:
        logging.info("FORCE_FULL_RELOAD enabled - will process all files")
    logging.info("=" * 60)
    
    # Get processed files for tracking
    processed_orders = _get_processed_files('stg_operations_department_order_data')
    processed_prices = _get_processed_files('stg_operations_department_line_item_data_prices')
    processed_products = _get_processed_files('stg_operations_department_line_item_data_products')
    processed_delays = _get_processed_files('stg_operations_department_order_delays')
    
    # Order data
    order_files = find_files("*order_data*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in order_files:
        should_process, reason = _should_process_file(file_path, 'stg_operations_department_order_data', processed_orders)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading order data from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_operations_department_order_data')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_operations_department_order_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_operations_department_order_data")
                processed_count += 1
    
    logging.info(f"Order data: {processed_count} files processed, {skipped_count} files skipped")
    
    # Line item prices
    prices_files = find_files("*line_item_data_prices*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in prices_files:
        should_process, reason = _should_process_file(file_path, 'stg_operations_department_line_item_data_prices', processed_prices)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading line item prices from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_operations_department_line_item_data_prices')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_operations_department_line_item_data_prices', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_operations_department_line_item_data_prices")
                processed_count += 1
    
    logging.info(f"Line item prices: {processed_count} files processed, {skipped_count} files skipped")
    
    # Line item products
    # Per PHYSICALMODEL.txt: fact_line_items requires product_sk, which comes from product_id in this table
    products_files = find_files("*line_item_data_products*", DATA_DIR)
    total_loaded = 0
    processed_count = 0
    skipped_count = 0
    for file_path in products_files:
        should_process, reason = _should_process_file(file_path, 'stg_operations_department_line_item_data_products', processed_products)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading line item products from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            # Log source columns for debugging
            logging.debug(f"  Source columns: {list(df.columns)}")
            df = _format_dataframe_for_staging(df, 'stg_operations_department_line_item_data_products')
            if df is not None and not df.empty:
                # line_item_id may not be in source files - it will come from prices table during fact loading
                # Only validate order_id and product_id which are essential
                required_cols = ['order_id', 'product_id']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logging.error(f"  ERROR: Missing required columns {missing_cols} in {file_path}. Required per PHYSICALMODEL.txt for fact_line_items.")
                else:
                    # If line_item_id is missing, log a warning but don't fail
                    if 'line_item_id' not in df.columns:
                        logging.warning(f"  WARNING: line_item_id not found in {file_path}. It will be obtained from line_item_data_prices during fact loading.")
                    # Check for NULL product_id values
                    null_product_count = df['product_id'].isna().sum() if 'product_id' in df.columns else len(df)
                    if null_product_count > 0:
                        logging.warning(f"  WARNING: {null_product_count} rows have NULL product_id (will be skipped in fact_line_items)")
                    
                df['file_source'] = str(file_path)
                df.to_sql('stg_operations_department_line_item_data_products', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                total_loaded += len(df)
                logging.info(f"  Loaded {len(df)} rows into stg_operations_department_line_item_data_products")
                logging.info(f"  Columns: {list(df.columns)}")
                processed_count += 1
    
    logging.info(f"Line item products: {processed_count} files processed, {skipped_count} files skipped")
    if total_loaded > 0:
        logging.info(f"Total loaded: {total_loaded} line item product records (product_id required per PHYSICALMODEL.txt)")
    
    # Order delays
    delays_files = find_files("*order_delays*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in delays_files:
        should_process, reason = _should_process_file(file_path, 'stg_operations_department_order_delays', processed_delays)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading order delays from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_operations_department_order_delays')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_operations_department_order_delays', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_operations_department_order_delays")
                processed_count += 1
    
    logging.info(f"Order delays: {processed_count} files processed, {skipped_count} files skipped")

def ingest_business_department():
    """Ingest Business Department data into staging tables"""
    _check_staging_tables()
    logging.info("=" * 60)
    logging.info("INGESTING: Business Department")
    if FORCE_FULL_RELOAD:
        logging.info("FORCE_FULL_RELOAD enabled - will process all files")
    logging.info("=" * 60)
    
    # Get processed files for tracking
    processed_products = _get_processed_files('stg_business_department_product_list')
    
    # Product list
    product_files = find_files("*product_list*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in product_files:
        should_process, reason = _should_process_file(file_path, 'stg_business_department_product_list', processed_products)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading product list from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_business_department_product_list')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_business_department_product_list', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_business_department_product_list")
                processed_count += 1
    
    logging.info(f"Product list: {processed_count} files processed, {skipped_count} files skipped")

def ingest_customer_management_department():
    """Ingest Customer Management Department data into staging tables"""
    _check_staging_tables()
    logging.info("=" * 60)
    logging.info("INGESTING: Customer Management Department")
    if FORCE_FULL_RELOAD:
        logging.info("FORCE_FULL_RELOAD enabled - will process all files")
    logging.info("=" * 60)
    
    # Get processed files for tracking
    processed_users = _get_processed_files('stg_customer_management_department_user_data')
    processed_jobs = _get_processed_files('stg_customer_management_department_user_job')
    processed_credit_cards = _get_processed_files('stg_customer_management_department_user_credit_card')
    
    # User data
    user_files = find_files("*user_data*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in user_files:
        should_process, reason = _should_process_file(file_path, 'stg_customer_management_department_user_data', processed_users)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading user data from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_customer_management_department_user_data')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_customer_management_department_user_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_customer_management_department_user_data")
                processed_count += 1
    
    logging.info(f"User data: {processed_count} files processed, {skipped_count} files skipped")
    
    # User job data
    user_job_files = find_files("*user_job*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in user_job_files:
        should_process, reason = _should_process_file(file_path, 'stg_customer_management_department_user_job', processed_jobs)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading user job data from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_customer_management_department_user_job')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_customer_management_department_user_job', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_customer_management_department_user_job")
                processed_count += 1
    
    logging.info(f"User job data: {processed_count} files processed, {skipped_count} files skipped")
    
    # User credit card data
    credit_card_files = find_files("*user_credit_card*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in credit_card_files:
        should_process, reason = _should_process_file(file_path, 'stg_customer_management_department_user_credit_card', processed_credit_cards)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading user credit card data from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            # Log source columns for debugging
            logging.debug(f"  Source columns: {list(df.columns)}")
            df = _format_dataframe_for_staging(df, 'stg_customer_management_department_user_credit_card')
            if df is not None and not df.empty:
                # Validate name column is present
                if 'name' not in df.columns:
                    logging.warning(f"  WARNING: 'name' column missing in {file_path}. This will cause dim_credit_card.name to be NULL.")
                else:
                    null_name_count = df['name'].isna().sum()
                    if null_name_count > 0:
                        logging.warning(f"  WARNING: {null_name_count} rows have NULL name in {file_path}")
                    else:
                        logging.info(f"  All {len(df)} rows have name populated")
                
                df['file_source'] = str(file_path)
                df.to_sql('stg_customer_management_department_user_credit_card', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_customer_management_department_user_credit_card")
                logging.info(f"  Columns loaded: {list(df.columns)}")
                processed_count += 1
    
    logging.info(f"User credit card data: {processed_count} files processed, {skipped_count} files skipped")

def ingest_enterprise_department():
    """Ingest Enterprise Department data into staging tables"""
    _check_staging_tables()
    logging.info("=" * 60)
    logging.info("INGESTING: Enterprise Department")
    if FORCE_FULL_RELOAD:
        logging.info("FORCE_FULL_RELOAD enabled - will process all files")
    logging.info("=" * 60)
    
    # Get processed files for tracking
    processed_merchants = _get_processed_files('stg_enterprise_department_merchant_data')
    processed_staff = _get_processed_files('stg_enterprise_department_staff_data')
    processed_order_merchant = _get_processed_files('stg_enterprise_department_order_with_merchant_data')
    
    # Merchant data
    merchant_files = find_files("*merchant_data*", DATA_DIR)
    # Exclude order_with_merchant_data files (they only have merchant_id)
    merchant_files = [f for f in merchant_files if 'order_with_merchant_data' not in str(f)]
    processed_count = 0
    skipped_count = 0
    for file_path in merchant_files:
        should_process, reason = _should_process_file(file_path, 'stg_enterprise_department_merchant_data', processed_merchants)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading merchant data from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_enterprise_department_merchant_data')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_enterprise_department_merchant_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_enterprise_department_merchant_data")
                processed_count += 1
    
    logging.info(f"Merchant data: {processed_count} files processed, {skipped_count} files skipped")
    
    # Staff data
    staff_files = find_files("*staff_data*", DATA_DIR)
    processed_count = 0
    skipped_count = 0
    for file_path in staff_files:
        should_process, reason = _should_process_file(file_path, 'stg_enterprise_department_staff_data', processed_staff)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading staff data from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            df = _format_dataframe_for_staging(df, 'stg_enterprise_department_staff_data')
            if df is not None and not df.empty:
                df['file_source'] = str(file_path)
                df.to_sql('stg_enterprise_department_staff_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                logging.info(f"  Loaded {len(df)} rows into stg_enterprise_department_staff_data")
                processed_count += 1
    
    logging.info(f"Staff data: {processed_count} files processed, {skipped_count} files skipped")
    
    # Order with merchant data
    # Per PHYSICALMODEL.txt: fact_orders requires staff_sk, which comes from staff_id in this table
    order_merchant_files = find_files("*order_with_merchant_data*", DATA_DIR)
    total_loaded = 0
    processed_count = 0
    skipped_count = 0
    for file_path in order_merchant_files:
        should_process, reason = _should_process_file(file_path, 'stg_enterprise_department_order_with_merchant_data', processed_order_merchant)
        
        if not should_process:
            logging.info(f"Skipping {file_path}: {reason}")
            skipped_count += 1
            continue
        
        logging.info(f"Loading order with merchant data from: {file_path} ({reason})")
        df = load_file(file_path, clean=True)
        if df is not None and not df.empty:
            # Log source columns for debugging
            logging.debug(f"  Source columns: {list(df.columns)}")
            df = _format_dataframe_for_staging(df, 'stg_enterprise_department_order_with_merchant_data')
            if df is not None and not df.empty:
                # Validate required columns per LOGICAL-PHYSICALMODEL.txt
                required_cols = ['order_id', 'merchant_id', 'staff_id']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logging.error(f"  ERROR: Missing required columns {missing_cols} in {file_path}. Required per PHYSICALMODEL.txt for fact_orders.")
                else:
                    # Check for NULL staff_id values
                    null_staff_count = df['staff_id'].isna().sum() if 'staff_id' in df.columns else len(df)
                    if null_staff_count > 0:
                        logging.warning(f"  WARNING: {null_staff_count} rows have NULL staff_id (will be skipped in fact_orders)")
                    
                    df['file_source'] = str(file_path)
                    df.to_sql('stg_enterprise_department_order_with_merchant_data', engine, if_exists='append', index=False, method='multi', chunksize=1000)
                    total_loaded += len(df)
                    logging.info(f"  Loaded {len(df)} rows into stg_enterprise_department_order_with_merchant_data")
                    logging.info(f"  Columns: {list(df.columns)}")
                    processed_count += 1
    
    logging.info(f"Order with merchant data: {processed_count} files processed, {skipped_count} files skipped")
    if total_loaded > 0:
        logging.info(f"Total loaded: {total_loaded} order-merchant records (staff_id required per LOGICAL-PHYSICALMODEL.txt)")

def main():
    """Main ingestion function - loads all departments"""
    logging.info("=" * 60)
    logging.info("STARTING DATA INGESTION")
    logging.info("=" * 60)
    
    # Check if staging tables exist
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'stg_marketing_department_campaign_data'
        """))
        if result.fetchone()[0] == 0:
            logging.error("Staging tables not created! Please run create_staging_tables task first.")
            return
    
    # Ingest per department
    ingest_marketing_department()
    ingest_operations_department()
    ingest_business_department()
    ingest_customer_management_department()
    ingest_enterprise_department()
    
    logging.info("=" * 60)
    logging.info("DATA INGESTION COMPLETE!")
    logging.info("=" * 60)

if __name__ == "__main__":
    main()
