#!/usr/bin/env python3
"""
Load Fact Tables
Loads transformed data into fact tables with dimension key lookups
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
from etl_pipeline_python import extract_numeric

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

def write_to_reject_table(table_name, order_id, error_reason, row_data=None, source_file=None):
    """Write rejected record to reject table"""
    try:
        import json
        from datetime import datetime, date
        
        # Prepare reject record
        reject_data = {
            'order_id': str(order_id) if order_id else None,
            'error_reason': error_reason,
            'source_file': source_file,
            'rejected_at': datetime.now()
        }
        
        # Add table-specific fields
        if table_name == 'reject_fact_orders':
            reject_data.update({
                'user_id': str(row_data.get('user_id')) if row_data and row_data.get('user_id') else None,
                'merchant_id': str(row_data.get('merchant_id')) if row_data and row_data.get('merchant_id') else None,
                'staff_id': str(row_data.get('staff_id')) if row_data and row_data.get('staff_id') else None,
                'transaction_date': str(row_data.get('transaction_date')) if row_data and row_data.get('transaction_date') else None,
            })
        elif table_name == 'reject_fact_line_items':
            reject_data.update({
                'product_id': str(row_data.get('product_id')) if row_data and row_data.get('product_id') else None,
                'line_item_id': str(row_data.get('line_item_id')) if row_data and row_data.get('line_item_id') else None,
            })
        elif table_name == 'reject_fact_campaign_transactions':
            reject_data.update({
                'user_id': str(row_data.get('user_id')) if row_data and row_data.get('user_id') else None,
                'campaign_id': str(row_data.get('campaign_id')) if row_data and row_data.get('campaign_id') else None,
                'transaction_date': str(row_data.get('transaction_date')) if row_data and row_data.get('transaction_date') else None,
            })
        
        # Convert raw_data to JSON string if needed
        # Handle NaN values properly (convert to None/null for valid JSON)
        def json_serializer(obj):
            """Custom JSON serializer that handles NaN, NaT, and other special values"""
            import numpy as np
            from datetime import date, datetime
            if pd.isna(obj):
                return None
            if isinstance(obj, (pd.Timestamp, datetime, date)):
                return str(obj)
            if isinstance(obj, (np.integer, np.floating)):
                return obj.item()
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        raw_data_json = None
        if row_data:
            if isinstance(row_data, str):
                raw_data_json = row_data
            else:
                # Convert dict/Series to dict first, handling NaN values
                if hasattr(row_data, 'to_dict'):
                    # Handle pandas Series
                    data_dict = row_data.to_dict()
                elif isinstance(row_data, dict):
                    data_dict = row_data
                else:
                    data_dict = dict(row_data)
                
                # Clean NaN values before JSON serialization
                import numpy as np
                cleaned_dict = {}
                for key, value in data_dict.items():
                    # Check for NaN/NaT values using multiple methods
                    is_nan = False
                    if pd.isna(value):
                        is_nan = True
                    elif isinstance(value, float):
                        is_nan = np.isnan(value)
                    elif isinstance(value, pd.Timestamp):
                        is_nan = pd.isna(value)
                    
                    if is_nan:
                        cleaned_dict[key] = None
                    elif isinstance(value, (pd.Timestamp, date, datetime)):
                        cleaned_dict[key] = str(value)
                    elif isinstance(value, (np.integer, np.floating)):
                        cleaned_dict[key] = value.item()
                    elif isinstance(value, np.ndarray):
                        cleaned_dict[key] = value.tolist()
                    else:
                        cleaned_dict[key] = value
                
                raw_data_json = json.dumps(cleaned_dict, default=json_serializer)
        
        # Insert into reject table
        with engine.begin() as conn:
            # Build INSERT statement dynamically based on table
            # Use a separate parameter for raw_data to avoid SQL syntax conflicts
            insert_params = {**reject_data, 'raw_data_json': raw_data_json}
            
            if table_name == 'reject_fact_orders':
                conn.execute(text("""
                    INSERT INTO reject_fact_orders (order_id, user_id, merchant_id, staff_id, transaction_date, error_reason, source_file, rejected_at, raw_data)
                    VALUES (:order_id, :user_id, :merchant_id, :staff_id, :transaction_date, :error_reason, :source_file, :rejected_at, CAST(:raw_data_json AS jsonb))
                """), insert_params)
            elif table_name == 'reject_fact_line_items':
                conn.execute(text("""
                    INSERT INTO reject_fact_line_items (order_id, product_id, line_item_id, error_reason, source_file, rejected_at, raw_data)
                    VALUES (:order_id, :product_id, :line_item_id, :error_reason, :source_file, :rejected_at, CAST(:raw_data_json AS jsonb))
                """), insert_params)
            elif table_name == 'reject_fact_campaign_transactions':
                conn.execute(text("""
                    INSERT INTO reject_fact_campaign_transactions (order_id, user_id, campaign_id, transaction_date, error_reason, source_file, rejected_at, raw_data)
                    VALUES (:order_id, :user_id, :campaign_id, :transaction_date, :error_reason, :source_file, :rejected_at, CAST(:raw_data_json AS jsonb))
                """), insert_params)
    except Exception as e:
        # Don't fail the ETL if reject table write fails - just log it
        logging.warning(f"Failed to write to reject table {table_name}: {e}")

def get_dimension_mappings():
    """Pre-load all dimension mappings for faster lookups"""
    logging.info("Pre-loading dimension mappings...")
    mappings = {}
    
    with engine.connect() as conn:
        # Date dimension mapping - ensure consistent date format
        date_map = {}
        result = conn.execute(text("SELECT date_sk, date FROM dim_date"))
        for row in result:
            date_val = row[1]
            # Format date consistently as YYYY-MM-DD
            if isinstance(date_val, str):
                date_str = date_val[:10]  # Take first 10 chars (YYYY-MM-DD)
            elif hasattr(date_val, 'strftime'):
                date_str = date_val.strftime('%Y-%m-%d')
            else:
                date_str = str(date_val)[:10]
            date_map[date_str] = row[0]
        mappings['date'] = date_map
        
        # User dimension mapping
        user_map = {}
        result = conn.execute(text("SELECT user_sk, user_id FROM dim_user"))
        for row in result:
            user_map[str(row[1])] = row[0]
        mappings['user'] = user_map
        
        # Merchant dimension mapping
        merchant_map = {}
        result = conn.execute(text("SELECT merchant_sk, merchant_id FROM dim_merchant"))
        for row in result:
            merchant_map[str(row[1])] = row[0]
        mappings['merchant'] = merchant_map
        
        # Staff dimension mapping
        staff_map = {}
        result = conn.execute(text("SELECT staff_sk, staff_id FROM dim_staff"))
        for row in result:
            staff_map[str(row[1])] = row[0]
        mappings['staff'] = staff_map
        
        # Product dimension mapping
        product_map = {}
        result = conn.execute(text("SELECT product_sk, product_id FROM dim_product"))
        for row in result:
            product_map[str(row[1])] = row[0]
        mappings['product'] = product_map
        
        # Campaign dimension mapping
        campaign_map = {}
        result = conn.execute(text("SELECT campaign_sk, campaign_id FROM dim_campaign"))
        for row in result:
            campaign_map[str(row[1])] = row[0]
        mappings['campaign'] = campaign_map
    
    logging.info(f"Loaded mappings: {len(mappings['date'])} dates, {len(mappings['user'])} users, "
                 f"{len(mappings['merchant'])} merchants, {len(mappings['staff'])} staff, "
                 f"{len(mappings['product'])} products, {len(mappings['campaign'])} campaigns")
    
    return mappings

def load_fact_orders(order_df, order_merchant_df, delays_df):
    """Load fact_orders from transformed DataFrames with chunked processing to avoid OOM"""
    if order_df is None or order_df.empty:
        logging.warning("No order data to load")
        return
    
    logging.info("Loading fact_orders...")
    
    # Merge order data with merchant and delays
    # Per PHYSICALMODEL.txt: fact_orders requires staff_sk, which comes from staff_id in order_with_merchant_data
    combined = order_df.copy()
    
    # Clean order_id columns for proper merging
    if 'order_id' in combined.columns:
        combined['order_id'] = combined['order_id'].astype(str).str.strip()
    
    if order_merchant_df is not None and not order_merchant_df.empty:
        # Clean order_id in order_merchant_df for proper merging
        if 'order_id' in order_merchant_df.columns:
            order_merchant_df = order_merchant_df.copy()
            order_merchant_df['order_id'] = order_merchant_df['order_id'].astype(str).str.strip()
        
        # Log columns before merge for debugging
        logging.info(f"order_df columns before merge: {list(combined.columns)}")
        logging.info(f"order_merchant_df columns: {list(order_merchant_df.columns)}")
        logging.info(f"order_df has {len(combined)} rows, order_merchant_df has {len(order_merchant_df)} rows")
        
        # Sample order_ids for debugging
        if not combined.empty:
            logging.info(f"Sample order_ids from order_df: {combined['order_id'].head(5).tolist()}")
        if not order_merchant_df.empty:
            logging.info(f"Sample order_ids from order_merchant_df: {order_merchant_df['order_id'].head(5).tolist()}")
        
        # Handle staff_id column conflict - prefer order_merchant_df's staff_id
        # Drop staff_id from order_df if it exists (we want the one from order_merchant_df)
        if 'staff_id' in combined.columns:
            combined = combined.drop(columns=['staff_id'], errors='ignore')
        
        # Merge, keeping staff_id from order_merchant_df
        # Use inner join to avoid duplicates - only keep orders that have merchant/staff data
        combined = combined.merge(order_merchant_df[['order_id', 'merchant_id', 'staff_id']], on='order_id', how='inner')
        
        # Log merge results
        matched_count = len(combined)
        logging.info(f"After merge: {matched_count} orders matched with merchant/staff data (inner join to avoid duplicates)")
        
        # Check for duplicate order_ids (shouldn't happen but log if it does)
        duplicate_count = combined['order_id'].duplicated().sum()
        if duplicate_count > 0:
            logging.warning(f"WARNING: Found {duplicate_count} duplicate order_ids after merge! This may cause data issues.")
            # Remove duplicates, keeping first occurrence
            combined = combined.drop_duplicates(subset=['order_id'], keep='first')
            logging.info(f"Removed duplicates, {len(combined)} unique orders remaining")
        
        # Validate staff_id after merge (required per PHYSICALMODEL.txt)
        if 'staff_id' in combined.columns:
            null_staff_count = combined['staff_id'].isna().sum()
            if null_staff_count > 0:
                logging.warning(f"WARNING: {null_staff_count} orders have NULL staff_id after merge. These will be skipped per PHYSICALMODEL.txt requirement.")
            else:
                logging.info(f"All {len(combined)} orders have staff_id populated (required for fact_orders per PHYSICALMODEL.txt)")
        else:
            logging.error("ERROR: staff_id column missing after merge! Required per PHYSICALMODEL.txt for fact_orders.")
    else:
        logging.error("ERROR: order_merchant_df is None or empty! Cannot load fact_orders without merchant/staff data.")
        return
    
    if delays_df is not None and not delays_df.empty:
        # Clean order_id in delays_df for proper merging
        delays_df = delays_df.copy()
        if 'order_id' in delays_df.columns:
            delays_df['order_id'] = delays_df['order_id'].astype(str).str.strip()
        combined = combined.merge(delays_df, on='order_id', how='left')
    
    # Get dimension mappings
    mappings = get_dimension_mappings()
    date_map = mappings['date']
    user_map = mappings['user']
    merchant_map = mappings['merchant']
    staff_map = mappings['staff']
    
    # Process in chunks to avoid OOM
    CHUNK_SIZE = 10000
    total_rows = len(combined)
    total_inserted = 0
    
    # Log column names and sample data for debugging
    logging.info(f"Combined DataFrame columns: {list(combined.columns)}")
    if not combined.empty:
        logging.info(f"Sample row columns: {dict(combined.iloc[0])}")
    
    logging.info(f"Processing {total_rows} orders in chunks of {CHUNK_SIZE}...")
    
    # Diagnostic counters
    skipped_no_user = 0
    skipped_no_merchant = 0
    skipped_no_staff = 0
    skipped_no_date = 0
    skipped_invalid_date = 0
    
    for chunk_start in range(0, total_rows, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
        chunk_df = combined.iloc[chunk_start:chunk_end]
        
        # Prepare data for this chunk
        fact_rows = []
        for _, row in chunk_df.iterrows():
            try:
                # Clean and normalize IDs for lookup
                user_id = str(row.get('user_id')).strip() if pd.notna(row.get('user_id')) else None
                merchant_id = str(row.get('merchant_id')).strip() if pd.notna(row.get('merchant_id')) else None
                staff_id = str(row.get('staff_id')).strip() if pd.notna(row.get('staff_id')) else None
                
                user_sk = user_map.get(user_id) if user_id else None
                merchant_sk = merchant_map.get(merchant_id) if merchant_id else None
                staff_sk = staff_map.get(staff_id) if staff_id else None
                
                # Track source file if available
                source_file = row.get('file_source') if 'file_source' in row else None
                row_dict = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
                
                if not user_sk:
                    skipped_no_user += 1
                    error_reason = f"user_id '{user_id}' not found in dim_user"
                    if skipped_no_user <= 5:  # Log first few examples
                        logging.debug(f"Skipping order {row.get('order_id')}: {error_reason}")
                    write_to_reject_table('reject_fact_orders', row.get('order_id'), error_reason, row_dict, source_file)
                    continue
                if not merchant_sk:
                    skipped_no_merchant += 1
                    error_reason = f"merchant_id '{merchant_id}' not found in dim_merchant"
                    if skipped_no_merchant <= 5:
                        logging.debug(f"Skipping order {row.get('order_id')}: {error_reason}")
                    write_to_reject_table('reject_fact_orders', row.get('order_id'), error_reason, row_dict, source_file)
                    continue
                if not staff_sk:
                    skipped_no_staff += 1
                    error_reason = f"staff_id '{staff_id}' not found in dim_staff"
                    if skipped_no_staff <= 5:
                        logging.debug(f"Skipping order {row.get('order_id')}: {error_reason}")
                    write_to_reject_table('reject_fact_orders', row.get('order_id'), error_reason, row_dict, source_file)
                    continue
                
                # Get date_sk
                transaction_date_val = row.get('transaction_date')
                if pd.isna(transaction_date_val):
                    skipped_invalid_date += 1
                    error_reason = "null transaction_date"
                    if skipped_invalid_date <= 5:
                        logging.debug(f"Skipping order {row.get('order_id')}: {error_reason}")
                    write_to_reject_table('reject_fact_orders', row.get('order_id'), error_reason, row_dict, source_file)
                    continue
                
                # Handle both date objects and strings
                if isinstance(transaction_date_val, pd.Timestamp):
                    date_str = transaction_date_val.strftime('%Y-%m-%d')
                elif hasattr(transaction_date_val, 'strftime'):  # Python date object
                    date_str = transaction_date_val.strftime('%Y-%m-%d')
                else:
                    # Try to parse as string
                    transaction_date = pd.to_datetime(transaction_date_val, errors='coerce')
                    if pd.isna(transaction_date):
                        skipped_invalid_date += 1
                        error_reason = f"invalid transaction_date format '{transaction_date_val}'"
                        if skipped_invalid_date <= 5:
                            logging.debug(f"Skipping order {row.get('order_id')}: {error_reason}")
                        write_to_reject_table('reject_fact_orders', row.get('order_id'), error_reason, row_dict, source_file)
                        continue
                    date_str = transaction_date.strftime('%Y-%m-%d')
                
                date_sk = date_map.get(date_str)
                if not date_sk:
                    skipped_no_date += 1
                    error_reason = f"date '{date_str}' not found in dim_date"
                    if skipped_no_date <= 5:
                        logging.debug(f"Skipping order {row.get('order_id')}: {error_reason}")
                    write_to_reject_table('reject_fact_orders', row.get('order_id'), error_reason, row_dict, source_file)
                    continue
                
                # Get estimated_arrival_days and delay_days
                estimated_arrival = row.get('estimated_arrival') or row.get('estimated_arrival_days')
                estimated_arrival_days = extract_numeric(estimated_arrival)
                
                delay_days_val = row.get('delay_days') or row.get('delay_in_days')
                delay_days = extract_numeric(delay_days_val) if delay_days_val is not None and pd.notna(delay_days_val) else 0
                
                fact_rows.append({
                    'order_id': str(row.get('order_id')),
                    'user_sk': user_sk,
                    'merchant_sk': merchant_sk,
                    'staff_sk': staff_sk,
                    'transaction_date_sk': date_sk,
                    'estimated_arrival_days': estimated_arrival_days,
                    'delay_days': delay_days
                })
            except Exception as e:
                error_reason = f"Exception during processing: {str(e)}"
                logging.warning(f"Error preparing order {row.get('order_id')}: {e}")
                row_dict = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
                source_file = row.get('file_source') if 'file_source' in row else None
                write_to_reject_table('reject_fact_orders', row.get('order_id'), error_reason, row_dict, source_file)
                continue
        
        # Insert this chunk immediately
        if fact_rows:
            logging.info(f"Inserting chunk {chunk_start//CHUNK_SIZE + 1} ({len(fact_rows)} orders, {chunk_start+1}-{chunk_end} of {total_rows})...")
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO fact_orders (order_id, user_sk, merchant_sk, staff_sk, transaction_date_sk, estimated_arrival_days, delay_days)
                    VALUES (:order_id, :user_sk, :merchant_sk, :staff_sk, :transaction_date_sk, :estimated_arrival_days, :delay_days)
                    ON CONFLICT (order_id) DO UPDATE SET
                        estimated_arrival_days = EXCLUDED.estimated_arrival_days,
                        delay_days = EXCLUDED.delay_days
                """), fact_rows)
            total_inserted += len(fact_rows)
        
        # Clear memory
        del fact_rows
        del chunk_df
    
    # Log diagnostic information
    if total_inserted == 0:
        logging.error("=" * 60)
        logging.error("DIAGNOSTIC: No orders were inserted!")
        logging.error(f"Total rows processed: {total_rows}")
        logging.error(f"Skipped - No user_sk: {skipped_no_user}")
        logging.error(f"Skipped - No merchant_sk: {skipped_no_merchant}")
        logging.error(f"Skipped - No staff_sk: {skipped_no_staff}")
        logging.error(f"Skipped - Invalid date: {skipped_invalid_date}")
        logging.error(f"Skipped - No date_sk: {skipped_no_date}")
        
        # Log sample data for debugging
        if not combined.empty:
            sample_row = combined.iloc[0]
            logging.error(f"Sample row data:")
            logging.error(f"  order_id: {sample_row.get('order_id')}")
            logging.error(f"  user_id: {sample_row.get('user_id')}")
            logging.error(f"  merchant_id: {sample_row.get('merchant_id')}")
            logging.error(f"  staff_id: {sample_row.get('staff_id')}")
            logging.error(f"  transaction_date: {sample_row.get('transaction_date')}")
        
        # Log dimension mapping sizes
        logging.error(f"Dimension mappings available:")
        logging.error(f"  Users: {len(user_map)}")
        logging.error(f"  Merchants: {len(merchant_map)}")
        logging.error(f"  Staff: {len(staff_map)}")
        logging.error(f"  Dates: {len(date_map)}")
        logging.error("=" * 60)
    
    # Get actual count from database to verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM fact_orders"))
        actual_db_count = result.fetchone()[0]
    
    logging.info("=" * 60)
    logging.info("ORDERS LOADED SUCCESSFULLY")
    logging.info("=" * 60)
    logging.info(f"Inserted/Updated: {total_inserted:,} orders")
    logging.info(f"Total orders in database: {actual_db_count:,}")
    logging.info("=" * 60)

def load_fact_line_items(prices_df, products_df):
    """Load fact_line_items from transformed DataFrames"""
    if prices_df is None or products_df is None:
        logging.warning("No line item data to load")
        return
    
    logging.info("Loading fact_line_items...")
    
    # Check if fact_line_items already has data - if so, warn user
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM fact_line_items"))
        existing_count = result.fetchone()[0]
        if existing_count > 0:
            logging.info("=" * 60)
            logging.info("NOTICE: Database already contains data")
            logging.info("=" * 60)
            logging.info(f"Current rows in fact_line_items: {existing_count:,}")
            logging.info("")
            logging.info("If you're loading the same data again, rows will be skipped.")
            logging.info("Only new data (that doesn't exist) will be inserted.")
            logging.info("This prevents duplicate data in your database.")
            logging.info("")
            logging.info("To reload everything, run: TRUNCATE TABLE fact_line_items;")
            logging.info("=" * 60)
    
    # Per PHYSICALMODEL.txt: fact_line_items requires product_sk, which comes from product_id
    # Validate product_id before merge
    if 'product_id' not in products_df.columns:
        logging.error("ERROR: product_id column missing in products_df! Required per PHYSICALMODEL.txt for fact_line_items.")
        return
    else:
        null_product_count = products_df['product_id'].isna().sum()
        if null_product_count > 0:
            logging.warning(f"WARNING: {null_product_count} products have NULL product_id. These will be skipped per PHYSICALMODEL.txt requirement.")
        else:
            logging.info(f"All {len(products_df)} products have product_id populated (required for fact_line_items per PHYSICALMODEL.txt)")
    
    # Merge by index position (matching original script behavior)
    # line_item_id should come from prices_df (it has the correct line_item_id)
    prices_df = prices_df.reset_index(drop=True)
    products_df = products_df.reset_index(drop=True)
    
    # Ensure both DataFrames have the same length for index-based merge
    min_len = min(len(prices_df), len(products_df))
    if len(prices_df) != len(products_df):
        logging.warning(f"WARNING: prices_df has {len(prices_df)} rows, products_df has {len(products_df)} rows. Truncating to {min_len} rows.")
        prices_df = prices_df.iloc[:min_len]
        products_df = products_df.iloc[:min_len]
    
    combined = products_df.merge(
        prices_df,
        left_index=True,
        right_index=True,
        how='left',
        suffixes=('', '_price')
    )
    
    # Use line_item_id from prices_df (it has the correct one)
    if 'line_item_id_price' in combined.columns:
        combined['line_item_id'] = combined['line_item_id_price']
        combined = combined.drop(columns=['line_item_id_price'], errors='ignore')
    elif 'line_item_id' not in combined.columns and 'line_item_id' in prices_df.columns:
        combined['line_item_id'] = prices_df['line_item_id'].values
    
    # Get dimension mappings
    mappings = get_dimension_mappings()
    product_map = mappings['product']
    
    # Get fact_orders mapping (normalize order_id keys)
    order_map = {}
    with engine.connect() as conn:
        result = conn.execute(text("SELECT order_id, user_sk, merchant_sk, staff_sk, transaction_date_sk FROM fact_orders"))
        for row in result:
            order_map[str(row[0]).strip()] = {
                'user_sk': row[1],
                'merchant_sk': row[2],
                'staff_sk': row[3],
                'transaction_date_sk': row[4]
            }
    
    # Process in chunks to avoid OOM
    CHUNK_SIZE = 10000
    total_rows = len(combined)
    total_inserted = 0
    
    # Diagnostic counters
    skipped_no_product = 0
    skipped_no_order = 0
    
    logging.info(f"Processing {total_rows:,} line items from staging tables in chunks of {CHUNK_SIZE:,}...")
    logging.info(f"Note: Rows that already exist in fact_line_items will be skipped to prevent duplicates.")
    
    for chunk_start in range(0, total_rows, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
        chunk_df = combined.iloc[chunk_start:chunk_end]
        
        # Prepare data for this chunk
        fact_rows = []
        for _, row in chunk_df.iterrows():
            try:
                # Clean and normalize product_id for lookup
                # Track source file if available
                source_file = row.get('file_source') if 'file_source' in row else None
                row_dict = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
                
                product_id = str(row.get('product_id')).strip() if pd.notna(row.get('product_id')) else None
                if not product_id:
                    skipped_no_product += 1
                    error_reason = "product_id is NULL"
                    if skipped_no_product <= 5:
                        logging.debug(f"Skipping line item {row.get('order_id')}: {error_reason}")
                    write_to_reject_table('reject_fact_line_items', row.get('order_id'), error_reason, row_dict, source_file)
                    continue
                
                product_sk = product_map.get(product_id)
                if not product_sk:
                    skipped_no_product += 1
                    error_reason = f"product_id '{product_id}' not found in dim_product"
                    if skipped_no_product <= 5:
                        logging.debug(f"Skipping line item {row.get('order_id')}: {error_reason}")
                    write_to_reject_table('reject_fact_line_items', row.get('order_id'), error_reason, row_dict, source_file)
                    continue
                
                order_id = str(row.get('order_id')).strip()
                order_keys = order_map.get(order_id)
                if not order_keys:
                    skipped_no_order += 1
                    error_reason = f"order_id '{order_id}' not found in fact_orders"
                    if skipped_no_order <= 5:
                        logging.debug(f"Skipping line item: {error_reason}")
                    write_to_reject_table('reject_fact_line_items', order_id, error_reason, row_dict, source_file)
                    continue
                
                fact_rows.append({
                    'order_id': order_id,
                    'product_sk': product_sk,
                    'user_sk': order_keys['user_sk'],
                    'merchant_sk': order_keys['merchant_sk'],
                    'staff_sk': order_keys['staff_sk'],
                    'transaction_date_sk': order_keys['transaction_date_sk'],
                    'price': float(row.get('price', 0)) if pd.notna(row.get('price')) else None,
                    'quantity': extract_numeric(row.get('quantity')) or 0
                })
            except Exception as e:
                error_reason = f"Exception during processing: {str(e)}"
                logging.warning(f"Error preparing line item {row.get('order_id')}: {e}")
                row_dict = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
                source_file = row.get('file_source') if 'file_source' in row else None
                write_to_reject_table('reject_fact_line_items', row.get('order_id'), error_reason, row_dict, source_file)
                continue
        
        # Insert this chunk immediately
        if fact_rows:
            logging.info(f"Inserting chunk {chunk_start//CHUNK_SIZE + 1} ({len(fact_rows)} line items, {chunk_start+1}-{chunk_end} of {total_rows})...")
            with engine.begin() as conn:
                # Get count before insert to calculate actual inserts
                result_before = conn.execute(text("SELECT COUNT(*) FROM fact_line_items"))
                count_before = result_before.fetchone()[0]
                
                conn.execute(text("""
                    INSERT INTO fact_line_items (order_id, product_sk, user_sk, merchant_sk, staff_sk, transaction_date_sk, price, quantity)
                    VALUES (:order_id, :product_sk, :user_sk, :merchant_sk, :staff_sk, :transaction_date_sk, :price, :quantity)
                    ON CONFLICT (order_id, product_sk) DO NOTHING
                """), fact_rows)
                
                # Get count after insert to calculate actual inserts (accounts for ON CONFLICT DO NOTHING)
                result_after = conn.execute(text("SELECT COUNT(*) FROM fact_line_items"))
                count_after = result_after.fetchone()[0]
                actual_inserted = count_after - count_before
                
                if actual_inserted < len(fact_rows):
                    skipped_conflicts = len(fact_rows) - actual_inserted
                    logging.info(f"  Inserted: {actual_inserted:,} new rows | Skipped: {skipped_conflicts:,} rows (already exist in database)")
                
            total_inserted += actual_inserted
        
        # Clear memory
        del fact_rows
        del chunk_df
    
    # Log diagnostic information
    if total_inserted == 0:
        logging.error("=" * 60)
        logging.error("DIAGNOSTIC: No line items were inserted!")
        logging.error(f"Total rows processed: {total_rows}")
        logging.error(f"Skipped - No product_sk: {skipped_no_product}")
        logging.error(f"Skipped - No order in fact_orders: {skipped_no_order}")
        
        # Log fact_orders status
        logging.error(f"fact_orders table has {len(order_map)} orders")
        if len(order_map) == 0:
            logging.error("ERROR: fact_orders is empty! fact_line_items requires fact_orders to be loaded first.")
        
        # Log dimension mapping sizes
        logging.error(f"Dimension mappings available:")
        logging.error(f"  Products: {len(product_map)}")
        logging.error("=" * 60)
    
    # Get actual count from database to verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM fact_line_items"))
        actual_db_count = result.fetchone()[0]
    
    if total_inserted == 0:
        logging.warning("=" * 60)
        logging.warning("NO NEW DATA INSERTED")
        logging.warning("=" * 60)
        logging.warning(f"Summary:")
        logging.warning(f"  Rows processed: {total_rows:,}")
        logging.warning(f"  New rows inserted: {total_inserted:,}")
        logging.warning(f"  Rows skipped (already exist): {total_rows - total_inserted:,}")
        logging.warning(f"  Total rows in database: {actual_db_count:,}")
        logging.warning("")
        logging.warning("Why this happened:")
        logging.warning("  All the data you're trying to load already exists in the database.")
        logging.warning("  This usually means you've run the ETL pipeline before with the same data.")
        logging.warning("")
        logging.warning("What you can do:")
        logging.warning("  - To reload everything: TRUNCATE TABLE fact_line_items;")
        logging.warning("  - To add new data: Add new files to staging tables")
        logging.warning("  - If this is expected: No action needed - your data is safe")
        logging.warning("=" * 60)
    elif total_inserted < total_rows:
        skipped_count = total_rows - total_inserted
        logging.info("=" * 60)
        logging.info("LOAD SUMMARY")
        logging.info("=" * 60)
        logging.info(f"Successfully inserted: {total_inserted:,} new rows")
        logging.info(f"Skipped (already exist): {skipped_count:,} rows")
        logging.info(f"Total rows in database: {actual_db_count:,}")
        logging.info("")
        logging.info("Note: Some rows were skipped because they already exist in the database.")
        logging.info("This is normal if you're re-running the ETL or have duplicate data in source files.")
        logging.info("")
        logging.info(f"Processed: {total_rows:,} rows | Inserted: {total_inserted:,} rows | Final count: {actual_db_count:,} rows")
        logging.info("")
        logging.info("Explanation of counts:")
        logging.info(f"  - Processed ({total_rows:,}): Total rows read from staging tables")
        logging.info(f"  - Inserted ({total_inserted:,}): New rows added to database")
        logging.info(f"  - Final count ({actual_db_count:,}): Total rows now in fact_line_items")
        logging.info(f"  - Difference ({total_rows - total_inserted:,}): Rows skipped (already existed)")
        logging.info("=" * 60)
    else:
        logging.info("=" * 60)
        logging.info("SUCCESS - All data loaded")
        logging.info("=" * 60)
        logging.info(f"Inserted: {total_inserted:,} new rows")
        logging.info(f"Total rows in database: {actual_db_count:,}")
        logging.info("=" * 60)

def load_fact_campaign_transactions(df):
    """Load fact_campaign_transactions from transformed DataFrame"""
    if df is None or df.empty:
        logging.warning("No campaign transaction data to load")
        return
    
    logging.info("Loading fact_campaign_transactions...")
    
    # Scenario 3 Support: Ensure Unknown campaign exists
    try:
        import load_dim
        unknown_campaign_sk = load_dim.ensure_unknown_campaign()
        logging.info(f"Unknown campaign_sk: {unknown_campaign_sk}")
    except Exception as e:
        logging.warning(f"Could not ensure Unknown campaign: {e}")
        unknown_campaign_sk = None
    
    # Get dimension mappings
    mappings = get_dimension_mappings()
    campaign_map = mappings['campaign']
    
    # Add Unknown campaign to map if not present
    if unknown_campaign_sk and 'UNKNOWN' not in campaign_map:
        campaign_map['UNKNOWN'] = unknown_campaign_sk
    
    # Get fact_orders mapping (normalize order_id keys)
    order_map = {}
    with engine.connect() as conn:
        result = conn.execute(text("SELECT order_id, user_sk, merchant_sk, transaction_date_sk FROM fact_orders"))
        for row in result:
            order_map[str(row[0]).strip()] = {
                'user_sk': row[1],
                'merchant_sk': row[2],
                'transaction_date_sk': row[3]
            }
    
    # Process in chunks to avoid OOM
    CHUNK_SIZE = 10000
    total_rows = len(df)
    total_inserted = 0
    
    logging.info(f"Processing {total_rows} campaign transactions in chunks of {CHUNK_SIZE}...")
    
    for chunk_start in range(0, total_rows, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
        chunk_df = df.iloc[chunk_start:chunk_end]
        
        # Prepare data for this chunk
        fact_rows = []
        for _, row in chunk_df.iterrows():
            try:
                # Track source file if available
                source_file = row.get('file_source') if 'file_source' in row else None
                row_dict = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
                
                # Clean and normalize campaign_id for lookup
                campaign_id = str(row.get('campaign_id')).strip() if pd.notna(row.get('campaign_id')) else None
                if not campaign_id:
                    # Scenario 3: Use Unknown campaign for missing campaign_id
                    campaign_sk = unknown_campaign_sk if unknown_campaign_sk else None
                    if not campaign_sk:
                        error_reason = "campaign_id is NULL and Unknown campaign not available"
                        write_to_reject_table('reject_fact_campaign_transactions', row.get('order_id'), error_reason, row_dict, source_file)
                        continue
                else:
                    campaign_sk = campaign_map.get(campaign_id)
                    # Scenario 3: Use Unknown campaign if campaign_id doesn't exist
                    if not campaign_sk and unknown_campaign_sk:
                        logging.debug(f"Campaign '{campaign_id}' not found, using Unknown campaign")
                        campaign_sk = unknown_campaign_sk
                    elif not campaign_sk:
                        error_reason = f"campaign_id '{campaign_id}' not found in dim_campaign and Unknown campaign not available"
                        write_to_reject_table('reject_fact_campaign_transactions', row.get('order_id'), error_reason, row_dict, source_file)
                        continue
                
                order_id = str(row.get('order_id')).strip()
                order_keys = order_map.get(order_id)
                if not order_keys:
                    error_reason = f"order_id '{order_id}' not found in fact_orders"
                    if total_inserted == 0 and len(fact_rows) == 0:  # Only log if we haven't inserted anything yet
                        logging.debug(f"Skipping campaign transaction: {error_reason}")
                    write_to_reject_table('reject_fact_campaign_transactions', order_id, error_reason, row_dict, source_file)
                    continue
                
                fact_rows.append({
                    'order_id': order_id,
                    'campaign_sk': campaign_sk,
                    'user_sk': order_keys['user_sk'],
                    'merchant_sk': order_keys['merchant_sk'],
                    'transaction_date_sk': order_keys['transaction_date_sk'],
                    'availed': row.get('availed')
                })
            except Exception as e:
                error_reason = f"Exception during processing: {str(e)}"
                logging.warning(f"Error preparing campaign transaction {row.get('order_id')}: {e}")
                row_dict = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
                source_file = row.get('file_source') if 'file_source' in row else None
                write_to_reject_table('reject_fact_campaign_transactions', row.get('order_id'), error_reason, row_dict, source_file)
                continue
        
        # Insert this chunk immediately
        if fact_rows:
            logging.info(f"Inserting chunk {chunk_start//CHUNK_SIZE + 1} ({len(fact_rows)} transactions, {chunk_start+1}-{chunk_end} of {total_rows})...")
            with engine.begin() as conn:
                # Get count before insert to calculate actual inserts
                result_before = conn.execute(text("SELECT COUNT(*) FROM fact_campaign_transactions"))
                count_before = result_before.fetchone()[0]
                
                conn.execute(text("""
                    INSERT INTO fact_campaign_transactions (order_id, campaign_sk, user_sk, merchant_sk, transaction_date_sk, availed)
                    VALUES (:order_id, :campaign_sk, :user_sk, :merchant_sk, :transaction_date_sk, :availed)
                    ON CONFLICT (campaign_sk, user_sk, merchant_sk, transaction_date_sk) DO NOTHING
                """), fact_rows)
                
                # Get count after insert to calculate actual inserts
                result_after = conn.execute(text("SELECT COUNT(*) FROM fact_campaign_transactions"))
                count_after = result_after.fetchone()[0]
                actual_inserted = count_after - count_before
                
                if actual_inserted < len(fact_rows):
                    skipped_conflicts = len(fact_rows) - actual_inserted
                    logging.info(f"  Inserted: {actual_inserted:,} new rows | Skipped: {skipped_conflicts:,} rows (already exist in database)")
                
            total_inserted += actual_inserted
        
        # Clear memory
        del fact_rows
        del chunk_df
    
    if total_inserted == 0:
        logging.error("=" * 60)
        logging.error("DIAGNOSTIC: No campaign transactions were inserted!")
        logging.error(f"Total rows processed: {total_rows}")
        
        # Log fact_orders status
        logging.error(f"fact_orders table has {len(order_map)} orders")
        if len(order_map) == 0:
            logging.error("ERROR: fact_orders is empty! fact_campaign_transactions requires fact_orders to be loaded first.")
        
        # Log dimension mapping sizes
        logging.error(f"Dimension mappings available:")
        logging.error(f"  Campaigns: {len(campaign_map)}")
        logging.error("=" * 60)
    
    # Get actual count from database to verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM fact_campaign_transactions"))
        actual_db_count = result.fetchone()[0]
    
    if total_inserted == 0:
        logging.warning("=" * 60)
        logging.warning("NO NEW CAMPAIGN TRANSACTIONS INSERTED")
        logging.warning("=" * 60)
        logging.warning(f"Summary:")
        logging.warning(f"  Rows processed: {total_rows:,}")
        logging.warning(f"  New rows inserted: {total_inserted:,}")
        logging.warning(f"  Rows skipped (already exist): {total_rows - total_inserted:,}")
        logging.warning(f"  Total rows in database: {actual_db_count:,}")
        logging.warning("")
        logging.warning("All data already exists in the database.")
        logging.warning("=" * 60)
    else:
        logging.info("=" * 60)
        logging.info("CAMPAIGN TRANSACTIONS LOADED SUCCESSFULLY")
        logging.info("=" * 60)
        logging.info(f"Inserted: {total_inserted:,} new transactions")
        logging.info(f"Total transactions in database: {actual_db_count:,}")
        logging.info("=" * 60)

