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
                
                if not user_sk:
                    skipped_no_user += 1
                    if skipped_no_user <= 5:  # Log first few examples
                        logging.debug(f"Skipping order {row.get('order_id')}: user_id '{user_id}' not found in dim_user")
                    continue
                if not merchant_sk:
                    skipped_no_merchant += 1
                    if skipped_no_merchant <= 5:
                        logging.debug(f"Skipping order {row.get('order_id')}: merchant_id '{merchant_id}' not found in dim_merchant")
                    continue
                if not staff_sk:
                    skipped_no_staff += 1
                    if skipped_no_staff <= 5:
                        logging.debug(f"Skipping order {row.get('order_id')}: staff_id '{staff_id}' not found in dim_staff")
                    continue
                
                # Get date_sk
                transaction_date_val = row.get('transaction_date')
                if pd.isna(transaction_date_val):
                    skipped_invalid_date += 1
                    if skipped_invalid_date <= 5:
                        logging.debug(f"Skipping order {row.get('order_id')}: null transaction_date")
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
                        if skipped_invalid_date <= 5:
                            logging.debug(f"Skipping order {row.get('order_id')}: invalid transaction_date '{transaction_date_val}'")
                        continue
                    date_str = transaction_date.strftime('%Y-%m-%d')
                
                date_sk = date_map.get(date_str)
                if not date_sk:
                    skipped_no_date += 1
                    if skipped_no_date <= 5:
                        logging.debug(f"Skipping order {row.get('order_id')}: date '{date_str}' not found in dim_date (available dates: {list(date_map.keys())[:5]}...)")
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
                logging.warning(f"Error preparing order {row.get('order_id')}: {e}")
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
    
    logging.info(f"Loaded {total_inserted} orders into fact_orders")

def load_fact_line_items(prices_df, products_df):
    """Load fact_line_items from transformed DataFrames"""
    if prices_df is None or products_df is None:
        logging.warning("No line item data to load")
        return
    
    logging.info("Loading fact_line_items...")
    
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
    
    logging.info(f"Processing {total_rows} line items in chunks of {CHUNK_SIZE}...")
    
    for chunk_start in range(0, total_rows, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
        chunk_df = combined.iloc[chunk_start:chunk_end]
        
        # Prepare data for this chunk
        fact_rows = []
        for _, row in chunk_df.iterrows():
            try:
                # Clean and normalize product_id for lookup
                product_id = str(row.get('product_id')).strip() if pd.notna(row.get('product_id')) else None
                if not product_id:
                    skipped_no_product += 1
                    if skipped_no_product <= 5:
                        logging.debug(f"Skipping line item {row.get('order_id')}: product_id is NULL")
                    continue
                
                product_sk = product_map.get(product_id)
                if not product_sk:
                    skipped_no_product += 1
                    if skipped_no_product <= 5:
                        logging.debug(f"Skipping line item {row.get('order_id')}: product_id '{product_id}' not found in dim_product")
                    continue
                
                order_id = str(row.get('order_id')).strip()
                order_keys = order_map.get(order_id)
                if not order_keys:
                    skipped_no_order += 1
                    if skipped_no_order <= 5:
                        logging.debug(f"Skipping line item: order_id '{order_id}' not found in fact_orders")
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
                logging.warning(f"Error preparing line item {row.get('order_id')}: {e}")
                continue
        
        # Insert this chunk immediately
        if fact_rows:
            logging.info(f"Inserting chunk {chunk_start//CHUNK_SIZE + 1} ({len(fact_rows)} line items, {chunk_start+1}-{chunk_end} of {total_rows})...")
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO fact_line_items (order_id, product_sk, user_sk, merchant_sk, staff_sk, transaction_date_sk, price, quantity)
                    VALUES (:order_id, :product_sk, :user_sk, :merchant_sk, :staff_sk, :transaction_date_sk, :price, :quantity)
                    ON CONFLICT (order_id, product_sk) DO NOTHING
                """), fact_rows)
            total_inserted += len(fact_rows)
        
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
    
    logging.info(f"Loaded {total_inserted} line items into fact_line_items")

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
                # Clean and normalize campaign_id for lookup
                campaign_id = str(row.get('campaign_id')).strip() if pd.notna(row.get('campaign_id')) else None
                if not campaign_id:
                    # Scenario 3: Use Unknown campaign for missing campaign_id
                    campaign_sk = unknown_campaign_sk if unknown_campaign_sk else None
                    if not campaign_sk:
                        continue
                else:
                    campaign_sk = campaign_map.get(campaign_id)
                    # Scenario 3: Use Unknown campaign if campaign_id doesn't exist
                    if not campaign_sk and unknown_campaign_sk:
                        logging.debug(f"Campaign '{campaign_id}' not found, using Unknown campaign")
                        campaign_sk = unknown_campaign_sk
                    elif not campaign_sk:
                        continue
                
                order_id = str(row.get('order_id')).strip()
                order_keys = order_map.get(order_id)
                if not order_keys:
                    if total_inserted == 0 and len(fact_rows) == 0:  # Only log if we haven't inserted anything yet
                        logging.debug(f"Skipping campaign transaction: order_id '{order_id}' not found in fact_orders")
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
                logging.warning(f"Error preparing campaign transaction {row.get('order_id')}: {e}")
                continue
        
        # Insert this chunk immediately
        if fact_rows:
            logging.info(f"Inserting chunk {chunk_start//CHUNK_SIZE + 1} ({len(fact_rows)} transactions, {chunk_start+1}-{chunk_end} of {total_rows})...")
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO fact_campaign_transactions (order_id, campaign_sk, user_sk, merchant_sk, transaction_date_sk, availed)
                    VALUES (:order_id, :campaign_sk, :user_sk, :merchant_sk, :transaction_date_sk, :availed)
                    ON CONFLICT (campaign_sk, user_sk, merchant_sk, transaction_date_sk) DO NOTHING
                """), fact_rows)
            total_inserted += len(fact_rows)
        
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
    
    logging.info(f"Loaded {total_inserted} campaign transactions into fact_campaign_transactions")

