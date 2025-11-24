#!/usr/bin/env python3
"""
ETL Script: Load Fact Tables from Staging Layer
Kimball Methodology - Fact Table Loading
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


def get_date_key(engine, date_value):
    """Get date_key from dim_date for a given date."""
    if pd.isna(date_value):
        return None
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT date_key FROM dim_date 
                WHERE date_actual = :date_val
            """), {"date_val": date_value})
            row = result.fetchone()
            return row[0] if row else None
    except:
        return None


def get_dim_key(engine, dim_table, dim_id_col, dim_id_value, source_system=None):
    """Get dimension key from a dimension table."""
    if pd.isna(dim_id_value) or dim_id_value == '':
        return None
    
    try:
        with engine.begin() as conn:
            if source_system:
                result = conn.execute(text(f"""
                    SELECT {dim_table}_key FROM {dim_table}
                    WHERE {dim_id_col} = :dim_id AND source_system = :source
                """), {"dim_id": str(dim_id_value), "source": source_system})
            else:
                result = conn.execute(text(f"""
                    SELECT {dim_table}_key FROM {dim_table}
                    WHERE {dim_id_col} = :dim_id
                """), {"dim_id": str(dim_id_value)})
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        logging.debug(f"Could not find {dim_table} key for {dim_id_value}: {e}")
        return None


def load_fact_sales(engine):
    """Load Sales Fact Table from staging tables."""
    etl_name = "load_fact_sales"
    started_at = datetime.now()
    
    try:
        logging.info("Loading fact_sales from staging...")
        
        # Get all order and line item staging tables
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()
        
        order_tables = [t for t in all_tables if 'order' in t.lower() and t.startswith('stg_')]
        line_item_tables = [t for t in all_tables if 'line_item' in t.lower() and t.startswith('stg_')]
        
        logging.info(f"Found {len(order_tables)} order tables and {len(line_item_tables)} line item tables")
        
        rows_inserted = 0
        rows_failed = 0
        
        # Process line item tables (grain: one row per line item)
        for table_name in line_item_tables:
            try:
                logging.info(f"Processing {table_name}...")
                df = pd.read_sql_table(table_name, con=engine)
                
                # Process in chunks for large tables
                chunk_size = 50000
                for chunk_start in range(0, len(df), chunk_size):
                    chunk = df.iloc[chunk_start:chunk_start + chunk_size]
                    
                    fact_rows = []
                    for _, row in chunk.iterrows():
                        try:
                            # Get dimension keys
                            order_date = pd.to_datetime(row.get('order_date', row.get('transaction_date', row.get('date'))), errors='coerce')
                            date_key = get_date_key(engine, order_date)
                            
                            customer_id = row.get('customer_id', row.get('user_id'))
                            customer_key = get_dim_key(engine, 'dim_customer', 'customer_id', customer_id, 'Customer Management Department')
                            
                            product_id = row.get('product_id', row.get('item_id'))
                            product_key = get_dim_key(engine, 'dim_product', 'product_id', product_id, 'Business Department')
                            
                            merchant_id = row.get('merchant_id', row.get('vendor_id'))
                            merchant_key = get_dim_key(engine, 'dim_merchant', 'merchant_id', merchant_id, 'Enterprise Department')
                            
                            staff_id = row.get('staff_id', row.get('employee_id'))
                            staff_key = get_dim_key(engine, 'dim_staff', 'staff_id', staff_id, 'Enterprise Department')
                            
                            campaign_id = row.get('campaign_id')
                            campaign_key = get_dim_key(engine, 'dim_campaign', 'campaign_id', campaign_id, 'Marketing Department') if campaign_id else None
                            
                            # Calculate measures
                            quantity = int(pd.to_numeric(row.get('quantity', 1), errors='coerce') or 1)
                            unit_price = float(pd.to_numeric(row.get('unit_price', row.get('price', 0)), errors='coerce') or 0)
                            discount_percent = float(pd.to_numeric(row.get('discount', row.get('discount_percent', 0)), errors='coerce') or 0)
                            discount_amount = (unit_price * quantity * discount_percent / 100) if discount_percent > 0 else 0
                            line_total = (unit_price * quantity) - discount_amount
                            
                            fact_rows.append({
                                'order_id': str(row.get('order_id', '')),
                                'line_item_id': str(row.get('line_item_id', row.get('id', ''))),
                                'order_date_key': date_key,
                                'customer_key': customer_key,
                                'product_key': product_key,
                                'merchant_key': merchant_key,
                                'staff_key': staff_key,
                                'campaign_key': campaign_key,
                                'quantity': quantity,
                                'unit_price': unit_price,
                                'discount_amount': discount_amount,
                                'discount_percent': discount_percent,
                                'line_total': line_total,
                                'order_status': str(row.get('order_status', row.get('status', ''))),
                                'payment_method': str(row.get('payment_method', '')),
                                'estimated_arrival_days': int(pd.to_numeric(row.get('estimated_arrival', row.get('estimated_arrival_days')), errors='coerce') or 0),
                                'source_system': 'Operations Department',
                                'source_file': table_name
                            })
                        except Exception as e:
                            logging.debug(f"Error processing row: {e}")
                            rows_failed += 1
                            continue
                    
                    # Bulk insert
                    if fact_rows:
                        df_fact = pd.DataFrame(fact_rows)
                        df_fact.to_sql('fact_sales', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
                        rows_inserted += len(fact_rows)
                        logging.info(f"  Inserted {len(fact_rows)} rows from {table_name}")
                
            except Exception as e:
                logging.error(f"Error processing {table_name}: {e}")
                rows_failed += 1
                continue
        
        completed_at = datetime.now()
        
        # Log ETL run
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
                "source_table": "stg_operations_department_*",
                "target_table": "fact_sales",
                "rows_processed": rows_inserted + rows_failed,
                "rows_inserted": rows_inserted,
                "rows_updated": 0,
                "rows_failed": rows_failed,
                "status": "success" if rows_failed == 0 else "partial",
                "error_msg": None,
                "started_at": started_at,
                "completed_at": completed_at,
                "duration": int((completed_at - started_at).total_seconds())
            })
        
        logging.info(f"✓ Loaded fact_sales: {rows_inserted} rows inserted, {rows_failed} failed")
        return True
        
    except Exception as e:
        completed_at = datetime.now()
        logging.error(f"✗ Failed to load fact_sales: {e}")
        
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
                "source_table": "stg_operations_department_*",
                "target_table": "fact_sales",
                "rows_processed": 0,
                "rows_inserted": 0,
                "rows_updated": 0,
                "rows_failed": 0,
                "status": "failed",
                "error_msg": str(e),
                "started_at": started_at,
                "completed_at": completed_at,
                "duration": int((completed_at - started_at).total_seconds())
            })
        
        return False


def load_fact_campaign_performance(engine):
    """Load Campaign Performance Fact Table."""
    etl_name = "load_fact_campaign_performance"
    started_at = datetime.now()
    source_table = "stg_marketing_department_transactional_campaign_data_csv"
    
    try:
        logging.info("Loading fact_campaign_performance from staging...")
        
        df = pd.read_sql_table(source_table, con=engine)
        rows_processed = len(df)
        
        rows_inserted = 0
        rows_failed = 0
        
        # Process in chunks
        chunk_size = 50000
        for chunk_start in range(0, len(df), chunk_size):
            chunk = df.iloc[chunk_start:chunk_start + chunk_size]
            
            fact_rows = []
            for _, row in chunk.iterrows():
                try:
                    transaction_date = pd.to_datetime(row.get('transaction_date', row.get('date')), errors='coerce')
                    date_key = get_date_key(engine, transaction_date)
                    
                    campaign_id = row.get('campaign_id')
                    campaign_key = get_dim_key(engine, 'dim_campaign', 'campaign_id', campaign_id, 'Marketing Department') if campaign_id else None
                    
                    customer_id = row.get('customer_id', row.get('user_id'))
                    customer_key = get_dim_key(engine, 'dim_customer', 'customer_id', customer_id, 'Customer Management Department')
                    
                    revenue = float(pd.to_numeric(row.get('revenue', row.get('amount', 0)), errors='coerce') or 0)
                    discount = float(pd.to_numeric(row.get('discount', 0), errors='coerce') or 0)
                    
                    fact_rows.append({
                        'campaign_key': campaign_key,
                        'transaction_date_key': date_key,
                        'customer_key': customer_key,
                        'transactions_count': 1,
                        'revenue_amount': revenue,
                        'discount_amount': discount,
                        'net_revenue_amount': revenue - discount,
                        'source_system': 'Marketing Department'
                    })
                except Exception as e:
                    logging.debug(f"Error processing row: {e}")
                    rows_failed += 1
                    continue
            
            if fact_rows:
                df_fact = pd.DataFrame(fact_rows)
                df_fact.to_sql('fact_campaign_performance', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
                rows_inserted += len(fact_rows)
        
        completed_at = datetime.now()
        
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
                "target_table": "fact_campaign_performance",
                "rows_processed": rows_processed,
                "rows_inserted": rows_inserted,
                "rows_updated": 0,
                "rows_failed": rows_failed,
                "status": "success" if rows_failed == 0 else "partial",
                "error_msg": None,
                "started_at": started_at,
                "completed_at": completed_at,
                "duration": int((completed_at - started_at).total_seconds())
            })
        
        logging.info(f"✓ Loaded fact_campaign_performance: {rows_inserted} rows inserted")
        return True
        
    except Exception as e:
        completed_at = datetime.now()
        logging.error(f"✗ Failed to load fact_campaign_performance: {e}")
        return False


def main():
    """Main ETL function to load all fact tables."""
    logging.info("=" * 60)
    logging.info("ShopZada ETL: Loading Fact Tables")
    logging.info("=" * 60)
    
    results = {}
    results['fact_sales'] = load_fact_sales(engine)
    results['fact_campaign_performance'] = load_fact_campaign_performance(engine)
    
    logging.info("=" * 60)
    logging.info("ETL Summary:")
    logging.info("=" * 60)
    
    successful = [k for k, v in results.items() if v]
    failed = [k for k, v in results.items() if not v]
    
    logging.info(f"✓ Successfully loaded: {len(successful)}/{len(results)} fact tables")
    for fact in successful:
        logging.info(f"  - {fact}")
    
    if failed:
        logging.warning(f"✗ Failed to load: {len(failed)} fact tables")
        for fact in failed:
            logging.warning(f"  - {fact}")
    
    logging.info("=" * 60)


if __name__ == "__main__":
    main()

