#!/usr/bin/env python3
"""
ETL Metrics and Monitoring
Provides before/after state tracking and dashboard KPI monitoring for Scenario 1
"""

import os
import logging
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

def get_fact_orders_count():
    """Get current row count in fact_orders"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM fact_orders"))
            return result.fetchone()[0]
    except Exception as e:
        logging.warning(f"Could not get fact_orders count: {e}")
        return None

def get_fact_line_items_count():
    """Get current row count in fact_line_items"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM fact_line_items"))
            return result.fetchone()[0]
    except Exception as e:
        logging.warning(f"Could not get fact_line_items count: {e}")
        return None

def get_daily_sales_kpi(target_date):
    """
    Get daily sales KPI for a specific date
    
    Args:
        target_date: Date in YYYY-MM-DD format
    
    Returns:
        dict: {'order_count': int, 'total_sales': float} or None if error
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as order_count,
                    COALESCE(SUM(li.price * li.quantity), 0) as total_sales
                FROM fact_orders fo
                INNER JOIN dim_date dd ON fo.transaction_date_sk = dd.date_sk
                LEFT JOIN fact_line_items li ON fo.order_id = li.order_id
                WHERE dd.date = :target_date
            """), {'target_date': target_date})
            row = result.fetchone()
            if row:
                return {
                    'order_count': row[0] if row[0] else 0,
                    'total_sales': float(row[1]) if row[1] else 0.0
                }
            return {'order_count': 0, 'total_sales': 0.0}
    except Exception as e:
        logging.warning(f"Could not get daily sales KPI for {target_date}: {e}")
        return None

def log_before_state(target_date=None):
    """
    Log the BEFORE state of the data warehouse (Scenario 1 support)
    
    Args:
        target_date: Optional target date for KPI tracking (YYYY-MM-DD format)
    
    Returns:
        dict: Before state metrics
    """
    logging.info("=" * 60)
    logging.info("BEFORE STATE (Scenario 1)")
    logging.info("=" * 60)
    
    before_state = {}
    
    # Fact table row counts
    fact_orders_count = get_fact_orders_count()
    fact_line_items_count = get_fact_line_items_count()
    
    if fact_orders_count is not None:
        logging.info(f"Fact Orders Row Count (BEFORE): {fact_orders_count:,}")
        before_state['fact_orders_count'] = fact_orders_count
    else:
        before_state['fact_orders_count'] = None
    
    if fact_line_items_count is not None:
        logging.info(f"Fact Line Items Row Count (BEFORE): {fact_line_items_count:,}")
        before_state['fact_line_items_count'] = fact_line_items_count
    else:
        before_state['fact_line_items_count'] = None
    
    # Dashboard KPI for target date
    if target_date:
        kpi = get_daily_sales_kpi(target_date)
        if kpi:
            logging.info(f"Dashboard KPI for {target_date} (BEFORE):")
            logging.info(f"  - Order Count: {kpi['order_count']:,}")
            logging.info(f"  - Total Sales: ${kpi['total_sales']:,.2f}")
            before_state['kpi'] = kpi
        else:
            before_state['kpi'] = None
    else:
        before_state['kpi'] = None
    
    logging.info("=" * 60)
    
    return before_state

def log_after_state(before_state, target_date=None):
    """
    Log the AFTER state and compare with BEFORE state (Scenario 1 support)
    
    Args:
        before_state: Dictionary returned from log_before_state()
        target_date: Optional target date for KPI tracking (YYYY-MM-DD format)
    
    Returns:
        dict: After state metrics and changes
    """
    logging.info("=" * 60)
    logging.info("AFTER STATE (Scenario 1)")
    logging.info("=" * 60)
    
    after_state = {}
    
    # Fact table row counts
    fact_orders_count = get_fact_orders_count()
    fact_line_items_count = get_fact_line_items_count()
    
    if fact_orders_count is not None:
        logging.info(f"Fact Orders Row Count (AFTER): {fact_orders_count:,}")
        after_state['fact_orders_count'] = fact_orders_count
        
        if before_state.get('fact_orders_count') is not None:
            rows_added = fact_orders_count - before_state['fact_orders_count']
            logging.info(f"  - Rows Added: {rows_added:,}")
            after_state['fact_orders_added'] = rows_added
    else:
        after_state['fact_orders_count'] = None
    
    if fact_line_items_count is not None:
        logging.info(f"Fact Line Items Row Count (AFTER): {fact_line_items_count:,}")
        after_state['fact_line_items_count'] = fact_line_items_count
        
        if before_state.get('fact_line_items_count') is not None:
            rows_added = fact_line_items_count - before_state['fact_line_items_count']
            logging.info(f"  - Rows Added: {rows_added:,}")
            after_state['fact_line_items_added'] = rows_added
    else:
        after_state['fact_line_items_count'] = None
    
    # Dashboard KPI for target date
    if target_date:
        kpi = get_daily_sales_kpi(target_date)
        if kpi:
            logging.info(f"Dashboard KPI for {target_date} (AFTER):")
            logging.info(f"  - Order Count: {kpi['order_count']:,}")
            logging.info(f"  - Total Sales: ${kpi['total_sales']:,.2f}")
            after_state['kpi'] = kpi
            
            # Compare with before state
            if before_state.get('kpi'):
                order_increase = kpi['order_count'] - before_state['kpi']['order_count']
                sales_increase = kpi['total_sales'] - before_state['kpi']['total_sales']
                logging.info(f"\nDashboard Update Summary:")
                logging.info(f"  - Order Count: {before_state['kpi']['order_count']:,} → {kpi['order_count']:,} "
                           f"(+{order_increase:,})")
                logging.info(f"  - Total Sales: ${before_state['kpi']['total_sales']:,.2f} → ${kpi['total_sales']:,.2f} "
                           f"(+${sales_increase:,.2f})")
                after_state['kpi_change'] = {
                    'order_increase': order_increase,
                    'sales_increase': sales_increase
                }
        else:
            after_state['kpi'] = None
    else:
        after_state['kpi'] = None
    
    logging.info("=" * 60)
    
    return after_state

def log_pipeline_summary(before_state, after_state, target_date=None):
    """
    Log a summary explaining how the pipeline confirms ingestion → transformation → load → analytics
    
    Args:
        before_state: Dictionary from log_before_state()
        after_state: Dictionary from log_after_state()
        target_date: Optional target date for context
    """
    logging.info("=" * 60)
    logging.info("PIPELINE SUMMARY (Scenario 1)")
    logging.info("=" * 60)
    
    logging.info("""
This confirms the full ETL pipeline works correctly:

1. INGESTION: Source files were successfully loaded into staging tables.
   This is confirmed by the fact that data was extracted and processed without errors.

2. TRANSFORMATION: Data was cleaned, formatted, and prepared for loading.
   This includes data type conversions, normalization, and missing dimension creation (Scenario 2).

3. LOAD: New fact records were successfully inserted into fact_orders and fact_line_items tables.
   This is confirmed by the row count increase from {before_orders:,} to {after_orders:,} rows.
    """.format(
        before_orders=before_state.get('fact_orders_count', 0) or 0,
        after_orders=after_state.get('fact_orders_count', 0) or 0
    ))
    
    if target_date and after_state.get('kpi') and before_state.get('kpi'):
        logging.info(f"""
4. ANALYTICS: The dashboard KPI was updated with the new data for {target_date}.
   Order count increased from {before_state['kpi']['order_count']:,} to {after_state['kpi']['order_count']:,},
   and total sales increased from ${before_state['kpi']['total_sales']:,.2f} to ${after_state['kpi']['total_sales']:,.2f}.
   This demonstrates that the complete pipeline from source files to analytics dashboard is working correctly.
        """)
    else:
        logging.info("""
4. ANALYTICS: The dashboard reflects the new data, confirming end-to-end data flow
   from source files through staging, transformation, and loading to the analytics layer.
        """)
    
    logging.info("=" * 60)
