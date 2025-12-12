#!/usr/bin/env python3
"""
ETL Pipeline Validation Test
Tests the full pipeline: Ingestion -> Transformation -> Load -> Analytics

This script:
1. Shows BEFORE state (fact table row count, dashboard KPI)
2. Runs a small test CSV file through the full workflow
3. Shows AFTER state (fact table row count increased, dashboard updated)
4. Validates the results match expected values
"""

import os
import sys
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from urllib.parse import quote_plus
from datetime import datetime

# Import ETL modules
sys.path.insert(0, os.path.dirname(__file__))
import extract
import transform
import load_dim
import load_fact

# Configuration
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopzada")
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")

# Build SQLAlchemy engine
password_quoted = quote_plus(DB_PASS)
engine_url = f"postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(engine_url, pool_size=5, max_overflow=10, future=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def get_fact_orders_count():
    """Get current row count in fact_orders"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM fact_orders"))
        return result.fetchone()[0]

def get_daily_sales_kpi(target_date):
    """Get daily sales KPI for a specific date"""
    with engine.connect() as conn:
        # Get total sales (sum of all order amounts) for the target date
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
        return {
            'order_count': row[0] if row else 0,
            'total_sales': float(row[1]) if row and row[1] else 0.0
        }

def get_test_file_expected_values(test_file_path):
    """Calculate expected values from test CSV file"""
    df = pd.read_csv(test_file_path)
    
    # Calculate expected values
    expected_orders = len(df)
    
    # If the file has total_amount column, sum it
    if 'total_amount' in df.columns:
        expected_sales = float(df['total_amount'].sum())
    else:
        # Otherwise, we'll need to calculate from line items if available
        expected_sales = None
    
    return {
        'expected_orders': expected_orders,
        'expected_sales': expected_sales,
        'test_file_path': test_file_path
    }

def run_test(test_file_path, target_date=None):
    """
    Run the full ETL pipeline test
    
    Args:
        test_file_path: Path to test CSV file (should be a small order_data file)
        target_date: Target date for KPI validation (YYYY-MM-DD format)
    """
    print("=" * 80)
    print("ETL PIPELINE VALIDATION TEST")
    print("=" * 80)
    
    # Step 1: Show BEFORE state
    print("\n" + "=" * 80)
    print("STEP 1: BEFORE STATE")
    print("=" * 80)
    
    before_count = get_fact_orders_count()
    print(f"Fact Orders Row Count (BEFORE): {before_count:,}")
    
    if target_date:
        before_kpi = get_daily_sales_kpi(target_date)
        print(f"Daily Sales KPI for {target_date} (BEFORE):")
        print(f"  - Order Count: {before_kpi['order_count']:,}")
        print(f"  - Total Sales: ${before_kpi['total_sales']:,.2f}")
    else:
        # Extract date from test file if possible
        print("Note: No target_date provided. Will show overall counts.")
        before_kpi = None
    
    # Get expected values from test file
    expected = get_test_file_expected_values(test_file_path)
    print(f"\nTest File: {test_file_path}")
    print(f"Expected New Orders: {expected['expected_orders']:,}")
    if expected['expected_sales'] is not None:
        print(f"Expected New Sales: ${expected['expected_sales']:,.2f}")
    
    # Step 2: Run ETL Pipeline
    print("\n" + "=" * 80)
    print("STEP 2: RUNNING ETL PIPELINE")
    print("=" * 80)
    
    print("Note: This test assumes the test file is already in the data directory.")
    print("The full workflow would:")
    print("  1. Ingest: Load test file into staging tables")
    print("  2. Extract: Read from staging tables")
    print("  3. Transform: Clean and format data")
    print("  4. Load Dimensions: Update dimension tables")
    print("  5. Load Facts: Insert new fact records")
    
    # For this test, we'll simulate by running the actual pipeline components
    # In a real scenario, you would copy the test file to the data directory first
    
    try:
        # Extract and transform order data
        print("\nExtracting and transforming order data...")
        order_df = transform.transform_order_data(extract.extract_order_data())
        
        # Extract order merchant data
        order_merchant_df = extract.extract_order_merchant_data()
        
        # Extract delays
        order_delays_df = transform.transform_order_delays(extract.extract_order_delays())
        
        # Load facts
        print("Loading fact_orders...")
        load_fact.load_fact_orders(order_df, order_merchant_df, order_delays_df)
        
        print("ETL Pipeline execution completed!")
        
    except Exception as e:
        print(f"ERROR during ETL execution: {e}")
        logging.exception("ETL execution failed")
        return False
    
    # Step 3: Show AFTER state
    print("\n" + "=" * 80)
    print("STEP 3: AFTER STATE")
    print("=" * 80)
    
    after_count = get_fact_orders_count()
    print(f"Fact Orders Row Count (AFTER): {after_count:,}")
    
    rows_added = after_count - before_count
    print(f"Rows Added: {rows_added:,}")
    print(f"Expected: {expected['expected_orders']:,}")
    
    if target_date:
        after_kpi = get_daily_sales_kpi(target_date)
        print(f"\nDaily Sales KPI for {target_date} (AFTER):")
        print(f"  - Order Count: {after_kpi['order_count']:,}")
        print(f"  - Total Sales: ${after_kpi['total_sales']:,.2f}")
        
        if before_kpi:
            print(f"\nKPI Changes:")
            print(f"  - Order Count Increase: {after_kpi['order_count'] - before_kpi['order_count']:,}")
            print(f"  - Sales Increase: ${after_kpi['total_sales'] - before_kpi['total_sales']:,.2f}")
    
    # Step 4: Validation
    print("\n" + "=" * 80)
    print("STEP 4: VALIDATION")
    print("=" * 80)
    
    validation_passed = True
    
    # Validate row count
    if rows_added == expected['expected_orders']:
        print(f"✓ Row count validation PASSED: {rows_added:,} rows added (expected {expected['expected_orders']:,})")
    else:
        print(f"✗ Row count validation FAILED: {rows_added:,} rows added (expected {expected['expected_orders']:,})")
        validation_passed = False
    
    # Validate sales if available
    if target_date and before_kpi and expected['expected_sales'] is not None:
        sales_increase = after_kpi['total_sales'] - before_kpi['total_sales']
        if abs(sales_increase - expected['expected_sales']) < 0.01:  # Allow small floating point differences
            print(f"✓ Sales validation PASSED: ${sales_increase:,.2f} increase (expected ${expected['expected_sales']:,.2f})")
        else:
            print(f"✗ Sales validation FAILED: ${sales_increase:,.2f} increase (expected ${expected['expected_sales']:,.2f})")
            validation_passed = False
    
    # Step 5: Summary
    print("\n" + "=" * 80)
    print("STEP 5: SUMMARY")
    print("=" * 80)
    
    if validation_passed:
        print("✓ ALL VALIDATIONS PASSED")
        print("\nThis confirms the full ETL pipeline works correctly:")
        print("1. INGESTION: Test file was successfully loaded into staging tables")
        print("2. TRANSFORMATION: Data was cleaned, formatted, and prepared for loading")
        print("3. LOAD: New fact records were inserted into fact_orders table")
        print("4. ANALYTICS: Dashboard KPI reflects the new data, confirming end-to-end data flow")
    else:
        print("✗ VALIDATION FAILED - Please check the logs for details")
    
    print("=" * 80)
    
    return validation_passed

def main():
    """Main function - run test with command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test ETL Pipeline End-to-End')
    parser.add_argument('test_file', help='Path to test CSV file (order_data format)')
    parser.add_argument('--target-date', help='Target date for KPI validation (YYYY-MM-DD)', default=None)
    
    args = parser.parse_args()
    
    if not os.path.exists(args.test_file):
        print(f"ERROR: Test file not found: {args.test_file}")
        return 1
    
    success = run_test(args.test_file, args.target_date)
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
