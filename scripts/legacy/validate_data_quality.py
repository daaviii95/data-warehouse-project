#!/usr/bin/env python3
"""
Data Quality Validation Script
Scans all source data files and validates against database
"""

import os
import sys
import logging
import pandas as pd
import json
import pickle
from pathlib import Path
from sqlalchemy import text
import sqlalchemy
from urllib.parse import quote_plus

# Configuration
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopzada")
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Build SQLAlchemy engine
password_quoted = quote_plus(DB_PASS)
engine_url = f"postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(engine_url, pool_size=5, max_overflow=10, future=True)

def load_file(file_path):
    """Load file based on extension"""
    ext = Path(file_path).suffix.lower()
    try:
        if ext == '.csv':
            return pd.read_csv(file_path, low_memory=False)
        elif ext == '.xlsx':
            return pd.read_excel(file_path)
        elif ext == '.json':
            with open(file_path, 'r') as f:
                data = json.load(f)
            return pd.json_normalize(data)
        elif ext == '.pickle':
            return pd.read_pickle(file_path)
        elif ext == '.parquet':
            return pd.read_parquet(file_path)
        elif ext == '.html':
            tables = pd.read_html(file_path)
            return tables[0] if tables else None
        else:
            logging.warning(f"Unsupported file type: {ext}")
            return None
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None

def validate_file(file_path, expected_columns=None):
    """Validate a single file"""
    print(f"\n{'='*80}")
    print(f"Validating: {file_path}")
    print(f"{'='*80}")
    
    df = load_file(file_path)
    if df is None:
        print("  ❌ FAILED: Could not load file")
        return False
    
    print(f"  ✓ Loaded: {len(df)} rows, {len(df.columns)} columns")
    print(f"  Columns: {list(df.columns)}")
    
    # Check for nulls
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        print(f"  ⚠️  Null values found:")
        for col, count in null_counts[null_counts > 0].items():
            pct = (count / len(df)) * 100
            print(f"     {col}: {count} ({pct:.2f}%)")
    else:
        print(f"  ✓ No null values")
    
    # Check for duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        print(f"  ⚠️  Duplicate rows: {duplicates} ({duplicates/len(df)*100:.2f}%)")
    else:
        print(f"  ✓ No duplicate rows")
    
    # Check expected columns
    if expected_columns:
        missing = set(expected_columns) - set(df.columns)
        if missing:
            print(f"  ⚠️  Missing expected columns: {missing}")
        else:
            print(f"  ✓ All expected columns present")
    
    # Sample data
    print(f"  Sample data (first 3 rows):")
    print(df.head(3).to_string())
    
    return True

def check_database_counts():
    """Check row counts in database"""
    print(f"\n{'='*80}")
    print("Database Row Counts")
    print(f"{'='*80}")
    
    tables = [
        'dim_campaign', 'dim_product', 'dim_user', 'dim_staff', 
        'dim_merchant', 'dim_user_job', 'dim_credit_card',
        'fact_orders', 'fact_line_items', 'fact_campaign_transactions'
    ]
    
    with engine.connect() as conn:
        for table in tables:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                print(f"  {table}: {count:,} rows")
            except Exception as e:
                print(f"  {table}: ERROR - {e}")

def check_referential_integrity():
    """Check referential integrity"""
    print(f"\n{'='*80}")
    print("Referential Integrity Checks")
    print(f"{'='*80}")
    
    checks = [
        ("fact_orders.user_sk", "dim_user.user_sk"),
        ("fact_orders.merchant_sk", "dim_merchant.merchant_sk"),
        ("fact_orders.staff_sk", "dim_staff.staff_sk"),
        ("fact_line_items.order_id", "fact_orders.order_id"),
        ("fact_line_items.product_sk", "dim_product.product_sk"),
        ("fact_campaign_transactions.order_id", "fact_orders.order_id"),
        ("fact_campaign_transactions.campaign_sk", "dim_campaign.campaign_sk"),
    ]
    
    with engine.connect() as conn:
        for fk_col, pk_col in checks:
            try:
                fk_table, fk_col_name = fk_col.split('.')
                pk_table, pk_col_name = pk_col.split('.')
                
                query = f"""
                    SELECT COUNT(*) 
                    FROM {fk_table} fk
                    LEFT JOIN {pk_table} pk ON fk.{fk_col_name} = pk.{pk_col_name}
                    WHERE pk.{pk_col_name} IS NULL AND fk.{fk_col_name} IS NOT NULL
                """
                result = conn.execute(text(query))
                orphaned = result.fetchone()[0]
                
                if orphaned > 0:
                    print(f"  ⚠️  {fk_col} -> {pk_col}: {orphaned:,} orphaned records")
                else:
                    print(f"  ✓ {fk_col} -> {pk_col}: All valid")
            except Exception as e:
                print(f"  ❌ {fk_col} -> {pk_col}: ERROR - {e}")

def check_data_consistency():
    """Check data consistency issues"""
    print(f"\n{'='*80}")
    print("Data Consistency Checks")
    print(f"{'='*80}")
    
    with engine.connect() as conn:
        # Check for negative values where they shouldn't exist
        checks = [
            ("fact_orders", "delay_days < 0", "Negative delay_days"),
            ("fact_orders", "estimated_arrival_days < 0", "Negative estimated_arrival_days"),
            ("fact_line_items", "quantity < 0", "Negative quantity"),
            ("dim_product", "price < 0", "Negative price"),
        ]
        
        for table, condition, description in checks:
            try:
                query = f"SELECT COUNT(*) FROM {table} WHERE {condition}"
                result = conn.execute(text(query))
                count = result.fetchone()[0]
                if count > 0:
                    print(f"  ⚠️  {description}: {count:,} records")
                else:
                    print(f"  ✓ {description}: None found")
            except Exception as e:
                print(f"  ❌ {description}: ERROR - {e}")

def main():
    """Main validation function"""
    print("="*80)
    print("SHOPZADA DATA QUALITY VALIDATION REPORT")
    print("="*80)
    
    data_path = Path(DATA_DIR)
    if not data_path.exists():
        print(f"ERROR: Data directory not found: {DATA_DIR}")
        return
    
    # Validate source files
    print("\n" + "="*80)
    print("SOURCE FILE VALIDATION")
    print("="*80)
    
    # Campaign data
    campaign_files = list(data_path.rglob("*campaign_data.csv"))
    for f in campaign_files:
        if 'transactional' not in str(f).lower():
            validate_file(f, expected_columns=['campaign_id', 'campaign_name', 'discount'])
    
    # Product data
    product_files = list(data_path.rglob("*product_list*"))
    for f in product_files:
        validate_file(f)
    
    # User data
    user_files = list(data_path.rglob("user_data.*"))
    for f in user_files:
        validate_file(f)
    
    user_job_files = list(data_path.rglob("user_job.*"))
    for f in user_job_files:
        validate_file(f)
    
    # Merchant data
    merchant_files = list(data_path.rglob("*merchant_data*"))
    for f in merchant_files:
        if 'order_with_merchant' not in str(f):
            validate_file(f)
    
    # Staff data
    staff_files = list(data_path.rglob("*staff_data*"))
    for f in staff_files:
        validate_file(f)
    
    # Order data
    order_files = list(data_path.rglob("order_data*"))
    for f in order_files[:3]:  # Sample first 3
        validate_file(f)
    
    # Line item data
    line_item_files = list(data_path.rglob("line_item_data*"))
    for f in line_item_files[:3]:  # Sample first 3
        validate_file(f)
    
    # Database validation
    check_database_counts()
    check_referential_integrity()
    check_data_consistency()
    
    print("\n" + "="*80)
    print("VALIDATION COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

