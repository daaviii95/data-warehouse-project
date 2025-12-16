#!/usr/bin/env python3
"""
Pre-process Raw Data Files to Parquet Format
Converts raw files (CSV, HTML, JSON, Excel, Pickle) to cleaned parquet files
for faster ingestion while preserving all features and scenarios.
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import json

# Import shared utilities
sys.path.insert(0, os.path.dirname(__file__))
from etl_pipeline_python import (
    load_file, find_files, clean_dataframe,
    _format_dataframe_for_staging  # We'll need to import this or recreate logic
)

# Configuration
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
CLEANED_DIR = os.getenv("CLEANED_DATA_DIR", os.path.join(DATA_DIR, "cleaned"))
FORCE_REPROCESS = os.getenv("FORCE_REPROCESS", "false").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def _get_parquet_path(original_file_path, data_dir, cleaned_dir):
    """
    Generate parquet file path from original file path
    
    Example:
        data/Operations Department/order_data.csv
        -> data/cleaned/Operations Department/order_data.parquet
    """
    original_path = Path(original_file_path)
    data_path = Path(data_dir)
    
    try:
        # Get relative path from data directory
        relative_path = original_path.relative_to(data_path)
        
        # Replace extension with .parquet
        parquet_name = relative_path.stem + ".parquet"
        
        # Build cleaned path maintaining directory structure
        cleaned_path = Path(cleaned_dir) / relative_path.parent / parquet_name
        
        return cleaned_path
    except ValueError:
        # File is outside data_dir, use filename only
        return Path(cleaned_dir) / (original_path.stem + ".parquet")

def _should_reprocess_file(original_file_path, parquet_path):
    """
    Determine if file should be reprocessed
    
    Returns:
        tuple: (should_reprocess: bool, reason: str)
    """
    if FORCE_REPROCESS:
        return True, "FORCE_REPROCESS is enabled"
    
    # If parquet doesn't exist, need to process
    if not parquet_path.exists():
        return True, "Parquet file does not exist"
    
    # Check if original file is newer than parquet
    try:
        original_mtime = os.path.getmtime(original_file_path)
        parquet_mtime = os.path.getmtime(parquet_path)
        
        if original_mtime > parquet_mtime:
            return True, f"Original file modified (original: {datetime.fromtimestamp(original_mtime)}, parquet: {datetime.fromtimestamp(parquet_mtime)})"
        else:
            return False, f"Parquet file is up to date (parquet: {datetime.fromtimestamp(parquet_mtime)})"
    except Exception as e:
        logging.warning(f"Could not check file modification times: {e}")
        return True, f"Could not verify file status, will reprocess: {e}"

def _save_parquet_with_metadata(df, parquet_path, original_file_path, table_name=None):
    """
    Save DataFrame to parquet with metadata about original file
    
    Metadata stored in parquet file metadata:
    - original_file_path: Path to original file
    - original_mtime: Modification time of original file
    - processed_at: When parquet was created
    - table_name: Target staging table (if applicable)
    """
    # Ensure directory exists
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Prepare metadata
    original_mtime = os.path.getmtime(original_file_path)
    metadata = {
        'original_file_path': str(original_file_path),
        'original_mtime': str(original_mtime),
        'processed_at': datetime.now().isoformat(),
    }
    if table_name:
        metadata['table_name'] = table_name
    
    # Convert metadata to string (parquet metadata must be strings)
    metadata_str = json.dumps(metadata)
    
    # Save parquet with metadata
    df.to_parquet(
        parquet_path,
        engine='pyarrow',
        compression='snappy',
        metadata={'source_metadata': metadata_str}
    )
    
    logging.info(f"Saved parquet: {parquet_path} ({len(df):,} rows)")

def _get_table_name_for_file(file_path):
    """
    Determine which staging table a file should be ingested into
    This helps with proper formatting during pre-processing
    """
    file_str = str(file_path).lower()
    
    # Marketing Department
    if 'campaign_data' in file_str and 'transactional' not in file_str:
        return 'stg_marketing_department_campaign_data'
    elif 'transactional_campaign_data' in file_str:
        return 'stg_marketing_department_transactional_campaign_data'
    
    # Operations Department
    elif 'order_data' in file_str:
        return 'stg_operations_department_order_data'
    elif 'line_item_data_prices' in file_str:
        return 'stg_operations_department_line_item_data_prices'
    elif 'line_item_data_products' in file_str:
        return 'stg_operations_department_line_item_data_products'
    elif 'order_delays' in file_str:
        return 'stg_operations_department_order_delays'
    
    # Business Department
    elif 'product_list' in file_str:
        return 'stg_business_department_product_list'
    
    # Customer Management Department
    elif 'user_data' in file_str and 'credit_card' not in file_str and 'job' not in file_str:
        return 'stg_customer_management_department_user_data'
    elif 'user_job' in file_str:
        return 'stg_customer_management_department_user_job'
    elif 'user_credit_card' in file_str:
        return 'stg_customer_management_department_user_credit_card'
    
    # Enterprise Department
    elif 'merchant_data' in file_str and 'order_with_merchant_data' not in file_str:
        return 'stg_enterprise_department_merchant_data'
    elif 'staff_data' in file_str:
        return 'stg_enterprise_department_staff_data'
    elif 'order_with_merchant_data' in file_str:
        return 'stg_enterprise_department_order_with_merchant_data'
    
    return None

def preprocess_file(file_path, data_dir=DATA_DIR, cleaned_dir=CLEANED_DIR):
    """
    Pre-process a single file to parquet format
    
    Args:
        file_path: Path to original file
        data_dir: Root data directory
        cleaned_dir: Directory to store cleaned parquet files
    
    Returns:
        tuple: (success: bool, parquet_path: Path or None, rows: int)
    """
    file_path = Path(file_path)
    
    # Skip if file doesn't exist
    if not file_path.exists():
        logging.warning(f"File does not exist: {file_path}")
        return False, None, 0
    
    # Skip if already parquet (no need to pre-process)
    if file_path.suffix.lower() == '.parquet':
        logging.debug(f"Skipping {file_path}: already parquet format")
        return False, None, 0
    
    # Determine parquet path
    parquet_path = _get_parquet_path(file_path, data_dir, cleaned_dir)
    
    # Check if reprocessing is needed
    should_reprocess, reason = _should_reprocess_file(file_path, parquet_path)
    if not should_reprocess:
        logging.debug(f"Skipping {file_path}: {reason}")
        return True, parquet_path, 0  # Already processed, return existing path
    
    logging.info(f"Pre-processing {file_path} -> {parquet_path} ({reason})")
    
    try:
        # Load and clean file (this applies clean_dataframe)
        df = load_file(file_path, clean=True)
        
        if df is None or df.empty:
            logging.warning(f"No data loaded from {file_path}")
            return False, None, 0
        
        # Determine target table for additional formatting
        table_name = _get_table_name_for_file(file_path)
        
        # Apply staging-specific formatting if table is known
        # Note: We apply basic cleaning here, full staging formatting happens during ingestion
        # This ensures parquet files are cleaned but not over-formatted
        # The ingest.py will still apply _format_dataframe_for_staging for type conversions
        
        if df is None or df.empty:
            logging.warning(f"No data after formatting for {file_path}")
            return False, None, 0
        
        # Save to parquet with metadata
        _save_parquet_with_metadata(df, parquet_path, file_path, table_name)
        
        return True, parquet_path, len(df)
        
    except Exception as e:
        logging.error(f"Error pre-processing {file_path}: {e}", exc_info=True)
        return False, None, 0

def preprocess_all_files(data_dir=DATA_DIR, cleaned_dir=CLEANED_DIR):
    """
    Pre-process all raw files in data directory to parquet format
    Excludes Test directory as per current behavior
    """
    logging.info("=" * 60)
    logging.info("PRE-PROCESSING RAW FILES TO PARQUET")
    logging.info("=" * 60)
    logging.info(f"Data directory: {data_dir}")
    logging.info(f"Cleaned directory: {cleaned_dir}")
    if FORCE_REPROCESS:
        logging.info("FORCE_REPROCESS enabled - will reprocess all files")
    logging.info("=" * 60)
    
    # Find all files (excluding Test directory via find_files)
    # We'll search for common patterns
    patterns = [
        "*campaign_data*",
        "*transactional_campaign_data*",
        "*order_data*",
        "*line_item_data_prices*",
        "*line_item_data_products*",
        "*order_delays*",
        "*product_list*",
        "*user_data*",
        "*user_job*",
        "*user_credit_card*",
        "*merchant_data*",
        "*staff_data*",
        "*order_with_merchant_data*"
    ]
    
    all_files = set()
    for pattern in patterns:
        files = find_files(pattern, data_dir)
        all_files.update(files)
    
    # Filter out parquet files (already in optimal format)
    files_to_process = [f for f in all_files if f.suffix.lower() != '.parquet']
    
    logging.info(f"Found {len(files_to_process)} files to pre-process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    total_rows = 0
    
    for file_path in sorted(files_to_process):
        success, parquet_path, rows = preprocess_file(file_path, data_dir, cleaned_dir)
        
        if success:
            if rows > 0:
                processed_count += 1
                total_rows += rows
            else:
                skipped_count += 1
        else:
            error_count += 1
    
    logging.info("=" * 60)
    logging.info("PRE-PROCESSING SUMMARY")
    logging.info("=" * 60)
    logging.info(f"Files processed: {processed_count}")
    logging.info(f"Files skipped (already up to date): {skipped_count}")
    logging.info(f"Files with errors: {error_count}")
    logging.info(f"Total rows processed: {total_rows:,}")
    logging.info("=" * 60)
    
    return processed_count, skipped_count, error_count

if __name__ == "__main__":
    preprocess_all_files()

