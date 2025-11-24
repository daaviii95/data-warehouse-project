#!/usr/bin/env python3
"""
Export all staging tables to Parquet format.

This script reads all `stg_*` tables from the ShopZada database and exports
them as standardized Parquet files in the staging layer directory.
"""

import os
import logging
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import sqlalchemy
from sqlalchemy import text, inspect

# CONFIG via env vars
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopzada")
STAGING_OUTPUT_DIR = os.getenv("STAGING_OUTPUT_DIR", "/opt/airflow/data/staging_parquet")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Build SQLAlchemy engine
password_quoted = quote_plus(DB_PASS)
engine_url = f"postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
logging.info(f"Connecting to DB: {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}")
engine = sqlalchemy.create_engine(engine_url, pool_size=5, max_overflow=10, future=True)

# Create output directory
output_path = Path(STAGING_OUTPUT_DIR)
output_path.mkdir(parents=True, exist_ok=True)
logging.info(f"Output directory: {output_path}")


def get_staging_tables(engine):
    """Get all staging tables (stg_*) from the database."""
    inspector = inspect(engine)
    all_tables = inspector.get_table_names()
    staging_tables = [t for t in all_tables if t.startswith("stg_")]
    logging.info(f"Found {len(staging_tables)} staging tables")
    return sorted(staging_tables)


def export_table_to_parquet(engine, table_name: str, output_dir: Path):
    """Export a single table to Parquet format."""
    logging.info(f"Exporting {table_name} to Parquet...")
    
    # Read table in chunks for large tables
    chunk_size = 100000
    parquet_file = output_dir / f"{table_name}.parquet"
    
    try:
        # Read entire table (or in chunks for very large tables)
        df = pd.read_sql_table(table_name, con=engine)
        
        # Write to Parquet with compression
        df.to_parquet(
            parquet_file,
            engine="pyarrow",
            compression="snappy",
            index=False,
            coerce_timestamps="us"  # Use microsecond precision for timestamps
        )
        
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        logging.info(
            f"✓ Exported {table_name}: {len(df):,} rows, {len(df.columns)} columns, "
            f"{file_size_mb:.2f} MB -> {parquet_file.name}"
        )
        
        return {
            "table": table_name,
            "rows": len(df),
            "columns": len(df.columns),
            "file_size_mb": file_size_mb,
            "file_path": str(parquet_file),
            "status": "success"
        }
        
    except Exception as e:
        logging.error(f"✗ Failed to export {table_name}: {e}")
        return {
            "table": table_name,
            "rows": 0,
            "columns": 0,
            "file_size_mb": 0,
            "file_path": None,
            "status": "failed",
            "error": str(e)
        }


def main():
    """Main export function."""
    logging.info("=" * 60)
    logging.info("ShopZada Staging Layer: Export to Parquet")
    logging.info("=" * 60)
    
    # Get all staging tables
    staging_tables = get_staging_tables(engine)
    
    if not staging_tables:
        logging.warning("No staging tables found. Run ingestion first.")
        return
    
    # Export each table
    results = []
    for table in staging_tables:
        result = export_table_to_parquet(engine, table, output_path)
        results.append(result)
    
    # Summary
    logging.info("=" * 60)
    logging.info("Export Summary:")
    logging.info("=" * 60)
    
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]
    
    total_rows = sum(r["rows"] for r in successful)
    total_size_mb = sum(r["file_size_mb"] for r in successful)
    
    logging.info(f"✓ Successfully exported: {len(successful)}/{len(results)} tables")
    logging.info(f"  Total rows: {total_rows:,}")
    logging.info(f"  Total size: {total_size_mb:.2f} MB")
    
    if failed:
        logging.warning(f"✗ Failed exports: {len(failed)} tables")
        for f in failed:
            logging.warning(f"  - {f['table']}: {f.get('error', 'Unknown error')}")
    
    logging.info(f"Parquet files saved to: {output_path}")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()

