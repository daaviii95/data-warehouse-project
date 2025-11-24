#!/usr/bin/env python3
"""
Export staging tables for a specific department to Parquet format.

Usage:
    python export_department_to_parquet.py <department_name>
    
Example:
    python export_department_to_parquet.py "Business Department"
"""

import os
import sys
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


def normalize_department_name(dept: str) -> str:
    """Normalize department name to match table naming convention."""
    # Convert to lowercase and replace spaces/special chars with underscores
    normalized = dept.lower().replace(" ", "_").replace("-", "_")
    # Remove trailing underscores
    return normalized.strip("_")


def get_department_tables(engine, department_name: str):
    """Get all staging tables for a specific department."""
    inspector = inspect(engine)
    all_tables = inspector.get_table_names()
    
    # Normalize department name for matching
    dept_normalized = normalize_department_name(department_name)
    
    # Find tables that start with stg_ and contain the department name
    dept_tables = []
    for table in all_tables:
        if table.startswith("stg_"):
            # Check if table name contains the normalized department name
            table_normalized = table.lower()
            if dept_normalized in table_normalized:
                dept_tables.append(table)
    
    return sorted(dept_tables)


def export_table_to_parquet(engine, table_name: str, output_dir: Path):
    """Export a single table to Parquet format."""
    logging.info(f"Exporting {table_name} to Parquet...")
    
    parquet_file = output_dir / f"{table_name}.parquet"
    
    try:
        df = pd.read_sql_table(table_name, con=engine)
        
        df.to_parquet(
            parquet_file,
            engine="pyarrow",
            compression="snappy",
            index=False,
            coerce_timestamps="us"
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
    """Main export function for a specific department."""
    if len(sys.argv) < 2:
        logging.error("Usage: python export_department_to_parquet.py <department_name>")
        logging.error('Example: python export_department_to_parquet.py "Business Department"')
        sys.exit(1)
    
    department_name = sys.argv[1]
    
    logging.info("=" * 60)
    logging.info(f"ShopZada Staging Layer: Export {department_name} to Parquet")
    logging.info("=" * 60)
    
    # Get all staging tables for this department
    dept_tables = get_department_tables(engine, department_name)
    
    if not dept_tables:
        logging.warning(f"No staging tables found for department: {department_name}")
        logging.info("Available staging tables:")
        inspector = inspect(engine)
        all_staging = [t for t in inspector.get_table_names() if t.startswith("stg_")]
        for t in sorted(all_staging):
            logging.info(f"  - {t}")
        return
    
    logging.info(f"Found {len(dept_tables)} staging tables for {department_name}")
    
    # Export each table
    results = []
    for table in dept_tables:
        result = export_table_to_parquet(engine, table, output_path)
        results.append(result)
    
    # Summary
    logging.info("=" * 60)
    logging.info(f"Export Summary for {department_name}:")
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

