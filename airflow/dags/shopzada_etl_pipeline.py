"""
ShopZada Kimball ETL Pipeline DAG.

This DAG orchestrates the complete ETL process:
1. Create Kimball dimensional model schema
2. Populate Date Dimension
3. Load Dimension Tables (SCD Type 1)
4. Load Fact Tables
5. Validate and snapshot results
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

SHOPZADA_REPO_ROOT = Path(os.environ.get("SHOPZADA_REPO_ROOT", "/opt/airflow/repo"))

DB_HOST = os.environ.get("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "shopzada")
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "postgres")


def create_kimball_schema(**_context) -> None:
    """Create Kimball dimensional model schema."""
    schema_file = SHOPZADA_REPO_ROOT / "sql" / "kimball_schema.sql"
    
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    import subprocess
    cmd = [
        "psql",
        f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        "-f", str(schema_file)
    ]
    
    logging.info(f"Creating Kimball schema from {schema_file}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logging.error(f"Schema creation failed: {result.stderr}")
        raise RuntimeError(f"Schema creation failed: {result.stderr}")
    
    logging.info("✓ Kimball schema created successfully")


def populate_date_dimension(**_context) -> None:
    """Populate Date Dimension table."""
    date_file = SHOPZADA_REPO_ROOT / "sql" / "populate_dim_date.sql"
    
    if not date_file.exists():
        raise FileNotFoundError(f"Date dimension file not found: {date_file}")
    
    import subprocess
    cmd = [
        "psql",
        f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        "-f", str(date_file)
    ]
    
    logging.info(f"Populating Date Dimension from {date_file}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logging.error(f"Date dimension population failed: {result.stderr}")
        raise RuntimeError(f"Date dimension population failed: {result.stderr}")
    
    logging.info("✓ Date Dimension populated successfully")


def load_dimensions(**_context) -> None:
    """Load all dimension tables from staging."""
    script = SHOPZADA_REPO_ROOT / "scripts" / "etl_dimensions.py"
    
    if not script.exists():
        raise FileNotFoundError(f"ETL script not found: {script}")
    
    env = os.environ.copy()
    env["POSTGRES_HOST"] = DB_HOST
    env["POSTGRES_PORT"] = DB_PORT
    env["POSTGRES_DB"] = DB_NAME
    env["POSTGRES_USER"] = DB_USER
    env["POSTGRES_PASSWORD"] = DB_PASS
    
    logging.info("Loading dimension tables...")
    subprocess.run(["python", str(script)], check=True, env=env)
    logging.info("✓ Dimension tables loaded")


def load_facts(**_context) -> None:
    """Load all fact tables from staging."""
    script = SHOPZADA_REPO_ROOT / "scripts" / "etl_facts.py"
    
    if not script.exists():
        raise FileNotFoundError(f"ETL script not found: {script}")
    
    env = os.environ.copy()
    env["POSTGRES_HOST"] = DB_HOST
    env["POSTGRES_PORT"] = DB_PORT
    env["POSTGRES_DB"] = DB_NAME
    env["POSTGRES_USER"] = DB_USER
    env["POSTGRES_PASSWORD"] = DB_PASS
    
    logging.info("Loading fact tables...")
    subprocess.run(["python", str(script)], check=True, env=env)
    logging.info("✓ Fact tables loaded")


def validate_etl(**_context) -> None:
    """Validate ETL results and generate summary."""
    from sqlalchemy import create_engine, text
    
    engine_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(engine_url)
    
    metrics = {}
    
    with engine.begin() as conn:
        # Count dimension records
        for dim in ['dim_date', 'dim_product', 'dim_customer', 'dim_merchant', 'dim_staff', 'dim_campaign']:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {dim}"))
            metrics[dim] = result.scalar_one()
        
        # Count fact records
        for fact in ['fact_sales', 'fact_campaign_performance']:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {fact}"))
            metrics[fact] = result.scalar_one()
        
        # Get latest ETL log
        result = conn.execute(text("""
            SELECT etl_name, status, rows_inserted, duration_seconds
            FROM etl_log
            ORDER BY created_at DESC
            LIMIT 10
        """))
        etl_logs = result.fetchall()
    
    logging.info("=" * 60)
    logging.info("ETL Validation Summary:")
    logging.info("=" * 60)
    logging.info("Dimension Tables:")
    for dim, count in metrics.items():
        if dim.startswith('dim_'):
            logging.info(f"  {dim}: {count:,} records")
    
    logging.info("Fact Tables:")
    for fact, count in metrics.items():
        if fact.startswith('fact_'):
            logging.info(f"  {fact}: {count:,} records")
    
    logging.info("Recent ETL Runs:")
    for log in etl_logs:
        logging.info(f"  {log[0]}: {log[1]} - {log[2]:,} rows in {log[3]}s")
    
    logging.info("=" * 60)


default_args = {
    "owner": "shopzada-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="shopzada_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger or can be scheduled
    catchup=False,
    max_active_runs=1,
    tags=["shopzada", "kimball", "etl", "dimensional"],
    description="Complete Kimball ETL pipeline: Schema → Dimensions → Facts → Validation",
) as dag:
    create_schema = PythonOperator(
        task_id="create_kimball_schema",
        python_callable=create_kimball_schema,
    )
    
    populate_date = PythonOperator(
        task_id="populate_date_dimension",
        python_callable=populate_date_dimension,
    )
    
    load_dims = PythonOperator(
        task_id="load_dimension_tables",
        python_callable=load_dimensions,
    )
    
    load_facts = PythonOperator(
        task_id="load_fact_tables",
        python_callable=load_facts,
    )
    
    validate = PythonOperator(
        task_id="validate_etl_results",
        python_callable=validate_etl,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # ETL Pipeline Flow
    create_schema >> populate_date >> load_dims >> load_facts >> validate

