"""
ShopZada Operations Department - Parquet Export DAG.

This DAG exports all Operations Department staging tables to Parquet format.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

SHOPZADA_REPO_ROOT = Path(os.environ.get("SHOPZADA_REPO_ROOT", "/opt/airflow/repo"))
EXPORT_SCRIPT = SHOPZADA_REPO_ROOT / "scripts" / "export_department_to_parquet.py"
STAGING_OUTPUT_DIR = Path(os.environ.get("STAGING_OUTPUT_DIR", "/opt/airflow/data/staging_parquet"))

DB_HOST = os.environ.get("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "shopzada")
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "postgres")

DEPARTMENT = "Operations Department"


def export_department_to_parquet(**_context) -> None:
    """Export Operations Department staging tables to Parquet."""
    if not EXPORT_SCRIPT.exists():
        raise FileNotFoundError(f"Export script not found: {EXPORT_SCRIPT}")

    STAGING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["POSTGRES_HOST"] = DB_HOST
    env["POSTGRES_PORT"] = DB_PORT
    env["POSTGRES_DB"] = DB_NAME
    env["POSTGRES_USER"] = DB_USER
    env["POSTGRES_PASSWORD"] = DB_PASS
    env["STAGING_OUTPUT_DIR"] = str(STAGING_OUTPUT_DIR)

    logging.info(f"Starting {DEPARTMENT} export to Parquet via {EXPORT_SCRIPT}")
    subprocess.run(
        ["python", str(EXPORT_SCRIPT), DEPARTMENT],
        check=True,
        env=env
    )
    logging.info(f"{DEPARTMENT} export to Parquet completed.")


default_args = {
    "owner": "shopzada-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="shopzada_operations_dept_parquet",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["shopzada", "kimball", "staging", "operations", "parquet"],
    description=f"Export {DEPARTMENT} staging tables to Parquet format.",
) as dag:
    export_to_parquet = PythonOperator(
        task_id="export_operations_dept_to_parquet",
        python_callable=export_department_to_parquet,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    export_to_parquet

