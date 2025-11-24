"""
ShopZada All Departments - Parquet Export Orchestrator DAG.

This parent DAG triggers all department-specific Parquet export DAGs in parallel.
Each department DAG can also be run independently.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "shopzada-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="shopzada_all_depts_parquet",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["shopzada", "kimball", "staging", "parquet", "orchestrator"],
    description="Orchestrates Parquet export for all ShopZada departments in parallel.",
) as dag:
    trigger_business = TriggerDagRunOperator(
        task_id="trigger_business_dept",
        trigger_dag_id="shopzada_business_dept_parquet",
        wait_for_completion=True,
    )

    trigger_customer = TriggerDagRunOperator(
        task_id="trigger_customer_dept",
        trigger_dag_id="shopzada_customer_dept_parquet",
        wait_for_completion=True,
    )

    trigger_enterprise = TriggerDagRunOperator(
        task_id="trigger_enterprise_dept",
        trigger_dag_id="shopzada_enterprise_dept_parquet",
        wait_for_completion=True,
    )

    trigger_marketing = TriggerDagRunOperator(
        task_id="trigger_marketing_dept",
        trigger_dag_id="shopzada_marketing_dept_parquet",
        wait_for_completion=True,
    )

    trigger_operations = TriggerDagRunOperator(
        task_id="trigger_operations_dept",
        trigger_dag_id="shopzada_operations_dept_parquet",
        wait_for_completion=True,
    )

    # Run all departments in parallel
    [trigger_business, trigger_customer, trigger_enterprise, trigger_marketing, trigger_operations]

