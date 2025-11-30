"""
ShopZada Data Quality Monitoring DAG
Runs data quality checks independently for monitoring purposes
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/repo/scripts')

default_args = {
    'owner': 'shopzada-data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_data_quality():
    """Run data quality checks"""
    import data_quality
    data_quality.run_all_checks()

with DAG(
    'shopzada_data_quality_monitoring',
    default_args=default_args,
    description='ShopZada Data Quality Monitoring - Independent DAG',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['shopzada', 'data-quality', 'monitoring'],
) as dag:

    quality_check = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_data_quality,
    )

