"""
ShopZada Analytical Views DAG
Regenerates analytical views without re-running the entire ETL pipeline

This DAG can be run independently to refresh analytical views after:
- Schema changes
- View definition updates
- Data corrections
- Without re-ingesting all data (saves time)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os
import re
from pathlib import Path

# Get the project root directory dynamically
# In Docker: /opt/airflow/dags (where DAGs are mounted)
# Locally: parent of workflows directory
DAG_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = DAG_DIR.parent

# Determine SQL path
# In Docker: SQL files are at /opt/airflow/repo/sql (not /opt/airflow/sql)
# Locally: SQL files are at PROJECT_ROOT/sql
SQL_DIR = PROJECT_ROOT / 'sql'

# Check for Docker repo structure first
docker_repo_sql = Path('/opt/airflow/repo/sql')
if docker_repo_sql.exists():
    # Docker environment
    SQL_DIR = docker_repo_sql

default_args = {
    'owner': 'shopzada-data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def execute_sql_file(sql_file_path):
    """Helper function to execute SQL file"""
    try:
        hook = PostgresHook(postgres_conn_id='shopzada_postgres')
        
        # SQL_DIR is already set correctly (Docker or local) in module initialization
        full_path = SQL_DIR / sql_file_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"SQL file not found: {full_path}")
        
        full_path = str(full_path)
        
        with open(full_path, 'r') as f:
            sql = f.read()
        
        if not sql.strip():
            raise ValueError(f"SQL file is empty: {full_path}")
        
        # Parse SQL to split by semicolons while respecting dollar-quoted strings and comments
        statements = []
        i = 0
        current_statement = ""
        in_dollar_quote = False
        dollar_tag = None
        in_single_quote = False
        in_double_quote = False
        
        while i < len(sql):
            char = sql[i]
            
            # Handle single-line comments (-- style) - only if not in quotes
            if char == '-' and i + 1 < len(sql) and sql[i + 1] == '-' and not in_dollar_quote and not in_single_quote and not in_double_quote:
                # Skip to end of line
                while i < len(sql) and sql[i] != '\n':
                    i += 1
                if i < len(sql):
                    current_statement += '\n'
                    i += 1
                continue
            
            # Handle string literals (single quotes)
            if char == "'" and not in_dollar_quote and not in_double_quote:
                if in_single_quote and i + 1 < len(sql) and sql[i + 1] == "'":
                    current_statement += "''"
                    i += 2
                    continue
                else:
                    in_single_quote = not in_single_quote
                    current_statement += char
                    i += 1
                    continue
            
            # Handle double quotes (for identifiers)
            if char == '"' and not in_dollar_quote and not in_single_quote:
                in_double_quote = not in_double_quote
                current_statement += char
                i += 1
                continue
            
            # Check for start of dollar quote
            if char == '$' and not in_dollar_quote and not in_single_quote and not in_double_quote:
                tag_start = i
                tag_end = i + 1
                
                if tag_end < len(sql) and sql[tag_end] == '$':
                    dollar_tag = '$$'
                    in_dollar_quote = True
                    current_statement += '$$'
                    i = tag_end + 1
                    continue
                
                while tag_end < len(sql) and sql[tag_end] != '$':
                    tag_end += 1
                if tag_end < len(sql):
                    dollar_tag = sql[tag_start:tag_end+1]
                    in_dollar_quote = True
                    current_statement += dollar_tag
                    i = tag_end + 1
                    continue
            
            # Check for end of dollar quote
            if in_dollar_quote:
                if sql[i:i+len(dollar_tag)] == dollar_tag:
                    current_statement += dollar_tag
                    i += len(dollar_tag)
                    in_dollar_quote = False
                    dollar_tag = None
                    continue
            
            # Check for semicolon (statement separator)
            if char == ';' and not in_dollar_quote and not in_single_quote and not in_double_quote:
                current_statement += char
                stmt = current_statement.strip()
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)
                current_statement = ""
                i += 1
                continue
            
            current_statement += char
            i += 1
        
        # Add remaining statement if any
        remaining = current_statement.strip()
        if remaining and not remaining.startswith('--'):
            lines = remaining.split('\n')
            cleaned_lines = []
            for line in lines:
                if '--' in line:
                    before_comment = line.split('--')[0].strip()
                    if before_comment:
                        cleaned_lines.append(before_comment)
                else:
                    cleaned_lines.append(line)
            cleaned = '\n'.join(cleaned_lines).strip()
            if cleaned:
                statements.append(cleaned)
        
        # Filter out empty statements
        filtered_statements = []
        for stmt in statements:
            cleaned = stmt
            lines = cleaned.split('\n')
            cleaned_lines = []
            for line in lines:
                if '--' in line:
                    before_comment = line.split('--')[0]
                    if before_comment.strip():
                        cleaned_lines.append(before_comment)
                else:
                    cleaned_lines.append(line)
            cleaned = '\n'.join(cleaned_lines)
            cleaned_no_ws = ''.join(cleaned.split())
            if cleaned_no_ws and not cleaned_no_ws.startswith('--'):
                filtered_statements.append(stmt)
        
        if not filtered_statements:
            import logging
            logging.warning(f"No valid SQL statements found in {sql_file_path}")
            return
        
        # Execute each statement
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                for statement in filtered_statements:
                    if statement.strip():
                        cursor.execute(statement)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise
            finally:
                cursor.close()
        finally:
            conn.close()
    except Exception as e:
        import logging
        logging.error(f"Error executing SQL file {sql_file_path}: {e}")
        raise

def exec_sql_03():
    execute_sql_file('sql/03_create_analytical_views.sql')

with DAG(
    'shopzada_analytical_views',
    default_args=default_args,
    description='ShopZada Analytical Views Regeneration - Run independently to refresh views without re-ingesting data',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['shopzada', 'analytical-views', 'dwh', 'power-bi'],
) as dag:

    # Task: Create/Refresh Analytical Views
    create_analytical_views = PythonOperator(
        task_id='create_analytical_views',
        python_callable=exec_sql_03,
        doc_md="""
        ## Analytical Views Creation/Refresh
        
        Creates or refreshes SQL views optimized for BI tools (Power BI, Tableau):
        - vw_campaign_performance - Campaign analysis
        - vw_merchant_performance - Merchant metrics
        - vw_customer_segment_revenue - Customer segments
        - vw_sales_by_time - Time-based trends
        - vw_product_performance - Product insights
        - vw_staff_performance - Staff productivity
        
        **Usage:**
        - Run this DAG independently when you need to refresh views
        - No need to re-run the entire ETL pipeline
        - Takes only seconds to complete
        - Safe to run multiple times (idempotent)
        """
    )

    # Single task - no dependencies needed
    create_analytical_views

