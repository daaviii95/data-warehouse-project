"""
ShopZada ETL Pipeline DAG
Kimball Methodology - Star Schema Data Warehouse

This DAG orchestrates the complete ETL pipeline:
1. Ingestion: Load data from various sources into staging tables
2. Transformation: Transform staging data into dimensional model
3. Data Quality: Validate data integrity and quality
4. Loading: Load into fact and dimension tables
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/repo/scripts')

def execute_sql_file(sql_file_path):
    """Helper function to execute SQL file"""
    try:
        hook = PostgresHook(postgres_conn_id='shopzada_postgres')
        full_path = f'/opt/airflow/repo/{sql_file_path}'
        
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"SQL file not found: {full_path}")
        
        with open(full_path, 'r') as f:
            sql = f.read()
        
        if not sql.strip():
            raise ValueError(f"SQL file is empty: {full_path}")
        
        # Execute SQL using connection to handle dollar-quoted strings properly
        # Parse SQL to split by semicolons while respecting DO $$ ... END $$; blocks
        statements = []
        i = 0
        current_statement = ""
        in_dollar_quote = False
        dollar_tag = None
        
        while i < len(sql):
            char = sql[i]
            
            # Check for start of dollar quote
            if char == '$' and not in_dollar_quote:
                # Look ahead to find the tag (could be $$ or $tag$)
                tag_start = i
                tag_end = i + 1
                
                # Check if it's $$ (empty tag)
                if tag_end < len(sql) and sql[tag_end] == '$':
                    dollar_tag = '$$'
                    in_dollar_quote = True
                    current_statement += '$$'
                    i = tag_end + 1
                    continue
                
                # Otherwise, find the tag (e.g., $tag$)
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
            
            # Check for semicolon (statement separator) - only if not in dollar quote
            if char == ';' and not in_dollar_quote:
                current_statement += char
                stmt = current_statement.strip()
                if stmt:
                    statements.append(stmt)
                current_statement = ""
                i += 1
                continue
            
            current_statement += char
            i += 1
        
        # Add remaining statement if any
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        # Execute each statement
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                for statement in statements:
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

default_args = {
    'owner': 'shopzada-data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_ingestion():
    """Run the ingestion script - now idempotent (skips already-ingested files)"""
    import ingest
    # The ingestion script now checks ingestion_log to skip already-ingested files
    ingest.main()

def run_data_quality_checks():
    """Run data quality validation checks"""
    import data_quality
    data_quality.run_all_checks()

# SQL execution wrapper functions (needed because lambdas can't be serialized by Airflow)
def exec_sql_01():
    execute_sql_file('sql/01_create_date_dimension.sql')

def exec_sql_02():
    execute_sql_file('sql/02_load_dim_campaign.sql')

def exec_sql_03():
    execute_sql_file('sql/03_load_dim_product.sql')

def exec_sql_04():
    execute_sql_file('sql/04_load_dim_user.sql')

def exec_sql_05():
    execute_sql_file('sql/05_load_dim_staff.sql')

def exec_sql_06():
    execute_sql_file('sql/06_load_dim_merchant.sql')

def exec_sql_07():
    execute_sql_file('sql/07_load_dim_user_job.sql')

def exec_sql_08():
    execute_sql_file('sql/08_load_dim_credit_card.sql')

def exec_sql_09():
    execute_sql_file('sql/09_load_fact_orders.sql')

def exec_sql_10():
    execute_sql_file('sql/10_load_fact_line_items.sql')

def exec_sql_11():
    execute_sql_file('sql/11_load_fact_campaign_transactions.sql')

def exec_sql_12():
    execute_sql_file('sql/12_create_analytical_views.sql')

with DAG(
    'shopzada_etl_pipeline',
    default_args=default_args,
    description='ShopZada Complete ETL Pipeline - Kimball Star Schema',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['shopzada', 'etl', 'kimball', 'dwh'],
) as dag:

    # Task 1: Data Ingestion (idempotent - skips already-ingested files)
    ingest_data = PythonOperator(
        task_id='ingest_data_to_staging',
        python_callable=run_ingestion,
        doc_md="""
        ## Data Ingestion Task (Idempotent)
        
        Ingests all data files from the `/data` directory into PostgreSQL staging tables.
        Supports multiple file formats: CSV, Parquet, JSON, Excel, Pickle, HTML.
        
        Creates staging tables with naming convention: `stg_<department>_<filename>`
        
        **Idempotent**: Automatically skips files that have already been successfully ingested
        (checked via `ingestion_log` table). This means you can safely re-run this task
        without re-ingesting all data.
        """
    )

    # Task 2: Create Date Dimension (must be done before fact tables)
    create_date_dimension = PythonOperator(
        task_id='create_date_dimension',
        python_callable=exec_sql_01,
        doc_md="""
        ## Date Dimension Creation
        
        Creates and populates the date dimension table (dim_date).
        This is a standard dimension in Kimball methodology for time-based analysis.
        """
    )

    # Task 3: Transform and Load Dimension Tables
    load_dim_campaign = PythonOperator(
        task_id='load_dim_campaign',
        python_callable=exec_sql_02,
    )

    load_dim_product = PythonOperator(
        task_id='load_dim_product',
        python_callable=exec_sql_03,
    )

    load_dim_user = PythonOperator(
        task_id='load_dim_user',
        python_callable=exec_sql_04,
    )

    load_dim_staff = PythonOperator(
        task_id='load_dim_staff',
        python_callable=exec_sql_05,
    )

    load_dim_merchant = PythonOperator(
        task_id='load_dim_merchant',
        python_callable=exec_sql_06,
    )

    load_dim_user_job = PythonOperator(
        task_id='load_dim_user_job',
        python_callable=exec_sql_07,
    )

    load_dim_credit_card = PythonOperator(
        task_id='load_dim_credit_card',
        python_callable=exec_sql_08,
    )

    # Task 4: Transform and Load Fact Tables (depend on dimensions)
    load_fact_orders = PythonOperator(
        task_id='load_fact_orders',
        python_callable=exec_sql_09,
    )

    load_fact_line_items = PythonOperator(
        task_id='load_fact_line_items',
        python_callable=exec_sql_10,
    )

    load_fact_campaign_transactions = PythonOperator(
        task_id='load_fact_campaign_transactions',
        python_callable=exec_sql_11,
    )

    # Task 5: Data Quality Checks
    data_quality_checks = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=run_data_quality_checks,
        doc_md="""
        ## Data Quality Validation
        
        Runs comprehensive data quality checks:
        - Referential integrity
        - Null value checks
        - Duplicate detection
        - Data type validation
        """
    )

    # Task 6: Create Analytical Views
    create_analytical_views = PythonOperator(
        task_id='create_analytical_views',
        python_callable=exec_sql_12,
        doc_md="""
        ## Analytical Views Creation
        
        Creates SQL views optimized for business intelligence and reporting:
        - Campaign performance metrics
        - Merchant performance analysis
        - Customer segment revenue analysis
        """
    )

    # Define task dependencies (Kimball ETL flow)
    ingest_data >> create_date_dimension
    
    # Dimensions can be loaded in parallel
    create_date_dimension >> [
        load_dim_campaign,
        load_dim_product,
        load_dim_user,
        load_dim_staff,
        load_dim_merchant
    ]
    
    # User-dependent dimensions
    load_dim_user >> [
        load_dim_user_job,
        load_dim_credit_card
    ]
    
    # Fact tables depend on all dimensions
    all_dimensions = [
        load_dim_campaign,
        load_dim_product,
        load_dim_user,
        load_dim_staff,
        load_dim_merchant,
        load_dim_user_job,
        load_dim_credit_card
    ]
    
    all_facts = [
        load_fact_orders,
        load_fact_line_items,
        load_fact_campaign_transactions
    ]
    
    # Set dependencies: all dimensions must complete before facts
    for dim in all_dimensions:
        dim >> all_facts
    
    # Data quality and views depend on fact tables
    for fact in all_facts:
        fact >> data_quality_checks
    
    data_quality_checks >> create_analytical_views

