"""
ShopZada ETL Pipeline DAG
Kimball Methodology - Star Schema Data Warehouse
Based on Physical Model (physicalmodel.txt)

This DAG orchestrates the complete ETL pipeline:
1. Schema Creation: Create all dimension and fact tables from physical model
2. Date Dimension: Populate date dimension
3. Ingestion: Load data from various sources into staging tables
4. Dimension Loading: Transform staging data into dimension tables
5. Fact Loading: Transform staging data into fact tables
6. Data Quality: Validate data integrity and quality
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
        # Also handle SQL comments (-- style)
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
                # Add newline to preserve statement structure
                if i < len(sql):
                    current_statement += '\n'
                    i += 1
                continue
            
            # Handle string literals (single quotes) - needed to avoid treating -- inside strings as comments
            if char == "'" and not in_dollar_quote and not in_double_quote:
                if in_single_quote and i + 1 < len(sql) and sql[i + 1] == "'":
                    # Escaped single quote ('')
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
            
            # Check for semicolon (statement separator) - only if not in quotes
            if char == ';' and not in_dollar_quote and not in_single_quote and not in_double_quote:
                current_statement += char
                stmt = current_statement.strip()
                # Only add non-empty statements that contain actual SQL (not just whitespace/comments)
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)
                current_statement = ""
                i += 1
                continue
            
            current_statement += char
            i += 1
        
        # Add remaining statement if any (and it's not just comments/whitespace)
        remaining = current_statement.strip()
        if remaining and not remaining.startswith('--'):
            # Remove any trailing comments
            lines = remaining.split('\n')
            cleaned_lines = []
            for line in lines:
                # Remove inline comments (-- style) but preserve the line if it has content before the comment
                if '--' in line:
                    before_comment = line.split('--')[0].strip()
                    if before_comment:
                        cleaned_lines.append(before_comment)
                else:
                    cleaned_lines.append(line)
            cleaned = '\n'.join(cleaned_lines).strip()
            if cleaned:
                statements.append(cleaned)
        
        # Filter out empty statements and statements that are only comments/whitespace
        filtered_statements = []
        for stmt in statements:
            # Remove SQL comments and whitespace to check if statement has actual content
            cleaned = stmt
            # Remove single-line comments
            lines = cleaned.split('\n')
            cleaned_lines = []
            for line in lines:
                if '--' in line:
                    # Keep only the part before the comment
                    before_comment = line.split('--')[0]
                    if before_comment.strip():
                        cleaned_lines.append(before_comment)
                else:
                    cleaned_lines.append(line)
            cleaned = '\n'.join(cleaned_lines)
            # Remove all whitespace
            cleaned_no_ws = ''.join(cleaned.split())
            # Only add if there's actual SQL content (not just comments/whitespace)
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

def run_python_etl():
    """Run Python-based ETL pipeline (similar to original SQLite scripts)"""
    import etl_pipeline_python
    etl_pipeline_python.main()

def run_data_quality_checks():
    """Run data quality validation checks"""
    import data_quality
    data_quality.run_all_checks()

# SQL execution wrapper functions (needed because lambdas can't be serialized by Airflow)
def exec_sql_01():
    execute_sql_file('sql/01_create_schema_from_physical_model.sql')

def exec_sql_02():
    execute_sql_file('sql/02_populate_dim_date.sql')

def exec_sql_13():
    execute_sql_file('sql/13_create_analytical_views.sql')

with DAG(
    'shopzada_etl_pipeline',
    default_args=default_args,
    description='ShopZada Complete ETL Pipeline - Kimball Star Schema (Physical Model)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['shopzada', 'etl', 'kimball', 'dwh', 'physical-model'],
) as dag:

    # Task 1: Create Schema from Physical Model
    create_schema = PythonOperator(
        task_id='create_schema',
        python_callable=exec_sql_01,
        doc_md="""
        ## Schema Creation
        
        Creates all dimension and fact tables exactly as specified in physicalmodel.txt.
        This includes:
        - Dimension tables: dim_campaign, dim_product, dim_user, dim_staff, dim_merchant, dim_date, dim_user_job, dim_credit_card
        - Fact tables: fact_orders, fact_line_items, fact_campaign_transactions
        """
    )

    # Task 2: Populate Date Dimension (must be done before fact tables)
    populate_date_dimension = PythonOperator(
        task_id='populate_date_dimension',
        python_callable=exec_sql_02,
        doc_md="""
        ## Date Dimension Population
        
        Populates the date dimension table (dim_date) with dates from 2020-01-01 to 2025-12-31.
        This is a standard dimension in Kimball methodology for time-based analysis.
        """
    )

    # Task 3: Python-based ETL (similar to original SQLite scripts)
    # This directly loads from data files into dimensions and facts
    python_etl = PythonOperator(
        task_id='run_python_etl',
        python_callable=run_python_etl,
        doc_md="""
        ## Python-based ETL Pipeline
        
        Similar to original SQLite scripts - directly reads data files and loads into PostgreSQL.
        - Reads from /opt/airflow/data directory
        - Merges data using pandas (like original scripts)
        - Loads directly into dimension and fact tables
        - Handles all file formats: CSV, Parquet, JSON, Excel, Pickle, HTML
        """
    )

    # Task 7: Data Quality Checks
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

    # Note: Analytical Views are now in a separate DAG (shopzada_analytical_views)
    # This allows refreshing views without re-running the entire ETL pipeline
    # Run 'shopzada_analytical_views' DAG independently when needed

    # Define task dependencies (Kimball ETL flow)
    # Option 1: Python-based ETL (similar to original SQLite scripts)
    # Step 1: Create schema first
    create_schema >> populate_date_dimension
    
    # Step 2: Run Python ETL (loads everything directly from data files)
    populate_date_dimension >> python_etl
    
    # Step 3: Data quality checks after Python ETL
    python_etl >> data_quality_checks
    
    # Note: Analytical views are now in a separate DAG for independent execution
    
    # Note: SQL-based ETL tasks (load_dim_*, load_fact_*) have been removed
    # as they require staging tables that are not created by the Python ETL pipeline.
    # The Python ETL pipeline loads directly from source files into dimension/fact tables.
    # If you need SQL-based loading, you would need to:
    # 1. Create staging tables first
    # 2. Load data into staging tables
    # 3. Then load from staging to dimension/fact tables
