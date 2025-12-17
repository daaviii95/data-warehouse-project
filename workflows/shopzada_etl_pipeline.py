"""
ShopZada ETL Pipeline DAG
Kimball Methodology - Star Schema Data Warehouse
Based on Physical Model (physicalmodel.txt)

This DAG orchestrates the complete ETL pipeline:
1. Schema: Create all dimension and fact tables from physical model
2. Staging: Create staging tables for raw data
3. Ingest: Load data per department into staging tables
4. Transform: Clean and transform data from staging
5. Load Dim: Load transformed data into dimension tables
6. Load Fact: Load transformed data into fact tables
7. Views: Create analytical views for BI tools
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os
import logging
from pathlib import Path

# Get the project root directory dynamically
# In Docker: /opt/airflow/dags (where DAGs are mounted)
# Locally: parent of workflows directory
DAG_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = DAG_DIR.parent

# Determine scripts and SQL paths
# In Docker: scripts are at /opt/airflow/repo/scripts (not /opt/airflow/scripts)
# Locally: scripts are at PROJECT_ROOT/scripts
SCRIPTS_DIR = PROJECT_ROOT / 'scripts'
SQL_DIR = PROJECT_ROOT / 'sql'

# Check for Docker repo structure first
docker_repo_scripts = Path('/opt/airflow/repo/scripts')
docker_repo_sql = Path('/opt/airflow/repo/sql')

# Add scripts directory to path (works in both Docker and local)
logging.info("=" * 60)
logging.info("INITIALIZING DAG: shopzada_etl_pipeline")
logging.info(f"DAG_DIR: {DAG_DIR}")
logging.info(f"PROJECT_ROOT: {PROJECT_ROOT}")
logging.info(f"Checking for scripts directory...")
logging.info(f"  Docker path: {docker_repo_scripts} (exists: {docker_repo_scripts.exists()})")
logging.info(f"  Local path: {SCRIPTS_DIR} (exists: {SCRIPTS_DIR.exists()})")

if docker_repo_scripts.exists():
    # Docker environment
    logging.info("Using Docker environment paths")
    sys.path.insert(0, str(docker_repo_scripts))
    SQL_DIR = docker_repo_sql
    logging.info(f"SQL_DIR set to: {SQL_DIR}")
elif SCRIPTS_DIR.exists():
    # Local environment
    logging.info("Using local environment paths")
    sys.path.insert(0, str(SCRIPTS_DIR))
    logging.info(f"SQL_DIR set to: {SQL_DIR}")
else:
    logging.warning(f"Scripts directory not found at {SCRIPTS_DIR} or {docker_repo_scripts}")
    logging.warning("DAG may fail to import modules!")

logging.info("=" * 60)

def execute_sql_file(sql_file_path):
    """Helper function to execute SQL file"""
    try:
        import logging
        logging.info(f"Attempting to execute SQL file: {sql_file_path}")
        logging.info(f"SQL_DIR is set to: {SQL_DIR}")
        logging.info(f"SQL_DIR exists: {SQL_DIR.exists()}")
        
        hook = PostgresHook(postgres_conn_id='shopzada_postgres')
        
        # Strip 'sql/' prefix if present (SQL_DIR already points to the sql directory)
        if sql_file_path.startswith('sql/'):
            sql_file_path = sql_file_path[4:]  # Remove 'sql/' prefix
            logging.info(f"Stripped 'sql/' prefix, using: {sql_file_path}")
        
        # SQL_DIR is already set correctly (Docker or local) in module initialization
        full_path = SQL_DIR / sql_file_path
        
        logging.info(f"Looking for SQL file at: {full_path}")
        logging.info(f"Full path exists: {full_path.exists()}")
        
        if not full_path.exists():
            # Try alternative paths for better error message
            alt_paths = [
                PROJECT_ROOT / sql_file_path,
                Path(f'/opt/airflow/repo/{sql_file_path}'),
                Path(f'/opt/airflow/dags/{sql_file_path}'),
            ]
            logging.error(f"SQL file not found at primary path: {full_path}")
            logging.error("Tried alternative paths:")
            for alt_path in alt_paths:
                logging.error(f"  - {alt_path} (exists: {alt_path.exists()})")
            raise FileNotFoundError(f"SQL file not found: {full_path}. Checked SQL_DIR={SQL_DIR}, file={sql_file_path}")
        
        full_path = str(full_path)
        logging.info(f"Successfully found SQL file at: {full_path}")
        
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
            logging.warning("This may cause downstream tasks to fail. Please check the SQL file.")
            # Don't raise error - allow task to complete but log warning
            return
        
        # Execute each statement
        import logging
        logging.info(f"Executing {len(filtered_statements)} SQL statements from {sql_file_path}")
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                for idx, statement in enumerate(filtered_statements, 1):
                    if statement.strip():
                        logging.debug(f"Executing statement {idx}/{len(filtered_statements)}")
                        cursor.execute(statement)
                conn.commit()
                logging.info(f"Successfully executed all statements from {sql_file_path}")
            except Exception as e:
                conn.rollback()
                logging.error(f"Error executing statement {idx}/{len(filtered_statements)}: {e}")
                logging.error(f"Failed statement: {statement[:200]}...")  # Log first 200 chars
                raise
            finally:
                cursor.close()
        finally:
            conn.close()
    except Exception as e:
        import logging
        logging.error(f"Error executing SQL file {sql_file_path}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        raise

default_args = {
    'owner': 'shopzada-data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# SQL execution wrapper functions
def exec_sql_00():
    """Create staging tables"""
    import logging
    logging.info("=" * 60)
    logging.info("EXECUTING: Create Staging Tables")
    logging.info("=" * 60)
    try:
        execute_sql_file('sql/00_create_staging_tables.sql')
        logging.info("=" * 60)
        logging.info("SUCCESS: Staging tables created")
        logging.info("=" * 60)
    except Exception as e:
        logging.error("=" * 60)
        logging.error(f"FAILED: Create staging tables - {e}")
        logging.error("=" * 60)
        raise

def exec_sql_01():
    """Create schema from physical model"""
    import logging
    logging.info("=" * 60)
    logging.info("EXECUTING: Create Schema")
    logging.info("=" * 60)
    try:
        execute_sql_file('sql/01_create_schema_from_physical_model.sql')
        logging.info("=" * 60)
        logging.info("SUCCESS: Schema created")
        logging.info("=" * 60)
    except Exception as e:
        logging.error("=" * 60)
        logging.error(f"FAILED: Create schema - {e}")
        logging.error("=" * 60)
        raise

def exec_sql_02():
    """Populate date dimension"""
    import logging
    logging.info("=" * 60)
    logging.info("EXECUTING: Populate Date Dimension")
    logging.info("=" * 60)
    try:
        execute_sql_file('sql/02_populate_dim_date.sql')
        logging.info("=" * 60)
        logging.info("SUCCESS: Date dimension populated")
        logging.info("=" * 60)
    except Exception as e:
        logging.error("=" * 60)
        logging.error(f"FAILED: Populate date dimension - {e}")
        logging.error("=" * 60)
        raise

def exec_sql_03():
    """Create analytical views"""
    import logging
    logging.info("=" * 60)
    logging.info("EXECUTING: Create Analytical Views")
    logging.info("=" * 60)
    try:
        execute_sql_file('sql/03_create_analytical_views.sql')
        logging.info("=" * 60)
        logging.info("SUCCESS: Analytical views created")
        logging.info("=" * 60)
    except Exception as e:
        logging.error("=" * 60)
        logging.error(f"FAILED: Create analytical views - {e}")
        logging.error("=" * 60)
        raise

# Ingestion functions
def run_ingest_marketing():
    """Ingest Marketing Department data into staging tables"""
    import ingest
    ingest.ingest_marketing_department()

def run_ingest_operations():
    """Ingest Operations Department data into staging tables"""
    import ingest
    ingest.ingest_operations_department()

def run_ingest_business():
    """Ingest Business Department data into staging tables"""
    import ingest
    ingest.ingest_business_department()

def run_ingest_customer_management():
    """Ingest Customer Management Department data into staging tables"""
    import ingest
    ingest.ingest_customer_management_department()

def run_ingest_enterprise():
    """Ingest Enterprise Department data into staging tables"""
    import ingest
    ingest.ingest_enterprise_department()

# Extract function
def run_extract():
    """Extract data from staging tables"""
    import extract
    
    logging.info("=" * 60)
    logging.info("EXTRACTING DATA FROM STAGING")
    logging.info("=" * 60)
    
    # Extract all data from staging
    logging.info("Extracting data from staging tables...")
    campaign_df = extract.extract_campaign_data()
    transactional_campaign_df = extract.extract_transactional_campaign_data()
    order_df = extract.extract_order_data()
    line_item_prices_df = extract.extract_line_item_prices()
    line_item_products_df = extract.extract_line_item_products()
    order_delays_df = extract.extract_order_delays()
    product_df = extract.extract_product_data()
    user_df = extract.extract_user_data()
    user_job_df = extract.extract_user_job_data()
    user_credit_card_df = extract.extract_user_credit_card_data()
    merchant_df = extract.extract_merchant_data()
    staff_df = extract.extract_staff_data()
    order_merchant_df = extract.extract_order_merchant_data()
    
    logging.info("=" * 60)
    logging.info("EXTRACTION COMPLETE")
    logging.info(f"Extracted: {len(campaign_df) if campaign_df is not None else 0} campaigns, "
                 f"{len(order_df) if order_df is not None else 0} orders, "
                 f"{len(product_df) if product_df is not None else 0} products, "
                 f"{len(user_df) if user_df is not None else 0} users")
    logging.info("=" * 60)
    
    # Return success indicator (DataFrames are too large for XCom)
    return "extraction_complete"

# Transform function
def run_transform():
    """Transform extracted data"""
    import extract
    import transform
    
    logging.info("=" * 60)
    logging.info("TRANSFORMING DATA")
    logging.info("=" * 60)
    
    # Extract all data from staging (extract task ensures data is ready)
    logging.info("Extracting data from staging tables...")
    campaign_df = extract.extract_campaign_data()
    transactional_campaign_df = extract.extract_transactional_campaign_data()
    order_df = extract.extract_order_data()
    line_item_prices_df = extract.extract_line_item_prices()
    line_item_products_df = extract.extract_line_item_products()
    order_delays_df = extract.extract_order_delays()
    product_df = extract.extract_product_data()
    user_df = extract.extract_user_data()
    user_job_df = extract.extract_user_job_data()
    user_credit_card_df = extract.extract_user_credit_card_data()
    merchant_df = extract.extract_merchant_data()
    staff_df = extract.extract_staff_data()
    order_merchant_df = extract.extract_order_merchant_data()
    
    # Transform all data
    logging.info("Transforming data...")
    campaign_df = transform.transform_campaign_data(campaign_df)
    transactional_campaign_df = transform.transform_transactional_campaign_data(transactional_campaign_df)
    order_df = transform.transform_order_data(order_df)
    line_item_prices_df = transform.transform_line_item_prices(line_item_prices_df)
    line_item_products_df = transform.transform_line_item_products(line_item_products_df)
    order_delays_df = transform.transform_order_delays(order_delays_df)
    product_df = transform.transform_product_data(product_df)
    user_df = transform.transform_user_data(user_df)
    user_job_df = transform.transform_user_job_data(user_job_df)
    user_credit_card_df = transform.transform_user_credit_card_data(user_credit_card_df)
    merchant_df = transform.transform_merchant_data(merchant_df)
    staff_df = transform.transform_staff_data(staff_df)
    
    logging.info("=" * 60)
    logging.info("TRANSFORMATION COMPLETE")
    logging.info(f"Transformed: {len(campaign_df) if campaign_df is not None else 0} campaigns, "
                 f"{len(order_df) if order_df is not None else 0} orders, "
                 f"{len(product_df) if product_df is not None else 0} products, "
                 f"{len(user_df) if user_df is not None else 0} users")
    logging.info("=" * 60)
    
    # Return success indicator (DataFrames are too large for XCom)
    return "transformation_complete"

# Load Dimensions function
def run_load_dim():
    """Load transformed data into dimension tables"""
    import extract
    import transform
    import load_dim
    
    logging.info("=" * 60)
    logging.info("LOADING DIMENSIONS")
    logging.info("=" * 60)
    
    # Scenario 3 Support: Ensure Unknown campaign exists before loading dimensions
    logging.info("=" * 60)
    logging.info("ENSURING UNKNOWN CAMPAIGN EXISTS")
    logging.info("=" * 60)
    try:
        campaign_sk = load_dim.ensure_unknown_campaign()
        logging.info(f"Unknown campaign ready with campaign_sk: {campaign_sk}")
    except Exception as e:
        logging.warning(f"Could not ensure Unknown campaign: {e}")
    
    # Extract and transform data (transform task ensures data is ready)
    logging.info("Extracting and transforming data for dimensions...")
    campaign_df = transform.transform_campaign_data(extract.extract_campaign_data())
    product_df = transform.transform_product_data(extract.extract_product_data())
    user_df = transform.transform_user_data(extract.extract_user_data())
    staff_df = transform.transform_staff_data(extract.extract_staff_data())
    merchant_df = transform.transform_merchant_data(extract.extract_merchant_data())
    user_job_df = transform.transform_user_job_data(extract.extract_user_job_data())
    user_credit_card_df = transform.transform_user_credit_card_data(extract.extract_user_credit_card_data())
    
    # Load dimensions
    load_dim.load_dim_campaign(campaign_df)
    
    # Scenario 3 Support: Update campaign transactions after loading campaigns
    logging.info("=" * 60)
    logging.info("UPDATING CAMPAIGN TRANSACTIONS FOR LATE-ARRIVING CAMPAIGNS")
    logging.info("=" * 60)
    try:
        count = load_dim.update_campaign_transactions_for_new_campaigns()
        if count > 0:
            logging.info(f"Updated {count} campaign transaction rows from Unknown to actual campaigns")
        else:
            logging.info("No campaign transactions needed updating")
    except Exception as e:
        logging.warning(f"Could not update campaign transactions: {e}")
    
    load_dim.load_dim_product(product_df)
    load_dim.load_dim_user(user_df)
    load_dim.load_dim_staff(staff_df)
    load_dim.load_dim_merchant(merchant_df)
    load_dim.load_dim_user_job(user_job_df)
    load_dim.load_dim_credit_card(user_credit_card_df)
    
    logging.info("=" * 60)
    logging.info("DIMENSIONS LOADED SUCCESSFULLY")
    logging.info("=" * 60)

# Load Facts function
def run_load_fact():
    """Load transformed data into fact tables"""
    import extract
    import transform
    import load_fact
    import load_dim
    import etl_metrics
    
    # Scenario 1 Support: Track before/after states if enabled
    ENABLE_SCENARIO1_METRICS = os.getenv("ENABLE_SCENARIO1_METRICS", "false").lower() == "true"
    TARGET_DATE = os.getenv("TARGET_DATE", None)  # Optional: YYYY-MM-DD format
    
    before_state = None
    if ENABLE_SCENARIO1_METRICS:
        before_state = etl_metrics.log_before_state(target_date=TARGET_DATE)
    
    logging.info("=" * 60)
    logging.info("LOADING FACTS")
    logging.info("=" * 60)
    
    # Extract and transform data (transform task ensures data is ready)
    logging.info("Extracting and transforming data for facts...")
    order_df = transform.transform_order_data(extract.extract_order_data())
    line_item_prices_df = transform.transform_line_item_prices(extract.extract_line_item_prices())
    line_item_products_df = transform.transform_line_item_products(extract.extract_line_item_products())
    order_delays_df = transform.transform_order_delays(extract.extract_order_delays())
    transactional_campaign_df = transform.transform_transactional_campaign_data(extract.extract_transactional_campaign_data())
    order_merchant_df = extract.extract_order_merchant_data()
    
    # Scenario 2 Support: Create missing dimensions from fact data before loading facts
    # This handles cases where new customers/products appear only in order/line_item data
    logging.info("=" * 60)
    logging.info("CREATING MISSING DIMENSIONS FROM FACT DATA")
    logging.info("=" * 60)
    new_users_created = load_dim.create_missing_users_from_orders(order_df)
    new_products_created = load_dim.create_missing_products_from_line_items(line_item_products_df)
    if new_users_created > 0 or new_products_created > 0:
        logging.info(f"Created {new_users_created} missing users and {new_products_created} missing products from fact data")
    else:
        logging.info("No missing dimensions found - all users and products already exist")
    logging.info("=" * 60)
    
    # Load facts (now all dimensions should exist)
    load_fact.load_fact_orders(order_df, order_merchant_df, order_delays_df)
    load_fact.load_fact_line_items(line_item_prices_df, line_item_products_df)
    load_fact.load_fact_campaign_transactions(transactional_campaign_df)
    
    logging.info("=" * 60)
    logging.info("FACTS LOADED SUCCESSFULLY")
    logging.info("=" * 60)
    
    # Scenario 1 Support: Log after state and summary if enabled
    if ENABLE_SCENARIO1_METRICS and before_state is not None:
        after_state = etl_metrics.log_after_state(before_state, target_date=TARGET_DATE)
        etl_metrics.log_pipeline_summary(before_state, after_state, target_date=TARGET_DATE)

with DAG(
    'shopzada_etl_pipeline',
    default_args=default_args,
    description='ShopZada Complete ETL Pipeline - Schema -> Staging -> Ingest -> Transform -> Load Dim -> Load Fact -> Views',
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
        """,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Task 2: Create Staging Tables
    create_staging_tables = PythonOperator(
        task_id='create_staging_tables',
        python_callable=exec_sql_00,
        doc_md="""
        ## Staging Tables Creation
        
        Creates staging tables for each department to store raw data before transformation.
        Staging tables follow naming convention: stg_{department}_{data_type}
        """,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Task 3: Populate Date Dimension (must be done before fact tables)
    populate_date_dimension = PythonOperator(
        task_id='populate_date_dimension',
        python_callable=exec_sql_02,
        doc_md="""
        ## Date Dimension Population
        
        Populates the date dimension table (dim_date) with dates from 2020-01-01 to 2025-12-31.
        This is a standard dimension in Kimball methodology for time-based analysis.
        """,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Task 4: Ingest Data Per Department -> Staging (Parallel Execution)
    ingest_marketing = PythonOperator(
        task_id='ingest_marketing_department',
        python_callable=run_ingest_marketing,
        doc_md="""
        ## Marketing Department Ingestion
        
        Ingests Marketing Department data into staging tables:
        - Campaign data (campaign_data.csv)
        - Transactional campaign data (transactional_campaign_data.csv)
        """
    )

    ingest_operations = PythonOperator(
        task_id='ingest_operations_department',
        python_callable=run_ingest_operations,
        doc_md="""
        ## Operations Department Ingestion
        
        Ingests Operations Department data into staging tables:
        - Order data (order_data_*.parquet, *.pickle, *.csv, *.xlsx, *.json, *.html)
        - Line item prices (line_item_data_prices*.csv, *.parquet)
        - Line item products (line_item_data_products*.csv, *.parquet)
        - Order delays (order_delays.html)
        """
    )

    ingest_business = PythonOperator(
        task_id='ingest_business_department',
        python_callable=run_ingest_business,
        doc_md="""
        ## Business Department Ingestion
        
        Ingests Business Department data into staging tables:
        - Product list (product_list.xlsx)
        """
    )

    ingest_customer_management = PythonOperator(
        task_id='ingest_customer_management_department',
        python_callable=run_ingest_customer_management,
        doc_md="""
        ## Customer Management Department Ingestion
        
        Ingests Customer Management Department data into staging tables:
        - User data (user_data.json)
        - User jobs (user_job.csv)
        - User credit cards (user_credit_card.pickle)
        """
    )

    ingest_enterprise = PythonOperator(
        task_id='ingest_enterprise_department',
        python_callable=run_ingest_enterprise,
        doc_md="""
        ## Enterprise Department Ingestion
        
        Ingests Enterprise Department data into staging tables:
        - Merchant data (merchant_data.html)
        - Staff data (staff_data.html)
        - Order with merchant data (order_with_merchant_data*.parquet, *.csv)
        """
    )

    # Task 5: Extract
    extract = PythonOperator(
        task_id='extract',
        python_callable=run_extract,
        doc_md="""
        ## Extract Data
        
        Extracts data from staging tables into pandas DataFrames:
        - Campaign data
        - Transactional campaign data
        - Order data
        - Line item data
        - Product data
        - User data
        - Merchant data
        - Staff data
        """
    )

    # Task 6: Transform
    transform = PythonOperator(
        task_id='transform',
        python_callable=run_transform,
        doc_md="""
        ## Transform Data
        
        Applies transformations to extracted data:
        - Data cleaning (remove nulls, format strings)
        - Normalization (product types, discounts, etc.)
        - Formatting (names, addresses, phone numbers)
        - Data type conversions
        """
    )

    # Task 7: Load Dimensions
    load_dim = PythonOperator(
        task_id='load_dim',
        python_callable=run_load_dim,
        doc_md="""
        ## Load Dimensions
        
        Loads transformed data into dimension tables:
        - dim_campaign
        - dim_product
        - dim_user
        - dim_staff
        - dim_merchant
        - dim_user_job
        - dim_credit_card
        """
    )

    # Task 8: Load Facts
    load_fact = PythonOperator(
        task_id='load_fact',
        python_callable=run_load_fact,
        doc_md="""
        ## Load Facts
        
        Loads transformed data into fact tables with dimension key lookups:
        - fact_orders
        - fact_line_items
        - fact_campaign_transactions
        
        **Scenario 1 Support**: Set environment variable `ENABLE_SCENARIO1_METRICS=true` 
        and optionally `TARGET_DATE=YYYY-MM-DD` to enable before/after state tracking 
        and dashboard KPI monitoring.
        """
    )

    # Task 9: Create Analytical Views
    create_analytical_views = PythonOperator(
        task_id='create_analytical_views',
        python_callable=exec_sql_03,
        doc_md="""
        ## Analytical Views Creation
        
        Creates SQL views optimized for BI tools:
        - vw_campaign_performance
        - vw_merchant_performance
        - vw_customer_segment_revenue
        - vw_sales_by_time
        - vw_product_performance
        - vw_staff_performance
        - vw_segment_revenue_by_time
        """
    )

    # Define task dependencies: Schema -> Staging -> Ingest -> Extract -> Transform -> Load Dim -> Load Fact -> Views
    # 1. Schema -> Staging -> Date Dimension
    create_schema >> create_staging_tables >> populate_date_dimension
    
    # 2. Date Dimension -> Ingest (all departments in parallel)
    populate_date_dimension >> [
        ingest_marketing,
        ingest_operations,
        ingest_business,
        ingest_customer_management,
        ingest_enterprise
    ]
    
    # 3. All ingestion tasks -> Extract
    [
        ingest_marketing,
        ingest_operations,
        ingest_business,
        ingest_customer_management,
        ingest_enterprise
    ] >> extract
    
    # 4. Extract -> Transform -> Load Dim -> Load Fact -> Views
    extract >> transform >> load_dim >> load_fact >> create_analytical_views
