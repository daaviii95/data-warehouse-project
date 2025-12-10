#!/usr/bin/env python3
"""
ShopZada ETL Pipeline - Python-based (Similar to original SQLite scripts)
Adapted for PostgreSQL and Physical Model structure
Reads from raw data directory and loads into PostgreSQL following Kimball methodology
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import re
from pathlib import Path
from urllib.parse import quote_plus
import sqlalchemy
from sqlalchemy import text

# Configuration
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "shopzada-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopzada")
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Build SQLAlchemy engine
password_quoted = quote_plus(DB_PASS)
engine_url = f"postgresql+psycopg2://{DB_USER}:{password_quoted}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(engine_url, pool_size=5, max_overflow=10, future=True)

def load_file(file_path, clean=True):
    """
    Load file based on extension and optionally clean it
    Args:
        file_path: Path to file
        clean: Whether to apply data cleaning (default: True)
    """
    ext = file_path.suffix.lower()
    try:
        if ext == '.csv':
            # Try tab-separated first (many files use tabs), then comma-separated
            try:
                # Read first few lines to detect separator
                with open(file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline()
                    second_line = f.readline() if f else ''
                    f.seek(0)  # Reset to beginning
                    
                    # Count separators
                    tab_count = first_line.count('\t')
                    comma_count = first_line.count(',')
                    
                    # Determine separator
                    if tab_count > comma_count and tab_count > 0:
                        sep = '\t'
                    elif comma_count > 0:
                        sep = ','
                    else:
                        # Default to tab for this dataset
                        sep = '\t'
                    
                    # Read with detected separator
                    df = pd.read_csv(file_path, sep=sep, encoding='utf-8', quotechar='"')
                    
                    # Check if header was misread (contains tabs in column name)
                    if len(df.columns) > 0 and '\t' in str(df.columns[0]):
                        # Header wasn't split - manually parse
                        logging.info(f"Header misread for {file_path}, manually parsing...")
                        with open(file_path, 'r', encoding='utf-8') as f:
                            lines = [line.rstrip('\n\r') for line in f.readlines() if line.strip()]
                            if lines:
                                # Parse header (first line)
                                header_line = lines[0]
                                # Check if first character is tab (index column)
                                if header_line.startswith('\t'):
                                    header_parts = header_line.split(sep)[1:]  # Skip index
                                else:
                                    header_parts = header_line.split(sep)
                                
                                # Clean header
                                header = [col.strip().strip('"').strip("'") for col in header_parts if col.strip()]
                                
                                # Parse data rows
                                data_rows = []
                                for line in lines[1:]:
                                    parts = line.split(sep)
                                    # Skip index column (first part if it's just a number)
                                    if len(parts) > 0 and parts[0].strip().isdigit():
                                        parts = parts[1:]
                                    # Clean each part
                                    parts = [part.strip().strip('"').strip("'") for part in parts]
                                    # Match length to header (may have extra columns)
                                    if len(parts) >= len(header):
                                        data_rows.append(parts[:len(header)])
                                    elif len(parts) > 0:
                                        # Pad with empty strings if needed
                                        parts.extend([''] * (len(header) - len(parts)))
                                        data_rows.append(parts)
                                
                                # Create DataFrame
                                if data_rows and header:
                                    df = pd.DataFrame(data_rows, columns=header)
                    
                    # Remove index column if present (check after potential manual parsing)
                    if len(df.columns) > 0:
                        first_col = str(df.columns[0])
                        # Check if first column is an index (unnamed or just digits)
                        if first_col == 'Unnamed: 0' or (first_col.strip().isdigit() and len(df.columns) > 1):
                            df = df.iloc[:, 1:]
                        # Also check if first column values are just sequential numbers
                        elif len(df) > 0 and df.columns[0] in df.columns:
                            first_col_values = df[df.columns[0]].astype(str)
                            if all(val.strip().isdigit() for val in first_col_values.head(10) if pd.notna(val)):
                                df = df.iloc[:, 1:]  # Likely an index column
                    
                    # Apply data cleaning
                    df = clean_dataframe(df, file_type='csv')
                    return df
            except Exception as e:
                logging.warning(f"Error reading CSV {file_path}: {e}")
                return None
        elif ext == '.parquet':
            df = pd.read_parquet(file_path)
            df = clean_dataframe(df, file_type='parquet')
            return df
        elif ext == '.json':
            df = pd.read_json(file_path)
            df = clean_dataframe(df, file_type='json')
            return df
        elif ext == '.xlsx' or ext == '.xls':
            df = pd.read_excel(file_path)
            df = clean_dataframe(df, file_type='excel')
            return df
        elif ext == '.pickle' or ext == '.pkl':
            df = pd.read_pickle(file_path)
            df = clean_dataframe(df, file_type='pickle')
            return df
        elif ext == '.html':
            # Try to read HTML tables
            try:
                tables = pd.read_html(file_path)
                if tables:
                    df = tables[0]  # Return first table
                    
                    # Clean column names - remove leading/trailing whitespace
                    df.columns = [col.strip() if isinstance(col, str) else str(col).strip() for col in df.columns]
                    
                    # Remove index column if it's the first column (unnamed, empty, or numeric)
                    if len(df.columns) > 0:
                        first_col = df.columns[0]
                        first_col_str = str(first_col).strip()
                        
                        # Check if first column is an index column
                        is_index_col = False
                        
                        # Check if it's unnamed
                        if first_col_str == '' or first_col_str.startswith('Unnamed'):
                            is_index_col = True
                        # Check if it's numeric (0, 1, 2, etc.)
                        elif first_col_str.isdigit() or first_col_str == '0':
                            is_index_col = True
                        # Check if first column values are sequential numbers (0, 1, 2, ...)
                        elif len(df) > 0:
                            try:
                                first_col_values = df[first_col].astype(str).str.strip()
                                # Check if all values are sequential numbers starting from 0
                                if all(val.isdigit() for val in first_col_values.head(10) if pd.notna(val)):
                                    numeric_vals = [int(v) for v in first_col_values.head(10) if v.isdigit()]
                                    if numeric_vals == list(range(len(numeric_vals))):
                                        is_index_col = True
                            except:
                                pass
                        
                        if is_index_col:
                            df = df.iloc[:, 1:]  # Skip first column
                            # Re-clean column names after removing index
                            df.columns = [col.strip() if isinstance(col, str) else str(col).strip() for col in df.columns]
                    
                    # Apply data cleaning
                    df = clean_dataframe(df, file_type='html')
                    return df
            except Exception as e:
                logging.warning(f"Error reading HTML file {file_path}: {e}")
            return None
        else:
            logging.warning(f"Unsupported file type: {ext}")
            return None
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None

def find_files(pattern, data_dir):
    """Find files matching pattern"""
    files = []
    data_path = Path(data_dir)
    if not data_path.exists():
        logging.error(f"Data directory does not exist: {data_dir}")
        return files
    
    for file_path in data_path.rglob(pattern):
        if file_path.is_file():
            files.append(file_path)
    return sorted(files)

def clean_dataframe(df, file_type=''):
    """
    Comprehensive data cleaning function - cleans dataframe before ingestion
    Similar to the cleaned dataset approach in reference files
    """
    if df is None or df.empty:
        return df
    
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # 1. Remove unwanted columns (like old_id, index columns, etc.)
    columns_to_drop = []
    for col in df.columns:
        col_str = str(col).lower().strip()
        # Drop old_id columns (as in reference files)
        if 'old_id' in col_str:
            columns_to_drop.append(col)
        # Drop unnamed index columns
        elif col_str.startswith('unnamed') or col_str == '':
            columns_to_drop.append(col)
        # Drop columns that are just sequential numbers (0, 1, 2, ...)
        elif col_str.isdigit():
            # Check if values are sequential
            try:
                if len(df) > 0:
                    first_vals = df[col].head(10).astype(str).str.strip()
                    if all(v.isdigit() for v in first_vals if pd.notna(v)):
                        numeric_vals = [int(v) for v in first_vals if v.isdigit()]
                        if len(numeric_vals) > 0 and numeric_vals == list(range(numeric_vals[0], numeric_vals[0] + len(numeric_vals))):
                            columns_to_drop.append(col)
            except:
                pass
    
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop)
        logging.debug(f"Dropped columns: {columns_to_drop}")
    
    # 2. Clean column names - normalize spaces, underscores, case
    df.columns = [str(col).strip().replace(' ', '_').replace('-', '_') for col in df.columns]
    # Remove duplicate underscores
    df.columns = [re.sub(r'_+', '_', col) for col in df.columns]
    # Remove leading/trailing underscores
    df.columns = [col.strip('_') for col in df.columns]
    
    # 3. Clean string columns - remove leading/trailing whitespace, handle null strings
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            # Convert to string first, handling NaN values
            df[col] = df[col].astype(str)
            # Remove leading/trailing whitespace
            df[col] = df[col].str.strip()
            # Replace common null representations with actual NaN
            null_values = ['nan', 'None', 'null', 'N/A', 'NA', 'n/a', 'NULL', '<null>', 'None', 'NaN', 'NAN', 'null', 'NULL', '']
            df[col] = df[col].replace(null_values, np.nan)
            # Remove extra whitespace within strings (multiple spaces to single space)
            df[col] = df[col].apply(lambda x: re.sub(r'\s+', ' ', x) if pd.notna(x) and isinstance(x, str) else x)
            # Convert back to object type (strings)
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    # 4. Remove completely empty rows
    df = df.dropna(how='all')
    
    # 5. Reset index after cleaning
    df = df.reset_index(drop=True)
    
    logging.debug(f"Cleaned dataframe: {len(df)} rows, {len(df.columns)} columns")
    
    return df

def extract_numeric(value):
    """Extract numeric value from string (e.g., '3days' -> 3, '8days' -> 8)"""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    # Convert to string and extract first number
    str_value = str(value).strip()
    # Try to find numeric part (handles "3days", "8 days", "15days", etc.)
    match = re.search(r'\d+', str_value)
    if match:
        return int(match.group())
    return None

def clean_value(value):
    """Convert NaN/None to None, and clean string values"""
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        # Handle various null representations (including <null> seen in database)
        if value.lower() in ('nan', 'none', 'null', 'n/a', 'na', '', '<null>', 'null', 'none'):
            return None
        # Remove angle brackets if present
        if value.startswith('<') and value.endswith('>'):
            inner = value[1:-1].strip().lower()
            if inner in ('null', 'none', 'nan', 'n/a', 'na'):
                return None
    return value

def parse_discount(value):
    """Parse discount value from various formats (1%, 1pct, 1percent, 10%%, etc.)"""
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    # Extract numeric value from string
    str_value = str(value).strip().lower()
    # Remove common suffixes
    str_value = re.sub(r'[%pctpercent]+', '', str_value)
    # Extract number
    match = re.search(r'\d+\.?\d*', str_value)
    if match:
        return float(match.group())
    return None

def format_product_type(value):
    """Format product type for cleaner display"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Replace underscores with spaces and title case
    str_value = str_value.replace('_', ' ').replace('-', ' ')
    # Title case but preserve acronyms
    words = str_value.split()
    formatted = []
    for word in words:
        if word.isupper() and len(word) > 1:
            formatted.append(word)
        else:
            formatted.append(word.title())
    return ' '.join(formatted)

def format_name(value):
    """Format person names - title case, remove extra spaces"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove extra spaces
    str_value = re.sub(r'\s+', ' ', str_value)
    # Title case but preserve common prefixes/suffixes
    # Handle names like "O'Brien", "McDonald", "van der Berg"
    words = str_value.split()
    formatted = []
    for word in words:
        word_lower = word.lower()
        if word_lower in ['van', 'der', 'de', 'la', 'le']:
            formatted.append(word_lower)
        elif word_lower.startswith("mc") or word_lower.startswith("mac"):
            # Preserve Mc/Mac capitalization (e.g., McDonald, MacLeod)
            if len(word) > 2:
                formatted.append(word[0].upper() + word[1].lower() + word[2:].title())
            else:
                formatted.append(word.title())
        elif word.startswith("O'") or word.startswith("o'") or word.startswith("D'") or word.startswith("d'"):
            # Handle O'Brien, D'Angelo, etc.
            if len(word) > 2:
                formatted.append(word[0].upper() + word[1] + word[2:].title())
            else:
                formatted.append(word.title())
        else:
            formatted.append(word.title())
    return ' '.join(formatted)

def format_address(value, field_type='street'):
    """Format address fields - title case for city/state/country, preserve street formatting"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove extra spaces
    str_value = re.sub(r'\s+', ' ', str_value)
    
    if field_type in ['city', 'state', 'country']:
        # Title case for city, state, country
        # Handle special cases like "New York", "Los Angeles"
        words = str_value.split()
        formatted = []
        for word in words:
            # Preserve common prefixes
            if word.lower() in ['of', 'the', 'and', 'de', 'la', 'le', 'van', 'der']:
                formatted.append(word.lower())
            else:
                formatted.append(word.title())
        return ' '.join(formatted)
    else:
        # For street addresses, preserve more formatting (numbers, abbreviations)
        return str_value

def format_phone_number(value):
    """Format phone numbers - standardize format"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove all non-digit characters except + at the start
    digits_only = re.sub(r'[^\d+]', '', str_value)
    # If starts with +, keep it
    if digits_only.startswith('+'):
        return digits_only
    # Otherwise, return cleaned digits
    return re.sub(r'\D', '', str_value) if str_value else None

def format_gender(value):
    """Standardize gender values"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip().lower()
    # Map common variations to standard format
    gender_map = {
        'm': 'Male',
        'male': 'Male',
        'f': 'Female',
        'female': 'Female',
        'other': 'Other',
        'o': 'Other',
        'non-binary': 'Other',
        'nb': 'Other'
    }
    return gender_map.get(str_value, str_value.title() if str_value else None)

def format_user_type(value):
    """Standardize user type values"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove extra spaces and title case
    str_value = re.sub(r'\s+', ' ', str_value)
    return str_value.title()

def format_job_title(value):
    """Format job titles - title case"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove extra spaces
    str_value = re.sub(r'\s+', ' ', str_value)
    # Title case but preserve common abbreviations
    words = str_value.split()
    formatted = []
    for word in words:
        # Preserve common job title abbreviations
        if word.upper() in ['CEO', 'CTO', 'CFO', 'VP', 'SVP', 'EVP', 'PM', 'HR', 'IT', 'QA', 'UI', 'UX']:
            formatted.append(word.upper())
        else:
            formatted.append(word.title())
    return ' '.join(formatted)

def format_job_level(value):
    """Format job levels - standardize"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove extra spaces
    str_value = re.sub(r'\s+', ' ', str_value)
    # Title case
    return str_value.title()

def format_credit_card_number(value):
    """Format credit card numbers - mask or standardize"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', str_value)
    # Return cleaned digits (don't mask for data warehouse - keep full number)
    return digits_only if digits_only else None

def format_issuing_bank(value):
    """Format issuing bank names - title case"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove extra spaces
    str_value = re.sub(r'\s+', ' ', str_value)
    # Title case
    return str_value.title()

def format_campaign_description(value):
    """Clean campaign descriptions - remove extra quotes and format"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove triple quotes if present
    str_value = re.sub(r'^"""', '', str_value)
    str_value = re.sub(r'"""$', '', str_value)
    # Remove extra quotes
    str_value = re.sub(r'^"', '', str_value)
    str_value = re.sub(r'"$', '', str_value)
    # Remove extra spaces
    str_value = re.sub(r'\s+', ' ', str_value)
    return str_value.strip()

def format_product_name(value):
    """Format product names - title case"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove extra spaces
    str_value = re.sub(r'\s+', ' ', str_value)
    # Title case
    return str_value.title()

def format_campaign_name(value):
    """Format campaign names - preserve original but clean"""
    if pd.isna(value) or value is None:
        return None
    str_value = str(value).strip()
    # Remove extra spaces
    str_value = re.sub(r'\s+', ' ', str_value)
    return str_value

def load_dim_campaign():
    """Load campaign dimension"""
    logging.info("Loading dim_campaign...")
    # Only load campaign_data.csv, not transactional_campaign_data.csv
    files = find_files("*campaign_data.csv", DATA_DIR)
    # Filter out transactional files
    files = [f for f in files if 'transactional' not in str(f).lower()]
    if not files:
        logging.warning("No campaign_data files found")
        return
    
    all_data = []
    for file_path in files:
        df = load_file(file_path)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        logging.warning("No campaign data loaded")
        return
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Log available columns for debugging
    logging.info(f"Available columns in campaign data: {list(combined.columns)}")
    logging.info(f"First few rows:\n{combined.head()}")
    
    # Additional cleaning specific to campaign data
    # (clean_dataframe already handles old_id, but we check again for safety)
    if 'old_id' in combined.columns:
        combined = combined.drop(columns=['old_id'])
        logging.info("Dropped old_id column from campaign data")
    
    # Normalize column names (handle case variations and spaces)
    column_mapping = {}
    for col in combined.columns:
        col_lower = col.lower().strip().replace(' ', '_')
        if 'campaign_id' in col_lower or (col_lower == 'id' and 'campaign_id' not in column_mapping):
            column_mapping['campaign_id'] = col
        elif 'campaign_name' in col_lower or (col_lower == 'name' and 'campaign_name' not in column_mapping):
            column_mapping['campaign_name'] = col
        elif 'campaign_description' in col_lower or (col_lower == 'description' and 'campaign_description' not in column_mapping):
            column_mapping['campaign_description'] = col
        elif 'discount' in col_lower:
            column_mapping['discount'] = col
    
    logging.info(f"Column mapping: {column_mapping}")
    
    # Warn if required columns are missing
    if 'campaign_id' not in column_mapping:
        logging.warning("campaign_id column not found! Available columns: " + str(list(combined.columns)))
    
    # Insert into database
    with engine.begin() as conn:
        for _, row in combined.iterrows():
            # Get values using mapped column names or fallback to direct access
            campaign_id = row.get(column_mapping.get('campaign_id', 'campaign_id'))
            campaign_name = row.get(column_mapping.get('campaign_name', 'campaign_name'))
            campaign_description = row.get(column_mapping.get('campaign_description', 'campaign_description'))
            discount_val = row.get(column_mapping.get('discount', 'discount'))
            
            # Handle NaN values
            if pd.isna(campaign_id):
                logging.warning(f"Skipping row with null campaign_id: {row.to_dict()}")
                continue
            
            conn.execute(text("""
                INSERT INTO dim_campaign (campaign_id, campaign_name, campaign_description, discount)
                VALUES (:campaign_id, :campaign_name, :campaign_description, :discount)
                ON CONFLICT (campaign_id) DO UPDATE SET
                    campaign_name = EXCLUDED.campaign_name,
                    campaign_description = EXCLUDED.campaign_description,
                    discount = EXCLUDED.discount
            """), {
                'campaign_id': str(campaign_id) if not pd.isna(campaign_id) else None,
                'campaign_name': format_campaign_name(clean_value(campaign_name)),
                'campaign_description': format_campaign_description(clean_value(campaign_description)),
                'discount': parse_discount(discount_val)
            })
    
    logging.info(f"Loaded {len(combined)} campaigns into dim_campaign")

def load_dim_product():
    """Load product dimension"""
    logging.info("Loading dim_product...")
    files = find_files("*product_list*", DATA_DIR)
    if not files:
        logging.warning("No product_list files found")
        return
    
    all_data = []
    for file_path in files:
        df = load_file(file_path)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        logging.warning("No product data loaded")
        return
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Insert into database
    with engine.begin() as conn:
        for _, row in combined.iterrows():
            conn.execute(text("""
                INSERT INTO dim_product (product_id, product_name, product_type, price)
                VALUES (:product_id, :product_name, :product_type, :price)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_type = EXCLUDED.product_type,
                    price = EXCLUDED.price
            """), {
                'product_id': str(row.get('product_id')),
                'product_name': format_product_name(clean_value(row.get('product_name'))),
                'product_type': format_product_type(row.get('product_type')),
                'price': float(row.get('price', 0)) if pd.notna(row.get('price')) else None
            })
    
    logging.info(f"Loaded {len(combined)} products into dim_product")

def load_dim_user():
    """Load user dimension"""
    logging.info("Loading dim_user...")
    files = find_files("*user_data*", DATA_DIR)
    if not files:
        logging.warning("No user_data files found")
        return
    
    all_data = []
    for file_path in files:
        df = load_file(file_path)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        logging.warning("No user data loaded")
        return
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Insert into database
    with engine.begin() as conn:
        for _, row in combined.iterrows():
            conn.execute(text("""
                INSERT INTO dim_user (user_id, name, street, state, city, country, birthdate, gender, device_address, user_type, creation_date)
                VALUES (:user_id, :name, :street, :state, :city, :country, :birthdate, :gender, :device_address, :user_type, :creation_date)
                ON CONFLICT (user_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    street = EXCLUDED.street,
                    state = EXCLUDED.state,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    birthdate = EXCLUDED.birthdate,
                    gender = EXCLUDED.gender,
                    device_address = EXCLUDED.device_address,
                    user_type = EXCLUDED.user_type,
                    creation_date = EXCLUDED.creation_date
            """), {
                'user_id': str(row.get('user_id')),
                'name': format_name(clean_value(row.get('name'))),
                'street': format_address(clean_value(row.get('street')), 'street'),
                'state': format_address(clean_value(row.get('state')), 'state'),
                'city': format_address(clean_value(row.get('city')), 'city'),
                'country': format_address(clean_value(row.get('country')), 'country'),
                'birthdate': pd.to_datetime(row.get('birthdate'), errors='coerce') if pd.notna(row.get('birthdate')) else None,
                'gender': format_gender(clean_value(row.get('gender'))),
                'device_address': clean_value(row.get('device_address')),
                'user_type': format_user_type(clean_value(row.get('user_type'))),
                'creation_date': pd.to_datetime(row.get('creation_date'), errors='coerce') if pd.notna(row.get('creation_date')) else None
            })
    
    logging.info(f"Loaded {len(combined)} users into dim_user")

def load_dim_staff():
    """Load staff dimension"""
    logging.info("Loading dim_staff...")
    files = find_files("*staff_data*", DATA_DIR)
    if not files:
        logging.warning("No staff_data files found")
        return
    
    all_data = []
    for file_path in files:
        df = load_file(file_path)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        logging.warning("No staff data loaded")
        return
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Insert into database
    with engine.begin() as conn:
        for _, row in combined.iterrows():
            conn.execute(text("""
                INSERT INTO dim_staff (staff_id, name, street, state, city, country, job_level, contact_number, creation_date)
                VALUES (:staff_id, :name, :street, :state, :city, :country, :job_level, :contact_number, :creation_date)
                ON CONFLICT (staff_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    street = EXCLUDED.street,
                    state = EXCLUDED.state,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    job_level = EXCLUDED.job_level,
                    contact_number = EXCLUDED.contact_number,
                    creation_date = EXCLUDED.creation_date
            """), {
                'staff_id': str(row.get('staff_id')),
                'name': format_name(clean_value(row.get('name'))),
                'street': format_address(clean_value(row.get('street')), 'street'),
                'state': format_address(clean_value(row.get('state')), 'state'),
                'city': format_address(clean_value(row.get('city')), 'city'),
                'country': format_address(clean_value(row.get('country')), 'country'),
                'job_level': format_job_level(clean_value(row.get('job_level'))),
                'contact_number': format_phone_number(clean_value(row.get('contact_number'))),
                'creation_date': pd.to_datetime(row.get('creation_date'), errors='coerce') if pd.notna(row.get('creation_date')) else None
            })
    
    logging.info(f"Loaded {len(combined)} staff into dim_staff")

def load_dim_merchant():
    """Load merchant dimension"""
    logging.info("Loading dim_merchant...")
    # Find merchant_data files, but exclude order_with_merchant_data files
    all_files = find_files("*merchant_data*", DATA_DIR)
    files = [f for f in all_files if "order_with_merchant_data" not in str(f)]
    if not files:
        logging.warning("No merchant_data files found")
        return
    
    all_data = []
    for file_path in files:
        df = load_file(file_path)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        logging.warning("No merchant data loaded")
        return
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Log available columns for debugging
    logging.info(f"Available columns in merchant data: {list(combined.columns)}")
    if len(combined) > 0:
        logging.info(f"Sample merchant row (first 3 columns): {dict(list(combined.iloc[0].to_dict().items())[:3])}")
        logging.info(f"Sample merchant row (all): {combined.iloc[0].to_dict()}")
    
    # After clean_dataframe, column names are normalized (spaces to underscores, etc.)
    # Map expected column names to actual column names in the dataframe
    column_mapping = {}
    for col in combined.columns:
        col_str = str(col).strip()
        col_lower = col_str.lower()
        
        # Map merchant_id
        if 'merchant_id' in col_lower or (col_lower == 'id' and 'merchant_id' not in column_mapping):
            column_mapping['merchant_id'] = col
        # Map name
        elif col_lower == 'name' or col_lower == 'merchant_name':
            column_mapping['name'] = col
        # Map street
        elif col_lower == 'street' or col_lower == 'address':
            column_mapping['street'] = col
        # Map state
        elif col_lower == 'state':
            column_mapping['state'] = col
        # Map city
        elif col_lower == 'city':
            column_mapping['city'] = col
        # Map country
        elif col_lower == 'country':
            column_mapping['country'] = col
        # Map contact_number
        elif 'contact' in col_lower and 'number' in col_lower:
            column_mapping['contact_number'] = col
        elif 'contact' in col_lower and 'contact_number' not in column_mapping:
            column_mapping['contact_number'] = col
        elif 'phone' in col_lower and 'contact_number' not in column_mapping:
            column_mapping['contact_number'] = col
        # Map creation_date
        elif 'creation' in col_lower and 'date' in col_lower:
            column_mapping['creation_date'] = col
        elif 'creation' in col_lower and 'creation_date' not in column_mapping:
            column_mapping['creation_date'] = col
    
    logging.info(f"Merchant column mapping: {column_mapping}")
    logging.info(f"Available columns after cleaning: {list(combined.columns)}")
    
    # Warn if critical columns are missing
    if 'merchant_id' not in column_mapping:
        logging.error(f"CRITICAL: merchant_id column not found! Available columns: {list(combined.columns)}")
        return
    
    # Log which columns were found vs not found
    expected_cols = ['merchant_id', 'name', 'street', 'state', 'city', 'country', 'contact_number', 'creation_date']
    missing_cols = [col for col in expected_cols if col not in column_mapping]
    if missing_cols:
        logging.warning(f"Merchant columns not mapped: {missing_cols}. Available columns: {list(combined.columns)}")
    
    # Test first row to see what values we get
    if len(combined) > 0:
        test_row = combined.iloc[0]
        merchant_id_col = column_mapping.get('merchant_id')
        logging.info(f"Test row merchant_id column: {merchant_id_col}, value: {test_row[merchant_id_col] if merchant_id_col else 'NOT FOUND'}")
        if 'name' in column_mapping:
            name_col = column_mapping['name']
            logging.info(f"Test row name column: {name_col}, value: {test_row[name_col]}")
        else:
            logging.warning("Name column not found in mapping!")
    
    # Insert into database
    with engine.begin() as conn:
        inserted = 0
        for idx, row in combined.iterrows():
            # Get merchant_id first (required)
            merchant_id_col = column_mapping.get('merchant_id')
            if not merchant_id_col or merchant_id_col not in row.index:
                logging.warning(f"Skipping row {idx}: merchant_id column not found")
                continue
            
            merchant_id = row[merchant_id_col]
            if pd.isna(merchant_id) or merchant_id is None or str(merchant_id).strip() == '':
                logging.warning(f"Skipping row {idx} with null/empty merchant_id")
                continue
            
            # Get all other values using column mapping - use .get() with direct column access
            name_val = row.get(column_mapping.get('name')) if column_mapping.get('name') in row.index else None
            street_val = row.get(column_mapping.get('street')) if column_mapping.get('street') in row.index else None
            state_val = row.get(column_mapping.get('state')) if column_mapping.get('state') in row.index else None
            city_val = row.get(column_mapping.get('city')) if column_mapping.get('city') in row.index else None
            country_val = row.get(column_mapping.get('country')) if column_mapping.get('country') in row.index else None
            contact_val = row.get(column_mapping.get('contact_number')) if column_mapping.get('contact_number') in row.index else None
            creation_val = row.get(column_mapping.get('creation_date')) if column_mapping.get('creation_date') in row.index else None
            
            # Debug first few rows
            if inserted < 3:
                logging.info(f"Row {idx} - merchant_id: {merchant_id}, name: {name_val}, street: {street_val}, state: {state_val}")
            
            try:
                conn.execute(text("""
                    INSERT INTO dim_merchant (merchant_id, name, street, state, city, country, contact_number, creation_date)
                    VALUES (:merchant_id, :name, :street, :state, :city, :country, :contact_number, :creation_date)
                    ON CONFLICT (merchant_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        street = EXCLUDED.street,
                        state = EXCLUDED.state,
                        city = EXCLUDED.city,
                        country = EXCLUDED.country,
                        contact_number = EXCLUDED.contact_number,
                        creation_date = EXCLUDED.creation_date
                """), {
                    'merchant_id': str(merchant_id).strip(),
                    'name': format_name(clean_value(name_val)),
                    'street': format_address(clean_value(street_val), 'street'),
                    'state': format_address(clean_value(state_val), 'state'),
                    'city': format_address(clean_value(city_val), 'city'),
                    'country': format_address(clean_value(country_val), 'country'),
                    'contact_number': format_phone_number(clean_value(contact_val)),
                    'creation_date': pd.to_datetime(creation_val, errors='coerce') if pd.notna(creation_val) and creation_val is not None else None
                })
                inserted += 1
            except Exception as e:
                logging.warning(f"Error inserting merchant {merchant_id} at row {idx}: {e}")
                continue
    
    logging.info(f"Loaded {inserted} merchants into dim_merchant (out of {len(combined)} total rows)")

def load_dim_user_job():
    """Load user job dimension (outrigger)"""
    logging.info("Loading dim_user_job...")
    files = find_files("*user_job*", DATA_DIR)
    if not files:
        logging.warning("No user_job files found")
        return
    
    all_data = []
    for file_path in files:
        df = load_file(file_path)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        logging.warning("No user job data loaded")
        return
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Insert into database
    with engine.begin() as conn:
        for _, row in combined.iterrows():
            conn.execute(text("""
                INSERT INTO dim_user_job (user_id, job_title, job_level)
                VALUES (:user_id, :job_title, :job_level)
                ON CONFLICT DO NOTHING
            """), {
                'user_id': str(row.get('user_id')),
                'job_title': format_job_title(clean_value(row.get('job_title'))),
                'job_level': format_job_level(clean_value(row.get('job_level')))
            })
    
    logging.info(f"Loaded {len(combined)} user jobs into dim_user_job")

def load_dim_credit_card():
    """Load credit card dimension (outrigger)"""
    logging.info("Loading dim_credit_card...")
    files = find_files("*user_credit_card*", DATA_DIR)
    if not files:
        logging.warning("No user_credit_card files found")
        return
    
    all_data = []
    for file_path in files:
        df = load_file(file_path)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        logging.warning("No credit card data loaded")
        return
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Insert into database
    with engine.begin() as conn:
        for _, row in combined.iterrows():
            conn.execute(text("""
                INSERT INTO dim_credit_card (user_id, name, credit_card_number, issuing_bank)
                VALUES (:user_id, :name, :credit_card_number, :issuing_bank)
                ON CONFLICT DO NOTHING
            """), {
                'user_id': str(row.get('user_id')),
                'name': format_name(clean_value(row.get('name'))),
                'credit_card_number': format_credit_card_number(clean_value(str(row.get('credit_card_number')) if pd.notna(row.get('credit_card_number')) else None)),
                'issuing_bank': format_issuing_bank(clean_value(row.get('issuing_bank')))
            })
    
    logging.info(f"Loaded {len(combined)} credit cards into dim_credit_card")

def get_column_value(row, possible_names):
    """Get column value trying multiple possible column names (handles normalized names)"""
    for name in possible_names:
        if name in row.index:
            return row[name]
    return None

def load_fact_orders():
    """Load fact_orders - combines order data, merchant data, and delays"""
    logging.info("Loading fact_orders...")
    
    # Load order data files
    order_files = find_files("*order_data*", DATA_DIR)
    if not order_files:
        logging.warning("No order_data files found")
        return
    
    order_data_list = []
    for file_path in order_files:
        df = load_file(file_path)
        if df is not None:
            order_data_list.append(df)
    
    if not order_data_list:
        logging.warning("No order data loaded")
        return
    
    order_data = pd.concat(order_data_list, ignore_index=True)
    
    # Log available columns for debugging
    logging.info(f"Available columns in order_data: {list(order_data.columns)}")
    
    # Load merchant/staff data
    merchant_files = find_files("*order_with_merchant_data*", DATA_DIR)
    merchant_data = None
    if merchant_files:
        merchant_list = []
        for file_path in merchant_files:
            df = load_file(file_path)
            if df is not None:
                merchant_list.append(df)
        if merchant_list:
            merchant_data = pd.concat(merchant_list, ignore_index=True)
            # Merge order data with merchant data
            order_data = order_data.merge(
                merchant_data,
                on='order_id',
                how='left'
            )
            logging.info(f"After merchant merge, columns: {list(order_data.columns)}")
    
    # Load delays
    delay_files = find_files("*order_delays*", DATA_DIR)
    delays = None
    if delay_files:
        delay_list = []
        for file_path in delay_files:
            df = load_file(file_path)
            if df is not None:
                delay_list.append(df)
        if delay_list:
            delays = pd.concat(delay_list, ignore_index=True)
            # Merge with delays
            order_data = order_data.merge(
                delays,
                on='order_id',
                how='left'
            )
            logging.info(f"After delays merge, columns: {list(order_data.columns)}")
    
    # Pre-load all dimension mappings (much faster than per-row queries)
    logging.info("Pre-loading dimension mappings...")
    with engine.connect() as conn:
        # Date dimension mapping
        date_map = {}
        result = conn.execute(text("SELECT date_sk, date FROM dim_date"))
        for row in result:
            date_map[str(row[1])] = row[0]
        
        # User dimension mapping
        user_map = {}
        result = conn.execute(text("SELECT user_sk, user_id FROM dim_user"))
        for row in result:
            user_map[str(row[1])] = row[0]
        
        # Merchant dimension mapping
        merchant_map = {}
        result = conn.execute(text("SELECT merchant_sk, merchant_id FROM dim_merchant"))
        for row in result:
            merchant_map[str(row[1])] = row[0]
        
        # Staff dimension mapping
        staff_map = {}
        result = conn.execute(text("SELECT staff_sk, staff_id FROM dim_staff"))
        for row in result:
            staff_map[str(row[1])] = row[0]
    
    logging.info(f"Loaded mappings: {len(date_map)} dates, {len(user_map)} users, {len(merchant_map)} merchants, {len(staff_map)} staff")
    
    # Prepare data for bulk insert
    logging.info("Preparing fact_orders data for bulk insert...")
    fact_rows = []
    
    for _, row in order_data.iterrows():
        try:
            # Get dimension keys from pre-loaded maps
            user_id = str(row.get('user_id'))
            merchant_id = str(row.get('merchant_id'))
            staff_id = str(row.get('staff_id'))
            
            user_sk = user_map.get(user_id)
            merchant_sk = merchant_map.get(merchant_id)
            staff_sk = staff_map.get(staff_id)
            
            if not user_sk or not merchant_sk or not staff_sk:
                continue
            
            # Get date_sk
            transaction_date = pd.to_datetime(row.get('transaction_date'), errors='coerce')
            if pd.isna(transaction_date):
                continue
            date_str = transaction_date.strftime('%Y-%m-%d')
            date_sk = date_map.get(date_str)
            if not date_sk:
                continue
            
            # Get estimated_arrival_days - try multiple column name variations
            estimated_arrival = get_column_value(row, ['estimated_arrival', 'estimated arrival', 'estimated_arrival_days'])
            estimated_arrival_days = extract_numeric(estimated_arrival)
            
            # Get delay_days - try multiple column name variations
            delay_days_val = get_column_value(row, ['delay_days', 'delay in days', 'delay_in_days'])
            delay_days = extract_numeric(delay_days_val) if delay_days_val is not None else -1
            
            fact_rows.append({
                'order_id': str(row.get('order_id')),
                'user_sk': user_sk,
                'merchant_sk': merchant_sk,
                'staff_sk': staff_sk,
                'transaction_date_sk': date_sk,
                'estimated_arrival_days': estimated_arrival_days,
                'delay_days': delay_days
            })
        except Exception as e:
            logging.warning(f"Error preparing order {row.get('order_id')}: {e}")
            continue
    
    # Bulk insert using executemany (much faster than row-by-row)
    if fact_rows:
        logging.info(f"Bulk inserting {len(fact_rows)} orders...")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fact_orders (order_id, user_sk, merchant_sk, staff_sk, transaction_date_sk, estimated_arrival_days, delay_days)
                VALUES (:order_id, :user_sk, :merchant_sk, :staff_sk, :transaction_date_sk, :estimated_arrival_days, :delay_days)
                ON CONFLICT (order_id) DO UPDATE SET
                    estimated_arrival_days = EXCLUDED.estimated_arrival_days,
                    delay_days = EXCLUDED.delay_days
            """), fact_rows)
    
    logging.info(f"Loaded {len(fact_rows)} orders into fact_orders")

def load_fact_line_items():
    """Load fact_line_items - combines prices and products by row position"""
    logging.info("Loading fact_line_items...")
    
    # Load prices
    prices_files = find_files("*line_item_data_prices*", DATA_DIR)
    if not prices_files:
        logging.warning("No line_item_data_prices files found")
        return
    
    prices_list = []
    for file_path in prices_files:
        df = load_file(file_path)
        if df is not None:
            prices_list.append(df)
    
    if not prices_list:
        logging.warning("No prices data loaded")
        return
    
    prices_data = pd.concat(prices_list, ignore_index=True)
    
    # Load products
    products_files = find_files("*line_item_data_products*", DATA_DIR)
    if not products_files:
        logging.warning("No line_item_data_products files found")
        return
    
    products_list = []
    for file_path in products_files:
        df = load_file(file_path)
        if df is not None:
            products_list.append(df)
    
    if not products_list:
        logging.warning("No products data loaded")
        return
    
    products_data = pd.concat(products_list, ignore_index=True)
    
    # Merge by row position (like original Python script)
    # The original script uses left_index=True, right_index=True to merge by position
    # We need to ensure both dataframes are in the same order and have same number of rows per order_id
    # Reset index to use positional merge
    prices_data = prices_data.reset_index(drop=True)
    products_data = products_data.reset_index(drop=True)
    
    # Merge by index position (matching original script behavior)
    combined = products_data.merge(
        prices_data,
        left_index=True,
        right_index=True,
        how='left',
        suffixes=('', '_price')
    )
    
    # Clean up duplicate order_id column if exists
    if 'order_id_price' in combined.columns:
        combined = combined.drop(columns=['order_id_price'])
    
    # Pre-load dimension mappings
    logging.info("Pre-loading dimension mappings for fact_line_items...")
    with engine.connect() as conn:
        # Product dimension mapping
        product_map = {}
        result = conn.execute(text("SELECT product_sk, product_id FROM dim_product"))
        for row in result:
            product_map[str(row[1])] = row[0]
        
        # Fact_orders mapping (order_id -> dimension keys)
        order_map = {}
        result = conn.execute(text("SELECT order_id, user_sk, merchant_sk, staff_sk, transaction_date_sk FROM fact_orders"))
        for row in result:
            order_map[str(row[0])] = {
                'user_sk': row[1],
                'merchant_sk': row[2],
                'staff_sk': row[3],
                'transaction_date_sk': row[4]
            }
    
    logging.info(f"Loaded mappings: {len(product_map)} products, {len(order_map)} orders")
    
    # Prepare data for bulk insert
    logging.info("Preparing fact_line_items data for bulk insert...")
    fact_rows = []
    
    for _, row in combined.iterrows():
        try:
            # Get product_sk from pre-loaded map
            product_id = str(row.get('product_id'))
            product_sk = product_map.get(product_id)
            if not product_sk:
                continue
            
            # Get keys from fact_orders
            order_id = str(row.get('order_id'))
            order_keys = order_map.get(order_id)
            if not order_keys:
                continue
            
            fact_rows.append({
                'order_id': order_id,
                'product_sk': product_sk,
                'user_sk': order_keys['user_sk'],
                'merchant_sk': order_keys['merchant_sk'],
                'staff_sk': order_keys['staff_sk'],
                'transaction_date_sk': order_keys['transaction_date_sk'],
                'price': float(row.get('price', 0)) if pd.notna(row.get('price')) else None,
                'quantity': extract_numeric(row.get('quantity')) or 0
            })
        except Exception as e:
            logging.warning(f"Error preparing line item {row.get('order_id')}: {e}")
            continue
    
    # Bulk insert
    if fact_rows:
        logging.info(f"Bulk inserting {len(fact_rows)} line items...")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fact_line_items (order_id, product_sk, user_sk, merchant_sk, staff_sk, transaction_date_sk, price, quantity)
                VALUES (:order_id, :product_sk, :user_sk, :merchant_sk, :staff_sk, :transaction_date_sk, :price, :quantity)
                ON CONFLICT (order_id, product_sk) DO NOTHING
            """), fact_rows)
    
    logging.info(f"Loaded {len(fact_rows)} line items into fact_line_items")

def load_fact_campaign_transactions():
    """Load fact_campaign_transactions"""
    logging.info("Loading fact_campaign_transactions...")
    
    files = find_files("*transactional_campaign_data*", DATA_DIR)
    if not files:
        logging.warning("No transactional_campaign_data files found")
        return
    
    all_data = []
    for file_path in files:
        df = load_file(file_path)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        logging.warning("No campaign transaction data loaded")
        return
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Fill nulls like original script
    combined['campaign_id'] = combined['campaign_id'].fillna('CAMPAIGN00000')
    combined['availed'] = combined['availed'].fillna('Not Applicable')
    
    # Pre-load dimension mappings
    logging.info("Pre-loading dimension mappings for fact_campaign_transactions...")
    with engine.connect() as conn:
        # Campaign dimension mapping
        campaign_map = {}
        result = conn.execute(text("SELECT campaign_sk, campaign_id FROM dim_campaign"))
        for row in result:
            campaign_map[str(row[1])] = row[0]
        
        # Fact_orders mapping (order_id -> dimension keys)
        order_map = {}
        result = conn.execute(text("SELECT order_id, user_sk, merchant_sk, transaction_date_sk FROM fact_orders"))
        for row in result:
            order_map[str(row[0])] = {
                'user_sk': row[1],
                'merchant_sk': row[2],
                'transaction_date_sk': row[3]
            }
    
    logging.info(f"Loaded mappings: {len(campaign_map)} campaigns, {len(order_map)} orders")
    
    # Prepare data for bulk insert
    logging.info("Preparing fact_campaign_transactions data for bulk insert...")
    fact_rows = []
    
    for _, row in combined.iterrows():
        try:
            # Get campaign_sk from pre-loaded map
            campaign_id = str(row.get('campaign_id'))
            campaign_sk = campaign_map.get(campaign_id)
            if not campaign_sk:
                continue
            
            # Get keys from fact_orders
            order_id = str(row.get('order_id'))
            order_keys = order_map.get(order_id)
            if not order_keys:
                continue
            
            # Convert availed
            availed_val = row.get('availed')
            if pd.isna(availed_val) or availed_val == 'Not Applicable':
                availed = None
            elif str(availed_val).lower() in ('1', 'true'):
                availed = 1
            else:
                availed = 0
            
            fact_rows.append({
                'order_id': order_id,
                'campaign_sk': campaign_sk,
                'user_sk': order_keys['user_sk'],
                'merchant_sk': order_keys['merchant_sk'],
                'transaction_date_sk': order_keys['transaction_date_sk'],
                'availed': availed
            })
        except Exception as e:
            logging.warning(f"Error preparing campaign transaction {row.get('order_id')}: {e}")
            continue
    
    # Bulk insert
    if fact_rows:
        logging.info(f"Bulk inserting {len(fact_rows)} campaign transactions...")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fact_campaign_transactions (order_id, campaign_sk, user_sk, merchant_sk, transaction_date_sk, availed)
                VALUES (:order_id, :campaign_sk, :user_sk, :merchant_sk, :transaction_date_sk, :availed)
                ON CONFLICT (campaign_sk, user_sk, merchant_sk, transaction_date_sk) DO NOTHING
            """), fact_rows)
    
    logging.info(f"Loaded {len(fact_rows)} campaign transactions into fact_campaign_transactions")

def main():
    """Main ETL pipeline - similar to original Python scripts"""
    logging.info("=" * 60)
    logging.info("Starting ShopZada ETL Pipeline (Python-based)")
    logging.info("=" * 60)
    
    # Check if schema exists
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'dim_campaign'
        """))
        if result.fetchone()[0] == 0:
            logging.error("Schema not created! Please run create_schema task first.")
            return
    
    # Load dimensions first
    load_dim_campaign()
    load_dim_product()
    load_dim_user()
    load_dim_staff()
    load_dim_merchant()
    load_dim_user_job()
    load_dim_credit_card()
    
    # Load facts (depend on dimensions)
    load_fact_orders()
    load_fact_line_items()
    load_fact_campaign_transactions()
    
    logging.info("=" * 60)
    logging.info("ETL Pipeline Complete!")
    logging.info("=" * 60)

if __name__ == "__main__":
    main()

