#!/usr/bin/env python3
"""
Transform Data
Applies data cleaning and transformation to extracted staging data
"""

import os
import sys
import logging
import pandas as pd

# Import utilities from etl_pipeline_python
sys.path.insert(0, os.path.dirname(__file__))
from etl_pipeline_python import (
    clean_dataframe, clean_value, parse_discount, format_product_type,
    format_name, format_address, format_phone_number, format_gender,
    format_user_type, format_job_title, format_job_level,
    format_credit_card_number, format_issuing_bank,
    format_campaign_description, format_product_name, format_campaign_name,
    extract_numeric
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def transform_campaign_data(df):
    """Transform campaign data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming campaign data...")
    # Remove file_source and loaded_at columns
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    # Apply formatting
    if 'campaign_name' in df.columns:
        df['campaign_name'] = df['campaign_name'].apply(lambda x: format_campaign_name(clean_value(x)))
    if 'campaign_description' in df.columns:
        df['campaign_description'] = df['campaign_description'].apply(lambda x: format_campaign_description(clean_value(x)))
    if 'discount' in df.columns:
        df['discount'] = df['discount'].apply(parse_discount)
    
    return df

def transform_product_data(df):
    """Transform product data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming product data...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    if 'product_name' in df.columns:
        df['product_name'] = df['product_name'].apply(lambda x: format_product_name(clean_value(x)))
    if 'product_type' in df.columns:
        df['product_type'] = df['product_type'].apply(format_product_type)
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    return df

def transform_user_data(df):
    """Transform user data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming user data...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    if 'name' in df.columns:
        df['name'] = df['name'].apply(lambda x: format_name(clean_value(x)))
    if 'street' in df.columns:
        df['street'] = df['street'].apply(lambda x: format_address(clean_value(x)))
    if 'gender' in df.columns:
        df['gender'] = df['gender'].apply(format_gender)
    if 'user_type' in df.columns:
        df['user_type'] = df['user_type'].apply(format_user_type)
    
    return df

def transform_user_job_data(df):
    """Transform user job data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming user job data...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    if 'job_title' in df.columns:
        df['job_title'] = df['job_title'].apply(lambda x: format_job_title(clean_value(x)))
    if 'job_level' in df.columns:
        # Clean [null] strings and convert to None
        def clean_job_level(val):
            if pd.isna(val) or val is None:
                return None
            val_str = str(val).strip()
            if val_str.lower() in ['[null]', 'null', 'none', '']:
                return None
            return format_job_level(val_str)
        df['job_level'] = df['job_level'].apply(clean_job_level)
    
    return df

def transform_user_credit_card_data(df):
    """Transform user credit card data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming user credit card data...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    # Format name (required per PHYSICALMODEL.txt)
    if 'name' in df.columns:
        df['name'] = df['name'].apply(lambda x: format_name(clean_value(x)))
    if 'credit_card_number' in df.columns:
        df['credit_card_number'] = df['credit_card_number'].apply(lambda x: format_credit_card_number(clean_value(x)))
    if 'issuing_bank' in df.columns:
        df['issuing_bank'] = df['issuing_bank'].apply(lambda x: format_issuing_bank(clean_value(x)))
    
    return df

def transform_merchant_data(df):
    """Transform merchant data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming merchant data...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    if 'name' in df.columns:
        df['name'] = df['name'].apply(lambda x: format_name(clean_value(x)))
    if 'street' in df.columns:
        df['street'] = df['street'].apply(lambda x: format_address(clean_value(x)))
    # Format contact_number (required per PHYSICALMODEL.txt)
    if 'contact_number' in df.columns:
        df['contact_number'] = df['contact_number'].apply(lambda x: format_phone_number(clean_value(x)) if pd.notna(x) else None)
    # Ensure creation_date is preserved (required per PHYSICALMODEL.txt)
    if 'creation_date' in df.columns:
        df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')
    
    return df

def transform_staff_data(df):
    """Transform staff data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming staff data...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    if 'name' in df.columns:
        df['name'] = df['name'].apply(lambda x: format_name(clean_value(x)))
    if 'street' in df.columns:
        df['street'] = df['street'].apply(lambda x: format_address(clean_value(x)))
    
    return df

def transform_order_data(df):
    """Transform order data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming order data...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    # Convert transaction_date to date if it's a string
    if 'transaction_date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce').dt.date
    
    return df

def transform_line_item_prices(df):
    """Transform line item prices"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming line item prices...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    if 'discount' in df.columns:
        df['discount'] = df['discount'].apply(parse_discount)
    if 'quantity' in df.columns:
        df['quantity'] = df['quantity'].apply(extract_numeric)
    
    return df

def transform_line_item_products(df):
    """Transform line item products"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming line item products...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    return df

def transform_order_delays(df):
    """Transform order delays"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming order delays...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    if 'delay_days' in df.columns:
        df['delay_days'] = df['delay_days'].apply(extract_numeric)
    
    return df

def transform_transactional_campaign_data(df):
    """Transform transactional campaign data"""
    if df is None or df.empty:
        return df
    
    logging.info("Transforming transactional campaign data...")
    df = df.drop(columns=['file_source', 'loaded_at'], errors='ignore')
    
    # Convert availed to boolean/int
    if 'availed' in df.columns:
        def convert_availed(val):
            if pd.isna(val) or val == 'Not Applicable':
                return None
            if str(val).lower() in ('1', 'true', 'yes'):
                return 1
            return 0
        df['availed'] = df['availed'].apply(convert_availed)
    
    return df

