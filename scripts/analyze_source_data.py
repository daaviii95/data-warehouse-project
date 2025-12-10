#!/usr/bin/env python3
"""
Source Data Quality Analysis Script
Analyzes all source data files in /data folder for quality issues
Does NOT scan the database - only source files
"""

import os
import sys
import logging
import pandas as pd
import json
import pickle
from pathlib import Path
from collections import Counter
import re

# Configuration
DATA_DIR = os.getenv("DATA_DIR", "data")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def load_file(file_path):
    """Load file based on extension"""
    ext = Path(file_path).suffix.lower()
    try:
        if ext == '.csv':
            return pd.read_csv(file_path, low_memory=False)
        elif ext == '.xlsx':
            return pd.read_excel(file_path)
        elif ext == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return pd.json_normalize(data)
        elif ext == '.pickle':
            return pd.read_pickle(file_path)
        elif ext == '.parquet':
            return pd.read_parquet(file_path)
        elif ext == '.html':
            tables = pd.read_html(file_path)
            return tables[0] if tables else None
        else:
            logging.warning(f"Unsupported file type: {ext}")
            return None
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None

def find_typos_in_values(df, column_name, known_correct_values=None):
    """Find potential typos in a column by checking for similar values"""
    if column_name not in df.columns:
        return []
    
    # Get all unique values
    unique_values = df[column_name].dropna().unique()
    
    # Find potential typos (similar strings with small differences)
    typos = []
    if known_correct_values:
        for value in unique_values:
            value_str = str(value).strip().lower()
            for correct in known_correct_values:
                correct_str = str(correct).strip().lower()
                # Check for common typos
                if value_str != correct_str:
                    # Check for double letters (e.g., "Toolss" vs "Tools")
                    if value_str.replace(correct_str, '') in ['s', 'ss'] and correct_str in value_str:
                        typos.append((value, correct))
                    # Check for missing letters
                    elif len(value_str) == len(correct_str) - 1 and correct_str.startswith(value_str):
                        typos.append((value, correct))
                    # Check for extra letters
                    elif len(value_str) == len(correct_str) + 1 and value_str.startswith(correct_str):
                        typos.append((value, correct))
    
    return typos

def analyze_column_values(df, column_name):
    """Analyze a column for data quality issues"""
    if column_name not in df.columns:
        return {}
    
    issues = {
        'null_count': df[column_name].isnull().sum(),
        'null_percentage': (df[column_name].isnull().sum() / len(df)) * 100,
        'unique_values': df[column_name].nunique(),
        'value_counts': df[column_name].value_counts().head(10).to_dict(),
        'potential_issues': []
    }
    
    # Check for empty strings
    if df[column_name].dtype == 'object':
        empty_strings = (df[column_name].astype(str).str.strip() == '').sum()
        if empty_strings > 0:
            issues['potential_issues'].append(f"{empty_strings} empty strings found")
    
    # Check for inconsistent casing
    if df[column_name].dtype == 'object':
        unique_lower = set(str(v).lower() for v in df[column_name].dropna().unique())
        unique_actual = set(str(v) for v in df[column_name].dropna().unique())
        if len(unique_lower) < len(unique_actual):
            issues['potential_issues'].append("Inconsistent casing detected")
    
    # Check for extra whitespace
    if df[column_name].dtype == 'object':
        has_extra_whitespace = df[column_name].astype(str).str.strip().ne(df[column_name].astype(str)).sum()
        if has_extra_whitespace > 0:
            issues['potential_issues'].append(f"{has_extra_whitespace} values with extra whitespace")
    
    return issues

def analyze_file(file_path):
    """Comprehensive analysis of a single file"""
    print(f"\n{'='*80}")
    print(f"Analyzing: {file_path}")
    print(f"{'='*80}")
    
    df = load_file(file_path)
    if df is None:
        print("  ❌ FAILED: Could not load file")
        return None
    
    print(f"  ✓ Loaded: {len(df)} rows, {len(df.columns)} columns")
    print(f"  Columns: {list(df.columns)}")
    
    issues_found = []
    
    # Check for duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        issues_found.append(f"⚠️  Duplicate rows: {duplicates} ({duplicates/len(df)*100:.2f}%)")
    else:
        print(f"  ✓ No duplicate rows")
    
    # Analyze each column
    print(f"\n  Column Analysis:")
    for col in df.columns:
        col_issues = analyze_column_values(df, col)
        
        if col_issues['null_count'] > 0:
            print(f"    {col}:")
            print(f"      - Nulls: {col_issues['null_count']} ({col_issues['null_percentage']:.2f}%)")
        
        if col_issues['potential_issues']:
            for issue in col_issues['potential_issues']:
                issues_found.append(f"    {col}: {issue}")
                print(f"      ⚠️  {issue}")
        
        # Check for specific known issues based on column name
        col_lower = str(col).lower()
        
        # Product type analysis
        if 'product_type' in col_lower or 'type' in col_lower:
            unique_types = df[col].dropna().unique()
            print(f"    {col} unique values ({len(unique_types)}): {sorted(unique_types)}")
            
            # Check for typos in product types
            known_types = ['Tools', 'Cosmetics', 'Electronics', 'Apparel', 'Furniture', 
                          'Grocery', 'Kitchenware', 'Sports', 'Toys', 'Jewelry']
            typos = find_typos_in_values(df, col, known_types)
            if typos:
                for typo, correct in typos:
                    count = (df[col] == typo).sum()
                    issues_found.append(f"    {col}: Typo '{typo}' (should be '{correct}') - {count} occurrences")
                    print(f"      ⚠️  Typo: '{typo}' → '{correct}' ({count} occurrences)")
        
        # Name analysis
        if 'name' in col_lower and 'product' not in col_lower and 'campaign' not in col_lower:
            # Check for inconsistent formatting
            sample_names = df[col].dropna().head(20)
            has_mixed_case = any(str(n) != str(n).title() and str(n) != str(n).upper() and str(n) != str(n).lower() for n in sample_names)
            if has_mixed_case:
                issues_found.append(f"    {col}: Inconsistent name formatting")
                print(f"      ⚠️  Inconsistent name formatting detected")
        
        # Email analysis (if exists)
        if 'email' in col_lower:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_emails = df[col].astype(str).apply(lambda x: not re.match(email_pattern, x) if pd.notna(x) and x != '' else False).sum()
            if invalid_emails > 0:
                issues_found.append(f"    {col}: {invalid_emails} invalid email formats")
                print(f"      ⚠️  {invalid_emails} invalid email formats")
        
        # Phone number analysis
        if 'phone' in col_lower or 'contact' in col_lower:
            # Check for inconsistent formats
            sample_phones = df[col].dropna().head(20).astype(str)
            formats = set()
            for phone in sample_phones:
                # Extract digits only
                digits = re.sub(r'\D', '', phone)
                formats.add(len(digits))
            if len(formats) > 2:
                issues_found.append(f"    {col}: Inconsistent phone number formats")
                print(f"      ⚠️  Inconsistent phone number formats detected")
    
    # Check for common data quality issues across all columns
    print(f"\n  Overall Data Quality:")
    
    # Check for completely empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    if empty_rows > 0:
        issues_found.append(f"⚠️  {empty_rows} completely empty rows")
        print(f"    ⚠️  {empty_rows} completely empty rows")
    
    # Check for rows with mostly nulls (>80%)
    mostly_null_rows = (df.isnull().sum(axis=1) / len(df.columns) > 0.8).sum()
    if mostly_null_rows > 0:
        issues_found.append(f"⚠️  {mostly_null_rows} rows with >80% null values")
        print(f"    ⚠️  {mostly_null_rows} rows with >80% null values")
    
    # Sample data
    print(f"\n  Sample Data (first 3 rows):")
    print(df.head(3).to_string())
    
    return {
        'file': str(file_path),
        'rows': len(df),
        'columns': len(df.columns),
        'issues': issues_found
    }

def main():
    """Main analysis function"""
    print("="*80)
    print("SHOPZADA SOURCE DATA QUALITY ANALYSIS")
    print("="*80)
    print("Analyzing source files in:", DATA_DIR)
    
    data_path = Path(DATA_DIR)
    if not data_path.exists():
        print(f"ERROR: Data directory not found: {DATA_DIR}")
        return
    
    all_issues = []
    file_results = []
    
    # Find all data files
    data_files = []
    for ext in ['*.csv', '*.xlsx', '*.json', '*.pickle', '*.parquet', '*.html']:
        data_files.extend(list(data_path.rglob(ext)))
    
    print(f"\nFound {len(data_files)} data files to analyze")
    
    # Analyze each file
    for file_path in sorted(data_files):
        result = analyze_file(file_path)
        if result:
            file_results.append(result)
            if result['issues']:
                all_issues.extend(result['issues'])
    
    # Summary
    print("\n" + "="*80)
    print("ANALYSIS SUMMARY")
    print("="*80)
    
    print(f"\nFiles Analyzed: {len(file_results)}")
    print(f"Total Issues Found: {len(all_issues)}")
    
    if all_issues:
        print("\n⚠️  Issues Found:")
        for i, issue in enumerate(all_issues, 1):
            print(f"  {i}. {issue}")
    else:
        print("\n✅ No issues found!")
    
    # File-by-file summary
    print("\n" + "="*80)
    print("FILE SUMMARY")
    print("="*80)
    for result in file_results:
        print(f"\n{result['file']}:")
        print(f"  Rows: {result['rows']:,}")
        print(f"  Columns: {result['columns']}")
        if result['issues']:
            print(f"  Issues: {len(result['issues'])}")
            for issue in result['issues']:
                print(f"    - {issue}")
        else:
            print(f"  ✅ No issues")
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

