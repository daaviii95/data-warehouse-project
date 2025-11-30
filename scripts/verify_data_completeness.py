#!/usr/bin/env python3
"""
Data Completeness Verification Script
Checks if all expected data is present in staging and fact tables
"""

import sys
import os
sys.path.insert(0, '/opt/airflow/repo/scripts')

from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def get_table_count(hook, table_name):
    """Get row count for a table"""
    try:
        sql = f"SELECT COUNT(*) FROM {table_name}"
        result = hook.get_first(sql)
        return result[0] if result else 0
    except Exception as e:
        logger.warning(f"Could not count {table_name}: {e}")
        return 0

def get_staging_tables(hook, pattern):
    """Get all staging tables matching a pattern"""
    sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
          AND table_name LIKE %s
          AND table_name NOT LIKE '%%_tbl%%'
        ORDER BY table_name
    """
    result = hook.get_records(sql, parameters=(pattern,))
    return [row[0] for row in result] if result else []

def main():
    hook = PostgresHook(postgres_conn_id='shopzada_postgres')
    
    logger.info("=" * 80)
    logger.info("DATA COMPLETENESS VERIFICATION")
    logger.info("=" * 80)
    
    # 1. Check Staging Tables
    logger.info("\n📊 STAGING TABLES SUMMARY")
    logger.info("-" * 80)
    
    staging_totals = {}
    
    # Line Items - Prices
    prices_tables = get_staging_tables(hook, 'stg_operations_department_line_item_data_prices%')
    prices_total = sum(get_table_count(hook, table) for table in prices_tables)
    staging_totals['line_items_prices'] = prices_total
    logger.info(f"Line Items (Prices): {len(prices_tables)} tables, {prices_total:,} records")
    for table in prices_tables:
        count = get_table_count(hook, table)
        logger.info(f"  - {table}: {count:,}")
    
    # Line Items - Products
    products_tables = get_staging_tables(hook, 'stg_operations_department_line_item_data_products%')
    products_total = sum(get_table_count(hook, table) for table in products_tables)
    staging_totals['line_items_products'] = products_total
    logger.info(f"\nLine Items (Products): {len(products_tables)} tables, {products_total:,} records")
    for table in products_tables:
        count = get_table_count(hook, table)
        logger.info(f"  - {table}: {count:,}")
    
    # Orders - Operations
    order_tables = get_staging_tables(hook, 'stg_operations_department_order_data%')
    order_total = sum(get_table_count(hook, table) for table in order_tables)
    staging_totals['orders_operations'] = order_total
    logger.info(f"\nOrders (Operations): {len(order_tables)} tables, {order_total:,} records")
    for table in order_tables:
        count = get_table_count(hook, table)
        logger.info(f"  - {table}: {count:,}")
    
    # Orders - Merchant
    merchant_tables = get_staging_tables(hook, 'stg_enterprise_department_order_with_merchant_data%')
    merchant_total = sum(get_table_count(hook, table) for table in merchant_tables)
    staging_totals['orders_merchant'] = merchant_total
    logger.info(f"\nOrders (Merchant): {len(merchant_tables)} tables, {merchant_total:,} records")
    for table in merchant_tables:
        count = get_table_count(hook, table)
        logger.info(f"  - {table}: {count:,}")
    
    # Campaign Transactions
    campaign_tables = get_staging_tables(hook, 'stg_marketing_department_transactional_campaign_data%')
    campaign_total = sum(get_table_count(hook, table) for table in campaign_tables)
    staging_totals['campaign_transactions'] = campaign_total
    logger.info(f"\nCampaign Transactions: {len(campaign_tables)} tables, {campaign_total:,} records")
    for table in campaign_tables:
        count = get_table_count(hook, table)
        logger.info(f"  - {table}: {count:,}")
    
    # Order Delays
    delay_tables = get_staging_tables(hook, 'stg_operations_department_order_delays%')
    delay_total = sum(get_table_count(hook, table) for table in delay_tables)
    staging_totals['order_delays'] = delay_total
    logger.info(f"\nOrder Delays: {len(delay_tables)} tables, {delay_total:,} records")
    
    # Total Staging Records
    total_staging = sum(staging_totals.values())
    logger.info(f"\n{'='*80}")
    logger.info(f"TOTAL STAGING RECORDS: {total_staging:,}")
    logger.info(f"{'='*80}")
    
    # 2. Check Fact Tables
    logger.info("\n📈 FACT TABLES SUMMARY")
    logger.info("-" * 80)
    
    fact_orders = get_table_count(hook, 'fact_orders')
    fact_line_items = get_table_count(hook, 'fact_line_items')
    fact_campaign = get_table_count(hook, 'fact_campaign_transactions')
    
    logger.info(f"fact_orders: {fact_orders:,} records")
    logger.info(f"fact_line_items: {fact_line_items:,} records")
    logger.info(f"fact_campaign_transactions: {fact_campaign:,} records")
    
    total_fact = fact_orders + fact_line_items + fact_campaign
    logger.info(f"\n{'='*80}")
    logger.info(f"TOTAL FACT RECORDS: {total_fact:,}")
    logger.info(f"{'='*80}")
    
    # 3. Check Dimensions
    logger.info("\n📋 DIMENSION TABLES SUMMARY")
    logger.info("-" * 80)
    
    dimensions = [
        'dim_campaign', 'dim_product', 'dim_user', 'dim_staff', 
        'dim_merchant', 'dim_user_job', 'dim_credit_card', 'dim_date'
    ]
    
    dim_total = 0
    for dim in dimensions:
        count = get_table_count(hook, dim)
        dim_total += count
        logger.info(f"{dim}: {count:,} records")
    
    logger.info(f"\nTOTAL DIMENSION RECORDS: {dim_total:,}")
    
    # 4. Data Completeness Analysis
    logger.info("\n" + "=" * 80)
    logger.info("DATA COMPLETENESS ANALYSIS")
    logger.info("=" * 80)
    
    # Expected vs Actual
    logger.info(f"\nExpected Total Records: ~9,000,000")
    logger.info(f"Actual Staging Records: {total_staging:,}")
    logger.info(f"Actual Fact Records: {total_fact:,}")
    
    if total_fact == 0:
        logger.warning("\n⚠️  WARNING: Fact tables are empty!")
        logger.warning("   The ETL pipeline needs to be run to load data into fact tables.")
    else:
        completeness = (total_fact / total_staging * 100) if total_staging > 0 else 0
        logger.info(f"\nData Loading Completeness: {completeness:.2f}%")
        
        if completeness < 90:
            logger.warning(f"⚠️  WARNING: Only {completeness:.2f}% of staging data loaded into facts!")
    
    # Check for missing data
    logger.info("\n" + "-" * 80)
    logger.info("MISSING DATA CHECKS")
    logger.info("-" * 80)
    
    if fact_orders == 0:
        logger.warning("❌ fact_orders is empty - check load_fact_orders task")
    else:
        logger.info(f"✅ fact_orders has {fact_orders:,} records")
    
    if fact_line_items == 0:
        logger.warning("❌ fact_line_items is empty - check load_fact_line_items task")
    else:
        logger.info(f"✅ fact_line_items has {fact_line_items:,} records")
    
    if fact_campaign == 0:
        logger.warning("❌ fact_campaign_transactions is empty - check load_fact_campaign_transactions task")
    else:
        logger.info(f"✅ fact_campaign_transactions has {fact_campaign:,} records")
    
    logger.info("\n" + "=" * 80)
    logger.info("VERIFICATION COMPLETE")
    logger.info("=" * 80)
    
    return {
        'staging_total': total_staging,
        'fact_total': total_fact,
        'dimension_total': dim_total,
        'completeness': (total_fact / total_staging * 100) if total_staging > 0 else 0
    }

if __name__ == '__main__':
    main()

