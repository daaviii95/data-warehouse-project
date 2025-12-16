# Test Data Directory

This directory contains test data files organized by scenario for testing different aspects of the data warehouse ETL pipeline.

## Directory Structure

```
data/Test/
├── Scenario1/          # End-to-End Pipeline Test
├── Scenario2/          # New Customer and Product Creation
├── Scenario3/          # Late & Missing Campaign Data
├── Scenario4/          # Data Quality & Invalid Records
└── README.md          # This file
```

## Scenario Folders

### Scenario 1: End-to-End Pipeline Test
**Purpose**: Demonstrate incremental batch loading and end-to-end data flow

**Files**:
- `order_data_scenario1.csv` - 5 orders for date 2024-01-20
- `line_item_data_prices_scenario1.csv` - Line item prices
- `line_item_data_products_scenario1.csv` - Line item products
- `order_with_merchant_data_scenario1.csv` - Merchant/staff assignments
- `SCENARIO1_TEST_DATA_README.md` - Detailed testing guide

**See**: `Scenario1/SCENARIO1_TEST_DATA_README.md` for complete instructions

---

### Scenario 2: New Customer and Product Creation
**Purpose**: Demonstrate automatic dimension creation from fact data

**Files**:
- `order_data_scenario2.csv` - Orders with NEW_USER_SCENARIO2
- `line_item_data_prices_scenario2.csv` - Line item prices
- `line_item_data_products_scenario2.csv` - Line items with NEW products
- `order_with_merchant_data_scenario2.csv` - Merchant/staff assignments
- `SCENARIO2_TEST_DATA_README.md` - Detailed testing guide

**See**: `Scenario2/SCENARIO2_TEST_DATA_README.md` for complete instructions

---

### Scenario 3: Late & Missing Campaign Data
**Purpose**: Demonstrate handling of late-arriving dimension data (campaigns)

**Files**:
- `order_data_scenario3.csv` - Orders for campaign transactions
- `transactional_campaign_data_scenario3.csv` - Campaign transactions with missing campaigns
- `campaign_data_scenario3.csv` - Late-arriving campaign dimension data
- `SCENARIO3_TEST_DATA_README.md` - Detailed testing guide

**See**: `Scenario3/SCENARIO3_TEST_DATA_README.md` for complete instructions

---

### Scenario 4: Data Quality & Invalid Records
**Purpose**: Demonstrate data quality handling and reject table functionality

**Files**:
- `order_data_scenario4.csv` - Orders with invalid data
- `line_item_data_prices_scenario4.csv` - Line item prices with invalid data
- `line_item_data_products_scenario4.csv` - Line item products with invalid data
- `order_with_merchant_data_scenario4.csv` - Merchant data with invalid entries
- `transactional_campaign_data_scenario4.csv` - Campaign transactions with invalid data
- `SCENARIO4_TEST_DATA_README.md` - Detailed testing guide

**See**: `Scenario4/SCENARIO4_TEST_DATA_README.md` for complete instructions

---

## Usage

### To Use Test Data:

1. **Navigate to the scenario folder** (e.g., `Scenario1/`)
2. **Read the README** for that scenario to understand the test requirements
3. **Copy test files** to the appropriate department folders:
   - Operations Department files → `data/Operations Department/`
   - Marketing Department files → `data/Marketing Department/`
4. **Run the ETL pipeline** (Airflow DAG or Python script)
5. **Follow verification steps** in the scenario README

### Example:

```bash
# For Scenario 1
cd data/Test/Scenario1
# Copy files to Operations Department
cp *.csv "../../Operations Department/"
# Run ETL pipeline
# Follow instructions in SCENARIO1_TEST_DATA_README.md
```

---

## File Naming Conventions

All test files use prefixes to identify their scenario:
- `scenario1-*` - Scenario 1 test data
- `scenario2-*` - Scenario 2 test data
- `scenario3-*` - Scenario 3 test data
- `scenario4-*` - Scenario 4 test data

This makes it easy to:
- Identify test data in the database
- Clean up test data after testing
- Filter test data in queries

---

## Cleanup

After testing, you can remove test data using SQL queries provided in each scenario's README file. All test data uses the scenario prefix in order_ids and other identifiers for easy identification and cleanup.

---

## Notes

- All test data files are CSV format with comma separators
- Dates in test data should exist in `dim_date` (populated by `02_populate_dim_date.sql`)
- Some test data requires existing dimension members (users, products, merchants, staff)
- Some test data creates new dimension members (Scenario 2)
- Always read the scenario-specific README before testing

