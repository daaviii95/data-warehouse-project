# ShopZada Data Warehouse - Data Model Documentation

## Methodology: Kimball Star Schema

This document describes the dimensional model for ShopZada's Data Warehouse.

## Star Schema Overview

The data warehouse follows a **Star Schema** design with:
- **1 Date Dimension** (conformed dimension)
- **6 Core Dimensions** (Campaign, Product, User, Staff, Merchant, Date)
- **2 Outrigger Dimensions** (User Job, Credit Card)
- **3 Fact Tables** (Orders, Line Items, Campaign Transactions)

## Dimension Tables

### dim_date
**Type**: Conformed Dimension (Time)

| Column | Type | Description |
|--------|------|-------------|
| date_sk | INTEGER (PK) | Surrogate key (YYYYMMDD format) |
| date | DATE | Actual date |
| year | INTEGER | Year (2020-2025) |
| month | INTEGER | Month (1-12) |
| month_name | VARCHAR(50) | Month name |
| day | INTEGER | Day of month |
| quarter | VARCHAR(2) | Quarter (Q1-Q4) |
| day_of_week | INTEGER | Day of week (0-6) |
| day_name | VARCHAR(50) | Day name |
| is_weekend | BOOLEAN | Weekend flag |

**Grain**: One row per day from 2020-01-01 to 2025-12-31

---

### dim_campaign
**Type**: Dimension

| Column | Type | Description |
|--------|------|-------------|
| campaign_sk | SERIAL (PK) | Surrogate key |
| campaign_id | VARCHAR(50) (UK) | Business key |
| campaign_name | VARCHAR(255) | Campaign name |
| campaign_description | TEXT | Campaign description |
| discount | DECIMAL(10,2) | Discount percentage |

**Source**: Marketing Department - campaign_data.csv

---

### dim_product
**Type**: Dimension

| Column | Type | Description |
|--------|------|-------------|
| product_sk | SERIAL (PK) | Surrogate key |
| product_id | VARCHAR(50) (UK) | Business key |
| product_name | VARCHAR(255) | Product name |
| product_type | VARCHAR(100) | Product category |
| price | DECIMAL(10,2) | Base price |

**Source**: Business Department - product_list.xlsx

---

### dim_user
**Type**: Dimension

| Column | Type | Description |
|--------|------|-------------|
| user_sk | SERIAL (PK) | Surrogate key |
| user_id | VARCHAR(50) (UK) | Business key |
| name | VARCHAR(255) | Customer name |
| street | VARCHAR(255) | Street address |
| state | VARCHAR(100) | State |
| city | VARCHAR(100) | City |
| country | VARCHAR(100) | Country |
| birthdate | DATE | Date of birth |
| gender | VARCHAR(20) | Gender |
| device_address | VARCHAR(255) | Device IP/MAC |
| user_type | VARCHAR(50) | User type/segment |
| creation_date | TIMESTAMP | Account creation date |

**Source**: Customer Management Department - user_data.json

---

### dim_staff
**Type**: Dimension

| Column | Type | Description |
|--------|------|-------------|
| staff_sk | SERIAL (PK) | Surrogate key |
| staff_id | VARCHAR(50) (UK) | Business key |
| name | VARCHAR(255) | Staff name |
| street | VARCHAR(255) | Street address |
| state | VARCHAR(100) | State |
| city | VARCHAR(100) | City |
| country | VARCHAR(100) | Country |
| job_level | VARCHAR(50) | Job level |
| contact_number | VARCHAR(50) | Contact number |
| creation_date | TIMESTAMP | Employment start date |

**Source**: Enterprise Department - staff_data.html

---

### dim_merchant
**Type**: Dimension

| Column | Type | Description |
|--------|------|-------------|
| merchant_sk | SERIAL (PK) | Surrogate key |
| merchant_id | VARCHAR(50) (UK) | Business key |
| name | VARCHAR(255) | Merchant name |
| street | VARCHAR(255) | Street address |
| state | VARCHAR(100) | State |
| city | VARCHAR(100) | City |
| country | VARCHAR(100) | Country |
| contact_number | VARCHAR(50) | Contact number |
| creation_date | TIMESTAMP | Merchant registration date |

**Source**: Enterprise Department - merchant_data.html

---

### dim_user_job
**Type**: Outrigger Dimension (linked to dim_user)

| Column | Type | Description |
|--------|------|-------------|
| user_job_sk | SERIAL (PK) | Surrogate key |
| user_id | VARCHAR(50) (FK) | References dim_user.user_id |
| job_title | VARCHAR(255) | Job title |
| job_level | VARCHAR(50) | Job level |

**Source**: Customer Management Department - user_job.csv

---

### dim_credit_card
**Type**: Outrigger Dimension (linked to dim_user)

| Column | Type | Description |
|--------|------|-------------|
| credit_card_sk | SERIAL (PK) | Surrogate key |
| user_id | VARCHAR(50) (FK) | References dim_user.user_id |
| name | VARCHAR(255) | Cardholder name |
| credit_card_number | VARCHAR(50) | Card number |
| issuing_bank | VARCHAR(255) | Bank name |

**Source**: Customer Management Department - user_credit_card.pickle

---

## Fact Tables

### fact_orders
**Type**: Transaction Fact Table

| Column | Type | Description |
|--------|------|-------------|
| order_id | TEXT | Order identifier (business key from source files) |
| user_sk | INTEGER (FK) | References dim_user |
| merchant_sk | INTEGER (FK) | References dim_merchant |
| staff_sk | INTEGER (FK) | References dim_staff |
| transaction_date_sk | INTEGER (FK) | References dim_date |
| estimated_arrival_days | INTEGER | Estimated delivery days |
| delay_days | INTEGER | Actual delay days |

**Primary Key**: order_id (unique constraint)

**Grain**: One row per order

**Source**: Operations Department - order_data files + order_with_merchant_data + order_delays

---

### fact_line_items
**Type**: Transaction Fact Table

| Column | Type | Description |
|--------|------|-------------|
| order_id | TEXT | Order identifier |
| product_sk | INTEGER (FK) | References dim_product |
| user_sk | INTEGER (FK) | References dim_user |
| merchant_sk | INTEGER (FK) | References dim_merchant |
| staff_sk | INTEGER (FK) | References dim_staff |
| transaction_date_sk | INTEGER (FK) | References dim_date |
| price | DECIMAL(10,2) | Line item price |
| quantity | INTEGER | Quantity ordered |

**Primary Key**: (order_id, product_sk) - unique constraint

**Grain**: One row per line item

**Source**: Operations Department - line_item_data files (prices + products)

---

### fact_campaign_transactions
**Type**: Snapshot Fact Table

| Column | Type | Description |
|--------|------|-------------|
| order_id | TEXT | Order identifier |
| campaign_sk | INTEGER (FK) | References dim_campaign |
| user_sk | INTEGER (FK) | References dim_user |
| merchant_sk | INTEGER (FK) | References dim_merchant |
| transaction_date_sk | INTEGER (FK) | References dim_date |
| availed | INTEGER | Campaign availed flag (0/1) |

**Primary Key**: (campaign_sk, user_sk, merchant_sk, transaction_date_sk)

**Grain**: One row per campaign transaction

**Source**: Marketing Department - transactional_campaign_data.csv

---

## Relationships

```
fact_orders
  ├── dim_user (user_sk)
  ├── dim_merchant (merchant_sk)
  ├── dim_staff (staff_sk)
  └── dim_date (transaction_date_sk)

fact_line_items
  ├── dim_product (product_sk)
  ├── dim_user (user_sk)
  ├── dim_merchant (merchant_sk)
  ├── dim_staff (staff_sk)
  └── dim_date (transaction_date_sk)

fact_campaign_transactions
  ├── dim_campaign (campaign_sk)
  ├── dim_user (user_sk)
  ├── dim_merchant (merchant_sk)
  └── dim_date (transaction_date_sk)

dim_user
  ├── dim_user_job (user_id)
  └── dim_credit_card (user_id)
```

## Surrogate Key Strategy

All dimensions use **auto-incrementing surrogate keys** (SERIAL) to:
- Handle slowly changing dimensions (SCD Type 1)
- Ensure referential integrity
- Support historical tracking

## Data Quality Constraints

- **Referential Integrity**: All foreign keys must reference valid dimension keys
- **Not Null**: Critical fact table columns are NOT NULL
- **Unique Constraints**: Business keys are unique in dimensions
- **Data Type Validation**: Automated checks for valid ranges (e.g., prices > 0)
- **Reject Tables**: Invalid data captured in reject tables (reject_fact_orders, reject_fact_line_items, reject_fact_campaign_transactions)
  - Stores error reasons and raw data in JSONB format
  - Prevents silent data corruption

