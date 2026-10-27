# Scaler Retail Data Platform - AI Agent Build Instructions

## System Prompt for AI Agent
You are a Senior Data Engineer. I want you to build an end-to-end data pipeline using PySpark and SQL based on the architecture described in `Lecture 24 - Capstone.pdf` (located in the root directory). 

## Project Environment & Context
*   **IDE:** VS Code
*   **Source Data:** All source CSV files are located in the `./data` directory. 
*   **Target Engine:** PySpark (Local mode for development). We will simulate HDFS and Hive using local file paths (e.g., `./datalake/raw/`, `./datalake/staging/`, `./datalake/analytics/`).
*   **Company:** Scaler Retail Pvt. Ltd. (Multi-channel retail).

Please write modular, well-commented PySpark code (e.g., `src/raw_ingestion.py`, `src/staging_transform.py`, `src/analytics_build.py`, `src/business_insights.py`) to execute the following pipeline.

---

## Phase 1: Raw Layer (Ingestion)
**Goal:** Simulate ingestion of CSV files from `./data` into an immutable raw layer (`./datalake/raw/`).
1. Read the following datasets from `./data`: `customers`, `stores`, `products`, `inventory`, `orders`, `order_items`, `payments`, `clickstream`.
2. Write them to `./datalake/raw/<dataset_name>/ingest_date=YYYY-MM-DD/` without applying any complex transformations.
3. Treat this layer as a direct copy of the source to prevent data loss. Enforce basic schema types (e.g., `TIMESTAMP` for dates/times, `DECIMAL` for currency) but allow nulls to capture everything.

## Phase 2: Staging Layer (Transformation & Validation)
**Goal:** Read from `./datalake/raw/`, clean the data, and write to `./datalake/staging/` as Parquet files with Snappy compression. **Crucial:** Discard invalid records without failing the job.
1. **Customers:** Deduplicate by `customer_id`, lowercase emails, trim names.
2. **Orders:** Filter out rows where `total_amount < 0`. Retain only valid statuses (Placed, Shipped, Delivered, Cancelled). Ensure `order_date` is not null.
3. **Payments:** Deduplicate by `payment_id`. Flag payments where `amount_paid` does not match `orders.total_amount` (via join) as `is_failed_payment`.
4. **Order Items:** Filter `quantity <= 0`. Drop null `product_id`. Derive `item_total = (quantity * item_price) - discount_amount`.
5. **Inventory:** Filter negative stock. Standardize `location_type` to 'Store' or 'Warehouse'.
6. **Clickstream:** Standardize `event_type` to lowercase (view, add_to_cart, purchase, search).
7. **Enrichments:** 
   - Add `customer_city` to Orders staging.
   - Add `category` to Order Items staging.
8. **Optimization:** Partition `orders` and `clickstream` staging tables by date (`order_date` and `event_date`). Write as Parquet/Snappy.

## Phase 3: Analytics (Gold) Layer (Star Schema)
**Goal:** Build a Star Schema optimized for BI queries in `./datalake/analytics/`.
1. **Create Dimensions (with Surrogate Keys - SK):**
   - `dim_customer`: SK `customer_key`, derive `customer_segment` (Low, Medium, High based on total spend).
   - `dim_product`: SK `product_key`, derive `price_band` (Low, Medium, Premium).
   - `dim_store`: SK `store_key`, derive `store_type` tier (Tier 1 vs Tier 2 based on region/city).
   - `dim_date`: SK `date_key` (YYYYMMDD).
2. **Create Fact Table (`fact_sales_analytics`):**
   - **Grain:** One row per order line item.
   - Join Staging Orders, Order Items, and Payments.
   - Replace business keys (`customer_id`, `product_id`, `store_id`) with Surrogate Keys from the dimensions.
   - Calculate metrics: `gross_amount`, `discount_amount`, `net_amount`.
   - Partition the output by `year` and `month` derived from `order_date`. Write as Parquet/Snappy.

## Phase 4: Business Insights
**Goal:** Query the Gold layer to answer business questions and output results to `./datalake/business_output/`.
Write PySpark SQL queries to calculate and export:
1. Top 10 Products by Revenue.
2. Top 10 Customers by Spending.
3. Average Order Value (AOV).
4. Customer Lifetime Value (CLV).
5. Monthly Active Customers.
6. Revenue per Store & Region.

Please begin by generating the PySpark script for Phase 1: Raw Layer.