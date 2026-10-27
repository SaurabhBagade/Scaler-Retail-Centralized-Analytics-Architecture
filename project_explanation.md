# Scaler Retail Data Platform: End-to-End Omni-Channel Data Engineering Pipeline

## 1. Project Overview & Business Problem
**Client:** Scaler Retail Pvt. Ltd. (A multi-city retail business with both physical stores and online e-commerce).
**The Problem:** The company was generating massive amounts of data across various silos (offline POS, warehouse inventory, online web logs, digital payments). They lacked a single source of truth, making it impossible to perform unified analytics on sales, inventory health, and customer behavior.
**The Solution:** I architected an automated, end-to-end data pipeline using PySpark. The pipeline ingests raw CSV drops, cleans and standardizes the data, and models it into a high-performance Star Schema optimized for Business Intelligence (BI) reporting.

## 2. Architecture & Data Flow (The Medallion Architecture)

### Layer 1: Raw Layer (Bronze)
*   **Purpose:** Immutable storage of source data to prevent data loss and enable historical replays.
*   **What I did:** Read weekly CSV drops from the `./data` folder and wrote them directly to the raw layer (simulating HDFS/Hive external tables). 
*   **Key Concept:** Schema on Read. I applied basic data types (like Timestamps) but avoided strict filtering here so that we always have a perfect audit trail of exactly what the source systems delivered.

### Layer 2: Staging Layer (Silver)
*   **Purpose:** Data cleaning, validation, enrichment, and deduplication.
*   **What I did:** Built PySpark jobs that read from the Raw layer and applied business rules without failing the job on bad records. 
    *   *Validation:* Filtered out negative revenue, dropped invalid order statuses, and standardized text casing.
    *   *Enrichment:* Joined basic dimensions (like adding `customer_city` to the Orders table) to pre-compute data for downstream layers.
    *   *Storage:* Wrote the output as Parquet files with Snappy compression, partitioned large tables (Orders, Clickstream) by date to optimize performance.

### Layer 3: Analytics / Gold Layer (Star Schema)
*   **Purpose:** Business modeling optimized for fast aggregations and BI queries.
*   **What I did:** Transformed the normalized staging data into a Star Schema.
    *   **Dimensions:** Created `dim_customer`, `dim_product`, `dim_store`, and `dim_date`. I generated surrogate keys for each and created derived business categories (e.g., segmenting customers into Low/Medium/High spenders).
    *   **Fact Table (`fact_sales_analytics`):** Set the grain to one row per order line item. I joined staging orders, items, and payments, replaced source IDs with dimension Surrogate Keys, and calculated exact financial metrics (`gross_amount`, `net_amount`).
    *   **Optimization:** Partitioned the fact table by `year` and `month` to drastically reduce data scanning costs for time-based BI reports.

## 3. Key Technical Concepts to Highlight in Interviews

*   **Defensive Spark Programming:** Explain how your PySpark jobs handle bad data (e.g., dropping null keys or negative quantities using `.filter()`) instead of letting the job crash.
*   **Data Modeling (Surrogate Keys):** Explain that surrogate keys (integers generated in the data warehouse) are used instead of natural business keys (like a string `customer_id`) to improve join performance and handle historical changes (SCDs - Slowly Changing Dimensions).
*   **Performance Optimization:** 
    *   *Parquet/Snappy:* Columnar storage means BI tools only read the columns they need, while Snappy compression reduces disk I/O.
    *   *Partitioning:* Partitioning the fact table by Year/Month allows Spark to utilize "Partition Pruning" (skipping entire folders of data if a query only looks for a specific month).

## 4. Business Value Delivered
Using the Gold layer, the pipeline automatically computes essential business metrics and exports them to a `business_output` folder. Metrics include:
*   **AOV (Average Order Value) & CLV (Customer Lifetime Value):** For marketing ROI.
*   **Top Products & Stores by Revenue:** For inventory and regional strategy.
*   **Monthly Active Customers:** For growth tracking.

## 5. Resume Bullet Points
*   **Architected an end-to-end retail data platform** using HDFS, Hive, and Spark to process thousands of daily omni-channel transactions across sales, inventory, and clickstream domains.
*   **Engineered an immutable raw ingestion pipeline** using Hive external tables to ensure zero data loss and enable historical load replays from HDFS archive directories.
*   **Developed resilient PySpark data quality pipelines** (Staging Layer) to dynamically deduplicate records, enforce data types, and filter malformed records without failing execution jobs.
*   **Designed a highly-optimized Star Schema (Gold Layer)** by modeling a centralized sales fact table and multiple dimension tables with surrogate keys, applying year/month partitioning and Parquet/Snappy compression to accelerate BI workloads like Customer Lifetime Value (CLV) and product revenue tracking.