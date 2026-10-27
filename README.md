# Scaler Retail Data Engineering Project

This project implements the Scaler Retail medallion pipeline described in
`ai_agent_instructions.md` and `project_details.pdf`. PySpark runs locally and
uses the `datalake/` directory to simulate the raw, staging, and analytics
storage layers.

## Requirements

- Python 3.10+
- Java 17 or 21
- PySpark 4.1+ (`pip install -r requirements.txt`)

## Run the complete pipeline

From the project root, run all four phases in order:

```powershell
python -m src.run_pipeline
```

The default ingestion date is the current local date. Use a fixed date to
replay a specific source drop:

```powershell
python -m src.run_pipeline --ingest-date 2024-08-11
```

Each phase can also be run on its own:

```powershell
python -m src.raw_ingestion --ingest-date 2024-08-11
python -m src.staging_transform --ingest-date 2024-08-11
python -m src.analytics_build
python -m src.business_insights
```

### Phase 1 — Raw ingestion

Reads `customers.csv`, `stores.csv`, `products.csv`, `inventory.csv`,
`orders.csv`, `order_items.csv`, `payments.csv`, and `clickstream_logs.csv`
(stored as the `clickstream` dataset). Writes typed Parquet with nullable
schemas beneath `datalake/raw/<dataset>/ingest_date=YYYY-MM-DD/`. Malformed
CSV rows are retained in `_corrupt_record` instead of being discarded.
Ingestion refuses to overwrite an existing dataset/date partition; use a new
date or raw root for a replay.

### Phase 2 — Staging

Deduplicates customer, product, store, and payment keys; filters invalid
orders, order items, inventory, and clickstream events; normalizes customer
names/emails, inventory location types, and event types; flags payment amount
mismatches; and enriches orders and order items with customer city and product
category. Outputs Snappy-compressed Parquet under `datalake/staging/`, with
orders partitioned by `order_date` and clickstream partitioned by `event_date`.
Staging datasets are rebuilt on each run.

### Phase 3 — Analytics

Builds `dim_customer`, `dim_product`, `dim_store`, `dim_date`, and the
order-line-grain `fact_sales_analytics` table under `datalake/analytics/`.
Customer spend and product price bands use data-dependent terciles. Stores in
major metro cities (Ahmedabad, Bengaluru/Bangalore, Chennai, Delhi, Hyderabad,
Kolkata, Mumbai, and Pune) are classified Tier 1; other cities are Tier 2.
Dimension keys are generated deterministically from sorted business keys.
The fact table stores gross, discount, and net amounts and is partitioned by
year and month.

### Phase 4 — Business insights

Exports CSV results under `datalake/business_output/` for top products by
revenue, top customers by spending, average order value (AOV), customer
lifetime value (CLV), monthly active customers, and revenue per store/region.

## Paths

The eight input CSVs are expected in `data/`. The source dataset directory is
git-ignored because it contains large local data files. For custom locations,
the runner accepts `--source-dir`, `--raw-dir`, `--staging-dir`,
`--analytics-dir`, and `--business-output-dir`.

## Validate

Run the integration tests from the project root:

```powershell
python -m unittest discover -s tests -v
```
