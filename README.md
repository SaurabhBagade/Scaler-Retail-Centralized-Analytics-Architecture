# Scaler Retail Data Engineering Project

This project starts the Scaler Retail medallion pipeline with **Phase 1: raw
ingestion**, as requested in `ai_agent_instructions.md`. Source CSV files are
copied without business filtering to date-stamped Parquet paths under
`datalake/raw/`. The explicit nullable schemas type dates, timestamps, integer
counts, and currency values; malformed CSV rows are retained in the
`_corrupt_record` column rather than discarded.

## Requirements

- Python 3.10+
- Java 17+
- PySpark 4.x (`pip install -r requirements.txt`)

## Run

From the project root:

```powershell
python src\raw_ingestion.py
```

By default the job reads `data\` and writes to `datalake\raw\`, using the
current local date for the ingestion partition. Override these values with:

```powershell
python src\raw_ingestion.py --source-dir C:\path\to\data --output-dir C:\path\to\raw --ingest-date 2024-08-11
```

The eight input files are `customers.csv`, `stores.csv`, `products.csv`,
`inventory.csv`, `orders.csv`, `order_items.csv`, `payments.csv`, and
`clickstream_logs.csv` (stored as the `clickstream` dataset). Output paths are
`<raw-root>\<dataset>\ingest_date=YYYY-MM-DD\`.

Ingestion refuses to overwrite an existing dataset/date partition so raw
loads remain immutable. To replay a source drop, choose a new ingestion date
or a separate raw root. Ensure the source files and target paths are available
before starting; the job reports missing inputs and existing partitions as
errors.

## Validate

Run the focused integration tests from the project root:

```powershell
python -m unittest discover -s tests -v
```

## Pipeline status

- [x] Phase 1 — Raw ingestion
- [ ] Phase 2 — Staging and validation
- [ ] Phase 3 — Analytics star schema
- [ ] Phase 4 — Business insights
