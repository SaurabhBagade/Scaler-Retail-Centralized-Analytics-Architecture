"""Ingest source CSV files into an immutable, typed Parquet raw layer."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DataType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CORRUPT_RECORD_COLUMN = "_corrupt_record"
MONEY = DecimalType(18, 2)


def _schema(*fields: tuple[str, DataType]) -> StructType:
    return StructType(
        [StructField(name, data_type, nullable=True) for name, data_type in fields]
        + [StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True)]
    )


RAW_SCHEMAS = {
    "customers": _schema(
        ("customer_id", StringType()),
        ("first_name", StringType()),
        ("last_name", StringType()),
        ("email", StringType()),
        ("phone_number", StringType()),
        ("city", StringType()),
        ("state", StringType()),
        ("signup_date", DateType()),
        ("customer_type", StringType()),
        ("is_active", StringType()),
    ),
    "stores": _schema(
        ("store_id", StringType()),
        ("store_name", StringType()),
        ("city", StringType()),
        ("state", StringType()),
        ("region", StringType()),
        ("opening_date", DateType()),
        ("store_type", StringType()),
    ),
    "products": _schema(
        ("product_id", StringType()),
        ("product_name", StringType()),
        ("category", StringType()),
        ("sub_category", StringType()),
        ("brand", StringType()),
        ("price", MONEY),
        ("cost_price", MONEY),
        ("launch_date", DateType()),
        ("is_active", StringType()),
    ),
    "inventory": _schema(
        ("inventory_id", StringType()),
        ("product_id", StringType()),
        ("location_type", StringType()),
        ("location_id", StringType()),
        ("quantity_available", IntegerType()),
        ("reorder_level", IntegerType()),
        ("last_updated", TimestampType()),
    ),
    "orders": _schema(
        ("order_id", StringType()),
        ("customer_id", StringType()),
        ("order_channel", StringType()),
        ("store_id", StringType()),
        ("order_source", StringType()),
        ("order_date", DateType()),
        ("order_timestamp", TimestampType()),
        ("order_status", StringType()),
        ("total_amount", MONEY),
        ("payment_id", StringType()),
    ),
    "order_items": _schema(
        ("order_item_id", StringType()),
        ("order_id", StringType()),
        ("product_id", StringType()),
        ("quantity", IntegerType()),
        ("item_price", MONEY),
        ("discount_amount", MONEY),
        ("line_total", MONEY),
    ),
    "payments": _schema(
        ("payment_id", StringType()),
        ("order_id", StringType()),
        ("payment_method", StringType()),
        ("payment_status", StringType()),
        ("failure_reason", StringType()),
        ("payment_timestamp", TimestampType()),
        ("amount_paid", MONEY),
    ),
    "clickstream": _schema(
        ("event_id", StringType()),
        ("customer_id", StringType()),
        ("session_id", StringType()),
        ("event_type", StringType()),
        ("product_id", StringType()),
        ("event_timestamp", TimestampType()),
        ("device_type", StringType()),
        ("location_city", StringType()),
        ("page_url", StringType()),
        ("referrer_source", StringType()),
    ),
}

SOURCE_FILES = {
    "customers": "customers.csv",
    "stores": "stores.csv",
    "products": "products.csv",
    "inventory": "inventory.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "payments": "payments.csv",
    "clickstream": "clickstream_logs.csv",
}


def _spark_path(path: Path) -> str:
    """Return a file URI accepted by Hadoop on Windows and other platforms."""
    return path.resolve().as_uri()


def read_source(spark: SparkSession, source_file: Path, dataset: str) -> DataFrame:
    return (
        spark.read.schema(RAW_SCHEMAS[dataset])
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(_spark_path(source_file))
    )


def ingest(
    spark: SparkSession,
    source_dir: Path,
    output_dir: Path,
    ingest_date: date,
) -> list[Path]:
    """Write all source files without filtering into date-stamped Parquet paths."""
    source_paths = {
        dataset: source_dir / filename
        for dataset, filename in SOURCE_FILES.items()
    }
    missing = [path for path in source_paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Required source CSV file(s) are missing: "
            + ", ".join(str(path) for path in missing)
        )

    destinations = {
        dataset: output_dir / dataset / f"ingest_date={ingest_date.isoformat()}"
        for dataset in SOURCE_FILES
    }
    existing = [path for path in destinations.values() if path.exists()]
    if existing:
        raise FileExistsError(
            "Raw ingestion is immutable; refusing to overwrite existing path(s): "
            + ", ".join(str(path) for path in existing)
        )

    for dataset, source_file in source_paths.items():
        destination = destinations[dataset]
        read_source(spark, source_file, dataset).write.mode("errorifexists").parquet(
            _spark_path(destination)
        )
        print(f"Ingested {source_file.name} -> {destination}")

    return list(destinations.values())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest source CSV files into the immutable raw Parquet layer."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=PROJECT_ROOT / "data",
        help="Directory containing the source CSV files (default: ./data).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "datalake" / "raw",
        help="Raw-layer root directory (default: ./datalake/raw).",
    )
    parser.add_argument(
        "--ingest-date",
        type=date.fromisoformat,
        default=date.today(),
        metavar="YYYY-MM-DD",
        help="Partition date; defaults to the current local date.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("ScalerRetailRawIngestion")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        ingest(spark, args.source_dir, args.output_dir, args.ingest_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
