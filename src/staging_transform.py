"""Clean, validate, and enrich raw Scaler Retail data into the staging layer."""

import argparse
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import PROJECT_ROOT, create_spark, read_raw_dataset, write_parquet


VALID_ORDER_STATUSES = ("Placed", "Shipped", "Delivered", "Cancelled")
VALID_CLICKSTREAM_EVENTS = ("view", "add_to_cart", "purchase", "search")


def transform(
    spark: SparkSession,
    raw_root: Path,
    staging_root: Path,
    ingest_date: date,
) -> None:
    raw = {
        dataset: read_raw_dataset(spark, raw_root, dataset, ingest_date)
        for dataset in (
            "customers",
            "stores",
            "products",
            "inventory",
            "orders",
            "order_items",
            "payments",
            "clickstream",
        )
    }

    customers = (
        raw["customers"]
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("first_name", F.trim("first_name"))
        .withColumn("last_name", F.trim("last_name"))
        .withColumn("email", F.lower(F.trim("email")))
    )
    products = (
        raw["products"]
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )
    stores = (
        raw["stores"]
        .filter(F.col("store_id").isNotNull())
        .dropDuplicates(["store_id"])
    )

    orders = (
        raw["orders"]
        .filter(
            F.col("total_amount").isNotNull()
            & (F.col("total_amount") >= 0)
            & F.col("order_status").isin(*VALID_ORDER_STATUSES)
            & F.col("order_date").isNotNull()
        )
        .join(
            customers.select("customer_id", F.col("city").alias("customer_city")),
            on="customer_id",
            how="left",
        )
    )

    payments = (
        raw["payments"]
        .filter(F.col("payment_id").isNotNull())
        .dropDuplicates(["payment_id"])
        .alias("payment")
        .join(
            orders.select("payment_id", "total_amount")
            .dropDuplicates(["payment_id"])
            .alias("order"),
            on="payment_id",
            how="left",
        )
        .withColumn(
            "is_failed_payment",
            F.col("payment.amount_paid").isNull()
            | F.col("order.total_amount").isNull()
            | (F.col("payment.amount_paid") != F.col("order.total_amount")),
        )
        .drop("total_amount")
    )

    order_items = (
        raw["order_items"]
        .filter(
            (F.col("quantity") > 0) & F.col("product_id").isNotNull()
        )
        .withColumn(
            "item_total",
            F.col("quantity") * F.col("item_price") - F.col("discount_amount"),
        )
        .join(products.select("product_id", "category"), on="product_id", how="left")
    )

    inventory = (
        raw["inventory"]
        .withColumn(
            "location_type_normalized",
            F.upper(F.trim(F.col("location_type"))),
        )
        .filter(
            (F.col("quantity_available") >= 0)
            & F.col("location_type_normalized").isin("STORE", "WAREHOUSE")
        )
        .withColumn(
            "location_type",
            F.when(F.col("location_type_normalized") == "STORE", "Store").otherwise(
                "Warehouse"
            ),
        )
        .drop("location_type_normalized")
    )

    clickstream = (
        raw["clickstream"]
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .filter(F.col("event_type").isin(*VALID_CLICKSTREAM_EVENTS))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .filter(F.col("event_date").isNotNull())
    )

    datasets: dict[str, DataFrame] = {
        "customers": customers,
        "stores": stores,
        "products": products,
        "inventory": inventory,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "clickstream": clickstream,
    }
    for dataset, dataframe in datasets.items():
        partition_by = ()
        if dataset == "orders":
            partition_by = ("order_date",)
        elif dataset == "clickstream":
            partition_by = ("event_date",)
        write_parquet(dataframe, staging_root / dataset, partition_by)
        print(f"Staged {dataset} -> {staging_root / dataset}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-date", type=date.fromisoformat, required=True)
    parser.add_argument("--raw-dir", type=Path, default=PROJECT_ROOT / "datalake" / "raw")
    parser.add_argument(
        "--staging-dir", type=Path, default=PROJECT_ROOT / "datalake" / "staging"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark("ScalerRetailStaging")
    try:
        transform(spark, args.raw_dir, args.staging_dir, args.ingest_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
