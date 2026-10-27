"""Build the Gold star schema from the cleaned staging layer."""

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType
from pyspark.sql.window import Window

from src.common import PROJECT_ROOT, create_spark, spark_path, write_parquet


MONEY = DecimalType(18, 2)
TIER_1_CITIES = (
    "ahmedabad",
    "bengaluru",
    "bangalore",
    "chennai",
    "delhi",
    "hyderabad",
    "kolkata",
    "mumbai",
    "pune",
)


def _read_staging(spark: SparkSession, staging_root: Path, dataset: str) -> DataFrame:
    return spark.read.parquet(spark_path(staging_root / dataset))


def _add_surrogate_key(
    dataframe: DataFrame, business_key: str, surrogate_key: str
) -> DataFrame:
    return dataframe.withColumn(
        surrogate_key,
        F.row_number()
        .over(Window.orderBy(F.col(business_key).asc()))
        .cast("long"),
    )


def _build_date_dimension(spark: SparkSession, orders: DataFrame) -> DataFrame:
    date_bounds = orders.agg(
        F.min("order_date").alias("start_date"),
        F.max("order_date").alias("end_date"),
    ).first()
    if date_bounds.start_date is None or date_bounds.end_date is None:
        raise ValueError("Cannot build the date dimension: staged orders are empty.")

    start_date = date_bounds.start_date.isoformat()
    end_date = date_bounds.end_date.isoformat()
    dates = spark.sql(
        "SELECT explode(sequence("
        f"to_date('{start_date}'), to_date('{end_date}'), interval 1 day"
        ")) AS full_date"
    )
    return dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.weekofyear("full_date").alias("week_of_year"),
    )


def build(
    spark: SparkSession, staging_root: Path, analytics_root: Path
) -> None:
    customers = _read_staging(spark, staging_root, "customers")
    products = _read_staging(spark, staging_root, "products")
    stores = _read_staging(spark, staging_root, "stores")
    orders = _read_staging(spark, staging_root, "orders")
    order_items = _read_staging(spark, staging_root, "order_items")
    payments = _read_staging(spark, staging_root, "payments")

    customer_spend = orders.groupBy("customer_id").agg(
        F.sum("total_amount").alias("total_spend")
    )
    customer_dimension = (
        customers.join(customer_spend, on="customer_id", how="left")
        .withColumn("total_spend", F.coalesce("total_spend", F.lit(0).cast(MONEY)))
        .withColumn(
            "_spend_tier",
            F.ntile(3).over(
                Window.orderBy(F.col("total_spend"), F.col("customer_id"))
            ),
        )
        .withColumn(
            "customer_segment",
            F.when(F.col("_spend_tier") == 1, "Low")
            .when(F.col("_spend_tier") == 2, "Medium")
            .otherwise("High"),
        )
        .drop("_spend_tier")
    )
    dim_customer = _add_surrogate_key(
        customer_dimension, "customer_id", "customer_key"
    )

    product_dimension = (
        products.filter(F.col("product_id").isNotNull())
        .withColumn(
            "_price_tier",
            F.ntile(3).over(
                Window.orderBy(F.col("price").asc_nulls_last(), F.col("product_id"))
            ),
        )
        .withColumn(
            "price_band",
            F.when(F.col("price").isNull(), F.lit(None).cast(StringType()))
            .when(F.col("_price_tier") == 1, "Low")
            .when(F.col("_price_tier") == 2, "Medium")
            .otherwise("Premium"),
        )
        .drop("_price_tier")
    )
    dim_product = _add_surrogate_key(product_dimension, "product_id", "product_key")

    store_dimension = (
        stores.filter(F.col("store_id").isNotNull())
        .withColumn(
            "store_type_tier",
            F.when(
                F.lower(F.trim(F.col("city"))).isin(*TIER_1_CITIES), "Tier 1"
            ).otherwise("Tier 2"),
        )
    )
    dim_store = _add_surrogate_key(store_dimension, "store_id", "store_key")
    dim_date = _build_date_dimension(spark, orders)

    order_lines = (
        orders.alias("o")
        .join(
            order_items.alias("i"),
            F.col("o.order_id") == F.col("i.order_id"),
            "inner",
        )
        .select(
            F.col("o.order_id").alias("order_id"),
            F.col("i.order_item_id").alias("order_item_id"),
            F.col("o.customer_id").alias("_customer_id"),
            F.col("i.product_id").alias("_product_id"),
            F.col("o.store_id").alias("_store_id"),
            F.col("o.payment_id").alias("_payment_id"),
            F.col("o.order_date").alias("_order_date"),
            F.col("i.quantity").alias("quantity"),
            F.col("i.item_price").alias("item_price"),
            F.col("i.discount_amount").alias("discount_amount"),
        )
    )

    customer_keys = dim_customer.select(
        F.col("customer_id").alias("_customer_id"), "customer_key"
    )
    product_keys = dim_product.select(
        F.col("product_id").alias("_product_id"), "product_key"
    )
    store_keys = dim_store.select(F.col("store_id").alias("_store_id"), "store_key")
    date_keys = dim_date.select(
        F.col("full_date").alias("_order_date"), "date_key"
    )
    payment_flags = payments.select(
        F.col("payment_id").alias("_payment_id"), "is_failed_payment"
    ).dropDuplicates(["_payment_id"])

    fact = (
        order_lines.join(customer_keys, on="_customer_id", how="left")
        .join(product_keys, on="_product_id", how="left")
        .join(store_keys, on="_store_id", how="left")
        .join(date_keys, on="_order_date", how="inner")
        .join(payment_flags, on="_payment_id", how="left")
        .withColumn(
            "gross_amount",
            (F.col("quantity") * F.col("item_price")).cast(MONEY),
        )
        .withColumn(
            "discount_amount",
            F.coalesce(F.col("discount_amount"), F.lit(0).cast(MONEY)).cast(MONEY),
        )
        .withColumn(
            "net_amount",
            (F.col("gross_amount") - F.col("discount_amount")).cast(MONEY),
        )
        .withColumn("year", F.year("_order_date"))
        .withColumn("month", F.month("_order_date"))
        .select(
            "order_id",
            "order_item_id",
            "date_key",
            "customer_key",
            "product_key",
            "store_key",
            "quantity",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "is_failed_payment",
            "year",
            "month",
        )
    )

    dimensions = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "dim_date": dim_date,
    }
    for name, dataframe in dimensions.items():
        write_parquet(dataframe, analytics_root / name)
        print(f"Built {name} -> {analytics_root / name}")

    write_parquet(
        fact,
        analytics_root / "fact_sales_analytics",
        partition_by=("year", "month"),
    )
    print(f"Built fact_sales_analytics -> {analytics_root / 'fact_sales_analytics'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--staging-dir", type=Path, default=PROJECT_ROOT / "datalake" / "staging"
    )
    parser.add_argument(
        "--analytics-dir", type=Path, default=PROJECT_ROOT / "datalake" / "analytics"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark("ScalerRetailAnalytics")
    try:
        build(spark, args.staging_dir, args.analytics_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
