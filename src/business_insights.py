"""Calculate and export SQL-based business insights from the Gold layer."""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from src.common import PROJECT_ROOT, create_spark, spark_path


INSIGHT_QUERIES = {
    "top_10_products_by_revenue": """
        SELECT p.product_key, p.product_name, p.category,
               SUM(f.net_amount) AS revenue
        FROM fact_sales_analytics f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_key, p.product_name, p.category
        ORDER BY revenue DESC
        LIMIT 10
    """,
    "top_10_customers_by_spending": """
        SELECT c.customer_key, c.first_name, c.last_name,
               SUM(f.net_amount) AS total_spending
        FROM fact_sales_analytics f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_key, c.first_name, c.last_name
        ORDER BY total_spending DESC
        LIMIT 10
    """,
    "average_order_value": """
        SELECT ROUND(SUM(net_amount) / COUNT(DISTINCT order_id), 2) AS average_order_value
        FROM fact_sales_analytics
    """,
    "customer_lifetime_value": """
        SELECT c.customer_key, c.first_name, c.last_name,
               COUNT(DISTINCT f.order_id) AS lifetime_orders,
               SUM(f.net_amount) AS lifetime_value
        FROM fact_sales_analytics f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_key, c.first_name, c.last_name
        ORDER BY lifetime_value DESC
    """,
    "monthly_active_customers": """
        SELECT d.year, d.month, COUNT(DISTINCT f.customer_key) AS active_customers
        FROM fact_sales_analytics f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
    """,
    "revenue_per_store_region": """
        SELECT COALESCE(s.store_name, 'Online / Unknown') AS store_name,
               COALESCE(s.region, 'Unknown') AS region,
               SUM(f.net_amount) AS revenue
        FROM fact_sales_analytics f
        LEFT JOIN dim_store s ON f.store_key = s.store_key
        GROUP BY COALESCE(s.store_name, 'Online / Unknown'),
                 COALESCE(s.region, 'Unknown')
        ORDER BY revenue DESC
    """,
}


def export_insights(
    spark: SparkSession, analytics_root: Path, output_root: Path
) -> None:
    table_names = (
        "fact_sales_analytics",
        "dim_product",
        "dim_customer",
        "dim_store",
        "dim_date",
    )
    for table in table_names:
        spark.read.parquet(spark_path(analytics_root / table)).createOrReplaceTempView(
            table
        )

    for name, query in INSIGHT_QUERIES.items():
        result = spark.sql(query)
        destination = output_root / name
        (
            result.write.mode("overwrite")
            .option("header", "true")
            .csv(spark_path(destination))
        )
        print(f"Exported {name} -> {destination}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--analytics-dir", type=Path, default=PROJECT_ROOT / "datalake" / "analytics"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "datalake" / "business_output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark("ScalerRetailBusinessInsights")
    try:
        export_insights(spark, args.analytics_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
