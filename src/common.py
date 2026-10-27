"""Shared Spark and local-path helpers for the pipeline jobs."""

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def spark_path(path: Path) -> str:
    """Return a file URI accepted by Hadoop on Windows and other platforms."""
    return path.resolve().as_uri()


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def read_raw_dataset(
    spark: SparkSession, raw_root: Path, dataset: str, ingest_date: date
) -> DataFrame:
    dataset_root = raw_root / dataset
    partition_path = dataset_root / f"ingest_date={ingest_date.isoformat()}"
    return (
        spark.read.option("basePath", spark_path(dataset_root))
        .parquet(spark_path(partition_path))
    )


def write_parquet(
    dataframe: DataFrame, destination: Path, partition_by: tuple[str, ...] = ()
) -> None:
    writer = dataframe.write.mode("overwrite").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(spark_path(destination))
