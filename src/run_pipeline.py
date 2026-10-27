"""Run all four Scaler Retail data pipeline phases in sequence."""

import argparse
from datetime import date
from pathlib import Path

from src.analytics_build import build
from src.business_insights import export_insights
from src.common import PROJECT_ROOT, create_spark
from src.raw_ingestion import ingest
from src.staging_transform import transform


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--source-dir", type=Path, default=PROJECT_ROOT / "data")
    parser.add_argument(
        "--raw-dir", type=Path, default=PROJECT_ROOT / "datalake" / "raw"
    )
    parser.add_argument(
        "--staging-dir", type=Path, default=PROJECT_ROOT / "datalake" / "staging"
    )
    parser.add_argument(
        "--analytics-dir", type=Path, default=PROJECT_ROOT / "datalake" / "analytics"
    )
    parser.add_argument(
        "--business-output-dir",
        type=Path,
        default=PROJECT_ROOT / "datalake" / "business_output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark("ScalerRetailDataPipeline")
    try:
        ingest(spark, args.source_dir, args.raw_dir, args.ingest_date)
        transform(spark, args.raw_dir, args.staging_dir, args.ingest_date)
        build(spark, args.staging_dir, args.analytics_dir)
        export_insights(spark, args.analytics_dir, args.business_output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
