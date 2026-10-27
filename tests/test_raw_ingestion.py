import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, TimestampType

from src.raw_ingestion import (
    CORRUPT_RECORD_COLUMN,
    RAW_SCHEMAS,
    SOURCE_FILES,
    ingest,
)


class RawIngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("RawIngestionTests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_schemas_include_typed_nullable_columns_and_corrupt_record(self):
        for schema in RAW_SCHEMAS.values():
            self.assertTrue(all(field.nullable for field in schema.fields))
            self.assertEqual(schema.fields[-1].name, CORRUPT_RECORD_COLUMN)

        self.assertIsInstance(
            RAW_SCHEMAS["customers"]["signup_date"].dataType, DateType
        )
        self.assertIsInstance(RAW_SCHEMAS["products"]["price"].dataType, DecimalType)
        self.assertIsInstance(
            RAW_SCHEMAS["orders"]["order_timestamp"].dataType, TimestampType
        )

    def test_ingests_all_files_and_preserves_malformed_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "data"
            output_dir = root / "datalake" / "raw"
            source_dir.mkdir()

            for dataset, filename in SOURCE_FILES.items():
                fields = RAW_SCHEMAS[dataset].fieldNames()
                fields.remove(CORRUPT_RECORD_COLUMN)
                rows = [["" for _ in fields]]
                if dataset == "orders":
                    rows = [
                        ["O-1", "C-1", "Online", "", "App", "2024-08-11",
                         "2024-08-11 03:42:28", "Delivered", "12.50", "PAY-1"],
                        ["O-2", "C-2", "Online", "", "App", "2024-08-11",
                         "2024-08-11 03:42:28", "Placed", "not-money", "PAY-2"],
                    ]
                with (source_dir / filename).open(
                    "w", newline="", encoding="utf-8"
                ) as file:
                    writer = csv.writer(file)
                    writer.writerow(fields)
                    writer.writerows(rows)

            destinations = ingest(self.spark, source_dir, output_dir, date(2024, 8, 11))

            self.assertEqual(len(destinations), len(SOURCE_FILES))
            for dataset in SOURCE_FILES:
                target = output_dir / dataset / "ingest_date=2024-08-11"
                self.assertTrue(target.is_dir())
                self.assertEqual(
                    self.spark.read.parquet(str(target)).count(),
                    2 if dataset == "orders" else 1,
                )

            orders = self.spark.read.parquet(
                str(output_dir / "orders" / "ingest_date=2024-08-11")
            )
            malformed = orders.filter(orders.order_id == "O-2").first()
            self.assertIsNone(malformed.total_amount)
            self.assertIsNotNone(malformed[CORRUPT_RECORD_COLUMN])

            with self.assertRaises(FileExistsError):
                ingest(self.spark, source_dir, output_dir, date(2024, 8, 11))


if __name__ == "__main__":
    unittest.main()
