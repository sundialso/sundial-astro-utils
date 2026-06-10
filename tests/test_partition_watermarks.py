#!/usr/bin/env python3
"""Unit tests for partition-column watermark helpers."""
from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_module(qualified: str, path: Path):
    pkg_name = ""
    for part in qualified.split("."):
        pkg_name = f"{pkg_name}.{part}" if pkg_name else part
        if pkg_name not in sys.modules:
            sys.modules[pkg_name] = types.ModuleType(pkg_name)
    spec = importlib.util.spec_from_file_location(qualified, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


manifest_parser = _load_module(
    "sundial_airflow.backfill.manifest_parser",
    ROOT / "sundial_airflow/backfill/manifest_parser.py",
)
_load_module(
    "sundial_airflow.warehouses",
    ROOT / "sundial_airflow/warehouses.py",
)
watermarks = _load_module(
    "sundial_airflow.chunking.watermarks",
    ROOT / "sundial_airflow/chunking/watermarks.py",
)

_extract_partition_column = manifest_parser._extract_partition_column
partition_watermark_sql = watermarks.partition_watermark_sql
partition_watermarks_batch_sql = watermarks.partition_watermarks_batch_sql
_WatermarkQuery = watermarks._WatermarkQuery
_parse_watermark = watermarks._parse_watermark


class PartitionColumnExtractionTests(unittest.TestCase):
    def test_extracts_partition_column_from_start_ts(self):
        sql = """
        WHERE daily_cohort_date BETWEEN
          {{ start_ts('daily_cohort_date', 10000, '2024-01-01T00:00:00+00:00') }}
          AND {{ end_ts(0) }}
        """
        self.assertEqual(_extract_partition_column(sql), "daily_cohort_date")

    def test_returns_none_when_start_ts_missing(self):
        self.assertIsNone(_extract_partition_column("SELECT 1"))


class PartitionWatermarkSqlTests(unittest.TestCase):
    def test_builds_max_partition_query(self):
        sql = partition_watermark_sql(
            "OPENDOOR_ASTRO_DB.OPENDOOR_BACKFILL.accounts_created_daily_periodicity_1d",
            "daily_cohort_date",
        )
        self.assertEqual(
            sql,
            "SELECT MAX(daily_cohort_date) "
            "FROM OPENDOOR_ASTRO_DB.OPENDOOR_BACKFILL.accounts_created_daily_periodicity_1d",
        )

    def test_rejects_unsafe_partition_column(self):
        with self.assertRaises(ValueError):
            partition_watermark_sql("db.schema.table", "daily_cohort_date; DROP TABLE x")


class PartitionWatermarksBatchSqlTests(unittest.TestCase):
    def test_builds_union_all_query(self):
        sql = partition_watermarks_batch_sql(
            [
                _WatermarkQuery(
                    "orders",
                    "DB.SCHEMA.orders",
                    "event_ts",
                ),
                _WatermarkQuery(
                    "line_items",
                    "DB.SCHEMA.line_items",
                    "created_at",
                ),
            ]
        )
        self.assertIn(
            "SELECT 'orders' AS model_name, MAX(event_ts) AS watermark FROM DB.SCHEMA.orders",
            sql,
        )
        self.assertIn(
            "SELECT 'line_items' AS model_name, MAX(created_at) AS watermark "
            "FROM DB.SCHEMA.line_items",
            sql,
        )
        self.assertIn("UNION ALL", sql)

    def test_rejects_empty_batch(self):
        with self.assertRaises(ValueError):
            partition_watermarks_batch_sql([])


class ParseWatermarkTests(unittest.TestCase):
    def test_parses_datetime_string(self):
        self.assertEqual(
            _parse_watermark("2024-06-01 00:00:00").isoformat(),
            "2024-06-01T00:00:00",
        )


if __name__ == "__main__":
    unittest.main()
