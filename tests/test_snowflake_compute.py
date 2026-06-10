#!/usr/bin/env python3
"""Tests for Snowflake warehouse resize helpers."""
from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]

_airflow_exceptions = types.ModuleType("airflow.exceptions")


class AirflowSkipException(Exception):
    pass


_airflow_exceptions.AirflowSkipException = AirflowSkipException
sys.modules.setdefault("airflow", types.ModuleType("airflow"))
sys.modules["airflow.exceptions"] = _airflow_exceptions


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


_load_module(
    "sundial_airflow.backfill.manifest_parser",
    ROOT / "sundial_airflow/backfill/manifest_parser.py",
)
run_plan = _load_module(
    "sundial_airflow.chunking.run_plan",
    ROOT / "sundial_airflow/chunking/run_plan.py",
)
snowflake_compute = _load_module(
    "sundial_airflow.chunking.snowflake_compute",
    ROOT / "sundial_airflow/chunking/snowflake_compute.py",
)

SnowflakeComputeConfig = snowflake_compute.SnowflakeComputeConfig
boost_snowflake_warehouse = snowflake_compute.boost_snowflake_warehouse
run_plan_needs_chunked_compute = run_plan.run_plan_needs_chunked_compute


class RunPlanNeedsChunkedComputeTests(unittest.TestCase):
    def test_true_when_mapped_chunks_exist(self) -> None:
        plan = {
            "orders": {"disposition": "chunked", "chunks": [{"chunk_id": "2024-01"}]},
            "users": {"disposition": "single", "chunks": []},
        }
        self.assertTrue(run_plan_needs_chunked_compute(plan))

    def test_false_for_single_or_empty_plan(self) -> None:
        self.assertFalse(run_plan_needs_chunked_compute({}))
        self.assertFalse(
            run_plan_needs_chunked_compute(
                {"orders": {"disposition": "single", "chunks": []}},
            )
        )


class SnowflakeComputeValidationTests(unittest.TestCase):
    def test_rejects_invalid_warehouse_name(self) -> None:
        config = SnowflakeComputeConfig(
            warehouse_name="bad-name",
            chunked_size="LARGE",
        )
        with self.assertRaises(ValueError):
            boost_snowflake_warehouse(
                config=config,
                conn_id="snowflake_default",
                needs_boost=True,
            )

    @patch.object(snowflake_compute, "_run_snowflake")
    def test_boosts_and_returns_original_size(self, mock_run) -> None:
        mock_run.side_effect = [
            None,
            [("DBT_WH", "X-SMALL")],
            None,
            None,
            [("DBT_WH", "LARGE")],
        ]
        config = SnowflakeComputeConfig(
            warehouse_name="DBT_WH",
            chunked_size="LARGE",
        )
        original = boost_snowflake_warehouse(
            config=config,
            conn_id="snowflake_default",
            needs_boost=True,
        )
        self.assertEqual(original, "X-SMALL")
        self.assertEqual(mock_run.call_count, 5)
        self.assertIn("ALTER WAREHOUSE", mock_run.call_args_list[2][0][1])

    @patch.object(snowflake_compute, "_run_snowflake")
    def test_skips_when_no_chunked_compute(self, mock_run) -> None:
        config = SnowflakeComputeConfig(
            warehouse_name="DBT_WH",
            chunked_size="LARGE",
        )
        from airflow.exceptions import AirflowSkipException as _Skip

        with self.assertRaises(_Skip):
            boost_snowflake_warehouse(
                config=config,
                conn_id="snowflake_default",
                needs_boost=False,
            )
        mock_run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
