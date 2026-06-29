#!/usr/bin/env python3
"""Unit tests for chunked run-plan decisions."""
from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from datetime import date, datetime, timedelta
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
    "sundial_airflow.chunking.manifest_parser",
    ROOT / "sundial_airflow/chunking/manifest_parser.py",
)
run_plan = _load_module(
    "sundial_airflow.chunking.run_plan",
    ROOT / "sundial_airflow/chunking/run_plan.py",
)

CHUNKED = manifest_parser.CHUNKED
BackfillModel = manifest_parser.BackfillModel
build_run_plan = run_plan.build_run_plan


def _chunked_model(
    name: str = "orders",
    *,
    first: str = "2020-01-01",
    chunk_months: int = 6,
) -> BackfillModel:
    return BackfillModel(
        node_key=f"model.pkg.{name}",
        name=name,
        kind=CHUNKED,
        first_timestamp=date.fromisoformat(first),
        depends_on=[],
        chunk_months=chunk_months,
    )


class RunPlanTests(unittest.TestCase):
    def test_full_backfill_always_chunks(self) -> None:
        model = _chunked_model()
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={model.name: datetime(2024, 1, 1)},
            backfill_mode="full",
            execution_ts=date(2024, 6, 1),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertGreater(len(plan.chunks), 0)

    def test_no_watermark_chunks_from_anchor(self) -> None:
        model = _chunked_model()
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="none",
            execution_ts=date(2021, 1, 1),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(plan.chunks[0].start, date(2020, 1, 1))

    def test_small_incremental_gap_is_single(self) -> None:
        model = _chunked_model(chunk_months=6)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={model.name: datetime(2024, 3, 1)},
            backfill_mode="none",
            execution_ts=date(2024, 6, 1),
        )
        self.assertEqual(plans[model.name].disposition, "single")

    def test_large_incremental_gap_chunks_from_watermark(self) -> None:
        model = _chunked_model(chunk_months=6)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={model.name: datetime(2020, 6, 1)},
            backfill_mode="none",
            execution_ts=date(2024, 6, 1),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertGreaterEqual(plan.chunks[0].start, date(2020, 6, 1))

    def test_partial_without_window_is_single(self) -> None:
        model = _chunked_model()
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="partial",
            execution_ts=date(2024, 6, 1),
        )
        self.assertEqual(plans[model.name].disposition, "single")

    def test_partial_small_window_is_single(self) -> None:
        model = _chunked_model(chunk_months=6)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="partial",
            execution_ts=date(2024, 6, 1),
            window_start=date(2024, 1, 1),
            window_end=date(2024, 4, 1),
        )
        self.assertEqual(plans[model.name].disposition, "single")

    def test_partial_large_window_chunks(self) -> None:
        model = _chunked_model(chunk_months=6)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="partial",
            execution_ts=date(2024, 6, 1),
            window_start=date(2021, 1, 1),
            window_end=date(2022, 6, 1),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertGreater(len(plan.chunks), 1)
        self.assertGreaterEqual(plan.chunks[0].start, date(2021, 1, 1))
        self.assertLessEqual(plan.chunks[-1].end, date(2022, 6, 1))

    def test_partial_window_accepts_iso_strings(self) -> None:
        """Airflow params arrive as ISO date strings, not date objects."""
        model = BackfillModel(
            node_key="model.picsart_dbt.daily_growth_accounting_web",
            name="daily_growth_accounting_web",
            kind=CHUNKED,
            first_timestamp=date(2022, 1, 1),
            depends_on=[],
            chunk_months=6,
        )
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={model.name: datetime(2026, 6, 8)},
            backfill_mode="partial",
            execution_ts=date(2026, 6, 11),
            window_start="2025-11-01",
            window_end="2026-06-01",
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(len(plan.chunks), 2)

    def test_upper_bound_is_execution_ts_not_capped_at_today(self) -> None:
        """Grid is half-open through execution_ts; last inclusive day is day before."""
        model = _chunked_model(first="2020-01-01", chunk_months=6)
        future_end = date(2099, 1, 1)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="full",
            execution_ts=future_end,
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(plan.chunks[-1].end, future_end - timedelta(days=1))


if __name__ == "__main__":
    unittest.main()
