#!/usr/bin/env python3
"""Unit tests for chunked run-plan decisions."""
from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


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
    lag: int = 0,
    lookback: int = 0,
) -> BackfillModel:
    return BackfillModel(
        node_key=f"model.pkg.{name}",
        name=name,
        kind=CHUNKED,
        first_timestamp=date.fromisoformat(first),
        depends_on=[],
        chunk_months=chunk_months,
        lag=lag,
        lookback=lookback,
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
        self.assertEqual(plan.chunks[0].start, _utc(2020, 1, 1))

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
        self.assertGreaterEqual(plan.chunks[0].start, _utc(2020, 6, 1))

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
        self.assertGreaterEqual(plan.chunks[0].start, _utc(2021, 1, 1))
        # Partial end is the supplied end_ts verbatim (start-of-day datetime).
        self.assertEqual(plan.chunks[-1].end, _utc(2022, 6, 1))

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
        model = _chunked_model(first="2020-01-01", chunk_months=6)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="full",
            execution_ts=date(2099, 1, 1),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(plan.chunks[-1].end, _utc(2098, 12, 31, 23, 59, 59))

    def test_full_backfill_end_matches_macro_with_lag(self) -> None:
        model = _chunked_model(first="2020-01-01", chunk_months=6, lag=2)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="full",
            execution_ts=date(2026, 6, 29),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(plan.chunks[-1].end, _utc(2026, 6, 26, 23, 59, 59))

    def test_incremental_chunked_end_matches_macro(self) -> None:
        model = _chunked_model(first="2020-01-01", chunk_months=6, lag=0)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={model.name: datetime(2020, 6, 1)},
            backfill_mode="none",
            execution_ts=date(2026, 6, 29),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(plan.chunks[-1].end, _utc(2026, 6, 28, 23, 59, 59))

    def test_chunks_are_non_overlapping_interior_ends_minus_one_second(self) -> None:
        """Interior chunk end = next chunk start - 1s (no boundary double-count)."""
        model = _chunked_model(first="2024-01-01", chunk_months=6, lag=0)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="full",
            execution_ts=date(2025, 6, 1),
        )
        chunks = plans[model.name].chunks
        self.assertEqual(chunks[0].start, _utc(2024, 1, 1, 0, 0, 0))
        self.assertEqual(chunks[0].end, _utc(2024, 6, 30, 23, 59, 59))
        self.assertEqual(chunks[1].start, _utc(2024, 7, 1, 0, 0, 0))
        for earlier, later in zip(chunks, chunks[1:]):
            self.assertEqual(later.start - earlier.end, timedelta(seconds=1))

    def test_incremental_first_chunk_resumes_at_watermark_plus_one_second(self) -> None:
        """First chunk start = watermark + 1s (mirrors start_ts() with lookback=0)."""
        model = _chunked_model(first="2020-01-01", chunk_months=6, lookback=0)
        wm = datetime(2024, 3, 15, 9, 30, 0)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={model.name: wm},
            backfill_mode="none",
            execution_ts=date(2026, 6, 29),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(plan.chunks[0].start, _utc(2024, 3, 15, 9, 30, 1))

    def test_incremental_first_chunk_applies_lookback(self) -> None:
        """Lookback backs the first chunk start off by N days, floored at anchor."""
        model = _chunked_model(first="2020-01-01", chunk_months=6, lookback=3)
        wm = datetime(2024, 3, 15, 0, 0, 0)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={model.name: wm},
            backfill_mode="none",
            execution_ts=date(2026, 6, 29),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        # 2024-03-15 00:00:00 + 1s - 3 days.
        self.assertEqual(plan.chunks[0].start, _utc(2024, 3, 12, 0, 0, 1))

    def test_incremental_first_chunk_floored_at_anchor(self) -> None:
        """Lookback never pushes the first chunk start before the anchor."""
        model = _chunked_model(first="2024-01-01", chunk_months=6, lookback=400)
        wm = datetime(2024, 3, 15, 0, 0, 0)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={model.name: wm},
            backfill_mode="none",
            execution_ts=date(2026, 6, 29),
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(plan.chunks[0].start, _utc(2024, 1, 1, 0, 0, 0))

    def test_as_datetime_preserves_timezone_offset(self) -> None:
        parsed = run_plan._as_datetime("2024-03-15T09:30:00+05:30")
        self.assertEqual(parsed, _utc(2024, 3, 15, 4, 0, 0))

    def test_resume_start_compares_aware_watermark_and_anchor(self) -> None:
        wm = datetime(2024, 3, 15, 9, 30, 0, tzinfo=UTC)
        resume = run_plan._resume_start(wm, date(2024, 1, 1), lookback=0)
        self.assertEqual(resume, _utc(2024, 3, 15, 9, 30, 1))
        self.assertIsNotNone(resume.tzinfo)

    def test_partial_window_compares_aware_iso_strings(self) -> None:
        model = _chunked_model(first="2022-01-01", chunk_months=6)
        plans = build_run_plan(
            models={model.node_key: model},
            watermarks={},
            backfill_mode="partial",
            execution_ts=date(2026, 6, 11),
            window_start="2025-11-01T00:00:00+00:00",
            window_end="2026-06-01T23:59:59+00:00",
        )
        plan = plans[model.name]
        self.assertEqual(plan.disposition, "chunked")
        self.assertEqual(plan.chunks[-1].end, _utc(2026, 6, 1, 23, 59, 59))


if __name__ == "__main__":
    unittest.main()
