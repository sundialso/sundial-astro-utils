#!/usr/bin/env python3
"""Unit tests for canonical run windows and lossless subdivision."""
from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from datetime import date, datetime
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


window = _load_module(
    "sundial_airflow.chunking.window",
    ROOT / "sundial_airflow/chunking/window.py",
)


class WindowTests(unittest.TestCase):
    def test_end_ts_from_execution_subtracts_one_second(self) -> None:
        end = window.end_ts_from_execution("2024-04-01")
        self.assertEqual(end, datetime(2024, 3, 31, 23, 59, 59))

    def test_end_ts_from_execution_applies_lag_days(self) -> None:
        end = window.end_ts_from_execution("2024-04-01", lag=2)
        self.assertEqual(end, datetime(2024, 3, 29, 23, 59, 59))

    def test_end_ts_from_execution_preserves_time(self) -> None:
        end = window.end_ts_from_execution("2024-04-01T12:00:00")
        self.assertEqual(end, datetime(2024, 4, 1, 11, 59, 59))

    def test_incremental_window_defers_both_bounds(self) -> None:
        run = window.incremental_window(execution_ts=date(2024, 4, 1))
        self.assertTrue(run.defer_start)
        self.assertTrue(run.defer_end)
        self.assertEqual(run.end, datetime(2024, 3, 31, 23, 59, 59))

    def test_subdivide_incremental_months(self) -> None:
        run = window.incremental_window(execution_ts=date(2024, 4, 1))
        chunks = window.subdivide_window(
            anchor=date(2024, 1, 1),
            chunk_months=1,
            window=run,
            clip_start=date(2024, 1, 1),
        )
        self.assertEqual(len(chunks), 3)
        self.assertTrue(chunks[0].omit_start_override)
        self.assertEqual(chunks[0].end_ts, "2024-01-31T23:59:59")
        self.assertTrue(chunks[-1].omit_end_override)
        self.assertIsNone(chunks[-1].end_ts)

    def test_subdivide_incremental_respects_lag_cap(self) -> None:
        run = window.incremental_window(execution_ts=date(2024, 4, 1), lag=2)
        chunks = window.subdivide_window(
            anchor=date(2024, 1, 1),
            chunk_months=1,
            window=run,
            clip_start=date(2024, 1, 1),
        )
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[1].end_ts, "2024-02-29T23:59:59")
        self.assertEqual(chunks[2].end, date(2024, 3, 29))
        self.assertIsNone(chunks[2].end_ts)
        self.assertTrue(chunks[-1].omit_end_override)

    def test_subdivide_partial_exact_end(self) -> None:
        run = window.partial_backfill_window(
            partial_start="2024-01-15",
            partial_end="2024-04-01",
        )
        chunks = window.subdivide_window(
            anchor=date(2024, 1, 1),
            chunk_months=1,
            window=run,
            clip_start=date(2024, 1, 15),
        )
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0].start_ts, "2024-01-15")
        self.assertEqual(chunks[-1].end_ts, "2024-04-01")


if __name__ == "__main__":
    unittest.main()
