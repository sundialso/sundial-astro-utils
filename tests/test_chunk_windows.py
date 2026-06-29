#!/usr/bin/env python3
"""Unit tests for chunk window generation."""
from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from datetime import date, timedelta
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
chunk_windows_from_anchor = manifest_parser.chunk_windows_from_anchor


class ChunkWindowTests(unittest.TestCase):
    def test_full_range_from_anchor(self) -> None:
        windows = chunk_windows_from_anchor(
            date(2020, 1, 1), 6, date(2020, 1, 1), date(2021, 1, 1),
        )
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][2], "2020-01")

    def test_incremental_subset_starts_mid_history(self) -> None:
        windows = chunk_windows_from_anchor(
            date(2020, 1, 1), 6, date(2020, 7, 1), date(2021, 1, 1),
        )
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0][0], date(2020, 7, 1))

    def test_adjacent_chunks_are_non_overlapping_inclusive_windows(self) -> None:
        """Half-open grid [Jan,Jul) [Jul,Jan) → inclusive [Jan 1,Jun 30] [Jul 1,Dec 31]."""
        windows = chunk_windows_from_anchor(
            date(2020, 1, 1), 6, date(2020, 1, 1), date(2021, 1, 1),
        )
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0], (date(2020, 1, 1), date(2020, 6, 30), "2020-01"))
        self.assertEqual(windows[1], (date(2020, 7, 1), date(2020, 12, 31), "2020-07"))
        self.assertEqual(windows[1][0], windows[0][1] + timedelta(days=1))


if __name__ == "__main__":
    unittest.main()
