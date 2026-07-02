#!/usr/bin/env python3
"""Unit tests for SQL extraction of lag/lookback/first_timestamp."""
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
    "sundial_airflow.chunking.manifest_parser",
    ROOT / "sundial_airflow/chunking/manifest_parser.py",
)

_extract_lag = manifest_parser._extract_lag
_extract_lookback = manifest_parser._extract_lookback


class ExtractLookbackTests(unittest.TestCase):
    def test_plain_call(self) -> None:
        sql = "{{ start_ts('event_ts', 7, '2020-01-01') }}"
        self.assertEqual(_extract_lookback(sql), 7)

    def test_namespaced_call(self) -> None:
        sql = "{{ sundial_dbt_shared.start_ts('c', 0, '2022-03-01') }}"
        self.assertEqual(_extract_lookback(sql), 0)

    def test_multiple_takes_max(self) -> None:
        sql = (
            "{{ start_ts('a', 3, '2020-01-01') }} "
            "{{ start_ts('b', 10, '2020-01-01') }}"
        )
        self.assertEqual(_extract_lookback(sql), 10)

    def test_absent_defaults_zero(self) -> None:
        self.assertEqual(_extract_lookback("select 1"), 0)


class ExtractLagTests(unittest.TestCase):
    def test_plain_call(self) -> None:
        self.assertEqual(_extract_lag("{{ end_ts(2) }}", "m"), 2)

    def test_namespaced_call(self) -> None:
        self.assertEqual(_extract_lag("{{ sundial_dbt_shared.end_ts(0) }}", "m"), 0)

    def test_multiple_takes_max(self) -> None:
        sql = "{{ end_ts(1) }} ... {{ end_ts(5) }}"
        self.assertEqual(_extract_lag(sql, "m"), 5)

    def test_commented_call_ignored(self) -> None:
        sql = "-- end_ts(99) in a comment\nwhere ts <= {{ end_ts(1) }}"
        self.assertEqual(_extract_lag(sql, "m"), 1)

    def test_non_literal_lag_defaults_zero(self) -> None:
        sql = "{{ end_ts(var('lag')) }}"
        with self.assertLogs(manifest_parser.logger, level="WARNING") as captured:
            self.assertEqual(_extract_lag(sql, "m"), 0)
        self.assertTrue(any("non-literal lag" in msg for msg in captured.output))

    def test_mixed_literal_and_non_literal_defaults_zero(self) -> None:
        sql = "{{ end_ts(2) }} and {{ end_ts(var('lag')) }}"
        with self.assertLogs(manifest_parser.logger, level="WARNING") as captured:
            self.assertEqual(_extract_lag(sql, "mixed_model"), 0)
        self.assertTrue(any("non-literal lag" in msg for msg in captured.output))

    def test_absent_defaults_zero(self) -> None:
        self.assertEqual(_extract_lag("select 1", "m"), 0)


if __name__ == "__main__":
    unittest.main()
