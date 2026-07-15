#!/usr/bin/env python3
"""Unit tests for the Snowflake warehouse size ladder."""
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


scaling = _load_module(
    "sundial_airflow.warehouse_scaling",
    ROOT / "sundial_airflow/warehouse_scaling.py",
)


class WarehouseScalingTests(unittest.TestCase):
    def test_next_size_steps_up_one(self):
        self.assertEqual(scaling.next_warehouse_size("Medium"), "Large")
        self.assertEqual(scaling.next_warehouse_size("Large"), "X-Large")
        self.assertEqual(scaling.next_warehouse_size("X-Large"), "2X-Large")

    def test_next_size_accepts_enum_and_synonym_spellings(self):
        self.assertEqual(scaling.next_warehouse_size("MEDIUM"), "Large")
        self.assertEqual(scaling.next_warehouse_size("XLARGE"), "2X-Large")
        self.assertEqual(scaling.next_warehouse_size("XXLARGE"), "3X-Large")
        self.assertEqual(scaling.next_warehouse_size("2X-Large"), "3X-Large")

    def test_next_size_none_at_max_or_unknown(self):
        self.assertIsNone(scaling.next_warehouse_size("6X-Large"))
        self.assertIsNone(scaling.next_warehouse_size("bogus"))
        self.assertIsNone(scaling.next_warehouse_size(None))

    def test_esc_doubles_single_quotes(self):
        self.assertEqual(scaling._esc("a'b"), "a''b")


if __name__ == "__main__":
    unittest.main()
