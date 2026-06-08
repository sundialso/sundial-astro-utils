#!/usr/bin/env python3
"""Unit tests for chunk expand-kwargs helpers."""
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


chunk_spec = _load_module(
    "sundial_airflow.chunking.chunk_spec",
    ROOT / "sundial_airflow/chunking/chunk_spec.py",
)


class ChunkKwargsTests(unittest.TestCase):
    def test_as_timestamp_adds_midnight(self):
        self.assertEqual(chunk_spec.as_timestamp("2024-01-01"), "2024-01-01T00:00:00")

    def test_expand_kwargs_include_timestamps(self):
        out = chunk_spec.chunk_expand_kwargs(
            [{"chunk_id": "2024-01", "start": "2024-01-01", "end": "2024-07-01"}]
        )
        self.assertEqual(out[0]["chunk_id"], "2024-01")
        self.assertEqual(out[0]["chunk_start"], "2024-01-01T00:00:00")
        self.assertEqual(out[0]["chunk_end"], "2024-07-01T00:00:00")


if __name__ == "__main__":
    unittest.main()
