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

    def test_as_timestamp_end_adds_last_second(self):
        self.assertEqual(chunk_spec.as_timestamp_end("2024-01-31"), "2024-01-31T23:59:59")

    def test_expand_kwargs_include_timestamps(self):
        out = chunk_spec.chunk_expand_kwargs(
            [
                {
                    "chunk_id": "2024-01",
                    "start": "2024-01-01",
                    "end": "2024-01-31",
                    "omit_start_override": False,
                    "omit_end_override": False,
                }
            ]
        )
        self.assertEqual(out[0]["chunk_id"], "2024-01")
        self.assertEqual(out[0]["chunk_start"], "2024-01-01T00:00:00")
        self.assertEqual(out[0]["chunk_end"], "2024-01-31T23:59:59")

    def test_expand_kwargs_omit_macro_bounds(self):
        out = chunk_spec.chunk_expand_kwargs(
            [
                {
                    "chunk_id": "2024-03",
                    "start": "2024-03-01",
                    "end": "2024-03-31",
                    "omit_start_override": True,
                    "omit_end_override": True,
                }
            ]
        )
        self.assertEqual(out[0], {"chunk_id": "2024-03"})

    def test_expand_kwargs_partial_exact_bounds(self):
        out = chunk_spec.chunk_expand_kwargs(
            [
                {
                    "chunk_id": "2024-01",
                    "start": "2024-01-15",
                    "end": "2024-01-31",
                    "omit_start_override": False,
                    "omit_end_override": False,
                    "start_ts": "2024-01-15",
                    "end_ts": None,
                },
                {
                    "chunk_id": "2024-03",
                    "start": "2024-03-01",
                    "end": "2024-04-01",
                    "omit_start_override": False,
                    "omit_end_override": False,
                    "start_ts": None,
                    "end_ts": "2024-04-01",
                },
            ]
        )
        self.assertEqual(out[0]["chunk_start"], "2024-01-15T00:00:00")
        self.assertEqual(out[1]["chunk_end"], "2024-04-01T00:00:00")

    def test_run_expand_kwargs_single_returns_empty(self):
        self.assertEqual(
            chunk_spec.run_expand_kwargs({"disposition": "single", "chunks": []}),
            [],
        )

    def test_build_chunk_units_for_all_models(self):
        units = chunk_spec.build_chunk_units(
            {
                "m_a": {
                    "disposition": "chunked",
                    "chunks": [
                        {
                            "chunk_id": "2024-01",
                            "start": "2024-01-01",
                            "end": "2024-01-31",
                            "omit_start_override": False,
                            "omit_end_override": True,
                        },
                    ],
                },
                "m_b": {"disposition": "single", "chunks": []},
            }
        )
        self.assertEqual(len(units["m_a"]), 1)
        self.assertEqual(units["m_a"][0]["chunk_id"], "2024-01")
        self.assertNotIn("chunk_end", units["m_a"][0])
        self.assertEqual(units["m_b"], [])


if __name__ == "__main__":
    unittest.main()
