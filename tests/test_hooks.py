#!/usr/bin/env python3
"""Unit tests for chunked DAG skip hooks."""
from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

from airflow.exceptions import AirflowSkipException

ROOT = Path(__file__).resolve().parents[1]


def _load_hooks():
    if "sundial_airflow" not in sys.modules:
        sys.modules["sundial_airflow"] = types.ModuleType("sundial_airflow")
    spec = importlib.util.spec_from_file_location(
        "sundial_airflow.hooks",
        ROOT / "sundial_airflow/hooks.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["sundial_airflow.hooks"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


hooks = _load_hooks()
skip_chunked_precreate = hooks.skip_chunked_precreate


class SkipChunkedPrecreateTests(unittest.TestCase):
    def _context(self, *, full_refresh: bool, disposition: str):
        return {
            "params": {},
            "ti": types.SimpleNamespace(
                xcom_pull=lambda task_ids: {
                    "full_refresh": full_refresh,
                    "run_plan": {
                        "orders": {"disposition": disposition},
                    },
                }
            ),
        }

    def test_skips_when_not_full_refresh(self):
        with self.assertRaises(AirflowSkipException):
            skip_chunked_precreate(
                self._context(full_refresh=False, disposition="chunked"),
                "orders",
            )

    def test_skips_when_disposition_single(self):
        with self.assertRaises(AirflowSkipException):
            skip_chunked_precreate(
                self._context(full_refresh=True, disposition="single"),
                "orders",
            )

    def test_allows_full_refresh_chunked(self):
        skip_chunked_precreate(
            self._context(full_refresh=True, disposition="chunked"),
            "orders",
        )


if __name__ == "__main__":
    unittest.main()
