#!/usr/bin/env python3
"""Unit tests for source-test discovery (``tests:`` and ``data_tests:`` keys)."""
from __future__ import annotations

import importlib.util
import sys
import tempfile
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


source_discovery = _load_module(
    "sundial_airflow.source_discovery",
    ROOT / "sundial_airflow/source_discovery.py",
)
discover = source_discovery.discover_source_tables_with_tests


def _write_project(sources_yml: str) -> Path:
    project = Path(tempfile.mkdtemp())
    models = project / "models"
    models.mkdir()
    (models / "sources.yml").write_text(sources_yml)
    return project


class DiscoverSourceTablesWithTests(unittest.TestCase):
    def test_data_tests_key_is_discovered(self):
        # dbt 1.8+ spelling — the key tea and other modern tenants use.
        project = _write_project(
            """
version: 2
sources:
- name: TEA_ANDROID
  tables:
  - name: button_clicked_android
    data_tests:
      - source_has_recent_rows:
          arguments:
            timestamp_column: received_at
"""
        )
        self.assertEqual(discover(project), [("TEA_ANDROID", "button_clicked_android")])

    def test_legacy_tests_key_is_discovered(self):
        # Pre-1.8 spelling — still accepted by dbt, so we must still honour it.
        project = _write_project(
            """
version: 2
sources:
- name: SRC
  tables:
  - name: t1
    tests:
      - source_has_recent_rows
"""
        )
        self.assertEqual(discover(project), [("SRC", "t1")])

    def test_column_level_data_tests_is_discovered(self):
        project = _write_project(
            """
version: 2
sources:
- name: SRC
  tables:
  - name: t1
    columns:
    - name: id
      data_tests:
        - unique
        - not_null
"""
        )
        self.assertEqual(discover(project), [("SRC", "t1")])

    def test_table_without_tests_is_ignored(self):
        project = _write_project(
            """
version: 2
sources:
- name: SRC
  tables:
  - name: t1
    columns:
    - name: id
      description: no tests here
"""
        )
        self.assertEqual(discover(project), [])


if __name__ == "__main__":
    unittest.main()
