"""Tests for deployment feature flags."""
from __future__ import annotations

import importlib.util
import os
import unittest
from unittest import mock

_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "sundial_airflow",
    "feature_flags.py",
)
_spec = importlib.util.spec_from_file_location("_feature_flags_standalone", _PATH)
ff = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ff)


class FeatureFlagsTest(unittest.TestCase):
    def test_chunking_disabled_by_default(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(ff.is_chunking_enabled())
            self.assertEqual(ff.resolve_dag_schedules("0 8 * * *"), (None, "0 8 * * *"))

    def test_chunking_explicitly_disabled(self) -> None:
        with mock.patch.dict(os.environ, {ff.CHUNKING_ENABLED_ENV: "false"}):
            self.assertFalse(ff.is_chunking_enabled())

    def test_chunking_enabled(self) -> None:
        with mock.patch.dict(os.environ, {ff.CHUNKING_ENABLED_ENV: "true"}):
            self.assertTrue(ff.is_chunking_enabled())
            self.assertEqual(ff.resolve_dag_schedules("0 8 * * *"), ("0 8 * * *", None))

    def test_resolve_none_cron(self) -> None:
        self.assertEqual(ff.resolve_dag_schedules(None), (None, None))


if __name__ == "__main__":
    unittest.main()
