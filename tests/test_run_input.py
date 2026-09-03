"""Tests for shared DAG-run input parsing."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest import mock

from sundial_airflow.run_input import parse_run_input, run_input_from_context


class ParseRunInputTest(unittest.TestCase):
    def test_defaults_to_normal_all_models(self) -> None:
        run = parse_run_input({})
        self.assertEqual(run.backfill_mode, "none")
        self.assertEqual(run.run_context, "normal")
        self.assertIsNone(run.select)
        self.assertEqual(run.models_selector, "all")
        self.assertFalse(run.is_backfill)
        self.assertFalse(run.full_refresh)
        self.assertEqual(run.run_context_tag("acme"), "acme_normal")

    def test_select_and_exclude_are_stripped(self) -> None:
        run = parse_run_input({"select": " A+ ", "exclude": " tag:wip "})
        self.assertEqual(run.select, "A+")
        self.assertEqual(run.exclude, "tag:wip")
        self.assertEqual(run.models_selector, "A+")

    def test_blank_select_is_all(self) -> None:
        run = parse_run_input({"select": "  ", "exclude": None})
        self.assertIsNone(run.select)
        self.assertEqual(run.models_selector, "all")

    def test_partial_backfill_maps_run_context(self) -> None:
        run = parse_run_input(
            {
                "backfill_mode": "partial",
                "start_ts": "2025-01-01T00:00:00",
                "end_ts": "2025-06-30T23:59:59",
            }
        )
        self.assertEqual(run.run_context, "partial_backfill")
        self.assertTrue(run.is_backfill)
        self.assertEqual(run.start_ts, "2025-01-01T00:00:00")
        self.assertEqual(run.end_ts, "2025-06-30T23:59:59")

    def test_full_backfill_is_full_refresh(self) -> None:
        run = parse_run_input({"backfill_mode": "full", "execution_ts": "2026-03-19"})
        self.assertEqual(run.run_context, "full_backfill")
        self.assertTrue(run.full_refresh)
        self.assertEqual(run.execution_ts, "2026-03-19")

    def test_resolved_execution_ts_uses_param(self) -> None:
        run = parse_run_input({"execution_ts": "2026-04-02"})
        self.assertEqual(run.resolved_execution_ts(), "2026-04-02")

    def test_resolved_execution_ts_defaults_to_utc_today(self) -> None:
        run = parse_run_input({})
        frozen = datetime(2026, 9, 3, 15, 0, tzinfo=timezone.utc)
        with mock.patch("sundial_airflow.run_input.datetime") as dt:
            dt.now.return_value = frozen
            self.assertEqual(run.resolved_execution_ts(), "2026-09-03")


class RunInputFromContextTest(unittest.TestCase):
    def test_params_only_when_no_xcom(self) -> None:
        ctx = {"params": {"select": "A+", "backfill_mode": "none"}}
        run = run_input_from_context(ctx)
        self.assertEqual(run.models_selector, "A+")
        self.assertEqual(run.run_context, "normal")

    def test_prepare_xcom_overlays_resolved_window(self) -> None:
        ti = mock.Mock()
        ti.xcom_pull.return_value = {
            "run_context": "normal",
            "vars": {"execution_ts": "2026-04-02"},
        }
        ctx = {
            "params": {"backfill_mode": "none", "execution_ts": None},
            "ti": ti,
        }
        run = run_input_from_context(ctx)
        self.assertEqual(run.execution_ts, "2026-04-02")

    def test_xcom_failure_falls_back_to_params(self) -> None:
        ti = mock.Mock()
        ti.xcom_pull.side_effect = RuntimeError("xcom down")
        ctx = {
            "params": {"backfill_mode": "none", "execution_ts": "2026-05-01"},
            "ti": ti,
        }
        run = run_input_from_context(ctx)
        self.assertEqual(run.execution_ts, "2026-05-01")

    def test_custom_boundary_var_names(self) -> None:
        ti = mock.Mock()
        ti.xcom_pull.return_value = {
            "run_context": "partial_backfill",
            "vars": {
                "custom_start": "2025-01-01",
                "custom_end": "2025-06-30",
                "backfill_start_ts": "ignored",
            },
        }
        ctx = {
            "params": {"backfill_mode": "partial"},
            "ti": ti,
        }
        run = run_input_from_context(ctx, start_var="custom_start", end_var="custom_end")
        self.assertEqual(run.start_ts, "2025-01-01")
        self.assertEqual(run.end_ts, "2025-06-30")


if __name__ == "__main__":
    unittest.main()
