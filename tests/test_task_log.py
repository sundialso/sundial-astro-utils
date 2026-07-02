#!/usr/bin/env python3
"""Unit tests for task_log formatting."""
from __future__ import annotations

import importlib.util
import logging
import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_task_log():
    if "sundial_airflow" not in sys.modules:
        sys.modules["sundial_airflow"] = types.ModuleType("sundial_airflow")
    spec = importlib.util.spec_from_file_location(
        "sundial_airflow.task_log",
        ROOT / "sundial_airflow/task_log.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["sundial_airflow.task_log"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


task_log = _load_task_log()


class TaskLogTests(unittest.TestCase):
    def test_compact_chunk_ids_truncates_long_lists(self):
        chunks = [{"chunk_id": f"2024-{m:02d}"} for m in range(1, 13)]
        compact = task_log._compact_chunk_ids(chunks)
        self.assertIn("12 total", compact)
        self.assertIn("2024-01", compact)
        self.assertIn("2024-12", compact)

    def test_log_prepare_summary_includes_input_and_plans(self):
        with self.assertLogs(task_log.logger, level="INFO") as captured:
            task_log.log_prepare_dbt_args_summary(
                run_id="scheduled__2026-06-30T13:00:00+00:00",
                params={"select": "", "exclude": ""},
                param_field="target_schema",
                target_value="OPENDOOR_BACKFILL",
                warehouse="snowflake",
                backfill_mode="none",
                run_context="normal",
                full_refresh=False,
                dbt_vars={
                    "execution_ts": "2026-06-30",
                    "run_group_id": "scheduled__2026-06-30T13:00:00+00:00",
                },
                selected_models=None,
                run_plan={
                    "orders": {
                        "disposition": "chunked",
                        "chunks": [
                            {"chunk_id": "2024-01", "start": "a", "end": "b"},
                            {"chunk_id": "2024-07", "start": "c", "end": "d"},
                        ],
                    },
                },
                watermarks={"orders": "2026-06-28 23:59:59"},
            )
        text = "\n".join(captured.output)
        self.assertIn("prepare_dbt_args", text)
        self.assertIn("INPUT", text)
        self.assertIn("backfill_mode:  none", text)
        self.assertIn("CHUNKED RUN PLANS", text)
        self.assertIn("orders", text)
        self.assertIn("run_chunk", text)

    def test_log_prepare_summary_includes_partial_backfill_window(self):
        with self.assertLogs(task_log.logger, level="INFO") as captured:
            task_log.log_prepare_dbt_args_summary(
                run_id="manual__2026",
                params={},
                param_field="target_schema",
                target_value="OPENDOOR_BACKFILL",
                warehouse="snowflake",
                backfill_mode="partial",
                run_context="partial_backfill",
                full_refresh=False,
                dbt_vars={
                    "execution_ts": "2026-06-30",
                    "backfill_start_ts": "2025-09-01",
                    "backfill_end_ts": "2025-10-01",
                },
                selected_models=None,
                start_var="backfill_start_ts",
                end_var="backfill_end_ts",
            )
        text = "\n".join(captured.output)
        self.assertIn("backfill_start_ts: 2025-09-01", text)
        self.assertIn("backfill_end_ts:   2025-10-01", text)

    def test_quiet_sql_hook_loggers_suppresses_hook_info(self):
        captured: list[str] = []
        handler = logging.Handler()
        handler.emit = lambda record: captured.append(record.getMessage())  # type: ignore[method-assign]
        hook_log = logging.getLogger("airflow.providers.common.sql.hooks.sql")
        hook_log.addHandler(handler)
        hook_log.setLevel(logging.INFO)
        try:
            with task_log.quiet_sql_hook_loggers():
                hook_log.info("Running statement: SELECT 1")
            hook_log.info("after quiet")
        finally:
            hook_log.removeHandler(handler)
        self.assertEqual(captured, ["after quiet"])

    def test_log_prepare_summary_truncates_large_run_plans(self):
        run_plan = {
            f"model_{idx}": {
                "disposition": "chunked",
                "chunks": [{"chunk_id": f"2024-{idx:02d}"}],
            }
            for idx in range(20)
        }
        with self.assertLogs(task_log.logger, level="INFO") as captured:
            task_log.log_prepare_dbt_args_summary(
                run_id="manual__2026",
                params={},
                param_field="schema",
                target_value="DBT",
                warehouse="snowflake",
                backfill_mode="full",
                run_context="full_backfill",
                full_refresh=True,
                dbt_vars={"execution_ts": "2026-06-30"},
                selected_models=None,
                run_plan=run_plan,
            )
        text = "\n".join(captured.output)
        self.assertIn("20 model(s)", text)
        self.assertIn("and 17 more model(s)", text)
        self.assertNotIn("model_19", text)

        with self.assertLogs(task_log.logger, level="INFO") as captured:
            task_log.log_dbt_run_result(
                "orders",
                returncode=0,
                stdout="line1\nline2\nOK created",
                stderr="",
                chunk_id="2024-01",
            )
        self.assertEqual(len(captured.output), 1)
        self.assertIn("dbt run OK", captured.output[0])

    def test_log_dbt_run_result_failure_includes_output(self):
        with self.assertLogs(task_log.logger, level="ERROR") as captured:
            task_log.log_dbt_run_result(
                "orders",
                returncode=1,
                stdout="boom",
                stderr="error detail",
            )
        self.assertIn("FAILED", captured.output[0])
        self.assertIn("boom", captured.output[0])


if __name__ == "__main__":
    unittest.main()
