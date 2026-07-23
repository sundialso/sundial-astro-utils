"""Tests for the end-of-pipeline notification trigger."""
from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from unittest import mock

from airflow.exceptions import AirflowSkipException
from airflow.utils.types import DagRunType

from sundial_airflow import notify


def _context(
    run_type: DagRunType,
    *,
    execution_ts: str | None = "2026-07-20T00:00:00",
    backfill_mode: str = "none",
) -> dict[str, object]:
    return {
        "dag_run": mock.Mock(run_type=run_type),
        "params": {"execution_ts": execution_ts, "backfill_mode": backfill_mode},
        "run_id": "scheduled__2026-07-20",
    }

_MODULE = "sundial_airflow.notify"
_ENV = {
    "SUNDIAL_AI_SERVICE_URL": "https://gw.example/",
    "NOTIFICATION_TRIGGER_SECRET": "sekret",
}


class NotifyEndOfPipelineTest(unittest.TestCase):
    def test_skips_when_url_unset(self) -> None:
        with mock.patch.dict(os.environ, {"NOTIFICATION_TRIGGER_SECRET": "sekret"}, clear=True):
            with self.assertRaises(AirflowSkipException):
                notify.notify_end_of_pipeline(tenant="acme", run_date="2026-07-20", dag_id="d")

    def test_skips_when_secret_unset(self) -> None:
        with mock.patch.dict(os.environ, {"SUNDIAL_AI_SERVICE_URL": "https://gw.example"}, clear=True):
            with self.assertRaises(AirflowSkipException):
                notify.notify_end_of_pipeline(tenant="acme", run_date="2026-07-20", dag_id="d")

    def test_rejects_non_https_url(self) -> None:
        env = {**_ENV, "SUNDIAL_AI_SERVICE_URL": "http://gw.example"}
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValueError):
                notify.notify_end_of_pipeline(tenant="acme", run_date="2026-07-20", dag_id="d")

    def test_posts_expected_request_on_happy_path(self) -> None:
        with mock.patch.dict(os.environ, _ENV, clear=True), mock.patch(f"{_MODULE}.requests") as req:
            req.post.return_value = mock.Mock(ok=True, status_code=202)

            notify.notify_end_of_pipeline(
                tenant="acme", run_date="2026-07-20", dag_id="dbt_acme", run_id="manual__2026-07-20"
            )

            req.post.assert_called_once()
            _, kwargs = req.post.call_args
            self.assertEqual(
                req.post.call_args.args[0],
                "https://gw.example/ai/khruangbin/internal/notification-triggers/fire",
            )
            self.assertEqual(kwargs["headers"][notify._SECRET_HEADER], "sekret")
            self.assertEqual(kwargs["timeout"], 120)
            self.assertEqual(
                kwargs["json"],
                {
                    "tenant_slug": "acme",
                    "run_date": "2026-07-20",
                    "dag_id": "dbt_acme",
                    "source": "astro",
                    "run_id": "manual__2026-07-20",
                },
            )

    def test_run_id_defaults_to_none_when_omitted(self) -> None:
        with mock.patch.dict(os.environ, _ENV, clear=True), mock.patch(f"{_MODULE}.requests") as req:
            req.post.return_value = mock.Mock(ok=True, status_code=202)

            notify.notify_end_of_pipeline(tenant="acme", run_date="2026-07-20", dag_id="dbt_acme")

            _, kwargs = req.post.call_args
            self.assertIsNone(kwargs["json"]["run_id"])

    def test_raises_on_non_2xx(self) -> None:
        with mock.patch.dict(os.environ, _ENV, clear=True), mock.patch(f"{_MODULE}.requests") as req:
            response = mock.Mock(ok=False, status_code=500, text="boom")
            response.raise_for_status.side_effect = RuntimeError("500")
            req.post.return_value = response

            with self.assertRaises(RuntimeError):
                notify.notify_end_of_pipeline(tenant="acme", run_date="2026-07-20", dag_id="d")


class NotifyFromContextTest(unittest.TestCase):
    def test_fires_for_scheduled_run(self) -> None:
        with mock.patch(f"{_MODULE}.notify_end_of_pipeline") as fire:
            notify._notify_from_context(
                _context(DagRunType.SCHEDULED), tenant="acme", dag_id="dbt_acme"
            )
            fire.assert_called_once_with(
                tenant="acme",
                run_date="2026-07-20",
                dag_id="dbt_acme",
                run_id="scheduled__2026-07-20",
            )

    def test_skips_backfill_run(self) -> None:
        with mock.patch(f"{_MODULE}.notify_end_of_pipeline") as fire:
            with self.assertRaises(AirflowSkipException):
                notify._notify_from_context(
                    _context(DagRunType.BACKFILL_JOB), tenant="acme", dag_id="dbt_acme"
                )
            fire.assert_not_called()

    def test_fires_for_manual_run(self) -> None:
        with mock.patch(f"{_MODULE}.notify_end_of_pipeline") as fire:
            notify._notify_from_context(
                _context(DagRunType.MANUAL), tenant="acme", dag_id="dbt_acme"
            )
            fire.assert_called_once()

    def test_skips_param_driven_backfill(self) -> None:
        # A backfill_mode=full|partial run is triggered manually (run_type MANUAL)
        # but must still be treated as a backfill and skipped.
        for mode in ("full", "partial"):
            with mock.patch(f"{_MODULE}.notify_end_of_pipeline") as fire:
                with self.assertRaises(AirflowSkipException):
                    notify._notify_from_context(
                        _context(DagRunType.MANUAL, backfill_mode=mode),
                        tenant="acme",
                        dag_id="dbt_acme",
                    )
                fire.assert_not_called()

    def test_run_date_falls_back_to_utc_date_like_dbt(self) -> None:
        # execution_ts defaults to None; notify must mirror dbt's UTC-date fallback
        # (not send "None"), matching the data date the run processed.
        expected = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with mock.patch(f"{_MODULE}.notify_end_of_pipeline") as fire:
            notify._notify_from_context(
                _context(DagRunType.SCHEDULED, execution_ts=None),
                tenant="acme",
                dag_id="dbt_acme",
            )
            self.assertEqual(fire.call_args.kwargs["run_date"], expected)


if __name__ == "__main__":
    unittest.main()
