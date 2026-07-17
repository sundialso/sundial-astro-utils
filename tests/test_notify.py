"""Tests for the end-of-pipeline notification trigger."""
from __future__ import annotations

import unittest
from unittest import mock

from airflow.exceptions import AirflowNotFoundException, AirflowSkipException
from airflow.models import Connection

from sundial_airflow import notify

_MODULE = "sundial_airflow.notify"


def _conn(host: str | None = "api.sundial.so", password: str | None = "s3cret", schema: str | None = "https") -> Connection:
    return Connection(conn_id=notify.NOTIFY_CONN_ID, host=host, password=password, schema=schema)


class ResolveBaseUrlTest(unittest.TestCase):
    def test_prepends_scheme_when_missing(self) -> None:
        self.assertEqual(notify._resolve_base_url("api.sundial.so", "https"), "https://api.sundial.so")

    def test_defaults_scheme_to_https(self) -> None:
        self.assertEqual(notify._resolve_base_url("api.sundial.so", None), "https://api.sundial.so")

    def test_keeps_explicit_scheme_and_strips_trailing_slash(self) -> None:
        self.assertEqual(
            notify._resolve_base_url("http://localhost:8000/", "https"), "http://localhost:8000"
        )


class NotifyEndOfPipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        # Exercise the production (Airflow-connection) path, not the DO-NOT-MERGE
        # _TEST_MODE hardcode. Remove once the test scaffolding is stripped.
        patcher = mock.patch(f"{_MODULE}._TEST_MODE", False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_skips_when_connection_missing(self) -> None:
        with mock.patch(f"{_MODULE}.BaseHook") as base_hook:
            base_hook.get_connection.side_effect = AirflowNotFoundException("nope")
            with self.assertRaises(AirflowSkipException):
                notify.notify_end_of_pipeline(tenant="acme", run_date="2026-07-15", dag_id="dbt_acme")

    def test_skips_when_host_or_password_empty(self) -> None:
        for conn in (_conn(host=None), _conn(password=None)):
            with mock.patch(f"{_MODULE}.BaseHook") as base_hook:
                base_hook.get_connection.return_value = conn
                with self.assertRaises(AirflowSkipException):
                    notify.notify_end_of_pipeline(
                        tenant="acme", run_date="2026-07-15", dag_id="dbt_acme"
                    )

    def test_posts_expected_request_on_happy_path(self) -> None:
        with (
            mock.patch(f"{_MODULE}.BaseHook") as base_hook,
            mock.patch(f"{_MODULE}.requests") as req,
        ):
            base_hook.get_connection.return_value = _conn()
            req.post.return_value = mock.Mock(ok=True, status_code=202)

            notify.notify_end_of_pipeline(tenant="acme", run_date="2026-07-15", dag_id="dbt_acme")

            req.post.assert_called_once()
            _, kwargs = req.post.call_args
            self.assertEqual(
                req.post.call_args.args[0],
                "https://api.sundial.so/ai/khruangbin/internal/notification-triggers/fire",
            )
            self.assertEqual(
                kwargs["json"],
                {
                    "tenant_slug": "acme",
                    "run_date": "2026-07-15",
                    "dag_id": "dbt_acme",
                    "source": "astro",
                },
            )
            self.assertEqual(kwargs["headers"], {notify._SECRET_HEADER: "s3cret"})

    def test_raises_on_non_2xx(self) -> None:
        with (
            mock.patch(f"{_MODULE}.BaseHook") as base_hook,
            mock.patch(f"{_MODULE}.requests") as req,
        ):
            base_hook.get_connection.return_value = _conn()
            response = mock.Mock(ok=False, status_code=500, text="boom")
            response.raise_for_status.side_effect = RuntimeError("500")
            req.post.return_value = response

            with self.assertRaises(RuntimeError):
                notify.notify_end_of_pipeline(tenant="acme", run_date="2026-07-15", dag_id="dbt_acme")


if __name__ == "__main__":
    unittest.main()
