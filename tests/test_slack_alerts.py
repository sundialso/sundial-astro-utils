"""Tests for the Slack failure alert (worker-side ``all_done`` task)."""
from __future__ import annotations

import unittest
from unittest import mock

from airflow.exceptions import AirflowSkipException

from sundial_airflow import slack_alerts

_MODULE = "sundial_airflow.slack_alerts"
_GET_TASK_STATES = (
    "airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_task_states"
)
_RUN_ID = "scheduled__2026-07-20"


def _states(run_id: str = _RUN_ID, **task_states: str) -> dict[str, dict[str, str]]:
    """Shape the ``get_task_states`` return value: ``{run_id: {task_id: state}}``."""
    return {run_id: dict(task_states)}


class FailedTaskIdsTest(unittest.TestCase):
    def test_lists_only_failed_and_excludes_alert_task(self) -> None:
        with mock.patch(
            _GET_TASK_STATES,
            return_value=_states(
                model_a="failed",
                model_b="success",
                model_c="upstream_failed",
                **{slack_alerts.FAILURE_ALERT_TASK_ID: "running"},
            ),
        ):
            result = slack_alerts._get_failed_task_ids("dbt_acme", _RUN_ID)

        self.assertEqual(result, ["model_a"])

    def test_returns_sorted_failures(self) -> None:
        with mock.patch(
            _GET_TASK_STATES,
            return_value=_states(zeta="failed", alpha="failed", mid="failed"),
        ):
            result = slack_alerts._get_failed_task_ids("dbt_acme", _RUN_ID)

        self.assertEqual(result, ["alpha", "mid", "zeta"])

    def test_empty_when_no_failures(self) -> None:
        with mock.patch(_GET_TASK_STATES, return_value=_states(model_a="success")):
            self.assertEqual(slack_alerts._get_failed_task_ids("dbt_acme", _RUN_ID), [])

    def test_empty_when_run_id_absent(self) -> None:
        with mock.patch(_GET_TASK_STATES, return_value={}):
            self.assertEqual(slack_alerts._get_failed_task_ids("dbt_acme", _RUN_ID), [])


class SendFailureAlertTest(unittest.TestCase):
    def test_sends_message_listing_all_failed_tasks(self) -> None:
        ctx = {"run_id": _RUN_ID}
        with mock.patch(
            _GET_TASK_STATES,
            return_value=_states(model_a="failed", model_b="failed", ok="success"),
        ), mock.patch(f"{_MODULE}.SlackWebhookHook") as hook:
            slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")

            hook.assert_called_once_with(slack_webhook_conn_id=slack_alerts.SLACK_CONN_ID)
            hook.return_value.send_text.assert_called_once()
            sent = hook.return_value.send_text.call_args.args[0]
            self.assertIn("`acme`", sent)
            self.assertIn("Failed Tasks (2)", sent)
            self.assertIn("• `model_a`", sent)
            self.assertIn("• `model_b`", sent)
            self.assertIn(_RUN_ID, sent)

    def test_skips_when_nothing_failed(self) -> None:
        ctx = {"run_id": _RUN_ID}
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackWebhookHook") as hook:
            with self.assertRaises(AirflowSkipException):
                slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")

            hook.assert_not_called()

    def test_raises_when_send_fails(self) -> None:
        # Passthrough retry so the test doesn't sleep through tenacity backoff, and
        # so a send failure surfaces (the task must go red, not swallow the error).
        ctx = {"run_id": _RUN_ID}
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="failed")
        ), mock.patch(f"{_MODULE}._send_with_retry", lambda fn: fn), mock.patch(
            f"{_MODULE}.SlackWebhookHook"
        ) as hook:
            hook.return_value.send_text.side_effect = RuntimeError("slack down")
            with self.assertRaises(RuntimeError):
                slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")


class BuildFailureAlertTaskTest(unittest.TestCase):
    def test_task_has_all_done_trigger_rule(self) -> None:
        from datetime import datetime

        from airflow import DAG
        from airflow.utils.trigger_rule import TriggerRule

        with DAG(dag_id="t", start_date=datetime(2024, 1, 1), schedule=None):
            result = slack_alerts.build_failure_alert_task(tenant="acme", dag_id="t")

        operator = result.operator
        self.assertEqual(operator.task_id, slack_alerts.FAILURE_ALERT_TASK_ID)
        self.assertEqual(operator.trigger_rule, TriggerRule.ALL_DONE)


if __name__ == "__main__":
    unittest.main()
