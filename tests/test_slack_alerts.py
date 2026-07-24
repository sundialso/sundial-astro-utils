"""Tests for the Slack failure alert (worker-side ``one_failed`` task)."""
from __future__ import annotations

import unittest
from unittest import mock

from sundial_airflow import slack_alerts

_MODULE = "sundial_airflow.slack_alerts"


def _ti(task_id: str, state: str) -> mock.Mock:
    return mock.Mock(task_id=task_id, state=state)


def _context(*tis: mock.Mock, run_id: str = "scheduled__2026-07-20") -> dict[str, object]:
    return {"dag_run": mock.Mock(get_task_instances=lambda: list(tis)), "run_id": run_id}


class FormatFailedTasksTest(unittest.TestCase):
    def test_lists_failed_tasks_and_excludes_alert_task(self) -> None:
        ctx = _context(
            _ti("model_a", "failed"),
            _ti("model_b", "success"),
            _ti(slack_alerts.FAILURE_ALERT_TASK_ID, "failed"),
        )
        result = slack_alerts._format_failed_tasks(ctx, "[t]")
        self.assertIn("• `model_a`", result)
        self.assertNotIn("model_b", result)
        self.assertNotIn(slack_alerts.FAILURE_ALERT_TASK_ID, result)

    def test_caps_at_three_and_counts_remainder(self) -> None:
        ctx = _context(*(_ti(f"m{i}", "failed") for i in range(5)))
        result = slack_alerts._format_failed_tasks(ctx, "[t]")
        self.assertEqual(result.count("• `m"), 3)
        self.assertIn("…and 2 more", result)

    def test_falls_back_when_no_dag_run(self) -> None:
        result = slack_alerts._format_failed_tasks({"run_id": "x"}, "[t]")
        self.assertIn("Airflow UI", result)

    def test_falls_back_when_enumeration_raises(self) -> None:
        dag_run = mock.Mock()
        dag_run.get_task_instances.side_effect = RuntimeError("no db access")
        result = slack_alerts._format_failed_tasks(
            {"dag_run": dag_run, "run_id": "x"}, "[t]"
        )
        self.assertIn("Airflow UI", result)

    def test_falls_back_when_only_non_failed(self) -> None:
        ctx = _context(_ti("model_a", "success"))
        result = slack_alerts._format_failed_tasks(ctx, "[t]")
        self.assertIn("Airflow UI", result)


class SendFailureAlertTest(unittest.TestCase):
    def test_sends_message_on_happy_path(self) -> None:
        ctx = _context(_ti("model_a", "failed"))
        with mock.patch(f"{_MODULE}.SlackWebhookHook") as hook:
            slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")

            hook.assert_called_once_with(slack_webhook_conn_id=slack_alerts.SLACK_CONN_ID)
            hook.return_value.send_text.assert_called_once()
            sent = hook.return_value.send_text.call_args.args[0]
            self.assertIn("`acme`", sent)
            self.assertIn("• `model_a`", sent)

    def test_raises_when_send_fails(self) -> None:
        # Passthrough retry so the test doesn't sleep through tenacity backoff, and
        # so a send failure surfaces (the task must go red, not swallow the error).
        ctx = _context(_ti("model_a", "failed"))
        with mock.patch(f"{_MODULE}._send_with_retry", lambda fn: fn), mock.patch(
            f"{_MODULE}.SlackWebhookHook"
        ) as hook:
            hook.return_value.send_text.side_effect = RuntimeError("slack down")
            with self.assertRaises(RuntimeError):
                slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")


class BuildFailureAlertTaskTest(unittest.TestCase):
    def test_task_has_one_failed_trigger_rule(self) -> None:
        from datetime import datetime

        from airflow import DAG
        from airflow.utils.trigger_rule import TriggerRule

        with DAG(dag_id="t", start_date=datetime(2024, 1, 1), schedule=None):
            result = slack_alerts.build_failure_alert_task(tenant="acme", dag_id="t")

        operator = result.operator
        self.assertEqual(operator.task_id, slack_alerts.FAILURE_ALERT_TASK_ID)
        self.assertEqual(operator.trigger_rule, TriggerRule.ONE_FAILED)


if __name__ == "__main__":
    unittest.main()
