"""Tests for Slack failure and success alerts (worker-side ``all_done`` tasks)."""
from __future__ import annotations

import os
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


def _extras(value: str) -> mock._patch_dict:
    return mock.patch.dict(os.environ, {slack_alerts.EXTRA_CHANNELS_ENV_VAR: value})


class ResolveFailureAlertChannelsTest(unittest.TestCase):
    def test_etl_alerts_only_when_no_extras(self) -> None:
        with _extras(""):
            self.assertEqual(
                slack_alerts.resolve_failure_alert_channels(), ["#etl-alerts"]
            )

    def test_extra_is_appended_and_hash_prefixed(self) -> None:
        with _extras("tenant-alerts"):
            self.assertEqual(
                slack_alerts.resolve_failure_alert_channels(),
                ["#etl-alerts", "#tenant-alerts"],
            )

    def test_multiple_extras_keep_hash_and_ids(self) -> None:
        with _extras(" #ops-alerts , C0123456789 "):
            self.assertEqual(
                slack_alerts.resolve_failure_alert_channels(),
                ["#etl-alerts", "#ops-alerts", "C0123456789"],
            )

    def test_extra_naming_etl_alerts_is_not_duplicated(self) -> None:
        with _extras("etl-alerts,#etl-alerts"):
            self.assertEqual(
                slack_alerts.resolve_failure_alert_channels(), ["#etl-alerts"]
            )


class FailedTaskIdsTest(unittest.TestCase):
    def test_lists_only_failed_and_excludes_alert_tasks(self) -> None:
        with mock.patch(
            _GET_TASK_STATES,
            return_value=_states(
                model_a="failed",
                model_b="success",
                model_c="upstream_failed",
                **{
                    slack_alerts.FAILURE_ALERT_TASK_ID: "running",
                    slack_alerts.SUCCESS_ALERT_TASK_ID: "running",
                },
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


def _channels_posted_to(api_hook: mock.Mock) -> list[str]:
    return [call.kwargs["json"]["channel"] for call in api_hook.return_value.call.call_args_list]


class SendFailureAlertTest(unittest.TestCase):
    def test_sends_via_api_to_etl_alerts(self) -> None:
        ctx = {"run_id": _RUN_ID}
        with mock.patch(
            _GET_TASK_STATES,
            return_value=_states(model_a="failed", model_b="failed", ok="success"),
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")

            api_hook.assert_called_once_with(slack_conn_id=slack_alerts.SLACK_API_CONN_ID)
            payload = api_hook.return_value.call.call_args.kwargs["json"]
            self.assertEqual(payload["channel"], "#etl-alerts")
            self.assertIn("`acme`", payload["text"])
            self.assertIn("Failed Tasks (2)", payload["text"])
            self.assertIn("• `model_a`", payload["text"])
            self.assertIn("• `model_b`", payload["text"])
            self.assertIn(_RUN_ID, payload["text"])
            self.assertIn("*Models:* `all`", payload["text"])

    def test_extra_channel_gets_the_alert_too(self) -> None:
        ctx = {"run_id": _RUN_ID}
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="failed")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras("picsart-alerts"):
            slack_alerts._send_failure_alert(ctx, tenant="picsart", dag_id="dbt_picsart")

            self.assertEqual(
                _channels_posted_to(api_hook), ["#etl-alerts", "#picsart-alerts"]
            )

    def test_failing_extra_still_posts_to_etl_alerts_and_raises(self) -> None:
        ctx = {"run_id": _RUN_ID}
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="failed")
        ), mock.patch(f"{_MODULE}._send_with_retry", lambda fn: fn), mock.patch(
            f"{_MODULE}.SlackHook"
        ) as api_hook, _extras("private-channel"):
            api_hook.return_value.call.side_effect = [
                None,
                RuntimeError("not_in_channel"),
            ]
            with self.assertRaises(RuntimeError) as caught:
                slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")

            self.assertIn("#private-channel", str(caught.exception))
            self.assertEqual(
                _channels_posted_to(api_hook), ["#etl-alerts", "#private-channel"]
            )

    def test_skips_when_nothing_failed(self) -> None:
        ctx = {"run_id": _RUN_ID}
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook:
            with self.assertRaises(AirflowSkipException):
                slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")

            api_hook.assert_not_called()

    def test_raises_when_send_fails(self) -> None:
        ctx = {"run_id": _RUN_ID}
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="failed")
        ), mock.patch(f"{_MODULE}._send_with_retry", lambda fn: fn), mock.patch(
            f"{_MODULE}.SlackHook"
        ) as api_hook, _extras(""):
            api_hook.return_value.call.side_effect = RuntimeError("slack down")
            with self.assertRaises(RuntimeError):
                slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")

    def test_select_selector_is_shown_not_all(self) -> None:
        ctx = {"run_id": _RUN_ID, "params": {"select": "A+"}}
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="failed")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_failure_alert(ctx, tenant="acme", dag_id="dbt_acme")

            text = api_hook.return_value.call.call_args.kwargs["json"]["text"]
            self.assertIn("*Models:* `A+`", text)
            self.assertNotIn("*Models:* `all`", text)
            self.assertNotIn("Exclude", text)


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


def _success_ctx(
    *,
    params: dict | None = None,
    prep: dict | None = None,
) -> dict:
    ctx: dict = {"run_id": _RUN_ID, "params": params or {"backfill_mode": "none"}}
    if prep is not None:
        ti = mock.Mock()
        ti.xcom_pull.return_value = prep
        ctx["ti"] = ti
    return ctx


class SendSuccessAlertTest(unittest.TestCase):
    def test_sends_only_to_pipeline_completion_alerts(self) -> None:
        ctx = _success_ctx(
            params={"backfill_mode": "none", "execution_ts": "2026-09-01"},
        )
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras("tenant-alerts"):
            slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            api_hook.assert_called_once_with(slack_conn_id=slack_alerts.SLACK_API_CONN_ID)
            self.assertEqual(_channels_posted_to(api_hook), ["#pipeline-completion-alerts"])
            payload = api_hook.return_value.call.call_args.kwargs["json"]
            self.assertIn("`acme`", payload["text"])
            self.assertIn("*Run Type:* `normal`", payload["text"])
            self.assertIn("*Execution TS:* `2026-09-01`", payload["text"])
            self.assertIn("*Models:* `all`", payload["text"])
            self.assertIn(_RUN_ID, payload["text"])
            self.assertNotIn("Failed Tasks", payload["text"])
            self.assertNotIn("#etl-alerts", _channels_posted_to(api_hook))
            self.assertNotIn("#tenant-alerts", _channels_posted_to(api_hook))

    def test_full_backfill_includes_execution_ts(self) -> None:
        ctx = _success_ctx(
            params={"backfill_mode": "full", "execution_ts": "2026-03-19"},
            prep={
                "run_context": "full_backfill",
                "vars": {"execution_ts": "2026-03-19"},
            },
        )
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            text = api_hook.return_value.call.call_args.kwargs["json"]["text"]
            self.assertIn("*Run Type:* `full_backfill`", text)
            self.assertIn("*Execution TS:* `2026-03-19`", text)
            self.assertNotIn("Start TS", text)

    def test_partial_backfill_includes_start_and_end_ts(self) -> None:
        ctx = _success_ctx(
            params={
                "backfill_mode": "partial",
                "start_ts": "2025-01-01T00:00:00",
                "end_ts": "2025-06-30T23:59:59",
                "execution_ts": "2026-09-01",
            },
            prep={
                "run_context": "partial_backfill",
                "vars": {
                    "execution_ts": "2026-09-01",
                    "backfill_start_ts": "2025-01-01T00:00:00",
                    "backfill_end_ts": "2025-06-30T23:59:59",
                },
            },
        )
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_success_alert(ctx, tenant="picsart", dag_id="dbt_picsart")

            text = api_hook.return_value.call.call_args.kwargs["json"]["text"]
            self.assertIn("*Run Type:* `partial_backfill`", text)
            self.assertIn("*Start TS:* `2025-01-01T00:00:00`", text)
            self.assertIn("*End TS:* `2025-06-30T23:59:59`", text)
            self.assertNotIn("Execution TS", text)

    def test_prefers_prepare_xcom_over_params(self) -> None:
        ctx = _success_ctx(
            params={"backfill_mode": "none", "execution_ts": None},
            prep={"run_context": "normal", "vars": {"execution_ts": "2026-04-02"}},
        )
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            text = api_hook.return_value.call.call_args.kwargs["json"]["text"]
            self.assertIn("*Execution TS:* `2026-04-02`", text)

    def test_select_and_exclude_are_shown(self) -> None:
        ctx = _success_ctx(
            params={
                "backfill_mode": "none",
                "select": "A+",
                "exclude": "tag:wip",
            }
        )
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            text = api_hook.return_value.call.call_args.kwargs["json"]["text"]
            self.assertIn("*Models:* `A+`", text)
            self.assertIn("*Exclude:* `tag:wip`", text)
            self.assertNotIn("*Models:* `all`", text)

    def test_omits_missing_window_fields(self) -> None:
        ctx = _success_ctx(params={"backfill_mode": "none"})
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            text = api_hook.return_value.call.call_args.kwargs["json"]["text"]
            self.assertIn("*Run Type:* `normal`", text)
            self.assertNotIn("Execution TS", text)
            self.assertNotIn("None", text)

    def test_omits_none_partial_window_fields(self) -> None:
        ctx = _success_ctx(params={"backfill_mode": "partial"})
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            text = api_hook.return_value.call.call_args.kwargs["json"]["text"]
            self.assertIn("*Run Type:* `partial_backfill`", text)
            self.assertNotIn("Start TS", text)
            self.assertNotIn("End TS", text)
            self.assertNotIn("None", text)

    def test_falls_back_to_params_when_xcom_raises(self) -> None:
        ctx = _success_ctx(
            params={"backfill_mode": "none", "execution_ts": "2026-05-01"}
        )
        ctx["ti"] = mock.Mock()
        ctx["ti"].xcom_pull.side_effect = RuntimeError("xcom down")
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook, _extras(""):
            slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            text = api_hook.return_value.call.call_args.kwargs["json"]["text"]
            self.assertIn("*Execution TS:* `2026-05-01`", text)

    def test_skips_when_a_task_failed(self) -> None:
        ctx = _success_ctx()
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="failed")
        ), mock.patch(f"{_MODULE}.SlackHook") as api_hook:
            with self.assertRaises(AirflowSkipException):
                slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            api_hook.assert_not_called()

    def test_skips_when_send_fails(self) -> None:
        ctx = _success_ctx(params={"backfill_mode": "none", "execution_ts": "2026-09-01"})
        with mock.patch(
            _GET_TASK_STATES, return_value=_states(model_a="success")
        ), mock.patch(f"{_MODULE}._send_with_retry", lambda fn: fn), mock.patch(
            f"{_MODULE}.SlackHook"
        ) as api_hook, _extras(""):
            api_hook.return_value.call.side_effect = RuntimeError("slack down")
            with self.assertRaises(AirflowSkipException) as caught:
                slack_alerts._send_success_alert(ctx, tenant="acme", dag_id="dbt_acme")

            self.assertIn("#pipeline-completion-alerts", str(caught.exception))


class BuildSuccessAlertTaskTest(unittest.TestCase):
    def test_task_has_all_done_trigger_rule(self) -> None:
        from datetime import datetime

        from airflow import DAG
        from airflow.utils.trigger_rule import TriggerRule

        with DAG(dag_id="t", start_date=datetime(2024, 1, 1), schedule=None):
            result = slack_alerts.build_success_alert_task(tenant="acme", dag_id="t")

        operator = result.operator
        self.assertEqual(operator.task_id, slack_alerts.SUCCESS_ALERT_TASK_ID)
        self.assertEqual(operator.trigger_rule, TriggerRule.ALL_DONE)


if __name__ == "__main__":
    unittest.main()
