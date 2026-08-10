"""Unit tests for sundial_airflow.warehouse_sizing."""
from __future__ import annotations

import unittest
from unittest import mock

from airflow.exceptions import AirflowSkipException

from sundial_airflow import warehouse_sizing as wh


class EnvSizeTest(unittest.TestCase):
    def test_defaults(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(
                wh._env_size(wh._WH_SIZE_ENV, wh._DEFAULT_WH_SIZE),
                (wh._DEFAULT_WH_SIZE, "default"),
            )

    def test_override(self) -> None:
        with mock.patch.dict("os.environ", {wh._WH_SIZE_ENV: " Small "}, clear=True):
            self.assertEqual(
                wh._env_size(wh._WH_SIZE_ENV, wh._DEFAULT_WH_SIZE),
                ("Small", wh._WH_SIZE_ENV),
            )


class BackfillModeTest(unittest.TestCase):
    def test_none(self) -> None:
        self.assertEqual(wh._backfill_mode({"backfill_mode": "none"}), "none")
        self.assertEqual(wh._backfill_mode(None), "none")

    def test_full(self) -> None:
        self.assertEqual(wh._backfill_mode({"backfill_mode": "full"}), "full")

    def test_partial(self) -> None:
        self.assertEqual(wh._backfill_mode({"backfill_mode": "partial"}), "partial")


class QuoteIdentTest(unittest.TestCase):
    def test_simple(self) -> None:
        self.assertEqual(wh._quote_ident("ami_astro_wh"), '"AMI_ASTRO_WH"')

    def test_hyphenated(self) -> None:
        self.assertEqual(wh._quote_ident("my-warehouse"), '"MY-WAREHOUSE"')

    def test_embedded_double_quote(self) -> None:
        self.assertEqual(wh._quote_ident('wh"x'), '"WH""X"')

    def test_empty_raises(self) -> None:
        with self.assertRaises(ValueError):
            wh._quote_ident("")

    def test_whitespace_only_raises(self) -> None:
        with self.assertRaises(ValueError):
            wh._quote_ident("   ")


class ResolveConnIdTest(unittest.TestCase):
    def test_explicit(self) -> None:
        self.assertEqual(wh._resolve_conn_id("my_conn"), "my_conn")

    def test_no_adapter(self) -> None:
        with mock.patch("sundial_airflow.warehouses.get_adapter", return_value=None):
            self.assertIsNone(wh._resolve_conn_id(None))

    def test_import_error(self) -> None:
        with mock.patch(
            "sundial_airflow.warehouses.get_adapter", side_effect=ImportError
        ):
            self.assertIsNone(wh._resolve_conn_id(None))


class ResolveWarehouseTest(unittest.TestCase):
    def test_happy_path(self) -> None:
        with mock.patch.object(
            wh, "_resolve_conn_id", return_value="sf"
        ), mock.patch.object(wh, "_warehouse_from_conn", return_value="WH"):
            self.assertEqual(wh._resolve_warehouse("sf"), ("sf", "WH"))

    def test_no_conn(self) -> None:
        with mock.patch.object(wh, "_resolve_conn_id", return_value=None):
            self.assertIsNone(wh._resolve_warehouse(None))

    def test_no_warehouse(self) -> None:
        with mock.patch.object(
            wh, "_resolve_conn_id", return_value="sf"
        ), mock.patch.object(wh, "_warehouse_from_conn", return_value=None):
            self.assertIsNone(wh._resolve_warehouse("sf"))


class ResizeTest(unittest.TestCase):
    _CALL = dict(dag_id="d", run_id="r")

    def test_normal_uses_default(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True), mock.patch.object(
            wh, "_resolve_warehouse", return_value=("sf", "COMPUTE_WH")
        ), mock.patch.object(wh, "alter_warehouse_size") as alter:
            out = wh.resize_snowflake_warehouse(
                conn_id="sf", params={"backfill_mode": "none"}, **self._CALL
            )
        alter.assert_called_once_with(
            conn_id="sf", warehouse="COMPUTE_WH", size=wh._DEFAULT_WH_SIZE
        )
        self.assertFalse(out["backfill"])
        self.assertNotIn("skipped", out)

    def test_backfill_uses_backfill_size(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {wh._WH_SIZE_ENV: "Medium", wh._BACKFILL_SIZE_ENV: "X-Large"},
            clear=True,
        ), mock.patch.object(
            wh, "_resolve_warehouse", return_value=("sf", "COMPUTE_WH")
        ), mock.patch.object(wh, "alter_warehouse_size") as alter:
            out = wh.resize_snowflake_warehouse(
                conn_id="sf", params={"backfill_mode": "full"}, **self._CALL
            )
        alter.assert_called_once_with(
            conn_id="sf", warehouse="COMPUTE_WH", size="X-Large"
        )
        self.assertTrue(out["backfill"])

    def test_noop_when_unresolvable(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True), mock.patch.object(
            wh, "_resolve_warehouse", return_value=None
        ):
            out = wh.resize_snowflake_warehouse(
                conn_id="sf", params={"backfill_mode": "none"}, **self._CALL
            )
        self.assertTrue(out["skipped"])
        self.assertIsNone(out["warehouse"])


class RestoreTest(unittest.TestCase):
    _CALL = dict(dag_id="d", run_id="r")

    def test_skips_normal(self) -> None:
        with self.assertRaises(AirflowSkipException):
            wh.restore_snowflake_warehouse(
                conn_id="sf", params={"backfill_mode": "none"}, **self._CALL
            )

    def test_full_backfill_restores(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True), mock.patch.object(
            wh, "_resolve_warehouse", return_value=("sf", "COMPUTE_WH")
        ), mock.patch.object(wh, "alter_warehouse_size") as alter:
            wh.restore_snowflake_warehouse(
                conn_id="sf", params={"backfill_mode": "full"}, **self._CALL
            )
        alter.assert_called_once_with(
            conn_id="sf", warehouse="COMPUTE_WH", size=wh._DEFAULT_WH_SIZE
        )

    def test_partial_backfill_restores(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True), mock.patch.object(
            wh, "_resolve_warehouse", return_value=("sf", "COMPUTE_WH")
        ), mock.patch.object(wh, "alter_warehouse_size") as alter:
            wh.restore_snowflake_warehouse(
                conn_id="sf", params={"backfill_mode": "partial"}, **self._CALL
            )
        alter.assert_called_once_with(
            conn_id="sf", warehouse="COMPUTE_WH", size=wh._DEFAULT_WH_SIZE
        )

    def test_skips_when_unresolvable(self) -> None:
        with mock.patch.object(
            wh, "_resolve_warehouse", return_value=None
        ), self.assertRaises(AirflowSkipException):
            wh.restore_snowflake_warehouse(
                conn_id="sf", params={"backfill_mode": "full"}, **self._CALL
            )


class AlterTest(unittest.TestCase):
    def test_runs_sql(self) -> None:
        hook = mock.Mock()
        fake = mock.Mock(SnowflakeHook=mock.Mock(return_value=hook))
        with mock.patch.dict(
            "sys.modules",
            {
                "airflow.providers.snowflake": mock.Mock(),
                "airflow.providers.snowflake.hooks": mock.Mock(),
                "airflow.providers.snowflake.hooks.snowflake": fake,
            },
        ):
            wh.alter_warehouse_size(
                conn_id="sf", warehouse="ami_astro_wh", size="Medium"
            )
        sql = hook.run.call_args.args[0]
        self.assertIn('ALTER WAREHOUSE "AMI_ASTRO_WH"', sql)
        self.assertIn("WAREHOUSE_SIZE = 'Medium'", sql)

    def test_accepts_all_valid_sizes(self) -> None:
        for size in wh._VALID_SNOWFLAKE_SIZES:
            hook = mock.Mock()
            fake = mock.Mock(SnowflakeHook=mock.Mock(return_value=hook))
            with mock.patch.dict(
                "sys.modules",
                {
                    "airflow.providers.snowflake": mock.Mock(),
                    "airflow.providers.snowflake.hooks": mock.Mock(),
                    "airflow.providers.snowflake.hooks.snowflake": fake,
                },
            ):
                wh.alter_warehouse_size(conn_id="sf", warehouse="WH", size=size)
            self.assertTrue(hook.run.called, f"Expected SQL call for size={size!r}")

    def test_rejects_invalid_size(self) -> None:
        with self.assertRaises(ValueError):
            wh.alter_warehouse_size(conn_id="sf", warehouse="WH", size="Huge")

    def test_rejects_typo(self) -> None:
        with self.assertRaises(ValueError):
            wh.alter_warehouse_size(conn_id="sf", warehouse="WH", size="Mediun")

    def test_rejects_empty(self) -> None:
        with self.assertRaises(ValueError):
            wh.alter_warehouse_size(conn_id="sf", warehouse="WH", size="")

    def test_rejects_injection_attempt(self) -> None:
        with self.assertRaises(ValueError):
            wh.alter_warehouse_size(conn_id="sf", warehouse="WH", size="Medium'; DROP")


if __name__ == "__main__":
    unittest.main()
