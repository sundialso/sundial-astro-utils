"""Tests for the Snowflake CSID attribution patch.

Loaded standalone, like the other suites here, so importing the package (and
with it Airflow and Cosmos) is not a prerequisite. Each test plants a fake
connector so the payload never touches an installed ``snowflake-connector-python``
— it patches classes in place, and ``sys.modules`` restoration is by reference,
so a real class would stay wrapped for the rest of the process.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest

_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "_sundial_csid.py",
)


def _load():
    spec = importlib.util.spec_from_file_location("_csid_standalone", _PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _purge_snowflake() -> None:
    for name in [k for k in sys.modules if k.startswith("snowflake")]:
        del sys.modules[name]


def _fake_connector():
    """Plant a fake connector package exposing ``SnowflakeConnection``."""
    _purge_snowflake()

    class SnowflakeConnection:
        body_calls = 0

        def __init__(self, connection_name=None, connections_file_path=None, **kwargs):
            type(self).body_calls += 1
            self.connection_name = connection_name
            self.connections_file_path = connections_file_path
            self.kwargs = kwargs

    root = types.ModuleType("snowflake")
    connector = types.ModuleType("snowflake.connector")
    connection = types.ModuleType("snowflake.connector.connection")
    # Mirror the real layout: defined in ``.connection``, re-exported from the
    # root, so the lookup order is exercised.
    connection.SnowflakeConnection = SnowflakeConnection
    connector.SnowflakeConnection = SnowflakeConnection
    sys.modules["snowflake"] = root
    sys.modules["snowflake.connector"] = connector
    sys.modules["snowflake.connector.connection"] = connection
    return SnowflakeConnection


class CsidTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._saved_modules = {
            k: v for k, v in sys.modules.items() if k.startswith("snowflake")
        }
        self._saved_meta_path = list(sys.meta_path)

    def tearDown(self) -> None:
        _purge_snowflake()
        sys.modules.update(self._saved_modules)
        # A pending finder would leak into unrelated tests.
        sys.meta_path[:] = self._saved_meta_path

    @staticmethod
    def _hooks() -> list:
        return [
            f
            for f in sys.meta_path
            if getattr(f, "_sundial_snowflake_csid_hook", False)
        ]


class DbtAttributionTest(CsidTestCase):
    def test_rewrites_the_hardcoded_dbt_tag(self) -> None:
        cls = _fake_connector()
        _load()

        conn = cls(account="acct", application="dbt")

        self.assertEqual(conn.kwargs["application"], "Sundial_Analytics")
        self.assertEqual(conn.kwargs["account"], "acct")  # rest passes through

    def test_rewrites_dbt_shaped_values(self) -> None:
        # Prefix-matching means an upstream rename can't silently drop
        # attribution.
        for value in ("dbt", "dbt-snowflake", "DBT", "dbt_cloud"):
            with self.subTest(value=value):
                cls = _fake_connector()
                _load()
                self.assertEqual(
                    cls(application=value).kwargs["application"], "Sundial_Analytics"
                )

    def test_supplies_the_csid_when_application_is_absent(self) -> None:
        cls = _fake_connector()
        _load()

        # Otherwise the connector falls back to "PythonConnector".
        self.assertEqual(cls(account="a").kwargs["application"], "Sundial_Analytics")

    def test_leaves_a_deliberate_value_alone(self) -> None:
        # Only the dbt default is ours to rewrite.
        for value in ("Sundial_Analytics", "streamlit", "PythonConnector"):
            with self.subTest(value=value):
                cls = _fake_connector()
                _load()
                self.assertEqual(cls(application=value).kwargs["application"], value)

    def test_does_not_stack_wrappers(self) -> None:
        cls = _fake_connector()
        _load()
        _load()

        conn = cls(application="dbt")

        # A stacked wrapper still yields the right tag, so count the body.
        self.assertEqual(cls.body_calls, 1)
        self.assertEqual(conn.kwargs["application"], "Sundial_Analytics")


class ConnectionsTomlTest(CsidTestCase):
    """``__init__`` reads connections.toml when a name is given or when the
    remaining kwargs are empty. Injecting ``application`` there is not free: an
    explicit kwarg outranks the file's own value, and the connector captures
    ``is_kwargs_empty`` *before* it looks at ``application``, so a non-empty
    kwargs stops a no-arg ``connect()`` loading the default connection."""

    def test_leaves_a_named_config_alone(self) -> None:
        cls = _fake_connector()
        _load()

        # The file may configure its own application; ours must not displace it.
        self.assertNotIn("application", cls(connection_name="c").kwargs)

    def test_leaves_a_positionally_named_config_alone(self) -> None:
        cls = _fake_connector()
        _load()

        # connection_name binds positionally too, so args has to be inspected.
        self.assertNotIn("application", cls("c").kwargs)

    def test_keeps_a_no_arg_call_empty(self) -> None:
        cls = _fake_connector()
        _load()

        # A single injected kwarg here would silently disable the whole
        # default-connection path, not just change the tag.
        self.assertEqual(cls().kwargs, {})

    def test_leaves_a_bare_connections_file_path_alone(self) -> None:
        cls = _fake_connector()
        _load()

        self.assertNotIn(
            "application", cls(connections_file_path="/etc/connections.toml").kwargs
        )

    def test_still_rewrites_dbt_alongside_a_named_config(self) -> None:
        cls = _fake_connector()
        _load()

        # An explicit kwarg already beats the file, so rewriting it costs
        # nothing and keeps attribution on a TOML-configured dbt profile.
        conn = cls(connection_name="c", application="dbt")
        self.assertEqual(conn.kwargs["application"], "Sundial_Analytics")


class DeferredImportTest(CsidTestCase):
    def test_defers_via_a_single_hook(self) -> None:
        _purge_snowflake()

        _load()
        self.assertEqual(len(self._hooks()), 1)

        # The patch waits for the import rather than paying it in every
        # process. Re-running must not pile up finders.
        _load()
        self.assertEqual(len(self._hooks()), 1)

    def test_patches_when_the_import_finally_happens(self) -> None:
        import tempfile

        # The real lazy path end-to-end: an on-disk package resolved through
        # sys.meta_path, as when dbt imports it after worker startup.
        with tempfile.TemporaryDirectory() as tmp:
            pkg = os.path.join(tmp, "snowflake", "connector")
            os.makedirs(pkg)
            open(os.path.join(tmp, "snowflake", "__init__.py"), "w").close()
            with open(os.path.join(pkg, "__init__.py"), "w") as fh:
                fh.write(
                    "class SnowflakeConnection:\n"
                    "    def __init__(self, connection_name=None,"
                    " connections_file_path=None, **kwargs):\n"
                    "        self.kwargs = kwargs\n"
                )
            _purge_snowflake()
            sys.path.insert(0, tmp)
            try:
                importlib.invalidate_caches()
                _load()
                self.assertEqual(len(self._hooks()), 1)

                import snowflake.connector

                conn = snowflake.connector.SnowflakeConnection(application="dbt")
                self.assertEqual(conn.kwargs["application"], "Sundial_Analytics")
                # The hook retires once it has done its one job.
                self.assertEqual(self._hooks(), [])
            finally:
                sys.path.remove(tmp)

    def test_patches_directly_when_already_imported(self) -> None:
        cls = _fake_connector()
        _load()

        self.assertEqual(cls(application="dbt").kwargs["application"], "Sundial_Analytics")
        # Nothing to wait for, so no finder is left behind.
        self.assertEqual(self._hooks(), [])


class NoConnectorTest(CsidTestCase):
    def test_is_a_quiet_no_op_and_records_no_failure(self) -> None:
        # A `python -c` on the image with nothing to do with warehouses. A
        # raising .pth would break every interpreter here.
        _purge_snowflake()
        module = _load()
        self.assertEqual(module.PATCH_FAILURES, {})


if __name__ == "__main__":
    unittest.main()
