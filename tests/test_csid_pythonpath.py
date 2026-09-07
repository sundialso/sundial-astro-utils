"""Tests for reaching the dbt venv's interpreter with the CSID patch.

``sundial_airflow.csid`` is imported directly rather than through the package,
so these run without Airflow or Cosmos installed.

The end-to-end case is the point of this file. The previous attempt shipped a
``.pth`` into the Airflow environment and was reasoned about from Cosmos source;
production showed every dbt session still tagged ``dbt``, because Cosmos shells
out to a separate virtualenv. So the mechanism is exercised for real here: a
throwaway interpreter, a directory on ``PYTHONPATH``, a fake connector, and an
assertion about what the patch did to it.
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def _load_csid_module():
    spec = importlib.util.spec_from_file_location(
        "_csid_helper_standalone", _ROOT / "sundial_airflow" / "csid.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


csid = _load_csid_module()


class SiteDirTest(unittest.TestCase):
    def test_ships_a_sitecustomize(self):
        self.assertTrue(Path(csid.csid_site_dir(), "sitecustomize.py").is_file())

    def test_holds_nothing_else_importable(self):
        # Everything here lands on the dbt subprocess's sys.path, ahead of its
        # own site-packages. Only sitecustomize and its payload may live here.
        names = {p.name for p in Path(csid.csid_site_dir()).glob("*.py")}
        self.assertTrue(names <= {"sitecustomize.py", "_sundial_csid.py"}, names)


class PythonPathTest(unittest.TestCase):
    def test_yields_only_pythonpath_when_given_nothing(self):
        # The Cosmos operators default to append_env=False, so this is layered
        # onto the env Cosmos builds rather than replacing a process env.
        self.assertEqual(
            csid.with_csid_pythonpath(), {"PYTHONPATH": csid.csid_site_dir()}
        )

    def test_preserves_other_variables(self):
        env = csid.with_csid_pythonpath({"DBT_PROFILES_DIR": "/tmp/p"})
        self.assertEqual(env["DBT_PROFILES_DIR"], "/tmp/p")

    def test_prepends_to_an_existing_pythonpath(self):
        env = csid.with_csid_pythonpath({"PYTHONPATH": f"/a{os.pathsep}/b"})
        self.assertEqual(
            env["PYTHONPATH"],
            os.pathsep.join([csid.csid_site_dir(), "/a", "/b"]),
        )

    def test_preserves_empty_components(self):
        # Python reads an empty component as the working directory, so dropping
        # one would take cwd off the dbt subprocess's sys.path. ``:/x`` is what
        # ``export PYTHONPATH=$PYTHONPATH:/x`` yields when the var was unset.
        for given in (f"/a{os.pathsep}{os.pathsep}/b", f"{os.pathsep}/b", f"/a{os.pathsep}"):
            with self.subTest(given=given):
                env = csid.with_csid_pythonpath({"PYTHONPATH": given})
                self.assertEqual(
                    env["PYTHONPATH"], os.pathsep.join([csid.csid_site_dir(), given])
                )

    def test_an_empty_value_contributes_nothing(self):
        # CPython treats PYTHONPATH="" exactly like unset. Splitting it would
        # leave a trailing empty component and *add* a cwd entry that was not
        # there — the mirror image of dropping a real one.
        self.assertEqual(
            csid.with_csid_pythonpath({"PYTHONPATH": ""}),
            {"PYTHONPATH": csid.csid_site_dir()},
        )

    def test_is_idempotent(self):
        once = csid.with_csid_pythonpath()
        self.assertEqual(csid.with_csid_pythonpath(once), once)

    def test_does_not_mutate_the_input(self):
        given = {"PYTHONPATH": "/a"}
        csid.with_csid_pythonpath(given)
        self.assertEqual(given, {"PYTHONPATH": "/a"})


class SubprocessAutoloadTest(unittest.TestCase):
    """The real check: does a fresh interpreter apply the patch via PYTHONPATH?"""

    def _run(self, tmp: Path, script: str, extra_path: list[str] | None = None):
        # A fake connector, since the payload patches classes in place and this
        # interpreter must not end up with a wrapped real one.
        pkg = tmp / "snowflake" / "connector"
        pkg.mkdir(parents=True, exist_ok=True)
        (tmp / "snowflake" / "__init__.py").write_text("")
        (pkg / "__init__.py").write_text(
            "from .connection import SnowflakeConnection\n"
        )
        (pkg / "connection.py").write_text(
            "class SnowflakeConnection:\n"
            "    def __init__(self, connection_name=None,"
            " connections_file_path=None, **kwargs):\n"
            "        self.kwargs = kwargs\n"
        )
        # Staged by setup.py in a wheel; in the source tree it sits at the root.
        staged = Path(csid.csid_site_dir()) / "_sundial_csid.py"
        if not staged.exists():
            staged.write_text((_ROOT / "_sundial_csid.py").read_text())
            self.addCleanup(staged.unlink)

        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        entries = [str(tmp), *(extra_path or [])]
        env.update(csid.with_csid_pythonpath({"PYTHONPATH": os.pathsep.join(entries)}))
        return subprocess.run(
            [sys.executable, "-c", textwrap.dedent(script)],
            capture_output=True,
            text=True,
            env=env,
            cwd=str(tmp),
            timeout=60,
        )

    def test_a_dbt_shaped_connect_comes_out_tagged(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._run(
                Path(tmpdir),
                """
                import sys
                assert "sitecustomize" in sys.modules, "sitecustomize did not autoload"
                import snowflake.connector
                from snowflake.connector.connection import SnowflakeConnection
                # The exact kwargs dbt-snowflake sends.
                c = SnowflakeConnection(account="a", user="u", application="dbt")
                print(c.kwargs["application"])
                """,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "Sundial_Analytics")

    def test_the_connector_is_not_imported_at_startup(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._run(
                Path(tmpdir),
                """
                import sys
                print("snowflake.connector" in sys.modules)
                """,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            # Every dbt subprocess pays interpreter startup; importing the
            # connector eagerly there would cost hundreds of ms.
            self.assertEqual(result.stdout.strip(), "False")

    def test_an_empty_component_still_puts_cwd_on_sys_path(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            # Run a script file rather than ``-c``: with ``-c`` sys.path[0] is
            # already "", which would mask whether the component survived.
            scripts = tmp / "scripts"
            scripts.mkdir()
            (scripts / "probe.py").write_text(
                "import os, sys\n"
                "print(os.getcwd() in sys.path or '' in sys.path)\n"
            )
            staged = Path(csid.csid_site_dir()) / "_sundial_csid.py"
            if not staged.exists():
                staged.write_text((_ROOT / "_sundial_csid.py").read_text())
                self.addCleanup(staged.unlink)

            env = dict(os.environ)
            env.update(
                csid.with_csid_pythonpath({"PYTHONPATH": f"{os.pathsep}{tmp}"})
            )
            result = subprocess.run(
                [sys.executable, str(scripts / "probe.py")],
                capture_output=True,
                text=True,
                env=env,
                cwd=str(tmp),
                timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "True")

    def test_a_shadowed_sitecustomize_still_runs(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            other = tmp / "other_site"
            other.mkdir()
            (other / "sitecustomize.py").write_text(
                "import os\nos.environ['SHADOWED_RAN'] = '1'\n"
            )
            result = self._run(
                tmp,
                """
                import os
                print(os.environ.get("SHADOWED_RAN"))
                """,
                extra_path=[str(other)],
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            # Our directory precedes site-packages, so a sitecustomize the dbt
            # venv already had must not be silently dropped.
            self.assertEqual(result.stdout.strip(), "1")


if __name__ == "__main__":
    unittest.main()
