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

import ast
import importlib.util
import os
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path
from typing import NamedTuple

_ROOT = Path(__file__).resolve().parent.parent


def _load_csid_module():
    spec = importlib.util.spec_from_file_location(
        "_csid_helper_standalone", _ROOT / "sundial_airflow" / "csid.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


csid = _load_csid_module()


_CSID_CALL = "with_csid_pythonpath"


class _Site(NamedTuple):
    lineno: int
    description: str
    covered: bool


def _enclosing_scopes(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    """Map each node to the function (or module) that contains it."""
    scopes: dict[ast.AST, ast.AST] = {}

    def walk(node: ast.AST, scope: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            inner = child if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) else scope
            scopes[child] = inner if child is not inner else scope
            walk(child, inner)

    scopes[tree] = tree
    walk(tree, tree)
    return scopes


def _env_expression(call: ast.Call) -> ast.expr | None:
    """The env passed to a call — directly, or inside Cosmos operator_args."""
    for kw in call.keywords:
        if kw.arg == "env":
            return kw.value
        if kw.arg == "operator_args" and isinstance(kw.value, ast.Dict):
            for key, value in zip(kw.value.keys, kw.value.values):
                if isinstance(key, ast.Constant) and key.value == "env":
                    return value
    return None


def _describe(call: ast.Call) -> str | None:
    """Name the dbt invocation this call represents, or None if it isn't one."""
    func = ast.unparse(call.func)
    if func.endswith("subprocess.run"):
        return "subprocess.run"
    if any(kw.arg == "dbt_executable_path" for kw in call.keywords):
        return f"{func}(dbt_executable_path=...)"
    if func.endswith("DbtTaskGroup"):
        return "DbtTaskGroup"
    if func.endswith("ensure_dbt_deps"):
        return "ensure_dbt_deps"
    return None


def _local_assignments(scope: ast.AST, name: str, before_lineno: int) -> list[ast.expr]:
    """Assignments to ``name`` in this function's own body, before ``before_lineno``.

    Neither restriction is incidental. An assignment *after* the call cannot
    cover it, and one inside a *nested* function says nothing about the
    enclosing body — without both, an unwrapped ``env`` reads as covered.
    """
    found: list[ast.expr] = []

    def walk(node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)
            ):
                continue
            if isinstance(child, ast.Assign) and child.lineno < before_lineno:
                if any(isinstance(t, ast.Name) and t.id == name for t in child.targets):
                    found.append(child.value)
            walk(child)

    walk(scope)
    return found


def dbt_invocation_sites(source: str) -> list[_Site]:
    """Every dbt invocation in a module, and whether its env carries the CSID.

    A bare ``env=env`` is resolved against assignments to that name in the same
    function body, before the call. A covered ``env`` in another function, in a
    nested helper, or on a later line cannot vouch for this one.
    """
    tree = ast.parse(source)
    scopes = _enclosing_scopes(tree)
    sites: list[_Site] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        description = _describe(node)
        if description is None:
            continue

        env = _env_expression(node)
        covered = False
        if env is not None:
            covered = _CSID_CALL in ast.unparse(env)
            if not covered and isinstance(env, ast.Name):
                scope = scopes.get(node, tree)
                covered = any(
                    _CSID_CALL in ast.unparse(value)
                    for value in _local_assignments(scope, env.id, node.lineno)
                )
        sites.append(_Site(node.lineno, description, covered))

    return sites


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


class ChunkedModelCoverageTest(unittest.TestCase):
    """Every dbt-invoking surface in the factory must carry the CSID env.

    ami's first post-pin run tagged 35 of 192 dbt_venv sessions: the Cosmos
    task group and source tests were covered, the chunked models were not.
    Chunked models are excluded from the Cosmos group
    (``RenderConfig(exclude=_chunked_names)``) and built separately in
    ``chunking/graph.py``, which the first pass missed.
    """

    def _source(self, rel: str) -> str:
        return (_ROOT / rel).read_text()

    def test_chunked_model_run_carries_the_csid_env(self):
        src = self._source("sundial_airflow/chunking/graph.py")
        self.assertIn("with_csid_pythonpath({**os.environ, **profile_env})", src)
        self.assertNotIn("env = {**os.environ, **profile_env}", src)

    def test_chunk_test_operator_carries_the_csid_env(self):
        src = self._source("sundial_airflow/chunking/graph.py")
        self.assertIn("env=with_csid_pythonpath()", src)

    def test_every_dbt_invocation_carries_the_env(self):
        # Per-call-site, not module-wide totals: a second unpatched operator
        # alongside a well-covered one has to fail, and subprocess calls have
        # to be checked too. Both are things a count comparison misses.
        for rel in ("sundial_airflow/create_dag.py", "sundial_airflow/chunking/graph.py"):
            for site in dbt_invocation_sites(self._source(rel)):
                with self.subTest(module=rel, line=site.lineno, call=site.description):
                    self.assertTrue(
                        site.covered,
                        f"{rel}:{site.lineno} {site.description} has no env "
                        f"resolving to with_csid_pythonpath()",
                    )

    def test_the_guard_finds_every_known_invocation(self):
        # If the finder silently matched nothing, the test above would pass
        # vacuously — which is the failure mode it exists to prevent.
        counts = {
            rel: len(dbt_invocation_sites(self._source(rel)))
            for rel in ("sundial_airflow/create_dag.py", "sundial_airflow/chunking/graph.py")
        }
        self.assertGreaterEqual(counts["sundial_airflow/create_dag.py"], 3, counts)
        self.assertGreaterEqual(counts["sundial_airflow/chunking/graph.py"], 2, counts)

    def test_the_guard_catches_an_unpatched_addition(self):
        # The scenario that produced this bug: a new dbt invocation added
        # beside well-covered ones. Module-wide counting passes here.
        source = textwrap.dedent(
            """
            def build(dbt_executable, profile_config):
                env = with_csid_pythonpath({**os.environ, **profile_env})
                subprocess.run(cmd, env=env)
                covered = DbtTestLocalOperator(
                    dbt_executable_path=dbt_executable,
                    env=with_csid_pythonpath(),
                )
                forgotten = DbtTestLocalOperator(
                    dbt_executable_path=dbt_executable,
                )
            """
        )
        sites = dbt_invocation_sites(source)
        uncovered = [s for s in sites if not s.covered]
        self.assertEqual(len(uncovered), 1, [(s.lineno, s.description) for s in sites])
        self.assertIn("DbtTestLocalOperator", uncovered[0].description)

    def test_an_assignment_after_the_call_does_not_cover_it(self):
        # Resolution has to respect ordering: dbt has already run by the time
        # the wrapped env is built.
        source = textwrap.dedent(
            """
            def build(dbt_executable):
                subprocess.run(cmd, env=env)
                env = with_csid_pythonpath({**os.environ})
            """
        )
        uncovered = [s for s in dbt_invocation_sites(source) if not s.covered]
        self.assertEqual([s.description for s in uncovered], ["subprocess.run"])

    def test_a_nested_wrapped_assignment_does_not_cover_the_outer_call(self):
        # A wrapped env inside a helper says nothing about the enclosing body.
        source = textwrap.dedent(
            """
            def build(dbt_executable):
                def inner():
                    env = with_csid_pythonpath({**os.environ})
                env = {**os.environ}
                subprocess.run(cmd, env=env)
            """
        )
        uncovered = [s for s in dbt_invocation_sites(source) if not s.covered]
        self.assertEqual([s.description for s in uncovered], ["subprocess.run"])
