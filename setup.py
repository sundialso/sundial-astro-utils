"""Ship the CSID payload where each interpreter can autoload it.

A ``.pth`` only runs when it sits at the root of a directory on ``sys.path``,
which for a wheel means the root of the archive (the purelib scheme). setuptools
has no declarative route there: ``[tool.setuptools] data-files`` targets the
wheel's ``.data/data/`` tree, which installs relative to ``sys.prefix`` — the
venv root, never on ``sys.path`` — so the file installs but never executes.
Staging it into ``build_lib`` puts it at the archive root instead. That covers
the Airflow environment.

The dbt venv needs a second copy. Cosmos invokes dbt from its own virtualenv, so
that interpreter never processes our ``.pth``; it picks the payload up as
``sitecustomize`` off ``PYTHONPATH`` instead (see ``sundial_airflow.csid``). The
module is staged next to that ``sitecustomize.py`` so the repo keeps one copy of
the source and the wheel carries both.

Everything else about this distribution is declared in ``pyproject.toml``.
"""
import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py

_PTH = "_sundial_csid.pth"
_PAYLOAD = "_sundial_csid.py"
_CSID_SITE = Path("sundial_airflow") / "_csid_site"


class BuildPyWithPth(build_py):
    def run(self) -> None:
        super().run()
        build_lib = Path(self.build_lib)
        shutil.copy(_PTH, build_lib / _PTH)
        staged = build_lib / _CSID_SITE
        staged.mkdir(parents=True, exist_ok=True)
        shutil.copy(_PAYLOAD, staged / _PAYLOAD)


setup(cmdclass={"build_py": BuildPyWithPth})
