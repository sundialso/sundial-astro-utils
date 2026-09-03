"""Ship ``_sundial_csid.pth`` into site-packages.

A ``.pth`` only runs when it sits at the root of a directory on ``sys.path``,
which for a wheel means the root of the archive (the purelib scheme). setuptools
has no declarative route there: ``[tool.setuptools] data-files`` targets the
wheel's ``.data/data/`` tree, which installs relative to ``sys.prefix`` — the
venv root, never on ``sys.path`` — so the file installs but never executes.
Staging it into ``build_lib`` puts it at the archive root instead.

Everything else about this distribution is declared in ``pyproject.toml``.
"""
import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py

_PTH = "_sundial_csid.pth"


class BuildPyWithPth(build_py):
    def run(self) -> None:
        super().run()
        shutil.copy(_PTH, Path(self.build_lib) / _PTH)


setup(cmdclass={"build_py": BuildPyWithPth})
