"""Snowflake partner attribution (CSID) for Cosmos-run dbt.

Autoloaded by ``_sundial_csid.pth``, which ``site`` executes at every
interpreter startup once this distribution is installed. That reaches the
Airflow worker, which is where Cosmos actually runs dbt: with dbt-core
importable alongside Airflow, ``DbtLocalBaseOperator._discover_invocation_mode``
selects ``InvocationMode.DBT_RUNNER`` and invokes dbt in-process, ignoring
``ExecutionConfig.dbt_executable_path``. Every tenant on this package installs a
dbt adapter into the Airflow environment, so that is the branch taken.

``dbt-snowflake`` hardcodes ``application="dbt"`` in its
``snowflake.connector.connect`` call, with no profiles.yml key or env var to
override it, and the connector's ``SF_PARTNER`` fallback only fires when
``application`` is *absent*. So dbt sessions are credited to dbt rather than to
Sundial's CSID, and dbt is where the warehouse compute happens. Patching
``SnowflakeConnection.__init__`` does the same job as forking the adapter, with
no fork to re-cut on every dbt release.

A top-level module rather than a ``sundial_airflow`` submodule on purpose:
importing the package would pull Airflow and Cosmos into *every* Python process
on the image at interpreter startup. Nothing here imports beyond the stdlib, and
the connector itself is touched only once something else imports it.

Ground truth for whether attribution lands is
``ACCOUNT_USAGE.SESSIONS.CLIENT_APPLICATION_ID``, not this file.
"""
from __future__ import annotations

import importlib.util
import sys

#: Sundial's registered Snowflake CSID. Duplicated from the monorepo's
#: ``utils.warehouse_constants`` rather than imported — this runs in tenant
#: images that have no access to it.
SNOWFLAKE_APPLICATION_NAME = "Sundial_Analytics"

#: ``SnowflakeConnection.__init__``'s two named params, which bind ahead of
#: ``**kwargs`` and select the connections.toml path rather than the connection.
_CONFIG_FILE_ARGS = ("connection_name", "connections_file_path")

#: Marks an already-wrapped class, so a re-exec can't stack wrappers.
_PATCH_FLAG = "_sundial_csid_patched"


def _needs_override(application: object) -> bool:
    """True when ``application`` is unset or is a dbt adapter default.

    Prefix-matching rather than comparing to ``"dbt"`` means an upstream
    rename can't silently drop our attribution. Deliberate values survive.
    """
    if application is None:
        return True
    return str(application).lower().startswith("dbt")


def _resolves_via_config_file(args: tuple, kwargs: dict) -> bool:
    """True when the connector will draw parameters from ``connections.toml``.

    ``__init__(self, connection_name=None, connections_file_path=None, **kwargs)``
    reads the file when a name is given, or when the *remaining* kwargs are
    empty (the default connection). ``connection_name`` may arrive positionally,
    so it is pulled out of ``args`` as well.
    """
    connection_name = args[0] if args else kwargs.get("connection_name")
    if connection_name is not None:
        return True
    return not [k for k in kwargs if k not in _CONFIG_FILE_ARGS]


def _should_override(args: tuple, kwargs: dict) -> bool:
    """Whether injecting our CSID into this call is safe as well as wanted."""
    if "application" in kwargs:
        # An explicit value already outranks connections.toml, and its presence
        # means the default-connection path is not taken, so rewriting a
        # dbt-shaped one changes nothing but the tag. This is dbt's case.
        return _needs_override(kwargs["application"])
    # No explicit value, so injecting one is not free. It would displace
    # whatever connections.toml supplies, and — because the connector captures
    # `is_kwargs_empty` before it ever looks at `application` — it would also
    # stop a no-arg `connect()` from loading the default connection at all.
    return not _resolves_via_config_file(args, kwargs)


def _patch_connection_class(cls: type) -> None:
    """Wrap ``cls.__init__`` so it carries our CSID into the connect kwargs."""
    if getattr(cls, _PATCH_FLAG, False):
        return

    original_init = cls.__init__

    def __init__(self, *args, **kwargs) -> None:  # noqa: N807
        if _should_override(args, kwargs):
            kwargs["application"] = SNOWFLAKE_APPLICATION_NAME
        original_init(self, *args, **kwargs)

    cls.__init__ = __init__
    setattr(cls, _PATCH_FLAG, True)


def _apply_patch() -> bool:
    """Patch ``SnowflakeConnection`` if it's imported. Returns whether it was.

    ``connect`` and ``Connect`` are both thin wrappers over this class, so one
    patch point covers every entry path — dbt's included.
    """
    # Defined in ``.connection``, re-exported from the root; canonical first.
    for module_path in ("snowflake.connector.connection", "snowflake.connector"):
        module = sys.modules.get(module_path)
        if module is None:
            continue
        cls = getattr(module, "SnowflakeConnection", None)
        if isinstance(cls, type):
            _patch_connection_class(cls)
            return True
    return False


class _SnowflakeConnectorImportHook:
    """Applies the CSID patch the first time ``snowflake.connector`` imports.

    Lazy because this runs in *every* process on the image: eagerly importing
    the connector (cryptography, pyOpenSSL, ...) would cost hundreds of ms in
    processes that never touch a warehouse.
    """

    _sundial_snowflake_csid_hook = True
    _TARGET = "snowflake.connector"

    def find_spec(self, fullname: str, path: object = None, target: object = None):
        del path, target  # unused; the MetaPathFinder protocol passes them
        if fullname != self._TARGET:
            return None
        # Step aside so the delegation below doesn't re-enter us. Retiring
        # permanently is correct: the patch is only needed on first import.
        try:
            sys.meta_path.remove(self)
        except ValueError:  # another thread got here first
            return None

        spec = importlib.util.find_spec(fullname)
        if spec is None or spec.loader is None:
            return spec

        inner_exec_module = spec.loader.exec_module

        def exec_module(module: object) -> None:
            inner_exec_module(module)
            _apply_patch()

        spec.loader.exec_module = exec_module
        return spec


def install() -> None:
    """Patch now if the connector is loaded, else arm a hook for its import."""
    if _apply_patch():
        return  # already imported — nothing to wait for
    already_hooked = any(
        getattr(finder, "_sundial_snowflake_csid_hook", False) for finder in sys.meta_path
    )
    if not already_hooked:
        sys.meta_path.insert(0, _SnowflakeConnectorImportHook())


#: What ``install()`` raised, if anything. A raising ``.pth`` breaks every
#: interpreter on the image, so nothing may escape — but swallowing silently
#: would make a broken patch undiagnosable, so it is parked here for
#: ``python -c "import _sundial_csid; print(_sundial_csid.PATCH_FAILURES)"``.
PATCH_FAILURES: dict = {}

# The blind except is the point: no failure of a best-effort patch justifies
# breaking interpreter startup across the image.
try:
    install()
except Exception as exc:  # noqa: BLE001
    PATCH_FAILURES["partner_application"] = exc
