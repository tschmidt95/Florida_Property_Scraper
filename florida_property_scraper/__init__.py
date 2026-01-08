from __future__ import annotations

from pathlib import Path
from pkgutil import extend_path

# Repo-local import shim for `src/` layout.
#
# Tests spawn subprocesses that run `python -m florida_property_scraper[...]`.
# When the project isn't installed into the active environment, those subprocesses
# won't see `src/` on `sys.path`. This namespace-package shim makes the package
# importable from a repo checkout without affecting the installed distribution.

__path__ = extend_path(__path__, __name__)  # type: ignore[name-defined]

_SRC_PKG = Path(__file__).resolve().parents[1] / "src" / "florida_property_scraper"
if _SRC_PKG.exists():
    src_str = str(_SRC_PKG)
    # Prefer the real implementation under src/ over this shim directory.
    try:
        if src_str not in __path__:
            __path__.insert(0, src_str)  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover
        # Fallback for non-list path objects
        if src_str not in list(__path__):
            __path__ = [src_str, *list(__path__)]
