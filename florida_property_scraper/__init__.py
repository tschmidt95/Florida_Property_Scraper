"""Compatibility shim for src-layout imports.

This repository uses a src/ layout (real package lives in src/florida_property_scraper).
Some tests execute the CLI via `python -m florida_property_scraper` from the repo root.
In that scenario, Python would otherwise discover the top-level
`florida_property_scraper/` directory first and treat it as an incomplete namespace,
breaking `-m` execution.

This shim extends the package search path to include the real implementation.
It intentionally avoids importing the full implementation eagerly (which can be slow)
and instead exposes the canonical symbols lazily.
"""

from __future__ import annotations

from pathlib import Path
import sys

_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
_IMPL_PKG_DIR = _SRC_DIR / "florida_property_scraper"

if _IMPL_PKG_DIR.is_dir():
    # Ensure `import florida_property_scraper.*` can resolve to src/ implementation.
    src_str = str(_SRC_DIR)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

    impl_str = str(_IMPL_PKG_DIR)
    if impl_str not in list(__path__):  # type: ignore[name-defined]
        __path__.append(impl_str)  # type: ignore[name-defined]

    __all__ = ["FloridaPropertyScraper", "RunResult"]
else:
    __all__ = []


def __getattr__(name: str):
    if name == "FloridaPropertyScraper":
        from .scraper import FloridaPropertyScraper

        return FloridaPropertyScraper
    if name == "RunResult":
        from .run_result import RunResult

        return RunResult
    raise AttributeError(name)
