from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    v = str(raw).strip().lower()
    if v in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


@dataclass(frozen=True)
class FeatureFlags:
    """Central feature flag registry.

    Env vars are intentionally simple booleans to keep behavior predictable.

    Defaults MUST preserve current behavior.
    """

    geometry_search: bool
    triggers: bool
    sale_filtering: bool
    strict_schema_validation: bool

    @classmethod
    def from_env(cls) -> "FeatureFlags":
        # Defaults preserve current behavior.
        return cls(
            geometry_search=_env_bool("FPS_FEATURE_GEOMETRY_SEARCH", True),
            triggers=_env_bool("FPS_FEATURE_TRIGGERS", True),
            sale_filtering=_env_bool("FPS_FEATURE_SALE_FILTERING", True),
            strict_schema_validation=_env_bool(
                "FPS_FEATURE_STRICT_SCHEMA_VALIDATION", False
            ),
        )


@lru_cache(maxsize=1)
def get_flags() -> FeatureFlags:
    return FeatureFlags.from_env()


def reset_flags_cache() -> None:
    """Test helper to force env re-read."""

    get_flags.cache_clear()


def require_enabled(flag: bool, *, message: Optional[str] = None) -> None:
    if flag:
        return
    raise RuntimeError(message or "Feature is disabled")
