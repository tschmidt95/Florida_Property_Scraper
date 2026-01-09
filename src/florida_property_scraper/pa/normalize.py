from __future__ import annotations

from dataclasses import MISSING, Field, fields
from typing import Any, Dict, Mapping

from .schema import PAProperty


def apply_defaults(partial: Mapping[str, Any] | None) -> PAProperty:
    """Apply the PA canonical defaults to a partial PA dict.

    Rules:
    - Missing keys are filled with the PAProperty default.
    - None values are coerced to the default for that field type.
    - Extra keys are ignored.
    """

    if partial is None:
        partial = {}

    def _default_for_field(f: Field[Any]) -> Any:
        if f.default is not MISSING:
            return f.default
        if f.default_factory is not MISSING:  # type: ignore[comparison-overlap]
            return f.default_factory()  # type: ignore[misc]
        return None

    data: Dict[str, Any] = {}
    for f in fields(PAProperty):
        default_value = _default_for_field(f)
        if f.name in partial:
            value = partial.get(f.name)
            if value is None:
                value = default_value
            data[f.name] = value
        else:
            data[f.name] = default_value

    # Defensive: list fields must never be None.
    if data.get("exemptions") is None:
        data["exemptions"] = []
    if data.get("owner_names") is None:
        data["owner_names"] = []

    return PAProperty(**data)
