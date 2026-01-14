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
    if data.get("sources") is None:
        data["sources"] = []
    if data.get("field_provenance") is None:
        data["field_provenance"] = {}

    # Light normalization for filterable fields (avoid None/str surprises).
    # Keep this intentionally conservative: we do not infer missing values.
    try:
        data["zoning"] = str(data.get("zoning") or "")
    except Exception:
        data["zoning"] = ""
    try:
        data["use_type"] = str(data.get("use_type") or "")
    except Exception:
        data["use_type"] = ""

    def _to_float(v: Any) -> float:
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(str(v).strip().replace(",", ""))
        except Exception:
            return 0.0

    def _to_int(v: Any) -> int:
        return int(round(_to_float(v)))

    for k in (
        "living_sf",
        "building_sf",
        "land_sf",
        "just_value",
        "assessed_value",
        "taxable_value",
        "land_value",
        "improvement_value",
    ):
        if k in data:
            data[k] = _to_float(data.get(k))

    for k in ("bedrooms", "units", "building_count", "year_built", "effective_year"):
        if k in data:
            data[k] = _to_int(data.get(k))

    if "bathrooms" in data:
        data["bathrooms"] = _to_float(data.get("bathrooms"))

    return PAProperty(**data)
