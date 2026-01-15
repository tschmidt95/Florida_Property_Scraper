from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple


@dataclass(frozen=True)
class Condition:
    field: str
    op: str
    value: Any = None


@dataclass(frozen=True)
class Trigger:
    code: str
    all: List[Condition]


def _get_field(fields: Dict[str, Any], name: str) -> Tuple[bool, Any]:
    """Return (present, value) where present means the field exists and is non-None."""

    if name not in fields:
        return False, None
    v = fields.get(name)
    if v is None:
        return False, None
    return True, v


def _contains(haystack: Any, needle: Any) -> bool:
    if haystack is None or needle is None:
        return False
    return str(needle).lower() in str(haystack).lower()


def _in_list(v: Any, items: Any) -> bool:
    if v is None or items is None:
        return False
    if isinstance(items, (list, tuple, set)):
        return v in items
    return v == items


def eval_condition(fields: Dict[str, Any], cond: Condition) -> bool:
    """Evaluate a condition.

    Missing/None fields are treated as "unknown" and always return False.
    """

    present, v = _get_field(fields, cond.field)
    if not present:
        return False

    op = (cond.op or "").strip().lower()
    if op in ("=", "==", "equals"):
        return v == cond.value
    if op in ("!=", "not_equals"):
        return v != cond.value
    if op in (">", "gt"):
        try:
            return v > cond.value
        except TypeError:
            try:
                return float(v) > float(cond.value)
            except Exception:
                return False
    if op in (">=", "gte"):
        try:
            return v >= cond.value
        except TypeError:
            try:
                return float(v) >= float(cond.value)
            except Exception:
                return False
    if op in ("<", "lt"):
        try:
            return v < cond.value
        except TypeError:
            try:
                return float(v) < float(cond.value)
            except Exception:
                return False
    if op in ("<=", "lte"):
        try:
            return v <= cond.value
        except TypeError:
            try:
                return float(v) <= float(cond.value)
            except Exception:
                return False
    if op in ("contains",):
        return _contains(v, cond.value)
    if op in ("in", "in_list"):
        return _in_list(v, cond.value)
    if op in ("is_true",):
        return bool(v) is True
    if op in ("is_false",):
        return bool(v) is False

    raise ValueError(f"Unsupported op: {cond.op}")


def compile_filters(raw: Any) -> List[Condition]:
    """Compile request filters.

    Back-compat:
    - Historical format: a list of {field, op, value}

    New UI format:
    - An object with range-style keys, e.g.
      {
        min_sqft, max_sqft,
                min_acres, max_acres,
        min_year_built, max_year_built,
        min_beds, min_baths,
        min_value, max_value,
        min_land_value, max_land_value,
        min_building_value, max_building_value,
        zoning, property_type,
        last_sale_date_start, last_sale_date_end
      }

    Notes:
    - Missing/None fields are treated as unknown by eval_condition and do not match.
    - Zoning/property_type default to substring matching (case-insensitive).
    """

    if not raw:
        return []

    # New: object form.
    if isinstance(raw, dict):
        out: List[Condition] = []

        def _norm_choice(v: Any) -> str:
            s = str(v or "").strip()
            if not s:
                return "UNKNOWN"
            return " ".join(s.upper().split())

        def _num(v: Any) -> Any:
            # Keep ints/floats; attempt to parse numeric strings.
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return v
            try:
                s = str(v).strip().replace(",", "")
                if not s:
                    return None
                return float(s) if ("." in s) else int(s)
            except Exception:
                return None

        def _add(field: str, op: str, value: Any) -> None:
            if value is None:
                return
            out.append(Condition(field=field, op=op, value=value))

        _add("living_area_sqft", ">=", _num(raw.get("min_sqft")))
        _add("living_area_sqft", "<=", _num(raw.get("max_sqft")))

        # Explicit lot size acres (UI shorthand).
        _add("lot_size_acres", ">=", _num(raw.get("min_acres")))
        _add("lot_size_acres", "<=", _num(raw.get("max_acres")))

        # Lot size filters.
        # Preferred: explicit sqft keys.
        min_lot_sqft = _num(raw.get("min_lot_size_sqft"))
        max_lot_sqft = _num(raw.get("max_lot_size_sqft"))
        if min_lot_sqft is not None or max_lot_sqft is not None:
            _add("lot_size_sqft", ">=", min_lot_sqft)
            _add("lot_size_sqft", "<=", max_lot_sqft)
        else:
            # Legacy: unit + value.
            lot_unit = str(raw.get("lot_size_unit") or "sqft").strip().lower()
            min_lot = _num(raw.get("min_lot_size"))
            max_lot = _num(raw.get("max_lot_size"))
            if lot_unit == "acres":
                _add("lot_size_acres", ">=", min_lot)
                _add("lot_size_acres", "<=", max_lot)
            else:
                # Default to sqft for unknown/missing unit.
                _add("lot_size_sqft", ">=", min_lot)
                _add("lot_size_sqft", "<=", max_lot)

        _add("year_built", ">=", _num(raw.get("min_year_built")))
        _add("year_built", "<=", _num(raw.get("max_year_built")))

        _add("beds", ">=", _num(raw.get("min_beds")))
        _add("baths", ">=", _num(raw.get("min_baths")))

        # Value filters (API naming: total/land/building)
        _add("total_value", ">=", _num(raw.get("min_value")))
        _add("total_value", "<=", _num(raw.get("max_value")))
        _add("land_value", ">=", _num(raw.get("min_land_value")))
        _add("land_value", "<=", _num(raw.get("max_land_value")))
        _add("building_value", ">=", _num(raw.get("min_building_value")))
        _add("building_value", "<=", _num(raw.get("max_building_value")))

        zoning = raw.get("zoning")
        if isinstance(zoning, str) and zoning.strip():
            _add("zoning", "contains", zoning.strip())
        elif isinstance(zoning, (list, tuple)) and zoning:
            items = [str(x).strip() for x in zoning if str(x).strip()]
            if items:
                _add("zoning", "in_list", items)

        zoning_in = raw.get("zoning_in")
        if isinstance(zoning_in, (list, tuple)) and zoning_in:
            items = [_norm_choice(x) for x in zoning_in]
            items = [x for x in items if x]
            if items:
                _add("zoning_norm", "in_list", items)

        flu_in = raw.get("future_land_use_in")
        if isinstance(flu_in, (list, tuple)) and flu_in:
            items = [_norm_choice(x) for x in flu_in]
            items = [x for x in items if x]
            if items:
                _add("future_land_use_norm", "in_list", items)

        ptype = raw.get("property_type")
        if isinstance(ptype, str) and ptype.strip():
            _add("property_type", "contains", ptype.strip())
        elif isinstance(ptype, (list, tuple)) and ptype:
            items = [str(x).strip() for x in ptype if str(x).strip()]
            if items:
                _add("property_type", "in_list", items)

        d0 = raw.get("last_sale_date_start")
        d1 = raw.get("last_sale_date_end")
        if isinstance(d0, str) and d0.strip():
            _add("last_sale_date", ">=", d0.strip())
        if isinstance(d1, str) and d1.strip():
            _add("last_sale_date", "<=", d1.strip())

        return [c for c in out if c.field and c.op]

    # Historical: list form.
    if not isinstance(raw, list):
        raise ValueError("filters must be a list or object")
    out_list: List[Condition] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        out_list.append(
            Condition(
                field=str(item.get("field", "")),
                op=str(item.get("op", "")),
                value=item.get("value"),
            )
        )
    return [c for c in out_list if c.field and c.op]


def compile_triggers(raw: Any) -> List[Trigger]:
    if not raw:
        return []
    if not isinstance(raw, list):
        raise ValueError("triggers must be a list")
    out: List[Trigger] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code", "")).strip()
        conds_raw = item.get("all")
        if not code or not isinstance(conds_raw, list):
            continue
        conds: List[Condition] = []
        for c in conds_raw:
            if not isinstance(c, dict):
                continue
            conds.append(
                Condition(
                    field=str(c.get("field", "")).strip(),
                    op=str(c.get("op", "")).strip(),
                    value=c.get("value"),
                )
            )
        conds = [c for c in conds if c.field and c.op]
        if conds:
            out.append(Trigger(code=code, all=conds))
    return out


def apply_filters(fields: Dict[str, Any], filters: Sequence[Condition]) -> bool:
    missing_ok_raw = fields.get("__missing_ok_fields")
    missing_ok: set[str] = set()
    if isinstance(missing_ok_raw, (list, tuple, set)):
        missing_ok = {str(x) for x in missing_ok_raw if str(x)}

    for f in filters:
        present, _v = _get_field(fields, f.field)
        if (not present) and f.field in missing_ok:
            continue
        if not eval_condition(fields, f):
            return False
    return True


def eval_triggers(fields: Dict[str, Any], triggers: Sequence[Trigger]) -> List[str]:
    """Return matched trigger codes.

    If a trigger references a missing PA/computed field, it won't match.
    """

    matched: List[str] = []
    for t in triggers:
        ok = True
        for c in t.all:
            if not eval_condition(fields, c):
                ok = False
                break
        if ok:
            matched.append(t.code)
    return matched
