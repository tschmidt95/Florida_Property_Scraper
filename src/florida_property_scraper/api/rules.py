from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import re
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


PROPERTY_TYPE_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("sfr", ("single family", "single-family", "sfr", "sfh", "detached")),
    ("condo", ("condo", "condominium")),
    ("townhome", ("townhome", "town home", "townhouse", "town house")),
    ("mfh", ("multi", "duplex", "triplex", "fourplex", "mf", "mfh", "apartment")),
    ("mobile_home", ("mobile", "manufactured", "trailer")),
    ("mixed_use", ("mixed use", "mixed-use")),
    ("retail", ("retail", "store", "shop", "shopping")),
    ("office", ("office", "professional")),
    ("industrial", ("industrial", "warehouse", "manufactur", "distribution")),
    ("agricultural", ("ag", "agricultural", "farm", "ranch")),
    ("vacant_land", ("vacant", "land", "lot", "acreage")),
    ("commercial", ("commercial", "comm")),
    ("residential", ("residential", "res")),
]


def normalize_property_type(*values: Any) -> str:
    """Normalize property type from any combination of PA fields.

    Returns a deterministic enum-like string whenever inputs exist.
    """

    raw_parts: list[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        raw_parts.append(s)

    if not raw_parts:
        return ""

    combined = " ".join(raw_parts).strip().lower()
    combined = re.sub(r"\s+", " ", combined)

    for label, keywords in PROPERTY_TYPE_RULES:
        for kw in keywords:
            if kw in combined:
                return label

    # Normalize common coded land-use/class values.
    if re.match(r"^r\d+", combined):
        return "residential"
    if re.match(r"^c\d+", combined):
        return "commercial"

    # Fall back to a sanitized string so it is never empty when inputs exist.
    return combined


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

    # Date comparisons (sale date filters): accept ISO dates, MM/DD/YYYY, and ISO timestamps.
    def _as_date(v_any: Any) -> date | None:
        if v_any is None:
            return None
        if isinstance(v_any, date) and not isinstance(v_any, datetime):
            return v_any
        if isinstance(v_any, datetime):
            try:
                return v_any.date()
            except Exception:
                return None
        if not isinstance(v_any, str):
            return None
        s = v_any.strip()
        if not s:
            return None
        # ISO datetime
        if "T" in s:
            try:
                ss = s.replace("Z", "+00:00")
                return datetime.fromisoformat(ss).date()
            except Exception:
                return None
        # ISO date
        try:
            return date.fromisoformat(s)
        except Exception:
            pass
        # M/D/YYYY or MM/DD/YYYY
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
        if m:
            try:
                mm = int(m.group(1))
                dd = int(m.group(2))
                yy = int(m.group(3))
                return date(yy, mm, dd)
            except Exception:
                return None
        return None

    cv: Any = cond.value
    if cond.field in {"last_sale_date", "sale_date"} and op in {
        "=",
        "==",
        "equals",
        "!=",
        "not_equals",
        ">",
        "gt",
        ">=",
        "gte",
        "<",
        "lt",
        "<=",
        "lte",
    }:
        v_d = _as_date(v)
        c_d = _as_date(cv)
        if v_d is not None and c_d is not None:
            v = v_d
            cv = c_d

    if op in ("=", "==", "equals"):
        return v == cv
    if op in ("!=", "not_equals"):
        return v != cv
    if op in (">", "gt"):
        try:
            return v > cv
        except TypeError:
            try:
                return float(v) > float(cv)
            except Exception:
                return False
    if op in (">=", "gte"):
        try:
            return v >= cv
        except TypeError:
            try:
                return float(v) >= float(cv)
            except Exception:
                return False
    if op in ("<", "lt"):
        try:
            return v < cv
        except TypeError:
            try:
                return float(v) < float(cv)
            except Exception:
                return False
    if op in ("<=", "lte"):
        try:
            return v <= cv
        except TypeError:
            try:
                return float(v) <= float(cv)
            except Exception:
                return False
    if op in ("contains",):
        return _contains(v, cv)
    if op in ("in", "in_list"):
        return _in_list(v, cv)
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
            # For request-side filter inputs, blanks must be treated as unset.
            s = str(v or "").strip()
            if not s:
                return ""
            return " ".join(s.upper().split())

        def _num(v: Any) -> float | None:
            """Best-effort numeric parsing.

            Policy: always parse strings via float() (parseFloat semantics) so
            decimal inputs like "0.5" are preserved.
            """

            if v is None:
                return None
            if isinstance(v, (int, float)):
                try:
                    out_v = float(v)
                except Exception:
                    return None
                return out_v if out_v == out_v else None  # NaN guard
            try:
                s = str(v).strip().replace(",", "")
                if not s:
                    return None
                out_v = float(s)
                return out_v if out_v == out_v else None
            except Exception:
                return None

        def _year_int(v: Any) -> int | None:
            """Parse year-like values as int.

            Missing/blank returns None (never 0). Non-integer values are rejected.
            """

            if v is None:
                return None
            if isinstance(v, bool):
                return None
            if isinstance(v, int):
                return v if v > 0 else None
            if isinstance(v, float):
                if not (v == v):
                    return None
                if float(int(v)) != float(v):
                    return None
                y = int(v)
                return y if y > 0 else None
            try:
                s = str(v).strip().replace(",", "")
                if not s:
                    return None
                try:
                    y = int(s)
                    return y if y > 0 else None
                except Exception:
                    f = float(s)
                    if not (f == f):
                        return None
                    if float(int(f)) != float(f):
                        return None
                    y = int(f)
                    return y if y > 0 else None
            except Exception:
                return None

        def _add(field: str, op: str, value: Any) -> None:
            if value is None:
                return
            out.append(Condition(field=field, op=op, value=value))

        def _date(v: Any) -> date | None:
            if v is None:
                return None
            if isinstance(v, date) and not isinstance(v, datetime):
                return v
            if isinstance(v, datetime):
                try:
                    return v.date()
                except Exception:
                    return None
            if not isinstance(v, str):
                return None
            s = v.strip()
            if not s:
                return None
            if "T" in s:
                try:
                    ss = s.replace("Z", "+00:00")
                    return datetime.fromisoformat(ss).date()
                except Exception:
                    return None
            try:
                return date.fromisoformat(s)
            except Exception:
                pass
            m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
            if m:
                try:
                    mm = int(m.group(1))
                    dd = int(m.group(2))
                    yy = int(m.group(3))
                    return date(yy, mm, dd)
                except Exception:
                    return None
            return None

        _add("living_area_sqft", ">=", _num(raw.get("min_sqft")))
        _add("living_area_sqft", "<=", _num(raw.get("max_sqft")))

        # Lot size filters: normalize to sqft internally.
        # Preferred: explicit sqft keys.
        min_lot_sqft = _num(raw.get("min_lot_size_sqft"))
        max_lot_sqft = _num(raw.get("max_lot_size_sqft"))

        if min_lot_sqft is None and max_lot_sqft is None:
            # UI shorthand (acres) - only if sqft keys are not present.
            min_acres = _num(raw.get("min_acres"))
            max_acres = _num(raw.get("max_acres"))
            if min_acres is not None:
                min_lot_sqft = float(min_acres) * 43560.0
            if max_acres is not None:
                max_lot_sqft = float(max_acres) * 43560.0

        if min_lot_sqft is None and max_lot_sqft is None:
            # Legacy: unit + value.
            lot_unit = str(raw.get("lot_size_unit") or "sqft").strip().lower()
            min_lot = _num(raw.get("min_lot_size"))
            max_lot = _num(raw.get("max_lot_size"))
            if lot_unit == "acres":
                if min_lot is not None:
                    min_lot_sqft = float(min_lot) * 43560.0
                if max_lot is not None:
                    max_lot_sqft = float(max_lot) * 43560.0
            else:
                min_lot_sqft = min_lot
                max_lot_sqft = max_lot

        _add("lot_size_sqft", ">=", min_lot_sqft)
        _add("lot_size_sqft", "<=", max_lot_sqft)

        _add("year_built", ">=", _year_int(raw.get("min_year_built")))
        _add("year_built", "<=", _year_int(raw.get("max_year_built")))

        _add("beds", ">=", _num(raw.get("min_beds")))
        _add("baths", ">=", _num(raw.get("min_baths")))
        _add("ownership_years", ">=", _num(raw.get("min_ownership_years")))

        # Value filters (API naming: total/land/building)
        _add("total_value", ">=", _num(raw.get("min_value")))
        _add("total_value", "<=", _num(raw.get("max_value")))
        _add("total_value", ">=", _num(raw.get("min_just_value")))
        _add("total_value", "<=", _num(raw.get("max_just_value")))
        _add("assessed_value", ">=", _num(raw.get("min_assessed_value")))
        _add("assessed_value", "<=", _num(raw.get("max_assessed_value")))
        _add("taxable_value", ">=", _num(raw.get("min_taxable_value")))
        _add("taxable_value", "<=", _num(raw.get("max_taxable_value")))
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
            _add("property_type_norm", "contains", ptype.strip())
        elif isinstance(ptype, (list, tuple)) and ptype:
            items = [str(x).strip() for x in ptype if str(x).strip()]
            if items:
                _add("property_type_norm", "in_list", items)

        future_land_use = raw.get("future_land_use")
        if isinstance(future_land_use, str) and future_land_use.strip():
            _add("future_land_use", "contains", future_land_use.strip())

        d0 = _date(raw.get("last_sale_date_start"))
        d1 = _date(raw.get("last_sale_date_end"))
        if d0 is not None and d1 is not None and d0 > d1:
            # UX: if the user flips start/end, auto-swap.
            d0, d1 = d1, d0
        _add("last_sale_date", ">=", d0)
        _add("last_sale_date", "<=", d1)

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


def apply_filters_explain(
    fields: Dict[str, Any],
    filters: Sequence[Condition],
) -> tuple[bool, str | None]:
    """Evaluate filters and return (passed, reason).

    Reason format:
    - "<field>:missing" if required field is missing
    - "<field>:<op>" if condition evaluated to False
    """

    missing_ok_raw = fields.get("__missing_ok_fields")
    missing_ok: set[str] = set()
    if isinstance(missing_ok_raw, (list, tuple, set)):
        missing_ok = {str(x) for x in missing_ok_raw if str(x)}

    for f in filters:
        present, _v = _get_field(fields, f.field)
        if (not present) and f.field in missing_ok:
            continue
        if not present:
            return False, f"{f.field}:missing"
        try:
            if not eval_condition(fields, f):
                return False, f"{f.field}:{f.op}"
        except Exception:
            return False, f"{f.field}:{f.op}"
    return True, None


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
