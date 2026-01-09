from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


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
        return v > cond.value
    if op in (">=", "gte"):
        return v >= cond.value
    if op in ("<", "lt"):
        return v < cond.value
    if op in ("<=", "lte"):
        return v <= cond.value
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
    if not raw:
        return []
    if not isinstance(raw, list):
        raise ValueError("filters must be a list")
    out: List[Condition] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        out.append(Condition(field=str(item.get("field", "")), op=str(item.get("op", "")), value=item.get("value")))
    return [c for c in out if c.field and c.op]


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
    for f in filters:
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
