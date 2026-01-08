from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, List, Sequence, Tuple

from .filters import FILTER_FIELDS, FieldDefinition


@dataclass(frozen=True)
class BuiltQuery:
    where_sql: str
    params: List[Any]


_OP_RE = re.compile(r"^(?P<field>[a-zA-Z_][a-zA-Z0-9_]*)\s*(?P<op>>=|<=|!=|==|=|>|<)\s*(?P<value>.+)$")
_IN_RE = re.compile(r"^(?P<field>[a-zA-Z_][a-zA-Z0-9_]*)\s+in\s+\[(?P<items>.*)\]$")
_BETWEEN_RE = re.compile(
    r"^(?P<field>[a-zA-Z_][a-zA-Z0-9_]*)\s+between\s+(?P<a>[^\s]+)\s+and\s+(?P<b>[^\s]+)$",
    flags=re.IGNORECASE,
)
_CONTAINS_RE = re.compile(r"^(?P<field>[a-zA-Z_][a-zA-Z0-9_]*)\s+contains\s+\"(?P<value>.*)\"$", flags=re.IGNORECASE)


def _coerce_value(field: FieldDefinition, raw: str) -> Any:
    raw = raw.strip()
    if field.type == "int":
        return int(raw)
    if field.type == "float":
        return float(raw)
    if field.type == "bool":
        return raw.lower() in ("1", "true", "yes", "y")
    # date and str: keep as string
    # (dates should be ISO YYYY-MM-DD so lexicographic comparisons work)
    if (raw.startswith("\"") and raw.endswith("\"")) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1]
    return raw


def _parse_in_list_items(items: str) -> List[str]:
    # preserve leading zeros by treating as strings
    out: List[str] = []
    for part in items.split(","):
        v = part.strip()
        if not v:
            continue
        if (v.startswith("\"") and v.endswith("\"")) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        out.append(v)
    return out


def build_where(where_clauses: Sequence[str]) -> BuiltQuery:
    parts: List[str] = []
    params: List[Any] = []

    for clause in where_clauses:
        clause = clause.strip()
        if not clause:
            continue

        m_in = _IN_RE.match(clause)
        if m_in:
            field_name = m_in.group("field")
            field = FILTER_FIELDS.get(field_name)
            if not field:
                raise ValueError(f"Unknown field: {field_name}")
            items = _parse_in_list_items(m_in.group("items"))
            if not items:
                raise ValueError("in-list must contain at least one item")
            placeholders = ",".join(["?"] * len(items))
            parts.append(f"{field.db_column} IN ({placeholders})")
            params.extend(items)
            continue

        m_between = _BETWEEN_RE.match(clause)
        if m_between:
            field_name = m_between.group("field")
            field = FILTER_FIELDS.get(field_name)
            if not field:
                raise ValueError(f"Unknown field: {field_name}")
            a = _coerce_value(field, m_between.group("a"))
            b = _coerce_value(field, m_between.group("b"))
            parts.append(f"{field.db_column} BETWEEN ? AND ?")
            params.extend([a, b])
            continue

        m_contains = _CONTAINS_RE.match(clause)
        if m_contains:
            field_name = m_contains.group("field")
            field = FILTER_FIELDS.get(field_name)
            if not field:
                raise ValueError(f"Unknown field: {field_name}")
            value = m_contains.group("value")
            parts.append(f"{field.db_column} LIKE ?")
            params.append(f"%{value}%")
            continue

        m = _OP_RE.match(clause)
        if not m:
            raise ValueError(f"Unsupported where clause: {clause}")

        field_name = m.group("field")
        op = m.group("op")
        raw_value = m.group("value")
        field = FILTER_FIELDS.get(field_name)
        if not field:
            raise ValueError(f"Unknown field: {field_name}")

        sql_op = op
        if op == "=":
            sql_op = "="
        elif op == "==":
            sql_op = "="
        elif op in (">=", "<=", ">", "<", "!="):
            sql_op = op

        value = _coerce_value(field, raw_value)
        parts.append(f"{field.db_column} {sql_op} ?")
        params.append(value)

    return BuiltQuery(where_sql=" AND ".join(parts), params=params)
