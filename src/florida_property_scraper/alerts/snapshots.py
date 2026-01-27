"""
Snapshot + diff utilities (PA record JSON) for alerts.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any

@dataclass
class Diff:
    changed: dict[str, tuple[Any, Any]]
    def is_empty(self) -> bool:
        return not self.changed

def diff_records(old: dict | None, new: dict | None, keys: list[str]) -> Diff:
    old = old or {}
    new = new or {}
    changed = {}
    for k in keys:
        a = old.get(k)
        b = new.get(k)
        if a != b:
            changed[k] = (a, b)
    return Diff(changed=changed)
