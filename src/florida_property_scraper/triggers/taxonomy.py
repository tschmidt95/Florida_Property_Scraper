from __future__ import annotations

from enum import StrEnum


class TriggerKey(StrEnum):
    """Normalized trigger taxonomy (connector-agnostic)."""

    PERMIT_ISSUED = "permit_issued"
    OWNER_MAILING_CHANGED = "owner_mailing_changed"


def default_severity_for_trigger(trigger_key: str) -> int:
    """1 (low) .. 5 (high). Keep deterministic + conservative."""

    key = (trigger_key or "").strip().lower()
    if key == TriggerKey.PERMIT_ISSUED:
        return 2
    if key == TriggerKey.OWNER_MAILING_CHANGED:
        return 3
    return 1
