from __future__ import annotations

from enum import StrEnum


class TriggerKey(StrEnum):
    """Normalized trigger taxonomy (connector-agnostic)."""

    PERMIT_ISSUED = "permit_issued"

    # Permit categories (derived from permit_type/description, when available)
    PERMIT_DEMOLITION = "permit_demolition"
    PERMIT_STRUCTURAL = "permit_structural"
    PERMIT_ROOF = "permit_roof"
    PERMIT_HVAC = "permit_hvac"
    PERMIT_ELECTRICAL = "permit_electrical"
    PERMIT_PLUMBING = "permit_plumbing"
    PERMIT_WINDOWS = "permit_windows"
    PERMIT_DOORS = "permit_doors"
    PERMIT_SOLAR = "permit_solar"
    OWNER_MAILING_CHANGED = "owner_mailing_changed"


def default_severity_for_trigger(trigger_key: str) -> int:
    """1 (low) .. 5 (high). Keep deterministic + conservative."""

    key = (trigger_key or "").strip().lower()

    # Permit tiers:
    # - Strong = 4
    # - Support = 2
    if key in {
        TriggerKey.PERMIT_DEMOLITION,
        TriggerKey.PERMIT_STRUCTURAL,
        TriggerKey.PERMIT_ROOF,
        TriggerKey.PERMIT_HVAC,
        TriggerKey.PERMIT_ELECTRICAL,
        TriggerKey.PERMIT_PLUMBING,
    }:
        return 4
    if key in {TriggerKey.PERMIT_WINDOWS, TriggerKey.PERMIT_DOORS, TriggerKey.PERMIT_SOLAR}:
        return 2

    if key == TriggerKey.PERMIT_ISSUED:
        return 2
    if key == TriggerKey.OWNER_MAILING_CHANGED:
        return 3
    return 1
