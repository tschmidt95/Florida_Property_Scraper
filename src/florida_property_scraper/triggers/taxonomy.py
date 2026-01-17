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

    # Official records (recorded documents)
    OFFICIAL_RECORD = "official_record"
    DEED_WARRANTY = "deed_warranty"
    DEED_QUITCLAIM = "deed_quitclaim"
    DEED_TRUSTEE = "deed_trustee"
    DEED_NOMINAL = "deed_nominal"
    LIS_PENDENS = "lis_pendens"
    FORECLOSURE = "foreclosure"
    MORTGAGE = "mortgage"
    HELOC = "heloc"
    MORTGAGE_ASSIGNMENT = "mortgage_assignment"
    MORTGAGE_MODIFICATION = "mortgage_modification"
    SATISFACTION = "satisfaction"
    RELEASE = "release"
    LIEN_MECHANICS = "lien_mechanics"
    LIEN_IRS = "lien_irs"
    LIEN_HOA = "lien_hoa"
    LIEN_JUDGMENT = "lien_judgment"
    PROBATE = "probate"
    DIVORCE = "divorce"
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

    # Official records tiers
    if key in {
        TriggerKey.LIS_PENDENS,
        TriggerKey.FORECLOSURE,
        TriggerKey.PROBATE,
        TriggerKey.DIVORCE,
        TriggerKey.LIEN_IRS,
        TriggerKey.LIEN_JUDGMENT,
    }:
        return 5
    if key in {
        TriggerKey.DEED_WARRANTY,
        TriggerKey.DEED_QUITCLAIM,
        TriggerKey.DEED_TRUSTEE,
        TriggerKey.DEED_NOMINAL,
        TriggerKey.SATISFACTION,
        TriggerKey.RELEASE,
        TriggerKey.HELOC,
        TriggerKey.LIEN_MECHANICS,
        TriggerKey.LIEN_HOA,
    }:
        return 4
    if key in {
        TriggerKey.MORTGAGE,
        TriggerKey.MORTGAGE_ASSIGNMENT,
        TriggerKey.MORTGAGE_MODIFICATION,
        TriggerKey.OFFICIAL_RECORD,
    }:
        return 2

    if key == TriggerKey.PERMIT_ISSUED:
        return 2
    if key == TriggerKey.OWNER_MAILING_CHANGED:
        return 3
    return 1
