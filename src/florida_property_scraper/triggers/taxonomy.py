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
    #
    # These keys are meant to be stable and connector-agnostic.
    # Prefer the expanded categories below for new connectors.
    OFFICIAL_RECORD = "official_record"

    # deeds
    DEED_RECORDED = "deed_recorded"
    DEED_QUITCLAIM = "deed_quitclaim"
    DEED_WARRANTY = "deed_warranty"
    DEED_PR = "deed_pr"
    DEED_TRUSTEE = "deed_trustee"
    DEED_TO_TRUST = "deed_to_trust"
    DEED_TO_LLC = "deed_to_llc"
    DEED_NOMINAL_CONSIDERATION = "deed_nominal_consideration"

    # mortgages
    MORTGAGE_RECORDED = "mortgage_recorded"
    HELOC_RECORDED = "heloc_recorded"
    LOAN_MODIFICATION = "loan_modification"
    SUBORDINATION = "subordination"
    MORTGAGE_SATISFACTION = "mortgage_satisfaction"
    MORTGAGE_ASSIGNMENT = "mortgage_assignment"

    # distress
    NOTICE_OF_DEFAULT = "notice_of_default"
    LIS_PENDENS = "lis_pendens"
    FORECLOSURE_FILING = "foreclosure_filing"
    FORECLOSURE_JUDGMENT = "foreclosure_judgment"
    CERTIFICATE_OF_SALE = "certificate_of_sale"
    CERTIFICATE_OF_TITLE = "certificate_of_title"

    # liens
    MECHANICS_LIEN = "mechanics_lien"
    HOA_LIEN = "hoa_lien"
    IRS_TAX_LIEN = "irs_tax_lien"
    STATE_TAX_LIEN = "state_tax_lien"
    CODE_ENFORCEMENT_LIEN = "code_enforcement_lien"
    JUDGMENT_LIEN = "judgment_lien"
    UTILITY_LIEN = "utility_lien"
    LIEN_RELEASE = "lien_release"

    # ucc
    UCC_FILING = "ucc_filing"

    # placeholders (reserved)
    TAX_DEED_APPLICATION = "tax_deed_application"
    PROBATE_OPENED = "probate_opened"
    DIVORCE_FILED = "divorce_filed"
    EVICTION_FILING = "eviction_filing"

    # Tax collector (distress / payment events)
    DELINQUENT_TAX = "delinquent_tax"
    TAX_CERTIFICATE_ISSUED = "tax_certificate_issued"
    TAX_CERTIFICATE_REDEEMED = "tax_certificate_redeemed"
    PAYMENT_PLAN_STARTED = "payment_plan_started"
    PAYMENT_PLAN_DEFAULTED = "payment_plan_defaulted"

    # Code enforcement (distress / compliance)
    CODE_CASE_OPENED = "code_case_opened"
    UNSAFE_STRUCTURE = "unsafe_structure"
    CONDEMNATION = "condemnation"
    FINES_IMPOSED = "fines_imposed"
    LIEN_RELEASED = "lien_released"
    COMPLIANCE_ACHIEVED = "compliance_achieved"
    REPEAT_VIOLATION = "repeat_violation"

    # Back-compat keys retained for already-ingested rows / older stubs.
    # Prefer the expanded keys above going forward.
    FORECLOSURE = "foreclosure"
    MORTGAGE = "mortgage"
    HELOC = "heloc"
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
    # Critical: foreclosure*, lis_pendens, placeholders (tax deed, probate, divorce)
    if key in {
        TriggerKey.LIS_PENDENS,
        TriggerKey.TAX_DEED_APPLICATION,
        TriggerKey.DELINQUENT_TAX,
        TriggerKey.PROBATE_OPENED,
        TriggerKey.DIVORCE_FILED,
        TriggerKey.EVICTION_FILING,
        # Back-compat
        TriggerKey.PROBATE,
        TriggerKey.DIVORCE,
    }:
        return 5
    if key.startswith("foreclosure_") or key in {TriggerKey.FORECLOSURE}:
        return 5

    # Strong: liens, mortgage satisfaction, major deed categories
    if key in {
        TriggerKey.DEED_RECORDED,
        TriggerKey.DEED_WARRANTY,
        TriggerKey.DEED_QUITCLAIM,
        TriggerKey.DEED_PR,
        TriggerKey.DEED_TRUSTEE,
        TriggerKey.DEED_TO_TRUST,
        TriggerKey.DEED_TO_LLC,
        TriggerKey.MORTGAGE_SATISFACTION,
        TriggerKey.MECHANICS_LIEN,
        TriggerKey.HOA_LIEN,
        TriggerKey.IRS_TAX_LIEN,
        TriggerKey.STATE_TAX_LIEN,
        TriggerKey.CODE_ENFORCEMENT_LIEN,
        TriggerKey.JUDGMENT_LIEN,
        TriggerKey.UTILITY_LIEN,
        TriggerKey.LIEN_RELEASE,
        # Tax collector (strong)
        TriggerKey.TAX_CERTIFICATE_ISSUED,
        TriggerKey.TAX_CERTIFICATE_REDEEMED,
        TriggerKey.PAYMENT_PLAN_STARTED,
        TriggerKey.PAYMENT_PLAN_DEFAULTED,
        # Code enforcement (strong)
        TriggerKey.CODE_CASE_OPENED,
        TriggerKey.FINES_IMPOSED,
        TriggerKey.REPEAT_VIOLATION,
        # Back-compat
        TriggerKey.SATISFACTION,
        TriggerKey.RELEASE,
        TriggerKey.LIEN_MECHANICS,
        TriggerKey.LIEN_HOA,
        TriggerKey.LIEN_IRS,
        TriggerKey.LIEN_JUDGMENT,
    }:
        return 4

    # Critical: code enforcement distress
    if key in {
        TriggerKey.UNSAFE_STRUCTURE,
        TriggerKey.CONDEMNATION,
        TriggerKey.CODE_ENFORCEMENT_LIEN,
    }:
        return 5

    # Support: recorded mortgages/assignments/modifications, UCC, nominal consideration flags
    if key in {
        TriggerKey.MORTGAGE_RECORDED,
        TriggerKey.HELOC_RECORDED,
        TriggerKey.LOAN_MODIFICATION,
        TriggerKey.SUBORDINATION,
        TriggerKey.MORTGAGE_ASSIGNMENT,
        TriggerKey.NOTICE_OF_DEFAULT,
        TriggerKey.UCC_FILING,
        TriggerKey.DEED_NOMINAL_CONSIDERATION,
        # Back-compat
        TriggerKey.MORTGAGE,
        TriggerKey.HELOC,
        TriggerKey.MORTGAGE_MODIFICATION,
        TriggerKey.OFFICIAL_RECORD,
    }:
        return 2

    # Support: code enforcement resolutions
    if key in {
        TriggerKey.LIEN_RELEASED,
        TriggerKey.COMPLIANCE_ACHIEVED,
    }:
        return 2

    if key == TriggerKey.PERMIT_ISSUED:
        return 2
    if key == TriggerKey.OWNER_MAILING_CHANGED:
        return 3
    return 1
