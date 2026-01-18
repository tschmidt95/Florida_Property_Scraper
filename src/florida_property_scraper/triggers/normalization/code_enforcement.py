from __future__ import annotations

import re

from florida_property_scraper.triggers.taxonomy import TriggerKey


def _norm_text(*parts: object) -> str:
    out: list[str] = []
    for p in parts:
        if p is None:
            continue
        s = p if isinstance(p, str) else str(p)
        s = s.strip().lower()
        if s:
            out.append(s)
    return " ".join(out)


def normalize_code_enforcement_trigger_key(
    *,
    event_type: str | None = None,
    status: str | None = None,
    description: str | None = None,
) -> TriggerKey:
    """Map a raw code-enforcement event into a normalized TriggerKey.

    Offline + deterministic, intended for stub/db connectors.
    """

    t = _norm_text(event_type, status, description)
    if not t:
        return TriggerKey.OFFICIAL_RECORD

    # Critical
    if "condemn" in t or "condemnation" in t:
        return TriggerKey.CONDEMNATION
    if re.search(r"\bunsafe\b.*\bstructure\b|\bstructur\w*\b.*\bunsafe\b", t) or "unsafe_structure" in t:
        return TriggerKey.UNSAFE_STRUCTURE
    if re.search(r"\bdemolition\b.*\border\b|\border\b.*\bdemolition\b", t) or "demolition_order" in t:
        return TriggerKey.DEMOLITION_ORDER
    if re.search(r"\babatement\b.*\border\b|\border\b.*\babatement\b", t) or "abatement_order" in t:
        return TriggerKey.ABATEMENT_ORDER
    if re.search(r"\blien\b.*\brecord\w*\b|\brecord\w*\b.*\blien\b", t) and "released" not in t:
        # Keep this distinct from CODE_ENFORCEMENT_LIEN: "recorded" is often a more severe/legal step.
        return TriggerKey.LIEN_RECORDED
    if re.search(r"\bcode\b.*\blien\b|\blien\b.*\bcode\b", t) and "released" not in t:
        return TriggerKey.CODE_ENFORCEMENT_LIEN

    # Strong
    if re.search(r"\bcase\b.*\bopen\w*\b|\bopened\b.*\bcase\b", t) or "code_case_opened" in t:
        return TriggerKey.CODE_CASE_OPENED
    if re.search(r"\bboard\b.*\bhearing\b|\bhearing\b.*\bboard\b", t) or "board_hearing_set" in t:
        return TriggerKey.BOARD_HEARING_SET
    if re.search(r"\breinspection\b.*\bfail\w*\b|\bfail\w*\b.*\breinspection\b", t) or "reinspection_failed" in t:
        return TriggerKey.REINSPECTION_FAILED
    if re.search(r"\bfine\w*\b.*\bimpos\w*\b|\bimpos\w*\b.*\bfine\w*\b", t) or "fines_imposed" in t:
        return TriggerKey.FINES_IMPOSED
    if re.search(r"\brepeat\b.*\bviolation\b|\bviolation\b.*\brepeat\b", t) or "repeat_violation" in t:
        return TriggerKey.REPEAT_VIOLATION

    # Support
    if re.search(r"\blien\b.*\breleas\w*\b|\breleas\w*\b.*\blien\b", t) or "lien_released" in t:
        return TriggerKey.LIEN_RELEASED
    if re.search(r"\bcompliance\b.*\bachiev\w*\b|\bachiev\w*\b.*\bcompliance\b", t) or "compliance_achieved" in t:
        return TriggerKey.COMPLIANCE_ACHIEVED

    return TriggerKey.CODE_CASE_OPENED if "code" in t or "enforcement" in t else TriggerKey.OFFICIAL_RECORD
