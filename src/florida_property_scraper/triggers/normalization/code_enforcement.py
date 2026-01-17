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
    if re.search(r"\bcode\b.*\blien\b|\blien\b.*\bcode\b", t) and "released" not in t:
        return TriggerKey.CODE_ENFORCEMENT_LIEN

    # Strong
    if re.search(r"\bcase\b.*\bopen\w*\b|\bopened\b.*\bcase\b", t) or "code_case_opened" in t:
        return TriggerKey.CODE_CASE_OPENED
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
