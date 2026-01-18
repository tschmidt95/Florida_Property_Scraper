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


def normalize_liens_trigger_key(
    *,
    event_type: str | None = None,
    lien_type: str | None = None,
    status: str | None = None,
    description: str | None = None,
) -> TriggerKey:
    """Normalize a lien-related event into a TriggerKey.

    Offline + deterministic, intended for stub connectors and future recorder scrapers.

    Note: judgment "satisfaction" should map to LIEN_RELEASE (not mortgage satisfaction).
    """

    t = _norm_text(event_type, lien_type, status, description)
    if not t:
        return TriggerKey.OFFICIAL_RECORD

    # Releases/satisfactions (generic)
    if re.search(r"\breleas(e|ed|ing)\b|\bsatisf(action|ied)\b|\bdischarge\b", t):
        return TriggerKey.LIEN_RELEASE

    if "mechanic" in t or "construction" in t:
        return TriggerKey.MECHANICS_LIEN

    if "hoa" in t or "homeowners" in t or "condo" in t:
        return TriggerKey.HOA_LIEN

    if "irs" in t or "federal tax" in t:
        return TriggerKey.IRS_TAX_LIEN

    if "judgment" in t:
        return TriggerKey.JUDGMENT_LIEN

    # Fallback
    return TriggerKey.OFFICIAL_RECORD
