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


def normalize_courts_trigger_key(
    *,
    event_type: str | None = None,
    case_type: str | None = None,
    description: str | None = None,
) -> TriggerKey:
    """Normalize a court-docket event into a TriggerKey.

    Offline + deterministic. This is a stub-friendly heuristic that a future
    real scraper can reuse.
    """

    t = _norm_text(event_type, case_type, description)
    if not t:
        return TriggerKey.OFFICIAL_RECORD

    if "probate" in t or "estate" in t or "guardianship" in t:
        return TriggerKey.PROBATE_OPENED

    if "divorce" in t or re.search(r"\bdissolution\b", t):
        return TriggerKey.DIVORCE_FILED

    # Eviction/landlord-tenant/unlawful detainer
    if "eviction" in t or "landlord" in t or "tenant" in t or re.search(r"\bunlawful\s+detainer\b", t):
        return TriggerKey.EVICTION_FILING

    if "foreclos" in t:
        return TriggerKey.FORECLOSURE_FILING

    return TriggerKey.OFFICIAL_RECORD
