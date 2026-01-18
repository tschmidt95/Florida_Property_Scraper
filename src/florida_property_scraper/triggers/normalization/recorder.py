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


def normalize_recorder_trigger_key(
    *,
    doc_type: str | None = None,
    instrument: str | None = None,
    description: str | None = None,
) -> TriggerKey:
    """Normalize recorder document metadata into a TriggerKey.

    Offline + deterministic, focused on the additional recorder-expansion keys.
    """

    t = _norm_text(doc_type, instrument, description)
    if not t:
        return TriggerKey.OFFICIAL_RECORD

    # Critical distress
    if re.search(r"\blis\s*pendens\b|\blispendens\b", t):
        return TriggerKey.LIS_PENDENS

    # Notice of default (support by default mapping)
    if re.search(r"\bnotice\s+of\s+default\b|\bnod\b", t):
        return TriggerKey.NOTICE_OF_DEFAULT

    # Mortgages
    if re.search(r"\bassignment\b", t) and re.search(r"\bmortgage\b|\bsecurity\s+instrument\b", t):
        return TriggerKey.MORTGAGE_ASSIGNMENT

    if re.search(r"\bsatisf(action|ied)\b|\brelease\b|\bdischarge\b", t) and re.search(
        r"\bmortgage\b|\bsecurity\s+instrument\b", t
    ):
        return TriggerKey.MORTGAGE_SATISFACTION

    return TriggerKey.OFFICIAL_RECORD
