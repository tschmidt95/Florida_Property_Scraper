from __future__ import annotations

import re

from florida_property_scraper.triggers.taxonomy import TriggerKey


def _norm_text(*parts: object) -> str:
    out: list[str] = []
    for p in parts:
        if p is None:
            continue
        if isinstance(p, str):
            s = p
        else:
            s = str(p)
        s = s.strip().lower()
        if s:
            out.append(s)
    return " ".join(out)


def normalize_official_record_trigger_key(
    *,
    doc_type: str | None = None,
    instrument: str | None = None,
    description: str | None = None,
    parties: str | None = None,
    consideration: str | None = None,
) -> TriggerKey:
    """Map raw official record metadata to a normalized TriggerKey.

    This is intentionally heuristic and conservative. It should be:
    - offline
    - deterministic
    - resilient to recorder free-text variants
    """

    t = _norm_text(doc_type, instrument, description, parties)
    c = _norm_text(consideration)

    if not t:
        return TriggerKey.OFFICIAL_RECORD

    # --- Critical distress signals ---
    if re.search(r"\blis\s*pendens\b|\blispendens\b", t):
        return TriggerKey.LIS_PENDENS

    if re.search(r"\bforeclos(ure|ing)\b", t):
        if re.search(r"\bjudg(ment|mt)\b|\bfinal\s+judg\b", t):
            return TriggerKey.FORECLOSURE_JUDGMENT
        return TriggerKey.FORECLOSURE_FILING

    if re.search(r"\bcertificate\s+of\s+sale\b|\bcert\.?\s+of\s+sale\b", t):
        return TriggerKey.CERTIFICATE_OF_SALE
    if re.search(r"\bcertificate\s+of\s+title\b|\bcert\.?\s+of\s+title\b", t):
        return TriggerKey.CERTIFICATE_OF_TITLE

    # --- Liens (strong) ---
    if re.search(r"\bmechanic'?s\s+lien\b|\bconstruction\s+lien\b|\bclaim\s+of\s+lien\b", t):
        return TriggerKey.MECHANICS_LIEN

    if re.search(r"\bhoa\b|\bhome\s*owners\b|\bhomeowners\b|\bcondo(minium)?\b", t) and re.search(
        r"\blien\b", t
    ):
        return TriggerKey.HOA_LIEN

    if re.search(r"\birs\b|\bfederal\s+tax\s+lien\b", t):
        return TriggerKey.IRS_TAX_LIEN

    if re.search(r"\bstate\s+tax\s+lien\b|\bdept\.?\s+of\s+revenue\b", t):
        return TriggerKey.STATE_TAX_LIEN

    if re.search(r"\bcode\s+enforcement\b", t) and re.search(r"\blien\b", t):
        return TriggerKey.CODE_ENFORCEMENT_LIEN

    if re.search(r"\bjudg(e)?ment\b", t) and re.search(r"\blien\b", t):
        return TriggerKey.JUDGMENT_LIEN

    if re.search(r"\butility\b", t) and re.search(r"\blien\b", t):
        return TriggerKey.UTILITY_LIEN

    if re.search(r"\brelease\b|\bsatisfaction\b|\bdischarge\b", t) and re.search(r"\blien\b", t):
        return TriggerKey.LIEN_RELEASE

    # --- Deeds (strong / support) ---
    if re.search(r"\bdeed\b", t):
        if re.search(r"\bwarranty\b", t):
            return TriggerKey.DEED_WARRANTY
        if re.search(r"\bquit\s*claim\b|\bquitclaim\b", t):
            return TriggerKey.DEED_QUITCLAIM
        if re.search(r"\btrustee\b", t):
            return TriggerKey.DEED_TRUSTEE
        if re.search(r"\bpersonal\s+representative\b|\bpr\s+deed\b", t):
            return TriggerKey.DEED_PR
        if re.search(r"\bto\s+trust\b|\btrust\b", t) and re.search(r"\bdeed\b", t):
            return TriggerKey.DEED_TO_TRUST
        if re.search(r"\bllc\b|\blimited\s+liability\b", t):
            return TriggerKey.DEED_TO_LLC

        # Nominal consideration (support)
        combo = f"{t} {c}".strip()
        if re.search(r"\b\$0\b|\b0\b\s*\$|\bnominal\b|\bno\s+consideration\b|\bfor\s+love\s+and\s+affection\b", combo):
            return TriggerKey.DEED_NOMINAL_CONSIDERATION

        return TriggerKey.DEED_RECORDED

    # --- Mortgages (support / strong) ---
    if re.search(r"\bheloc\b|\bhome\s+equity\b", t):
        return TriggerKey.HELOC_RECORDED

    if re.search(r"\bmortgage\b", t) or re.search(r"\bsecurity\s+instrument\b", t):
        if re.search(r"\bsatisf(action|ied)\b|\brelease\b|\bdischarge\b", t):
            return TriggerKey.MORTGAGE_SATISFACTION
        if re.search(r"\bassignment\b", t):
            return TriggerKey.MORTGAGE_ASSIGNMENT
        if re.search(r"\bmodif(ication|y)\b|\bloan\s+modif\b", t):
            return TriggerKey.LOAN_MODIFICATION
        if re.search(r"\bsubordinat(e|ion)\b", t):
            return TriggerKey.SUBORDINATION
        return TriggerKey.MORTGAGE_RECORDED

    # --- UCC (support) ---
    if re.search(r"\bucc\b", t):
        return TriggerKey.UCC_FILING

    return TriggerKey.OFFICIAL_RECORD
