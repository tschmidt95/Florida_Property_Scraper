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


def normalize_tax_collector_trigger_key(
    *,
    event_type: str | None = None,
    status: str | None = None,
    description: str | None = None,
) -> TriggerKey:
    """Map a raw tax-collector event into a normalized TriggerKey.

    Offline + deterministic, intended for stub/db connectors.
    """

    t = _norm_text(event_type, status, description)
    if not t:
        return TriggerKey.OFFICIAL_RECORD  # conservative fallback

    # Critical
    if re.search(r"\bdelinquent\b.*\btax\b|\btax\b.*\bdelinquent\b", t) or "delinquent_tax" in t:
        return TriggerKey.DELINQUENT_TAX
    if re.search(r"\btax\s+deed\b.*\bapplication\b|\bapplication\b.*\btax\s+deed\b", t) or "tax_deed_application" in t:
        return TriggerKey.TAX_DEED_APPLICATION

    # Strong
    if re.search(r"\btax\s+certificate\b.*\bissued\b|\bcertificate\b.*\bissued\b", t) or "tax_certificate_issued" in t:
        return TriggerKey.TAX_CERTIFICATE_ISSUED
    if re.search(r"\btax\s+certificate\b.*\bredeem\w*\b|\bcertificate\b.*\bredeem\w*\b", t) or "tax_certificate_redeemed" in t:
        return TriggerKey.TAX_CERTIFICATE_REDEEMED
    if re.search(r"\bpayment\s+plan\b.*\bstart\w*\b", t) or "payment_plan_started" in t:
        return TriggerKey.PAYMENT_PLAN_STARTED
    if re.search(r"\bpayment\s+plan\b.*\bdefault\w*\b|\bplan\b.*\bdefault\w*\b", t) or "payment_plan_defaulted" in t:
        return TriggerKey.PAYMENT_PLAN_DEFAULTED

    # Fallback (still tax-family unknown)
    return TriggerKey.DELINQUENT_TAX if "tax" in t else TriggerKey.OFFICIAL_RECORD
