from __future__ import annotations

from dataclasses import dataclass

from florida_property_scraper.triggers.taxonomy import TriggerKey


@dataclass(frozen=True)
class PropertyAppraiserNormalized:
    trigger_key: TriggerKey
    details: dict


def normalize_property_appraiser_event(*, event_type: str, payload: dict | None = None) -> PropertyAppraiserNormalized:
    """Normalize a property-appraiser change event into a TriggerKey + details.

    This is intentionally offline + deterministic. Any scraping/fetching belongs in connectors.

    Details schema (stable contract):
      details["property_appraiser"] = {
        "old_owner": str|None,
        "new_owner": str|None,
        "old_mailing": str|None,
        "new_mailing": str|None,
        "source": str|None,
        "raw": dict
      }
    """

    et = (event_type or "").strip().lower()
    p = payload or {}

    def _as_str(v: object) -> str | None:
        if v is None:
            return None
        s = v if isinstance(v, str) else str(v)
        s = s.strip()
        return s or None

    old_owner = _as_str(p.get("old_owner"))
    new_owner = _as_str(p.get("new_owner"))
    old_mailing = _as_str(p.get("old_mailing"))
    new_mailing = _as_str(p.get("new_mailing"))
    source = _as_str(p.get("source"))

    if et in {"property_appraiser.owner_mailing_changed", "pa.owner_mailing_changed"}:
        tk = TriggerKey.OWNER_MAILING_CHANGED
    elif et in {"property_appraiser.owner_name_changed", "pa.owner_name_changed"}:
        tk = TriggerKey.OWNER_NAME_CHANGED
    elif et in {"property_appraiser.deed_last_sale_updated", "pa.deed_last_sale_updated"}:
        tk = TriggerKey.DEED_LAST_SALE_UPDATED
    else:
        # Keep conservative fallback for unknown PA events.
        tk = TriggerKey.OFFICIAL_RECORD

    details = {
        "property_appraiser": {
            "old_owner": old_owner,
            "new_owner": new_owner,
            "old_mailing": old_mailing,
            "new_mailing": new_mailing,
            "source": source,
            "raw": dict(p),
        }
    }

    return PropertyAppraiserNormalized(trigger_key=tk, details=details)
