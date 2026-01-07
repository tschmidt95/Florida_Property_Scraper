import json
from typing import Dict, List, Optional

from florida_property_scraper.normalize import normalize_address, normalize_text


def _sale_fields(observation: Dict[str, object]) -> Dict[str, str]:
    return {
        "last_sale_date": str(observation.get("last_sale_date") or ""),
        "last_sale_price": str(observation.get("last_sale_price") or ""),
        "deed_type": str(observation.get("deed_type") or ""),
    }


def generate_events(old_obs: Optional[Dict[str, object]], new_obs: Dict[str, object]) -> List[Dict[str, object]]:
    if not old_obs:
        return []
    events: List[Dict[str, object]] = []
    event_base = {
        "property_uid": new_obs.get("property_uid"),
        "county": new_obs.get("county"),
        "event_at": new_obs.get("observed_at"),
        "run_id": new_obs.get("run_id"),
    }
    old_owner = normalize_text(old_obs.get("owner_name"))
    new_owner = normalize_text(new_obs.get("owner_name"))
    if old_owner != new_owner:
        events.append(
            {
                **event_base,
                "event_type": "OWNER_CHANGED",
                "old_value": old_obs.get("owner_name"),
                "new_value": new_obs.get("owner_name"),
                "details_json": json.dumps({"field": "owner_name"}, ensure_ascii=True),
            }
        )
    old_mailing = normalize_address(old_obs.get("mailing_address"))
    new_mailing = normalize_address(new_obs.get("mailing_address"))
    if old_mailing != new_mailing:
        events.append(
            {
                **event_base,
                "event_type": "MAILING_ADDRESS_CHANGED",
                "old_value": old_obs.get("mailing_address"),
                "new_value": new_obs.get("mailing_address"),
                "details_json": json.dumps({"field": "mailing_address"}, ensure_ascii=True),
            }
        )
    old_sale = _sale_fields(old_obs)
    new_sale = _sale_fields(new_obs)
    if old_sale["last_sale_date"] != new_sale["last_sale_date"] or old_sale["last_sale_price"] != new_sale["last_sale_price"]:
        events.append(
            {
                **event_base,
                "event_type": "SALE_DETECTED",
                "old_value": json.dumps(old_sale, ensure_ascii=True),
                "new_value": json.dumps(new_sale, ensure_ascii=True),
                "details_json": json.dumps({"field": "sale"}, ensure_ascii=True),
            }
        )
    return events
