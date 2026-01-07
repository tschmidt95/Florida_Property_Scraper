import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional


def _list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def normalize_record(item: Dict[str, Any]) -> Dict[str, Any]:
    record = dict(item)
    record.setdefault("contact_phones", [])
    record.setdefault("contact_emails", [])
    record.setdefault("contact_addresses", [])
    record.setdefault("mortgage", [])
    record.setdefault("purchase_history", [])
    record["contact_phones"] = _list(record.get("contact_phones"))
    record["contact_emails"] = _list(record.get("contact_emails"))
    record["contact_addresses"] = _list(record.get("contact_addresses"))
    record["mortgage"] = _list(record.get("mortgage"))
    record["purchase_history"] = _list(record.get("purchase_history"))
    record.setdefault("zoning_current", "")
    record.setdefault("zoning_future", "")
    record.setdefault("owner_name", "")
    record.setdefault("mailing_address", "")
    record.setdefault("situs_address", "")
    record.setdefault("parcel_id", "")
    record.setdefault("property_url", "")
    record.setdefault("source_url", "")
    record.setdefault("county", "")
    record.setdefault("search_query", "")
    record["lead_score"] = compute_lead_score(record)
    record["dedupe_key"] = compute_dedupe_key(record)
    record["captured_at"] = record.get("captured_at") or datetime.utcnow().isoformat() + "Z"
    return record


def compute_dedupe_key(record: Dict[str, Any]) -> str:
    key_parts = [
        str(record.get("county", "")).strip().lower(),
        str(record.get("parcel_id", "")).strip().lower(),
        str(record.get("owner_name", "")).strip().lower(),
        str(record.get("situs_address", "")).strip().lower(),
    ]
    raw = "|".join(key_parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_lead_score(record: Dict[str, Any]) -> int:
    score = 0
    if record.get("owner_name"):
        score += 10
    if record.get("situs_address"):
        score += 10
    if record.get("mailing_address"):
        score += 5
    if record.get("contact_phones"):
        score += 15
    if record.get("contact_emails"):
        score += 15
    if record.get("mortgage"):
        score += 10
    if record.get("purchase_history"):
        score += 10
    if record.get("zoning_current"):
        score += 5
    if record.get("zoning_future"):
        score += 5
    return score


def record_to_json(record: Dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=True, sort_keys=True)
