REQUIRED_FIELDS = [
    "county",
    "state",
    "jurisdiction",
    "owner",
    "address",
    "land_size",
    "building_size",
    "bedrooms",
    "bathrooms",
    "zoning",
    "property_class",
    "raw_html",
]

from .records import PropertyRecord, normalize_record, clean_text, strip_html, is_html_like  # noqa: E402


def normalize_item(item):
    if item is None:
        item = {}
    normalized = {field: item.get(field, "") for field in REQUIRED_FIELDS}
    if not normalized.get("state"):
        normalized["state"] = "fl"
    if not normalized.get("county"):
        normalized["county"] = item.get("jurisdiction", "")
    if not normalized.get("jurisdiction"):
        normalized["jurisdiction"] = normalized.get("county", "")
    for key, value in item.items():
        if key not in normalized:
            normalized[key] = value
    return normalized
