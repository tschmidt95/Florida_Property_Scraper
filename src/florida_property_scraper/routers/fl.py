import re
from urllib.parse import quote_plus

from florida_property_scraper.routers.fl_coverage import FL_COUNTIES


def _flatten_entry(entry: dict) -> dict:
    flattened = dict(entry)
    capabilities = entry.get("capabilities", {})
    for key, value in capabilities.items():
        flattened.setdefault(key, value)
    if "parcel_layer" not in flattened:
        flattened["parcel_layer"] = {
            "type": "none",
            "endpoint": "",
            "id_field": "",
            "supports_geometry": False,
        }
    flattened.setdefault("notes", "")
    return flattened


_ENTRIES = {entry["slug"]: _flatten_entry(entry) for entry in FL_COUNTIES}


def canonicalize_jurisdiction_name(name: str) -> str:
    if not name:
        return ""
    cleaned = name.strip().lower()
    cleaned = re.sub(r"[\s\-]+", "_", cleaned)
    cleaned = re.sub(r"[^a-z0-9_]", "", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def get_entry(jurisdiction: str) -> dict:
    slug = canonicalize_jurisdiction_name(jurisdiction)
    entry = _ENTRIES.get(slug)
    if entry:
        return dict(entry)
    return {
        "slug": slug,
        "spider_key": f"{slug}_spider" if slug else "",
        "url_template": "",
        "query_param_style": "none",
        "pagination": "none",
        "page_param": "",
        "supports_query_param": False,
        "needs_form_post": False,
        "needs_pagination": False,
        "needs_js": False,
        "supports_owner_search": False,
        "supports_address_search": False,
        "notes": "No start url configured.",
    }


def build_request_plan(jurisdiction: str, query: str) -> dict:
    entry = get_entry(jurisdiction)
    if entry.get("needs_js"):
        return {
            "start_urls": [],
            "spider_key": entry.get("spider_key", ""),
            "needs_form_post": entry.get("needs_form_post", False),
            "pagination": entry.get("pagination", "none"),
            "page_param": entry.get("page_param", ""),
        }
    if entry.get("needs_form_post"):
        form_url = entry.get("form_url", "")
        return {
            "start_urls": [form_url] if form_url else [],
            "spider_key": entry.get("spider_key", ""),
            "needs_form_post": True,
            "pagination": entry.get("pagination", "none"),
            "page_param": entry.get("page_param", ""),
        }
    if entry.get("supports_query_param"):
        template = entry.get("url_template", "")
        if not template:
            return {
                "start_urls": [],
                "spider_key": entry.get("spider_key", ""),
                "needs_form_post": False,
                "pagination": entry.get("pagination", "none"),
                "page_param": entry.get("page_param", ""),
            }
        encoded = quote_plus(query or "")
        return {
            "start_urls": [template.format(query=encoded)],
            "spider_key": entry.get("spider_key", ""),
            "needs_form_post": False,
            "pagination": entry.get("pagination", "none"),
            "page_param": entry.get("page_param", ""),
        }
    return {
        "start_urls": [],
        "spider_key": entry.get("spider_key", ""),
        "needs_form_post": entry.get("needs_form_post", False),
        "pagination": entry.get("pagination", "none"),
        "page_param": entry.get("page_param", ""),
    }


def build_start_urls(jurisdiction: str, query: str) -> list:
    return build_request_plan(jurisdiction, query)["start_urls"]


def enabled_jurisdictions() -> list:
    return sorted(_ENTRIES.keys())


def enabled_counties() -> list:
    return enabled_jurisdictions()
