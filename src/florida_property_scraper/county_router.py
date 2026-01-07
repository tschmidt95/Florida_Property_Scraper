import re
from urllib.parse import quote_plus


_COUNTY_ENTRIES = {
    "broward": {
        "slug": "broward",
        "spider_key": "broward_spider",
        "url_template": "https://www.broward.org/propertysearch/Pages/OwnerSearch.aspx?owner={query}",
        "query_param_style": "template",
        "pagination": "next_link",
        "page_param": "",
        "supports_owner_search": True,
        "supports_address_search": False,
        "notes": "Owner search via query parameter.",
    },
    "alachua": {
        "slug": "alachua",
        "spider_key": "alachua_spider",
        "url_template": "https://www.alachuaclerk.com/propertysearch/search.aspx?owner={query}",
        "query_param_style": "template",
        "pagination": "none",
        "page_param": "",
        "supports_owner_search": True,
        "supports_address_search": True,
        "notes": "Owner search via query parameter.",
    },
    "seminole": {
        "slug": "seminole",
        "spider_key": "seminole_spider",
        "url_template": "",
        "query_param_style": "form",
        "form_url": "https://www.seminolecountyfl.gov/property-search",
        "form_fields_template": {"owner": "{query}"},
        "pagination": "none",
        "page_param": "",
        "supports_owner_search": True,
        "supports_address_search": True,
        "notes": "Owner search via form submit.",
    },
    "orange": {
        "slug": "orange",
        "spider_key": "orange_spider",
        "url_template": "https://www.orangecountyfl.net/property-search?owner={query}",
        "query_param_style": "template",
        "pagination": "page_param",
        "page_param": "page",
        "supports_owner_search": True,
        "supports_address_search": True,
        "notes": "Owner search via query parameter.",
    },
    "palm_beach": {
        "slug": "palm_beach",
        "spider_key": "palm_beach_spider",
        "url_template": "https://www.pbcgov.org/papa/searchproperty.aspx?owner={query}",
        "query_param_style": "template",
        "pagination": "none",
        "page_param": "",
        "supports_owner_search": True,
        "supports_address_search": True,
        "notes": "Owner search via query parameter.",
    },
    "miami_dade": {
        "slug": "miami_dade",
        "spider_key": "miami_dade_spider",
        "url_template": "https://www.miami-dadeclerk.com/ocs/Search.aspx?search={query}",
        "query_param_style": "template",
        "pagination": "none",
        "page_param": "",
        "supports_owner_search": True,
        "supports_address_search": True,
        "notes": "Owner search via query parameter.",
    },
    "hillsborough": {
        "slug": "hillsborough",
        "spider_key": "hillsborough_spider",
        "url_template": "https://www.hillsboroughcounty.org/property-search?owner={query}",
        "query_param_style": "template",
        "pagination": "none",
        "page_param": "",
        "supports_owner_search": True,
        "supports_address_search": True,
        "notes": "Owner search via query parameter.",
    },
    "pinellas": {
        "slug": "pinellas",
        "spider_key": "pinellas_spider",
        "url_template": "https://www.pinellascounty.org/property-search?owner={query}",
        "query_param_style": "template",
        "pagination": "none",
        "page_param": "",
        "supports_owner_search": True,
        "supports_address_search": True,
        "notes": "Owner search via query parameter.",
    },
}


def canonicalize_county_name(name: str) -> str:
    if not name:
        return ""
    cleaned = name.strip().lower()
    cleaned = re.sub(r"[\s\-]+", "_", cleaned)
    cleaned = re.sub(r"[^a-z0-9_]", "", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def get_county_entry(slug: str) -> dict:
    slug = canonicalize_county_name(slug)
    entry = _COUNTY_ENTRIES.get(slug)
    if entry:
        return dict(entry)
    return {
        "slug": slug,
        "spider_key": f"{slug}_spider" if slug else "",
        "url_template": "",
        "query_param_style": "none",
        "pagination": "none",
        "page_param": "",
        "supports_owner_search": False,
        "supports_address_search": False,
        "notes": "No start url configured.",
    }


def build_start_urls(slug: str, query: str) -> list:
    entry = get_county_entry(slug)
    style = entry.get("query_param_style")
    if style == "template":
        template = entry.get("url_template", "")
        if not template:
            return []
        encoded = quote_plus(query or "")
        return [template.format(query=encoded)]
    if style == "form":
        form_url = entry.get("form_url", "")
        if not form_url:
            return []
        return [form_url]
    return []


def enabled_counties() -> list:
    return sorted(_COUNTY_ENTRIES.keys())
