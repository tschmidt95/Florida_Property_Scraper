from florida_property_scraper.routers import fl


_ROUTERS = {
    "fl": fl,
}


def get_router(state: str):
    state_key = (state or "").lower()
    return _ROUTERS.get(state_key)


def enabled_jurisdictions(state: str) -> list:
    router = get_router(state)
    if not router:
        return []
    return router.enabled_jurisdictions()


def build_start_urls(state: str, jurisdiction: str, query: str) -> list:
    router = get_router(state)
    if not router:
        return []
    return router.build_start_urls(jurisdiction, query)


def get_entry(state: str, jurisdiction: str) -> dict:
    router = get_router(state)
    if not router:
        return {
            "slug": "",
            "spider_key": "",
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
            "notes": "No router configured.",
        }
    return router.get_entry(jurisdiction)


def build_request_plan(state: str, jurisdiction: str, query: str) -> dict:
    router = get_router(state)
    if not router:
        return {"start_urls": []}
    return router.build_request_plan(jurisdiction, query)
