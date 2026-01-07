import warnings

from florida_property_scraper.routers.fl import canonicalize_jurisdiction_name
from florida_property_scraper.routers.registry import (
    build_request_plan as _build_request_plan,
    build_start_urls as _build_start_urls,
    enabled_jurisdictions as _enabled_jurisdictions,
    get_entry as _get_entry,
)


def canonicalize_county_name(name: str) -> str:
    warnings.warn(
        "county_router is deprecated; use florida_property_scraper.routers.fl instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return canonicalize_jurisdiction_name(name)


def get_county_entry(slug: str) -> dict:
    warnings.warn(
        "county_router is deprecated; use florida_property_scraper.routers.fl instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return _get_entry("fl", slug)


def build_start_urls(slug: str, query: str) -> list:
    warnings.warn(
        "county_router is deprecated; use florida_property_scraper.routers.fl instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return _build_start_urls("fl", slug, query)


def build_request_plan(slug: str, query: str) -> dict:
    warnings.warn(
        "county_router is deprecated; use florida_property_scraper.routers.fl instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return _build_request_plan("fl", slug, query)


def enabled_counties() -> list:
    warnings.warn(
        "county_router is deprecated; use florida_property_scraper.routers.fl instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return _enabled_jurisdictions("fl")
