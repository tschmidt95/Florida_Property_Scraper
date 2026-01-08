from florida_property_scraper.routers.fl import build_request_plan, enabled_counties
from florida_property_scraper.routers.fl import get_entry as get_county_entry


def test_county_entries_have_capability_flags():
    for slug in enabled_counties():
        entry = get_county_entry(slug)
        assert "supports_query_param" in entry
        assert "needs_form_post" in entry
        assert "needs_pagination" in entry
        assert "needs_js" in entry


def test_request_plan_respects_flags():
    seminole = get_county_entry("seminole")
    plan = build_request_plan("seminole", "Smith")
    assert seminole["needs_form_post"] is True
    assert plan["needs_form_post"] is True
    assert plan["start_urls"] == [seminole["form_url"]]

    broward = get_county_entry("broward")
    broward_plan = build_request_plan("broward", "Smith")
    assert broward["supports_query_param"] is True
    assert broward_plan["start_urls"]

    orange = get_county_entry("orange")
    assert orange["needs_pagination"] is True
