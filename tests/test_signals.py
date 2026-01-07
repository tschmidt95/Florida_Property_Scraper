from florida_property_scraper.signals import generate_events


def test_generate_events_owner_and_sale():
    old_obs = {
        "property_uid": "Orange:1",
        "county": "Orange",
        "owner_name": "Alice Smith",
        "mailing_address": "123 Main St",
        "last_sale_date": "2020-01-01",
        "last_sale_price": "100000",
        "observed_at": "2024-01-01T00:00:00Z",
        "run_id": "old",
    }
    new_obs = {
        "property_uid": "Orange:1",
        "county": "Orange",
        "owner_name": "Bob Smith",
        "mailing_address": "123 Main St",
        "last_sale_date": "2021-01-01",
        "last_sale_price": "150000",
        "observed_at": "2024-01-02T00:00:00Z",
        "run_id": "new",
    }
    events = generate_events(old_obs, new_obs)
    types = {e["event_type"] for e in events}
    assert "OWNER_CHANGED" in types
    assert "SALE_DETECTED" in types


def test_generate_events_no_old():
    events = generate_events(None, {"property_uid": "X"})
    assert events == []
