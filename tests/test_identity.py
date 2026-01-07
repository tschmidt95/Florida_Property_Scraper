from florida_property_scraper.identity import compute_property_uid


def test_property_uid_with_parcel():
    item = {"county": "Orange", "parcel_id": "123-456"}
    uid, parcel_id, warnings = compute_property_uid(item)
    assert uid == "Orange:123-456"
    assert parcel_id == "123-456"
    assert warnings == []


def test_property_uid_fallback():
    item = {"county": "Orange", "owner_name": "Jane Doe", "situs_address": "123 Main St"}
    uid, parcel_id, warnings = compute_property_uid(item)
    assert uid.startswith("Orange:")
    assert parcel_id is None
    assert any("fallback" in w.lower() for w in warnings)
