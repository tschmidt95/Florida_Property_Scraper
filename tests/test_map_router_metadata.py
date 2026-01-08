from florida_property_scraper.routers.fl import get_entry
from florida_property_scraper.routers.fl_coverage import FL_COUNTIES


def test_map_router_metadata_has_parcel_layer():
    for entry in FL_COUNTIES:
        router_entry = get_entry(entry["slug"])
        parcel_layer = router_entry.get("parcel_layer") or {"type": "none"}
        assert "type" in parcel_layer
