import tempfile

from florida_property_scraper.signals import generate_events
from florida_property_scraper.storage import SQLiteStore


def test_storage_events_flow():
    with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp:
        store = SQLiteStore(tmp.name)
        obs1 = {
            "property_uid": "Orange:1",
            "county": "Orange",
            "parcel_id": "1",
            "situs_address": "123 Main St",
            "owner_name": "Alice Smith",
            "mailing_address": "123 Main St",
            "last_sale_date": "2020-01-01",
            "last_sale_price": "100000",
            "deed_type": "WD",
            "source_url": "http://example.com",
            "raw_json": "{}",
            "observed_at": "2024-01-01T00:00:00Z",
            "run_id": "run1",
        }
        store.insert_observation(obs1)
        old = store.get_latest_observation("Orange:1")
        obs2 = dict(obs1)
        obs2.update(
            {
                "owner_name": "Bob Smith",
                "last_sale_date": "2021-01-01",
                "last_sale_price": "150000",
                "observed_at": "2024-01-02T00:00:00Z",
                "run_id": "run2",
            }
        )
        events = generate_events(old, obs2)
        store.insert_observation(obs2)
        store.insert_events(events)
        rows = store.conn.execute("SELECT event_type FROM events").fetchall()
        types = {row["event_type"] for row in rows}
        assert "OWNER_CHANGED" in types
        assert "SALE_DETECTED" in types
