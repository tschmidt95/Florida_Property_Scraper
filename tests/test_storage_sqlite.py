import sqlite3

from florida_property_scraper.storage import SQLiteStorage


def test_sqlite_storage_dedupes(tmp_path):
    db_path = tmp_path / "leads.sqlite"
    storage = SQLiteStorage(str(db_path))
    items = [
        {
            "county": "broward",
            "owner": "Jane Smith",
            "address": "1 Main St",
            "land_size": "1000",
            "building_size": "900",
            "bedrooms": "3",
            "bathrooms": "2",
            "zoning": "R1",
            "property_class": "Residential",
            "raw_html": "",
        },
        {
            "county": "broward",
            "owner": "Jane Smith",
            "address": "1 Main St",
            "land_size": "1000",
            "building_size": "900",
            "bedrooms": "3",
            "bathrooms": "2",
            "zoning": "R1",
            "property_class": "Residential",
            "raw_html": "",
        },
        {
            "county": "broward",
            "owner": "Jane Smith",
            "address": "2 Main St",
            "land_size": "",
            "building_size": "",
            "bedrooms": "",
            "bathrooms": "",
            "zoning": "",
            "property_class": "",
            "raw_html": "",
        },
    ]
    storage.save_items(items)
    storage.close()

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM owners")
    owners_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM properties")
    properties_count = cur.fetchone()[0]
    conn.close()

    assert owners_count == 1
    assert properties_count == 2
