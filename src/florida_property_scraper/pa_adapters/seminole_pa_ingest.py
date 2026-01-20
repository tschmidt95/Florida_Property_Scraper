import os
import json
from pathlib import Path
from florida_property_scraper.pa.storage import PASQLite
from florida_property_scraper.pa.schema import PAProperty

def load_seminole_geojson(geojson_path):
    with open(geojson_path) as f:
        gj = json.load(f)
    for feat in gj["features"]:
        props = feat["properties"]
        yield props

def main():
    # Use the same DB as Orange: leads.sqlite, pa_properties
    db_path = os.getenv("PA_DB", "./leads.sqlite")
    geojson_path = os.getenv("SEMINOLE_GEOJSON", "data/parcels/seminole_service_area_parcels.geojson")
    store = PASQLite(db_path)
    count = 0
    for props in load_seminole_geojson(geojson_path):
        # Map fields to PAProperty
        rec = PAProperty(
            county="seminole",
            parcel_id=str(props.get("PARCEL") or props.get("parcel_id") or props.get("PARCEL_ID") or props.get("pid") or props.get("strap") or props.get("OBJECTID") or "").strip(),
            situs_address=props.get("situs_address", ""),
            owner_names=[props.get("owner_name", "")],
            last_sale_date=props.get("last_sale_date"),
            last_sale_price=float(props.get("last_sale_price", 0) or 0),
        )
        store.upsert(rec)
        count += 1
    print(f"Ingested {count} seminole parcels into {db_path}")

if __name__ == "__main__":
    main()
