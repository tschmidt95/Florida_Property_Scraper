import argparse
import json
import os
import sqlite3
from shapely.geometry import shape, mapping

DB_PATH = os.path.join(os.path.dirname(__file__), '../../../data/parcels/parcels.sqlite')

SCHEMA = [
    '''CREATE TABLE IF NOT EXISTS parcels (
        county TEXT,
        parcel_id TEXT,
        geom_geojson TEXT,
        minx REAL,
        miny REAL,
        maxx REAL,
        maxy REAL,
        PRIMARY KEY(county, parcel_id)
    )''',
    '''CREATE VIRTUAL TABLE IF NOT EXISTS parcels_rtree USING rtree(
        rowid, minx, maxx, miny, maxy
    )''',
    '''CREATE TABLE IF NOT EXISTS parcels_pa (
        county TEXT,
        parcel_id TEXT,
        owner_name TEXT,
        situs_address TEXT,
        mailing_address TEXT,
        living_area_sqft INTEGER,
        beds REAL,
        baths REAL,
        year_built INTEGER,
        last_sale_date TEXT,
        last_sale_price INTEGER,
        updated_at TEXT,
        PRIMARY KEY(county, parcel_id)
    )'''
]

def create_schema(conn):
    c = conn.cursor()
    for stmt in SCHEMA:
        c.execute(stmt)
    conn.commit()

def ingest_geojson(county, input_path, conn):
    with open(input_path) as f:
        gj = json.load(f)
    features = gj['features']
    c = conn.cursor()
    count = 0
    minx = miny = float('inf')
    maxx = maxy = float('-inf')
    seen_ids = {}

    for feat in features:
        geom = shape(feat['geometry'])
        props = feat['properties']

        parcel_id = str(
            props.get("PARCEL")
            or props.get("PARCEL_KEY")
            or props.get("parcel_id")
            or props.get("PARCEL_ID")
            or props.get("strap")
            or props.get("pid")
            or ""
        ).strip()

        if not parcel_id:
            continue

        if parcel_id in seen_ids:
            seen_ids[parcel_id] += 1
            parcel_id = f"{parcel_id}#dup{seen_ids[parcel_id]}"
        else:
            seen_ids[parcel_id] = 0

        bbox = geom.bounds
        minx = min(minx, bbox[0])
        miny = min(miny, bbox[1])
        maxx = max(maxx, bbox[2])
        maxy = max(maxy, bbox[3])
        c.execute('REPLACE INTO parcels (county, parcel_id, geom_geojson, minx, miny, maxx, maxy) VALUES (?, ?, ?, ?, ?, ?, ?)',
                  (county, parcel_id, json.dumps(mapping(geom)), bbox[0], bbox[1], bbox[2], bbox[3]))
        # Insert into rtree
        c.execute('INSERT OR REPLACE INTO parcels_rtree (rowid, minx, maxx, miny, maxy) VALUES ((SELECT rowid FROM parcels WHERE county=? AND parcel_id=?), ?, ?, ?, ?)',
                  (county, parcel_id, bbox[0], bbox[2], bbox[1], bbox[3]))
        count += 1
    conn.commit()
    print(f"Ingested {count} features for {county}.")
    print(f"Dataset bbox: minx={minx}, miny={miny}, maxx={maxx}, maxy={maxy}")
    print("PASS ingest")

def main():
    parser = argparse.ArgumentParser(description='Ingest county parcel polygons into SQLite DB')
    parser.add_argument('--county', required=True)
    parser.add_argument('--input', required=True, help='Path to input GeoJSON file')
    args = parser.parse_args()
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    print(f"USING DB_PATH: {DB_PATH} -> {os.path.abspath(DB_PATH)}")
    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)
    ingest_geojson(args.county.lower(), args.input, conn)
    conn.close()

if __name__ == '__main__':
    main()
