import requests
import sqlite3
import os
from datetime import datetime, timedelta

def fetch_pa_fields(county, parcel_id, db_path=None, max_age_days=30):
    """
    Fetches PA fields for a parcel_id, using cache if available and fresh.
    Returns dict with at least: owner_name, situs_address, living_area_sqft, last_sale_date, last_sale_price.
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(__file__), '../../../data/parcels/parcels.sqlite')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT owner_name, situs_address, living_area_sqft, last_sale_date, last_sale_price, updated_at FROM parcels_pa WHERE county=? AND parcel_id=?", (county, parcel_id))
    row = c.fetchone()
    now = datetime.utcnow()
    if row:
        updated_at = row[5]
        if updated_at:
            updated_dt = datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%S")
            if now - updated_dt < timedelta(days=max_age_days):
                return {
                    'owner_name': row[0],
                    'situs_address': row[1],
                    'living_area_sqft': row[2],
                    'last_sale_date': row[3],
                    'last_sale_price': row[4],
                }
    # TODO: Implement real scraping/fetching logic for Seminole PA
    # For now, return explicit warning
    return {'warning': 'PA adapter not implemented for seminole'}
