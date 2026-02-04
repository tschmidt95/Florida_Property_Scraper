import json
import statistics
import urllib.request

BASE = "http://127.0.0.1:8000"
COUNTY = "seminole"

ping = json.load(urllib.request.urlopen(f"{BASE}/api/debug/ping", timeout=30))
print(
    {
        "ping_ok": ping.get("ok"),
        "parcels_count": ping.get("parcels_count"),
        "leads_db_ok": ping.get("leads_db_ok"),
    }
)

coverage_meta = json.load(urllib.request.urlopen(f"{BASE}/api/debug/source_coverage?county={COUNTY}", timeout=30))
print({
    "counts": coverage_meta.get("counts"),
    "fields_available": coverage_meta.get("fields"),
})

stats = json.load(urllib.request.urlopen(f"{BASE}/api/debug/db_stats?county={COUNTY}", timeout=30))
lat_range = stats.get("lat_range") or [28.5, 28.7]
lng_range = stats.get("lng_range") or [-81.4, -81.3]
lat_c = (lat_range[0] + lat_range[1]) / 2
lng_c = (lng_range[0] + lng_range[1]) / 2
pad = 0.01
poly = {
    "type": "Polygon",
    "coordinates": [
        [
            [lng_c - pad, lat_c - pad],
            [lng_c + pad, lat_c - pad],
            [lng_c + pad, lat_c + pad],
            [lng_c - pad, lat_c + pad],
            [lng_c - pad, lat_c - pad],
        ]
    ],
}

# Prefer a parcel that exists in parcel_id_map + pa_properties AND has geometry.
preferred_geom_id = None
try:
    import sqlite3

    lcon = sqlite3.connect("/workspaces/Florida_Property_Scraper/leads.sqlite")
    lcur = lcon.cursor()
    geom_rows = lcur.execute(
        """
        SELECT m.geom_parcel_id
        FROM parcel_id_map m
        JOIN pa_properties p
          ON lower(p.county)=lower(m.county)
         AND p.parcel_id=m.pa_parcel_id
        WHERE lower(m.county)=?
        LIMIT 2000
        """,
        (COUNTY,),
    ).fetchall()
    lcon.close()
    geom_ids = [r[0] for r in geom_rows if r and r[0]]
    if geom_ids:
        pcon = sqlite3.connect("/workspaces/Florida_Property_Scraper/data/parcels/parcels.sqlite")
        pcur = pcon.cursor()
        for i in range(0, len(geom_ids), 900):
            batch = geom_ids[i : i + 900]
            placeholders = ",".join(["?"] * len(batch))
            prow = pcur.execute(
                f"SELECT parcel_id, minx, miny, maxx, maxy FROM parcels WHERE parcel_id IN ({placeholders}) LIMIT 1",
                batch,
            ).fetchone()
            if prow:
                preferred_geom_id = prow[0]
                minx, miny, maxx, maxy = [float(x) for x in prow[1:]]
                pad = 0.0005
                poly = {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [minx - pad, miny - pad],
                            [maxx + pad, miny - pad],
                            [maxx + pad, maxy + pad],
                            [minx - pad, maxy + pad],
                            [minx - pad, miny - pad],
                        ]
                    ],
                }
                break
        pcon.close()
except Exception:
    pass


def search(extra=None):
    payload = {
        "polygon_geojson": poly,
        "limit": 200,
        "include_geometry": False,
        "explain": True,
    }
    if extra:
        payload.update(extra)
    req = urllib.request.Request(
        f"{BASE}/api/parcels/search?explain=1",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return json.load(urllib.request.urlopen(req, timeout=30))


base = search()
sample_pid = None
try:
    if base.get("records"):
        sample_pid = base["records"][0].get("parcel_id")
except Exception:
    sample_pid = None
vals = [
    r.get("living_area_sqft")
    for r in (base.get("records") or [])
    if isinstance(r.get("living_area_sqft"), (int, float))
]
median_sqft = int(statistics.median(vals)) if vals else 2000
print(
    {
        "base_total": base.get("total_count"),
        "base_returned": base.get("returned_count"),
        "median_living_sqft": median_sqft,
    }
)

field_stats = base.get("field_stats") or {}
fields = (field_stats.get("fields") or {}) if isinstance(field_stats, dict) else {}
coverage = {
    k: fields.get(k, {}).get("pct", 0)
    for k in (
        "living_area_sqft",
        "beds",
        "baths",
        "year_built",
        "zoning",
        "property_type",
    )
}
print({"coverage": coverage})
if sample_pid:
    sample_url = f"{BASE}/api/debug/sample_record?county={COUNTY}&parcel_id={sample_pid}"
    try:
        sample = json.load(urllib.request.urlopen(sample_url, timeout=30))
        print({
            "sample_pid": sample_pid,
            "mapping_row": sample.get("mapping_row"),
            "pa_present": bool(sample.get("pa_properties")),
            "sample_merge": sample.get("merged_record"),
            "sample_sources": sample.get("merged_sources"),
        })
        try:
            record_json = sample.get("record_json") or {}
            if isinstance(record_json, dict):
                key_hits = [
                    k
                    for k in record_json.keys()
                    if any(token in str(k).lower() for token in ("bed", "bath", "zoning", "future", "land_use"))
                ]
                print({"record_json_key_hits": sorted(key_hits)[:25]})
                print({
                    "record_json_fields": {
                        "bedrooms": record_json.get("bedrooms"),
                        "bathrooms": record_json.get("bathrooms"),
                        "zoning": record_json.get("zoning"),
                        "future_land_use": record_json.get("future_land_use"),
                        "land_use_code": record_json.get("land_use_code"),
                        "use_type": record_json.get("use_type"),
                    }
                })
        except Exception:
            pass
    except Exception as e:
        print({"sample_pid": sample_pid, "sample_error": str(e)})

min_sqft = search({"filters": {"min_sqft": median_sqft, "missing_policy": "strict"}})
print({"min_sqft_total": min_sqft.get("total_count"), "min_sqft_returned": min_sqft.get("returned_count")})

min_year = search({"filters": {"min_year_built": 1990, "missing_policy": "strict"}})
print({"min_year_total": min_year.get("total_count"), "min_year_returned": min_year.get("returned_count")})

min_beds = search({"filters": {"min_beds": 3, "missing_policy": "strict"}})
print({"min_beds_total": min_beds.get("total_count"), "min_beds_returned": min_beds.get("returned_count")})

zoning = search({"filters": {"zoning": "R", "missing_policy": "strict"}})
print({"zoning_total": zoning.get("total_count"), "zoning_returned": zoning.get("returned_count")})

try:
    import sqlite3

    lcon = sqlite3.connect("/workspaces/Florida_Property_Scraper/leads.sqlite")
    lcur = lcon.cursor()
    pa_total = lcur.execute("SELECT count(*) FROM pa_properties").fetchone()[0]
    pa_sem_exact = lcur.execute(
        "SELECT count(*) FROM pa_properties WHERE lower(county)=?",
        (COUNTY,),
    ).fetchone()[0]
    pa_sem_like = lcur.execute(
        "SELECT count(*) FROM pa_properties WHERE lower(county) LIKE ?",
        (f"%{COUNTY}%",),
    ).fetchone()[0]
    pa_beds = lcur.execute(
        "SELECT count(*) FROM pa_properties WHERE lower(county)=? AND bedrooms>0",
        (COUNTY,),
    ).fetchone()[0]
    pa_baths = lcur.execute(
        "SELECT count(*) FROM pa_properties WHERE lower(county)=? AND bathrooms>0",
        (COUNTY,),
    ).fetchone()[0]
    pa_zoning = lcur.execute(
        "SELECT count(*) FROM pa_properties WHERE lower(county)=? AND zoning!=''",
        (COUNTY,),
    ).fetchone()[0]
    map_count = lcur.execute(
        "SELECT count(*) FROM parcel_id_map WHERE lower(county)=?",
        (COUNTY,),
    ).fetchone()[0]
    counties = [
        r[0]
        for r in lcur.execute(
            "SELECT DISTINCT county FROM pa_properties ORDER BY county LIMIT 8"
        ).fetchall()
    ]
    sample_norm_match = None
    if sample_pid:
        def _norm_num(v: str) -> str:
            return "".join(ch for ch in str(v) if ch.isdigit()).lstrip("0")

        target = _norm_num(sample_pid)
        for row in lcur.execute(
            "SELECT parcel_id FROM pa_properties WHERE lower(county)=?",
            (COUNTY,),
        ).fetchall():
            pid = row[0]
            if _norm_num(pid) == target:
                sample_norm_match = pid
                break
    lcon.close()
    print(
        {
            "pa_total": pa_total,
            "pa_seminole_exact": pa_sem_exact,
            "pa_seminole_like": pa_sem_like,
            "pa_seminole_beds": pa_beds,
            "pa_seminole_baths": pa_baths,
            "pa_seminole_zoning": pa_zoning,
            "parcel_id_map_count": map_count,
            "pa_counties_sample": counties,
            "sample_pid_norm_match": sample_norm_match,
            "preferred_geom_id": preferred_geom_id,
        }
    )
    try:
        pcon = sqlite3.connect("/workspaces/Florida_Property_Scraper/data/parcels/parcels.sqlite")
        pcur = pcon.cursor()
        pa_table_count = pcur.execute(
            "SELECT count(*) FROM parcels_pa WHERE lower(county)=?",
            (COUNTY,),
        ).fetchone()[0]
        parcels_pa_match = None
        if sample_pid:
            target = "".join(ch for ch in str(sample_pid) if ch.isdigit()).lstrip("0")
            for row in pcur.execute(
                "SELECT parcel_id FROM parcels_pa WHERE lower(county)=?",
                (COUNTY,),
            ).fetchall():
                pid = row[0]
                if "".join(ch for ch in str(pid) if ch.isdigit()).lstrip("0") == target:
                    parcels_pa_match = pid
                    break
        pcon.close()
        print({"parcels_pa_count": pa_table_count, "parcels_pa_match": parcels_pa_match})
    except Exception:
        pass
except Exception:
    pass
