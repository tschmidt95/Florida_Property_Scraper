# Dev Baseline (Seminole)

## Known-good baseline
- Commit: c1e1339
- Why: last commit before the WIP parcel detail wiring changes. Polygon search + markers were stable here and the regression (missing `county` in query params) was introduced after this point.

## Quick start (reproducible)
1. Run the reset script:
   - `./scripts/dev_reset.sh`

2. Expected output:
   - `PASS dev_reset`
   - `API OK (git sha: <sha>)`
   - Logs written to:
     - `/tmp/api.log`
     - `/tmp/ui.log`

3. Verify UI:
   - Open the UI at `http://127.0.0.1:5173/`
   - Set county to `seminole`
   - Draw a polygon and click **Run**

4. Expected behavior:
   - `/tmp/api.log` includes:
     - `POST /api/parcels/search?county=seminole`
     - `GET /api/parcels/<id>?county=seminole&include_geometry=1`
   - Returned record count matches marker count (no 4/24 mismatch)

## Smoke test (CLI)
- Run: `./scripts/smoke_ui_api.sh`
- Expected: prints `PASS` with counts and exits 0.

Additional filter smoke:
- Run: `./scripts/smoke_filters.sh`
- Expected: prints `PASS smoke_filters` with base + filtered counts.

## Canonical data sources
- Parcels geometry: `data/parcels/parcels.sqlite` (env: `PARCELS_DB_PATH`)
   - Table: `parcels`
   - Key fields: `county`, `parcel_id`, `geom_geojson`, `minx`, `miny`, `maxx`, `maxy`
- Cached PA attributes: `leads.sqlite` (env: `PA_DB` / `LEADS_SQLITE_PATH`)
   - Table: `pa_properties` (see [src/florida_property_scraper/pa/storage.py](src/florida_property_scraper/pa/storage.py))
   - Key fields: `county`, `parcel_id`, `living_sf`, `land_sf`, `year_built`, `bedrooms`, `bathrooms`,
      `just_value`, `assessed_value`, `taxable_value`, `latitude`, `longitude`, `record_json`

## How search works (deterministic)
`POST /api/parcels/search` is local-only. No live scraping is performed.

Pipeline (see [src/florida_property_scraper/api/app.py](src/florida_property_scraper/api/app.py)):
1. Polygon/radius -> candidate parcel IDs from `parcels.sqlite`.
2. Lat/lng normalization: prefer cached PA `latitude/longitude`, else geometry centroid.
3. Join cached PA attributes from `leads.sqlite` (if present).
4. Apply filters with explicit missing-value policy.
5. Sort/limit.

### Missing-value policy (filters)
- Default: **include** records with missing values (they do **not** auto-exclude).
- To exclude missing values, set `exclude_missing=true` in the request payload.

### Explain mode
Use `explain=1` (query param) or `explain: true` in the JSON payload.
Response includes `explain.stage_counts`, `explain.dropped_reasons`, and `explain.missing_field_counts`.

## Enrich missing (explicit)
Use `POST /api/parcels/enrich` with payload:
`{ county, parcel_ids: [...], max_per_minute, limit }`

Response includes progress counts: `requested`, `cached`, `fetched_ok`, `fetched_failed`, `skipped`, `elapsed_s`.
