# Working State

## UI to backend proxy
- The UI always calls relative API paths like `/api/...`.
- Vite proxies `/api` to the backend at `http://127.0.0.1:8000` in dev.
- Frontend requests go through `apiFetch()`, which blocks absolute URLs and provides an actionable error if a request is not `/api/...`.

## Square footage normalization
- The canonical field is `living_area_sqft`.
- The backend normalizes it from (in priority order):
  1) `living_area_sqft` already present on the record
  2) `living_sf` / `building_sf`
  3) `parcels_pa.living_area_sqft`
  4) `parcel_table1.LIVING_AREA` / `parcel_table1.TOTAL_SQFT`
- Values are parsed leniently (ints, floats, numeric strings with commas).

### Missing policy
- If a user provides min/max sqft filters and does not specify `missing_policy`, the default is `lenient`.
- `lenient`: records missing sqft are not excluded by sqft filters.
- `strict`: records missing sqft are excluded.

## Coverage calculations
- `coverage_candidates` is computed from the candidate scan before filters.
- `coverage` is computed from the records returned to the UI.
- `field_stats` includes:
  - `present` and `missing` counts per field
  - `coverage` percentages per field
  - `sample_candidate` with raw source fields for troubleshooting

## What you should see when working
- MapSearch "Run" returns many Seminole records for a polygon search.
- Sqft filters (min/max) do not collapse results to a tiny number unless the data truly is sparse.
- Coverage shows a non-zero percentage when sqft data exists.
- If coverage is low and sqft filters are applied, a warning appears: "Most records missing sqft; consider lenient missing policy."

## Logs and startup
- Backend logs: `.logs/backend_8000.log`
- UI logs: `.logs/ui_5173.log`
- Startup scripts warn if local sqlite files are missing.
