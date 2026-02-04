# Backend Stability

## Response contract (GET/POST /api/parcels/search)
All 200 responses include:
- ok (bool)
- records (list)
- total_count (int)
- returned_count (int)
- has_more (bool)
- next_cursor (string|null)
- explain (object)

`explain` always includes:
- counts: totals and stage counts
- warnings: list of warning strings
- timings_ms: per-stage timings (empty if not enabled)
- filter_metrics: filter explain counters (empty unless explain is requested)
- field_stats: field presence counters
- debug_ids_sample: sample parcel_ids

Bad requests (invalid geometry/radius) return HTTP 400:
```
{ ok:false, error:"bad_request", detail:"...", hint:"..." }
```

Internal errors never return HTTP 500 from `/api/parcels/search`. They return HTTP 200 with empty results and:
- explain.warnings includes "internal_error"
- explain.error_id identifies the server-side exception

## Degraded mode behavior
- If parcels/leads DBs are missing, the API runs in degraded mode and returns empty results instead of failing.
- Warnings include `parcels_db_missing` and/or `leads_db_missing` when applicable.
- Multi-county requests with no geometry sources return an empty response and warning `multi_county_not_available`.

## Coverage gaps (truthful reporting)
Coverage is computed from local tables only. If a field is missing in available sources, coverage is reported as 0%.

Common gaps today:
- Beds/Baths/Zoning in Seminole: PA rows exist but these fields are empty.
- Owner name/address fields are only in PA `record_json` and not yet SQL-queryable in coverage, so they show 0%.

To improve coverage:
- Populate `pa_properties` with beds/baths/zoning from authoritative PA sources.
- Add structured columns or indexed JSON extraction for owner/address fields in `pa_properties`.
- Expand `parcel_table1` or add county-scoped tables when PA sources don’t provide these fields.

## Proof scripts
- scripts/proof_api_contract.py
- scripts/proof_filters_effective_when_data_exists.py

## Start commands (backend + UI)
- Backend: bash scripts/start_backend_minimal.sh
- UI (Vite): bash scripts/start_vite_only.sh

## Logs
- Backend log: .logs/backend_8000.log
- Backend PID: .logs/backend_8000.pid
- Vite log: web/.vite_dev_5173.log (when started via scripts/start_vite_only.sh)
