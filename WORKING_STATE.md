# Working State

## Recent fixes
- Parcel detail merges latest enrichment snapshot (merged fields + evidence IDs) without overwriting valid PA data.
- Added stub evidence providers (PA snapshot, permits, tax, code enforcement) to keep triggers functional.
- Evidence-based triggers now return explicit unavailable reasons for missing data instead of silent gaps.

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
- Proof scripts do not manage processes; use `scripts/start_*` to run services.

## Evidence-first enrichment + triggers
- `/api/enrich` returns `mode=evidence_only` with evidence rows and does not populate fields without evidence.
- Evidence persists in `leads.sqlite` tables: `provider_fetch_log`, `provider_raw`, `provider_evidence`, `parcel_enrichment_snapshot`.
- `/api/triggers/evaluate` returns all trigger keys; missing evidence returns `fired=false` with explicit `reason=unavailable:...`.
- Provider catalog endpoints: `/api/providers/catalog` and `/api/providers/status` (Seminole).
- Evidence-only UI banner shows: "Not enriched yet (evidence-only)."

## How enrichment works
- Providers return evidence-only results with provider metadata (`provider_id`, `provider_name`, `county`, `parcel_id`, `fetched_at`).
- Evidence includes `source_type`, `source_url`, `raw_reference`, `confidence_label`, `confidence` (score), `content_hash`, `extract_method`.
- Only evidence with `confidence_label=high` is merged into fields (threshold configurable via `FPS_EVIDENCE_MIN_CONFIDENCE`).
- Raw responses are stored in `provider_raw`; fetch metadata is logged in `provider_fetch_log`.
- `provider_evidence` is upserted by `(provider_key, county, parcel_id, field, content_hash)`.
- Enrichment snapshots select the best evidence per field (confidence, then recency).

## How to add a new provider (official source)
- Create a provider class in `src/florida_property_scraper/enrichment/providers/` implementing `PropertyProvider`.
- Use fixture mode first: add sample JSON under `fixtures/providers/<provider_key>/`.
- Store raw responses with `SQLiteStore.store_provider_raw` and evidence with `upsert_provider_evidence`.
- Register the provider in `enrichment/providers/registry.py` and in `providers/catalog.py` (status `implemented`).

## Manual ingest fallback
- `/api/providers/manual_ingest` accepts JSON evidence bundles for a parcel.
- Use for official-first data when no live connector is available yet.

## How triggers are evaluated from evidence
- Evaluation lives in `src/florida_property_scraper/triggers/evidence_rules.py`.
- Each trigger rule references evidence fields and emits `evidence_ids` + `fields_used`.
- Current evidence-based triggers include: `permit_recent_major`, `permit_recent_minor`, `code_enforcement_open_case`, `tax_delinquent`, `deed_transfer_recent`, `foreclosure_or_lis_pendens`, `owner_mailing_change`, `absentee_owner`, `out_of_state_owner`, `mortgage_recent`, `equity_high`, `equity_low`, `ownership_long_term`, `ownership_short_term`.

## Still stubbed / placeholders
- Live provider fetches beyond SQLite (code enforcement, courts, liens, utilities) remain placeholders pending external auth.
