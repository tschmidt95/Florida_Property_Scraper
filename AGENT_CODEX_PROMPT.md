# AgentCodex Prompt (Workspace)

## Operating Rules

- Orange-first: prioritize Orange County OCPA enrichment + Orange-focused proofs.
- Graceful degradation: if a county/source does not provide a field, do not error; return empty option lists and treat filters as no-ops when selections are empty.
- Proof-driven: every deliverable that changes behavior must have a `scripts/prove_*.sh` and a committed `PROOF_*.txt` artifact.
- Do not break the existing Orange enrichment proof: `scripts/prove_orange_enrich.sh`.
- Keep changes minimal per deliverable; commit and push per deliverable.

## Deliverables

### 2.6 — Zoning + Future Land Use (FLU) multi-select filters

- Backend:
  - Request: support `filters.zoning_in: string[]` and `filters.future_land_use_in: string[]`.
  - Normalize for matching: trim, uppercase, collapse whitespace; empty → `UNKNOWN`.
  - Response: include `zoning_options: string[]` and `future_land_use_options: string[]` computed from the baseline (unfiltered) polygon/radius candidate set.
- Frontend:
  - Add multi-select filter UIs for Current Zoning and Future Land Use with search box + checkbox list + Select All / Clear.
  - Preserve selections across rerenders; clear selections only when geometry is cleared or county changes.
- Proof:
  - Add `scripts/prove_orange_zoning_flu_filter.sh` → `PROOF_ORANGE_ZONING_FLU_FILTER.txt`.
  - Commit message must include: `Zoning/FLU multi-select filters`.

### 2.7 — Parcel size filter with unit toggle (acres / sqft)

- Backend:
  - Request: support min/max parcel size with `filters.lot_size_unit` (`acres`|`sqft`) and numeric min/max.
  - Normalize: compute both sqft and acres when possible.
  - Response: include `lot_size_sqft` and `lot_size_acres` per record when available.
- Frontend:
  - Add Parcel Size controls: unit toggle + min/max inputs.
- Proof:
  - Add `scripts/prove_orange_lot_size_filter.sh` → `PROOF_ORANGE_LOT_SIZE_FILTER.txt`.
  - Commit message must include: `Parcel size filter`.
