# Florida_Property_Scraper
Scan properties and look up owner information in Florida

---

## Address Lookup UI

- Open the app (dev):
  - Start backend: `./scripts/dev_backend.sh` (http://localhost:8000)
  - Start frontend: `./scripts/dev_frontend.sh` (http://localhost:5173)
- Visit the forwarded web URL (Codespaces will expose the port). Type an address, select a county, and click "Lookup" to get a Property Card.
- Env vars:
  - `LEADS_SQLITE_PATH` — path to the SQLite leads DB (defaults to `./leads.sqlite`)
  - `LIVE=1` — enable live fetching (disabled by default)
  - `CONTACT_PROVIDER` / `CONTACT_API_KEY` — enable contact enrichment (opt-in)


## Daily workflow

### Start

- No-terminal path (VS Code):
  - Terminal → Run Task… → **Start: Fullstack (8000)**
  - This starts single-port FastAPI serving the built UI on `:8000`.

### Proof (Live parcels)

- Terminal → Run Task… → **Proof: Live Search (FDOR)**
  - This runs `scripts/prove_live_search.sh` and writes real output to `PROOF_LIVE_SEARCH.txt`.
  - It exits non-zero if live results are missing.

### Proof (Orange enrichment)

- Terminal → Run Task… → **Proof: Orange Enrich (OCPA)**
  - This runs `scripts/prove_orange_enrich.sh` and writes real output to `PROOF_ORANGE_ENRICH.txt`.
  - It exits non-zero unless at least **2** parcels have real OCPA fields populated:
    `year_built`, `living_area_sqft` (or living sqft), `total_value`, `last_sale_price`, and `data_sources` includes an `ocpaservices.ocpafl.org` URL.

### Orange PA (OCPA) source

Current integration uses OCPA's ASP.NET HTML flow (cookie+form POST):

- Landing/search form: https://ocpaservices.ocpafl.org/Searches/ParcelSearch.aspx
- Parcel detail pages: `DisplayParcel.aspx` links returned from search results

If OCPA ever exposes a stable JSON/ArcGIS endpoint, we can switch the provider to that for higher reliability.

### View

- Codespaces forwarded URL format: `https://<CODESPACE_NAME>-8000.app.github.dev/`
- VS Code: open the **Ports** tab → find port `8000` → **Open in Browser**.
- Verify `http://127.0.0.1:8000/api/debug/ping` shows `env.FPS_USE_FDOR_CENTROIDS=1`.

### Stop

- Terminal → Run Task… → **Stop: Kill 8000** (safe even if nothing is running).
- Or: Terminal → Terminate Task… → select **Start: Fullstack (8000)**.

### Resume

- Reopen the Codespace.
- Terminal → Run Task… → **Start: Fullstack (8000)**.
- Open the forwarded URL via the **Ports** tab (port `8000`).


## Getting started (local)

- Create and activate a virtual environment:

  python -m venv .venv
  source .venv/bin/activate

- Install the package and test deps:

  python -m pip install --upgrade pip setuptools wheel
  python -m pip install -e .[test]

### Pre-commit (recommended)

We use `pre-commit` to enforce formatting + basic hygiene (Ruff + Ruff format, whitespace, YAML validation).

- Install dev tools:

  python -m pip install -r requirements-dev.txt

- Install git hooks:

  pre-commit install

- Run locally (CI runs this too):

  pre-commit run --all-files

- Run smoke tests:

  python -m pytest -q tests/test_smoke.py

- Run the Flask demo app (after installing Flask):

  python web_app.py  # open http://localhost:5000 and enable Demo mode

- Run the Scrapy runner against a local fixture:

  python -m florida_property_scraper.backend.scrapy_runner --spider-name broward_spider --start-urls '["file:///absolute/path/to/tests/fixtures/broward_sample.html"]'

---

## Live mode usage

Run live requests against county sites (network required):

  PYTHONPATH=./src python3 -m florida_property_scraper --live --query "Smith" --counties "broward" --max-items 3
  PYTHONPATH=./src python3 -m florida_property_scraper --live --query "Smith" --counties "alachua" --max-items 3
  PYTHONPATH=./src python3 -m florida_property_scraper --live --query "Smith" --counties "seminole" --max-items 3
  PYTHONPATH=./src python3 -m florida_property_scraper --live --query "Smith" --counties "orange" --max-items 3
  PYTHONPATH=./src python3 -m florida_property_scraper --live --query "Smith" --counties "palm_beach" --max-items 3

Optional live smoke tests:

  RUN_LIVE_TESTS=1 PYTHONPATH=./src python3 -m pytest -q -m live

---

## Permits (WIP)

Chosen Florida public permits portal (Phase 1 discovery):

- Seminole County, FL: https://semc-egov.aspgov.com/Click2GovBP/

Notes:

- CI never hits live websites.
- Live permits sync is **explicitly gated**: set `LIVE=1` to enable `/api/permits/sync`.

---

## Map Mode (Map Search)

- Start the single-port app (serves API + built UI on `:8000`):
  - VS Code: Terminal → Run Task… → **Start: Fullstack (8000)**
  - Or CLI: `bash scripts/up.sh`
- Open the UI:
  - Local: http://127.0.0.1:8000/
  - Codespaces: open the forwarded URL from the **Ports** tab (port `8000`).
- Use the Map Search:
  - Pick a county.
  - Draw either a polygon (lasso) or a circle (radius).
  - Click **Run** to POST your geometry to `/api/parcels/search`.
  - Results render as markers on the map and in the results panel.
- If the backend returns an error or zero results, the UI shows a banner and loads a deterministic DEMO dataset (so it’s never “silent”).
- Debug: expand the debug section to see the last request/response and API `/api/debug/ping` status.

## Native backend

- `python3 -m florida_property_scraper --query "John Smith" --counties "broward,orange"`
- `python3 -m florida_property_scraper --backend native --query "John Smith" --counties "broward,orange"`

Run the API:

  uvicorn florida_property_scraper.api.app:app --reload

---

## UI dev

From repo root:

```bash
# Install frontend deps
npm --prefix web install

# Run backend API (http://localhost:8000)
uvicorn florida_property_scraper.api.app:app --reload --host 0.0.0.0 --port 8000

# Run frontend dev server (proxy /api -> http://localhost:8000)
npm --prefix web run dev -- --host 0.0.0.0 --port 5173

# Verify API
curl "http://127.0.0.1:8000/api/search?q=smith&county=Orange"

# Run both (single command)
bash scripts/dev.sh

# Build UI
npm --prefix web run build
```

Environment variables:

- `POSTGIS_ENABLED=1` to use PostGIS for map layers
- `POSTGIS_DSN=...` connection string

## Adding a new state

Scaffold a state router module:

  python scripts/add_state.py --state xx --name "State Name"

Add counties for that state with the generator:

  python scripts/add_county.py --state xx --slug sample_county --url-template "https://example.gov/search?owner={query}" --columns owner,address

### State scaffolding

Examples:

  python3 scripts/add_state.py --state ga --name "Georgia"
  python3 scripts/add_county.py --state fl --slug duval --url-template "https://example.gov/search?owner={query}" --columns owner,address,property_class,zoning
  python3 scripts/add_county.py --dry-run --state fl --slug duval --url-template "https://example.gov/search?owner={query}" --columns owner,address

One source of truth: `src/florida_property_scraper/routers/<state>.py` is canonical; `src/florida_property_scraper/county_router.py` is compatibility only.

## Post-publish smoke check (automated)

We run a post-publish smoke check after a release is published that pulls the published GHCR image and performs a minimal import test to ensure the package is present and importable.

- How to re-run manually: open the `Post-publish smoke check` workflow and use **Run workflow** (workflow_dispatch) with the `tag` input set to the tag you want to validate.
- Notes on timing: releases sometimes publish slightly before the registry is fully populated. The workflow includes a retry/backoff and a delayed retry that waits and re-checks automatically; if the pull ultimately fails the action prints a useful link to the publish workflows filtered by tag:

  https://github.com/<owner>/<repo>/actions/runs?query=tag%3A<your-tag>

Replace `<owner>/<repo>` and `<your-tag>` with the repository and tag to inspect the publish job logs.

If you want, you can also dispatch the workflow manually with the `tag` of a known-published image for immediate verification.

### Scheduled image monitoring

A daily scheduled job runs at 02:00 UTC and checks `ghcr.io/tschmidt95/florida-scraper:latest` by default (it can be dispatched manually with a specific `tag`). This provides ongoing verification that published images remain importable.

If the scheduled monitor fails repeatedly (default threshold: 3 consecutive failures), an automatic GitHub Issue will be created (label `monitor-failure`) to notify maintainers and centralize triage.

### Optional: External notifications (template)

We provide a **template** workflow that can send an external notification (Slack, Microsoft Teams, webhook endpoints) when an issue labeled `monitor-failure` is opened.

- File: `.github/workflows/notify_placeholder_v2.yml` (v2 replaces legacy `notify_placeholder.yml`)
- How to enable: add a repository secret named `ALERT_WEBHOOK_URL` with your webhook URL (Slack, Teams, or custom). If you want to select provider-specific payloads, also add `ALERT_PROVIDER` with one of `slack`, `teams`, or `generic` (defaults to `slack`).
- Notes: the template is intentionally non-invasive (it exits if `ALERT_WEBHOOK_URL` is not configured). When you add the secret, the workflow will start posting notifications on new `monitor-failure` issues.

Testing:
1. Add secrets `ALERT_WEBHOOK_URL` (required) and optionally `ALERT_PROVIDER` (`slack`, `teams`, or `generic`).
2. (Optional) Add `ALERT_WEBHOOK_URL_TEST` to receive a harmless test payload before real notifications — useful for validating webhook formatting without sending the real alert.
3. Create a test issue with label `monitor-failure` to trigger the notification. If `ALERT_WEBHOOK_URL_TEST` is set, the workflow will first POST a small test JSON to that URL and log the response status and body excerpt; then it will send the real provider-specific payload to `ALERT_WEBHOOK_URL`.
4. The workflow sends provider-specific payloads (Slack Blocks for `slack`, actionable card for `teams`) and logs the webhook response status and a short response body excerpt in the action logs (helps with debugging).
5. If you want a richer Slack message (blocks with button), use `ALERT_PROVIDER=slack` (default). For Teams actionable card use `ALERT_PROVIDER=teams`.


## Local run (use repo code)

If you have another `florida_property_scraper` installed on your system, run the local code via:

```
./scripts/run_local.sh --query "SMITH" --counties "Hillsborough" --output ./tmp_results.jsonl
```

Smoke test:

```
python -m florida_property_scraper.cli --query "John Smith" --counties "Orange,Seminole" --max-items 5
```

By default the CLI now saves as it goes:

- `./results.jsonl` for file output
- `./leads.sqlite` for SQLite storage (dedupe + scoring)

To disable either, pass `--no-output` and/or `--no-store`.
---
