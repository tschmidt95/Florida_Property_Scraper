# Florida_Property_Scraper
Scan properties and look up owner information in Florida

---

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

## Map Mode (planned)

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
