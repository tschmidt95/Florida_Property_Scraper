# Operational Runbook

## Environment

```bash
export PARCELS_DB_PATH=/workspaces/Florida_Property_Scraper/data/parcels/parcels.sqlite
export PA_DB=/workspaces/Florida_Property_Scraper/leads.sqlite
```

## Compile check

```bash
python -m compileall -q src
```

## Start API

```bash
python -m uvicorn florida_property_scraper.api.app:app --host 0.0.0.0 --port 8000 --log-level debug
```

## Debug DB stats (suggested polygon)

```bash
curl -sS "http://127.0.0.1:8000/api/debug/db_stats?county=seminole" | python -m json.tool | head -n 120
```

## Smoke filters

```bash
bash -x scripts/smoke_filters.sh
```

## Tests

```bash
pytest -q tests/test_api_search_monotonicity.py
```
