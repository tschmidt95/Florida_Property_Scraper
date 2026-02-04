# Build Proofs (2026-02-04)

## UI + API (live)

### UI head (20 lines)
```
<!doctype html>
<html lang="en">
  <head>
    <script type="module">import { injectIntoGlobalHook } from "/@react-refresh";
injectIntoGlobalHook(window);
window.$RefreshReg$ = () => {};
window.$RefreshSig$ = () => (type) => type;</script>

    <script type="module" src="/@vite/client"></script>

    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link
      href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Playfair+Display:wght@500;600;700&display=swap"
      rel="stylesheet"
    />
    <title>Florida Property Scraper</title>
  </head>
```

### API /debug/ping
```
{
  "ok": true,
  "version": "dev",
  "time": "2026-02-04T00:40:32.079776+00:00",
  "server_time": "2026-02-04T00:40:32.079776+00:00",
  "db_ok": true,
  "parcels_db_ok": true,
  "leads_db_ok": true,
  "parcels_db_path": "/workspaces/Florida_Property_Scraper/data/parcels/parcels.sqlite",
  "leads_db_path": "/workspaces/Florida_Property_Scraper/leads.sqlite",
  "parcels_count": 181058,
  "rollups_count": 10,
  "git": {
    "sha": "9f1bf3a",
    "branch": "fix/recover-working-state"
  },
  "env": {
    "FPS_USE_FDOR_CENTROIDS": "",
    "PA_DB": "",
    "LEADS_SQLITE_PATH": "",
    "APP_GIT_SHA": "",
    "APP_GIT_BRANCH": ""
  }
}
```

## Polygon paging (Seminole small)
```
{'page': 1, 'returned': 50, 'total': 92, 'has_more': True, 'next_cursor': True}
{'page': 2, 'returned': 42, 'total': 92, 'has_more': False, 'next_cursor': False}
{'unique_ids': 92, 'total_count': 92}
```

## Filters (Seminole small)
```
{'base_total': 92, 'base_returned': 92, 'median_living_sqft': 2457}
{'min_sqft_total': 28, 'min_sqft_returned': 28}
{'min_year_total': 38, 'min_year_returned': 38}
{'min_beds_total': 0, 'min_beds_returned': 0}
{'zoning_total': 0, 'zoning_returned': 0}
```

## Trigger (permit_hvac)
```
{'parcel_id': '01202930000100000', 'base_total': 279, 'trigger_total': 1}
```
