#!/usr/bin/env bash
set -euo pipefail

export LEADS_SQLITE_PATH="${LEADS_SQLITE_PATH:-/workspaces/Florida_Property_Scraper/leads.sqlite}"

uvicorn florida_property_scraper.api.app:app \
	--host 0.0.0.0 \
	--port 8000 \
	--reload \
	--reload-dir /workspaces/Florida_Property_Scraper/src \
	--reload-exclude /workspaces/Florida_Property_Scraper/web/node_modules
