#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

if [[ -d ".venv" ]]; then
	# shellcheck disable=SC1091
	source .venv/bin/activate
fi

mkdir -p .logs
rm -f .logs/backend_8000.log || true
rm -f .logs/backend_8000.pid || true
export PYTHONUNBUFFERED=1
export PYTHONPATH=/workspaces/Florida_Property_Scraper/src
export PARCELS_DB_PATH=/workspaces/Florida_Property_Scraper/data/parcels/parcels.sqlite
export LEADS_SQLITE_PATH=/workspaces/Florida_Property_Scraper/leads.sqlite

nohup python -m uvicorn florida_property_scraper.api.app:app --host 127.0.0.1 --port 8000 --workers 1 --log-level info > .logs/backend_8000.log 2>&1 &
echo $! > .logs/backend_8000.pid

for i in {1..30}; do
	if ! ps -p "$(cat .logs/backend_8000.pid)" >/dev/null 2>&1; then
		echo "FAIL uvicorn not running"
		exit 2
	fi
	if curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
		break
	fi
	sleep 1
done

if ! curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
	echo "FAIL backend health did not return 200"
	exit 2
fi

curl -sS http://127.0.0.1:8000/api/health | python -m json.tool || true
python scripts/port_inspect.py
