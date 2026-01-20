#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

mkdir -p .logs
rm -f .logs/backend_8000.log || true
export PYTHONUNBUFFERED=1
export PYTHONPATH=/workspaces/Florida_Property_Scraper/src

nohup python -m uvicorn florida_property_scraper.api.app:app --host 0.0.0.0 --port 8000 --workers 1 --log-level info > .logs/backend_8000.log 2>&1 &

sleep 1
curl -sS http://127.0.0.1:8000/api/debug/ping | python -m json.tool || true

HEAD=$(git rev-parse --short HEAD)
ping_sha=$(curl -sS http://127.0.0.1:8000/api/debug/ping | python -c "import sys,json; print(json.load(sys.stdin)['git']['sha'])" 2>/dev/null || true)
echo "HEAD=${HEAD}"
echo "ping_sha=${ping_sha}"
echo "ASSERT ping_sha == HEAD"
if [[ -n "${ping_sha}" && "${ping_sha}" == "${HEAD}" ]]; then
	echo "PASS backend_up"
else
	echo "FAIL backend_up"
	exit 2
fi
python scripts/port_inspect.py
