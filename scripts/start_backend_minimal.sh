#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

if [[ -d ".venv" ]]; then
	# shellcheck disable=SC1091
	source .venv/bin/activate
fi

mkdir -p .logs
rm -f .logs/backend_8000.log || true

is_listening_pid() {
	local pid="$1"
	if [[ -z "$pid" ]]; then
		return 1
	fi
	if ! command -v lsof >/dev/null 2>&1; then
		return 1
	fi
	lsof -t -iTCP:8000 -sTCP:LISTEN 2>/dev/null | grep -q "^${pid}$"
}

if [[ -f .logs/backend_8000.pid ]]; then
	pid=$(cat .logs/backend_8000.pid || true)
	if [[ -n "$pid" ]] && ps -p "$pid" >/dev/null 2>&1; then
		if curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
			echo "Backend already running (pid=$pid)"
			exit 0
		fi
		if is_listening_pid "$pid"; then
			kill "$pid" >/dev/null 2>&1 || true
			sleep 1
			if ps -p "$pid" >/dev/null 2>&1; then
				kill -9 "$pid" >/dev/null 2>&1 || true
			fi
		fi
	fi
	rm -f .logs/backend_8000.pid || true
fi
export PYTHONUNBUFFERED=1
export PYTHONPATH=/workspaces/Florida_Property_Scraper/src
export PARCELS_DB_PATH=/workspaces/Florida_Property_Scraper/data/parcels/parcels.sqlite
export LEADS_SQLITE_PATH=/workspaces/Florida_Property_Scraper/leads.sqlite

if [[ ! -f "$PARCELS_DB_PATH" ]]; then
	echo "WARN parcels DB missing: $PARCELS_DB_PATH"
fi
if [[ ! -f "$LEADS_SQLITE_PATH" ]]; then
	echo "WARN leads DB missing: $LEADS_SQLITE_PATH"
fi

if curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
	echo "Backend already running on :8000"
	exit 0
fi

nohup python -m uvicorn florida_property_scraper.api.app:app --host 127.0.0.1 --port 8000 --workers 1 --log-level info > .logs/backend_8000.log 2>&1 &
launcher_pid=$!
echo "Backend starting (logs: .logs/backend_8000.log)"

listener_pid=""
if command -v lsof >/dev/null 2>&1; then
	for i in {1..30}; do
		listener_pid="$(lsof -t -iTCP:8000 -sTCP:LISTEN 2>/dev/null | head -n 1 || true)"
		if [[ -n "$listener_pid" ]]; then
			break
		fi
		if ! ps -p "$launcher_pid" >/dev/null 2>&1; then
			break
		fi
		sleep 1
	done
else
	echo "WARN lsof not found; using launcher pid for tracking"
fi

if [[ -n "$listener_pid" ]]; then
	echo "$listener_pid" > .logs/backend_8000.pid
else
	echo "$launcher_pid" > .logs/backend_8000.pid
fi

for i in {1..30}; do
	if [[ -n "$listener_pid" ]] && ! ps -p "$listener_pid" >/dev/null 2>&1; then
		echo "FAIL uvicorn listener not running"
		tail -n 120 .logs/backend_8000.log || true
		exit 2
	fi
	if [[ -z "$listener_pid" ]] && ! ps -p "$launcher_pid" >/dev/null 2>&1; then
		echo "FAIL uvicorn not running"
		tail -n 120 .logs/backend_8000.log || true
		exit 2
	fi
	if curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
		break
	fi
	sleep 1
done

if ! curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
	echo "FAIL backend health did not return 200"
	tail -n 120 .logs/backend_8000.log || true
	exit 2
fi

curl -sS http://127.0.0.1:8000/api/health | python -m json.tool || true
python scripts/port_inspect.py
