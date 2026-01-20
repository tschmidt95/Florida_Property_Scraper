#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

python scripts/kill_port.py 8000 || true
python scripts/kill_port.py 5173 || true
sleep 1
python scripts/port_inspect.py

out8000=$(python scripts/port_inspect.py 8000)
out5173=$(python scripts/port_inspect.py 5173)
echo "$out8000"
echo "$out5173"

if echo "$out8000" | grep -q "port=8000 FREE" && echo "$out5173" | grep -q "port=5173 FREE"; then
	echo "PASS ports_free"
else
	echo "FAIL ports_free"
	exit 2
fi
