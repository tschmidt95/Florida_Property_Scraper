#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

for i in $(seq 1 20); do
  if curl -sS http://127.0.0.1:8000/api/debug/ping >/dev/null; then
    echo "OK $i"
  else
    echo "FAIL $i"
    echo "== FREE =="; free -h || true
    echo "== TOP MEM =="; ps aux --sort=-%mem | head -n 16 || true
    echo "== PORTS =="; python scripts/port_inspect.py || true
    echo "== BACKEND LOG TAIL =="; tail -n 120 .logs/backend_8000.log || true
    exit 2
  fi
  sleep 1
done

echo "PASS ping_20s"
