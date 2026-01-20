#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

mkdir -p .logs
rm -f .logs/vite_5173.log || true

(cd web && nohup npm run dev -- --host 0.0.0.0 --port 5173 > ../.logs/vite_5173.log 2>&1 &)

sleep 2

tmp_html=$(mktemp)

echo "== HTML (first 40) =="
curl -sS http://127.0.0.1:5173/ -o "$tmp_html"
head -n 40 "$tmp_html"

echo "== HEADERS (first 12) =="
curl -I http://127.0.0.1:5173/ | head -n 12

echo "== ASSERT Vite dev markers present =="
if grep -q "/@vite/client" "$tmp_html" && grep -q "/src/main" "$tmp_html"; then
	echo "PASS vite_serving_source"
else
	echo "FAIL vite_serving_source"
	echo "Hint: tail .logs/vite_5173.log"
	exit 2
fi

python scripts/port_inspect.py
