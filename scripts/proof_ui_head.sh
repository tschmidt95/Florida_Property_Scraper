#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

health_tmp="$(mktemp)"
cleanup() {
	rm -f "$health_tmp"
}
trap cleanup EXIT

if ! curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
	echo "FAIL backend not running. Start with: scripts/start_backend_minimal.sh"
	exit 2
fi

if ! curl -fsS http://127.0.0.1:5173/ >/dev/null 2>&1; then
	echo "FAIL UI not running. Start with: scripts/start_ui_minimal.sh"
	exit 2
fi

sleep 2

echo "--- UI head 50 ---"
html=$(curl -sS http://127.0.0.1:5173/ | head -n 50)
echo "$html"
echo "$html" | grep -q "id=\"root\"" && echo "PASS ui root" || { echo "FAIL ui root not found"; exit 2; }

echo "--- UI /api/health ---"
curl_json_or_fail "http://127.0.0.1:5173/api/health" "$health_tmp"
python -m json.tool "$health_tmp"
