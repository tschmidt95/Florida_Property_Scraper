#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

bash scripts/start_vite_only.sh

sleep 2

echo "--- UI head 50 ---"
html=$(curl -sS http://127.0.0.1:5173/ | head -n 50)
echo "$html"
echo "$html" | grep -q "id=\"root\"" && echo "PASS ui root" || { echo "FAIL ui root not found"; exit 2; }
