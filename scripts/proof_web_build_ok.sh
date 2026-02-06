#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper/web

npm run build

echo "PASS web_build_ok"
