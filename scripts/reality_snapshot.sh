#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

pwd
git rev-parse --abbrev-ref HEAD
git rev-parse --short HEAD
git status --porcelain=v1 -b
python -V
node -v
npm -v
python scripts/port_inspect.py

echo "PASS reality_snapshot"
