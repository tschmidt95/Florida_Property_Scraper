#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

mkdir -p .logs
rm -f .logs/ui_5173.log || true
rm -f .logs/ui_5173.pid || true

if [[ ! -d "web/node_modules" ]]; then
  cd web
  npm install
  cd ..
fi

cd web
nohup npm run dev -- --host 0.0.0.0 --port 5173 --strictPort > ../.logs/ui_5173.log 2>&1 &
echo $! > ../.logs/ui_5173.pid
