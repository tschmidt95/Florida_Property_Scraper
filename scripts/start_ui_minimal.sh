#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

mkdir -p .logs
rm -f .logs/ui_5173.log || true

if [[ -f .logs/ui_5173.pid ]]; then
  pid=$(cat .logs/ui_5173.pid || true)
  if [[ -n "$pid" ]] && ps -p "$pid" >/dev/null 2>&1; then
    if curl -fsS http://127.0.0.1:5173/ >/dev/null 2>&1; then
      echo "UI already running (pid=$pid)"
      exit 0
    fi
    kill "$pid" >/dev/null 2>&1 || true
    sleep 1
    if ps -p "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  fi
  rm -f .logs/ui_5173.pid || true
fi

rm -f .logs/ui_5173.pid || true

if curl -fsS http://127.0.0.1:5173/ >/dev/null 2>&1; then
  echo "UI already running on :5173"
  exit 0
fi

if [[ ! -d "web/node_modules" ]]; then
  cd web
  npm install
  cd ..
fi

cd web
nohup npm run dev -- --host 0.0.0.0 --port 5173 --strictPort > ../.logs/ui_5173.log 2>&1 &
echo $! > ../.logs/ui_5173.pid
echo "UI starting (logs: .logs/ui_5173.log)"

for i in {1..30}; do
  if ! ps -p "$(cat ../.logs/ui_5173.pid)" >/dev/null 2>&1; then
    echo "FAIL UI not running"
    tail -n 80 ../.logs/ui_5173.log || true
    exit 2
  fi
  if curl -fsS http://127.0.0.1:5173/ >/dev/null 2>&1; then
    if curl -fsS http://127.0.0.1:5173/api/health >/dev/null 2>&1; then
      echo "UI up (API proxy OK)"
    else
      echo "UI up (WARN: API proxy not reachable yet)"
    fi
    exit 0
  fi
  sleep 1
done

echo "FAIL UI health did not return 200"
tail -n 80 ../.logs/ui_5173.log || true
exit 2
