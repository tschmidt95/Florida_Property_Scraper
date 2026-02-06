#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

port_listener_pid() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
    return 0
  fi
  if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null | awk -v p=":${port}" '$0 ~ p { if (match($0, /pid=([0-9]+)/, m)) { print m[1]; exit } }'
    return 0
  fi
  return 1
}

kill_listener_pid() {
  local port="$1"
  local killed=""
  local pid
  pid="$(port_listener_pid "$port")"
  if [[ -n "$pid" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
    sleep 1
    if ps -p "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    killed="$pid"
  fi
  echo "$killed"
}

backend_killed="$(kill_listener_pid 8000)"
ui_killed="$(kill_listener_pid 5173)"
vite_killed="${ui_killed}"

rm -f .logs/backend_8000.pid .logs/ui_5173.pid .logs/vite_5173.pid || true

backend_pkill=""
ui_pkill=""

if [[ -n "$(port_listener_pid 8000)" ]]; then
  pkill -f "uvicorn .*--port 8000" >/dev/null 2>&1 || true
  pkill -f "python -m uvicorn .*--port 8000" >/dev/null 2>&1 || true
  backend_pkill="pkill"
fi
if [[ -n "$(port_listener_pid 5173)" ]]; then
  pkill -f "vite.*5173" >/dev/null 2>&1 || true
  pkill -f "node .*vite" >/dev/null 2>&1 || true
  ui_pkill="pkill"
fi

echo "Stopped: backend=${backend_killed:-none} ui=${ui_killed:-none} vite=${vite_killed:-none} backend_fallback=${backend_pkill:-none} ui_fallback=${ui_pkill:-none}"

if [[ -x "scripts/stop_8000_5173.sh" ]]; then
  bash scripts/stop_8000_5173.sh || true
fi

python scripts/port_inspect.py || true