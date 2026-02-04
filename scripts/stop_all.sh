#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

port_listener_pid() {
  local port="$1"
  if ! command -v lsof >/dev/null 2>&1; then
    return 1
  fi
  lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
}

kill_pid_file_if_listening() {
  local port="$1"
  local pid_file="$2"
  local killed=""
  if [[ -f "$pid_file" ]]; then
    local pid
    pid=$(cat "$pid_file" || true)
    if [[ -n "$pid" ]] && ps -p "$pid" >/dev/null 2>&1; then
      local listener_pid
      listener_pid="$(port_listener_pid "$port")"
      if [[ -n "$listener_pid" && "$listener_pid" == "$pid" ]]; then
        kill "$pid" >/dev/null 2>&1 || true
        sleep 1
        if ps -p "$pid" >/dev/null 2>&1; then
          kill -9 "$pid" >/dev/null 2>&1 || true
        fi
        killed="$pid"
      fi
    fi
    rm -f "$pid_file" || true
  fi
  echo "$killed"
}

backend_killed="$(kill_pid_file_if_listening 8000 .logs/backend_8000.pid)"
ui_killed="$(kill_pid_file_if_listening 5173 .logs/ui_5173.pid)"
vite_killed="$(kill_pid_file_if_listening 5173 .logs/vite_5173.pid)"

if [[ -z "$backend_killed" ]]; then
  pkill -f "uvicorn.*8000" >/dev/null 2>&1 || true
fi
if [[ -z "$ui_killed" && -z "$vite_killed" ]]; then
  pkill -f "vite.*5173" >/dev/null 2>&1 || true
  pkill -f "node .*vite" >/dev/null 2>&1 || true
fi

echo "Stopped: backend=${backend_killed:-none} ui=${ui_killed:-none} vite=${vite_killed:-none}"

if [[ -x "scripts/stop_8000_5173.sh" ]]; then
  bash scripts/stop_8000_5173.sh || true
fi

python scripts/port_inspect.py || true