#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

mkdir -p .logs
rm -f .logs/ui_5173.log || true

listener_pid_for_port() {
  local port="$1"
  if [[ -z "$port" ]]; then
    return 1
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof -t -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
    return 0
  fi
  if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null | awk -v p=":${port}" '$0 ~ p { if (match($0, /pid=([0-9]+)/, m)) { print m[1]; exit } }'
    return 0
  fi
  return 1
}

is_listening_pid() {
  local port="$1"
  local pid="$2"
  if [[ -z "$pid" || -z "$port" ]]; then
    return 1
  fi
  local listener_pid
  listener_pid="$(listener_pid_for_port "$port")"
  [[ -n "$listener_pid" && "$listener_pid" == "$pid" ]]
}

if curl -fsS http://127.0.0.1:5173/ >/dev/null 2>&1; then
  listener_pid="$(listener_pid_for_port 5173)"
  if [[ -n "$listener_pid" ]]; then
    echo "$listener_pid" > .logs/ui_5173.pid
    echo "UI already running (pid=$listener_pid)"
  else
    echo "UI already running on :5173"
  fi
  exit 0
fi

listener_pid="$(listener_pid_for_port 5173)"
if [[ -n "$listener_pid" ]]; then
  echo "FAIL port 5173 already in use (pid=$listener_pid)"
  exit 2
fi

if [[ -f .logs/ui_5173.pid ]]; then
  pid=$(cat .logs/ui_5173.pid || true)
  if [[ -n "$pid" ]] && ps -p "$pid" >/dev/null 2>&1; then
    if is_listening_pid 5173 "$pid"; then
      echo "FAIL port 5173 already in use (pid=$pid)"
      exit 2
    fi
  fi
  rm -f .logs/ui_5173.pid || true
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
