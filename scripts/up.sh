#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_FILE="$ROOT_DIR/.uvicorn_8000.log"
PID_FILE="$ROOT_DIR/.uvicorn_8000.pid"

kill_port() {
  local port="$1"
  echo "$ lsof -t -iTCP:${port} -sTCP:LISTEN | xargs -r kill -9"
  # shellcheck disable=SC2086
  lsof -t -iTCP:${port} -sTCP:LISTEN 2>/dev/null | xargs -r kill -9 || true
}

# Step 1: hard reset port listeners
cd "$ROOT_DIR"
kill_port 8000
kill_port 5173
kill_port 3968

build_frontend_if_needed() {
  if [[ "${FORCE_BUILD:-0}" == "1" ]]; then
    return 0
  fi
  [[ ! -f "$ROOT_DIR/web/dist/index.html" ]]
}

if build_frontend_if_needed; then
  echo "$ (cd web && npm ci && npm run build)"
  cd "$ROOT_DIR/web"
  if [[ -f package-lock.json ]]; then
    npm ci
  else
    npm install
  fi
  npm run build
  cd "$ROOT_DIR"
fi

port_is_listening() {
  # Prefer lsof; fallback to ss.
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:8000 -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null | grep -q ':8000'
    return $?
  fi
  return 1
}

# Step 2: start backend (single port, no reload)
echo "$ source .venv/bin/activate"
# shellcheck disable=SC1091
source "$ROOT_DIR/.venv/bin/activate"

echo "$ export LEADS_SQLITE_PATH=/workspaces/Florida_Property_Scraper/leads.sqlite"
export LEADS_SQLITE_PATH=/workspaces/Florida_Property_Scraper/leads.sqlite

# Kill previous PID if our pidfile exists
if [[ -f "$PID_FILE" ]]; then
  old_pid="$(cat "$PID_FILE" || true)"
  if [[ -n "$old_pid" ]]; then
    echo "$ kill -9 $old_pid || true"
    kill -9 "$old_pid" 2>/dev/null || true
  fi
fi

echo "$ : > .uvicorn_8000.log"
: > "$LOG_FILE"

echo "$ nohup python -m uvicorn florida_property_scraper.api.app:app --host 0.0.0.0 --port 8000 --reload > .uvicorn_8000.log 2>&1 &"
nohup python -m uvicorn florida_property_scraper.api.app:app --host 0.0.0.0 --port 8000 --reload > "$LOG_FILE" 2>&1 &
UVICORN_PID=$!
echo "$ echo UVICORN_PID=$UVICORN_PID"
echo "UVICORN_PID=$UVICORN_PID"
echo "$UVICORN_PID" > "$PID_FILE"

# If the process already died, fail immediately with the fresh log.
sleep 0.1
if ! kill -0 "$UVICORN_PID" 2>/dev/null; then
  echo "uvicorn exited immediately"
  echo "$ ps -p $UVICORN_PID -o pid,ppid,etime,cmd || true"
  ps -p "$UVICORN_PID" -o pid,ppid,etime,cmd || true
  echo "$ tail -n 200 .uvicorn_8000.log || true"
  tail -n 200 "$LOG_FILE" || true
  exit 1
fi

# Wait up to 10s for the port to actually be LISTENING.
echo "$ lsof -nP -iTCP:8000 -sTCP:LISTEN  (or ss fallback)"
listening=0
for _ in {1..40}; do
  if ! kill -0 "$UVICORN_PID" 2>/dev/null; then
    break
  fi
  if port_is_listening; then
    listening=1
    break
  fi
  sleep 0.25
done

if [[ "$listening" -ne 1 ]]; then
  echo "uvicorn failed to bind/listen on port 8000"
  echo "$ ps -p $UVICORN_PID -o pid,ppid,etime,cmd || true"
  ps -p "$UVICORN_PID" -o pid,ppid,etime,cmd || true
  echo "$ lsof -nP -iTCP:8000 -sTCP:LISTEN || true"
  lsof -nP -iTCP:8000 -sTCP:LISTEN || true
  echo "$ ss -ltnp | grep ':8000' || true"
  ss -ltnp | grep ':8000' || true
  echo "$ tail -n 200 .uvicorn_8000.log || true"
  tail -n 200 "$LOG_FILE" || true
  exit 1
fi

# Then wait up to 10s for /health to return 200.
echo "$ curl -sSf http://127.0.0.1:8000/health"
ready=0
for _ in {1..40}; do
  if curl -sSf http://127.0.0.1:8000/health >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.25
done

if [[ "$ready" -ne 1 ]]; then
  echo "uvicorn failed to start"
  echo "$ ps -p $UVICORN_PID -o pid,ppid,etime,cmd || true"
  ps -p "$UVICORN_PID" -o pid,ppid,etime,cmd || true
  echo "$ lsof -nP -iTCP:8000 -sTCP:LISTEN || true"
  lsof -nP -iTCP:8000 -sTCP:LISTEN || true
  echo "$ ss -ltnp | egrep ':8000' || true"
  ss -ltnp | egrep ':8000' || true
  echo "$ tail -n 200 .uvicorn_8000.log || true"
  tail -n 200 "$LOG_FILE" || true
  exit 1
fi

LOCAL_URL="http://127.0.0.1:8000/"
echo "LOCAL_URL=$LOCAL_URL"

if [[ -n "${CODESPACE_NAME:-}" ]]; then
  echo "FORWARDED_URL=https://${CODESPACE_NAME}-8000.app.github.dev/"
else
  echo "FORWARDED_URL=(check Ports tab for port 8000)"
fi

# Step 3: verification (script must fail if any check fails)
set +e

echo "$ curl -sS http://127.0.0.1:8000/ | head -n 5"
out_html="$(curl -sS http://127.0.0.1:8000/ | head -n 5)"
rc1=$?
echo "$out_html"

echo "$ curl -sS http://127.0.0.1:8000/openapi.json | python -c \"import json,sys; d=json.load(sys.stdin); print('/api/lookup/address' in d.get('paths',{}))\""
openapi_ok="$(curl -sS http://127.0.0.1:8000/openapi.json | python -c "import json,sys; d=json.load(sys.stdin); print('/api/lookup/address' in d.get('paths',{}))")"
rc2=$?
echo "$openapi_ok"

echo "$ curl -sS -X POST http://127.0.0.1:8000/api/lookup/address -H \"Content-Type: application/json\" -d '{\"county\":\"seminole\",\"address\":\"105 Pineapple Lane\",\"include_contacts\":false}' | python -m json.tool"
lookup_out="$(curl -sS -X POST http://127.0.0.1:8000/api/lookup/address -H "Content-Type: application/json" -d '{"county":"seminole","address":"105 Pineapple Lane","include_contacts":false}' | python -m json.tool)"
rc3=$?
echo "$lookup_out"

set -e

if [[ "$rc1" -ne 0 || "$rc2" -ne 0 || "$rc3" -ne 0 || "$openapi_ok" != "True" ]]; then
  echo "verification failed"
  echo "$ ps -p $UVICORN_PID -o pid,ppid,etime,cmd || true"
  ps -p "$UVICORN_PID" -o pid,ppid,etime,cmd || true
  echo "$ lsof -nP -iTCP:8000 -sTCP:LISTEN || true"
  lsof -nP -iTCP:8000 -sTCP:LISTEN || true
  echo "$ ss -ltnp | grep ':8000' || true"
  ss -ltnp | grep ':8000' || true
  echo "$ tail -n 200 .uvicorn_8000.log || true"
  tail -n 200 "$LOG_FILE" || true
  exit 1
fi

# Sanity: ensure we served HTML
if ! echo "$out_html" | tr '[:upper:]' '[:lower:]' | grep -q "<!doctype html"; then
  echo "$ tail -n 200 .uvicorn_8000.log || true"
  tail -n 200 "$LOG_FILE" || true
  echo "ERROR: / did not look like HTML"
  exit 1
fi

# Keep backend alive; Ctrl+C should stop it.
trap 'echo "$ kill -TERM $UVICORN_PID"; kill -TERM "$UVICORN_PID" 2>/dev/null || true' INT TERM
wait "$UVICORN_PID"
