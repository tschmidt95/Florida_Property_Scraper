#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/_curl_json_helper.sh"

health_tmp_1="$(mktemp)"
health_tmp_2="$(mktemp)"
cleanup() {
	rm -f "$health_tmp_1" "$health_tmp_2"
}
trap cleanup EXIT

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

echo "== backend health: start =="
curl_json_or_fail "http://127.0.0.1:8000/api/health" "$health_tmp_1"
python -m json.tool "$health_tmp_1"

sleep 5

echo "== backend health: after 5s =="
curl_json_or_fail "http://127.0.0.1:8000/api/health" "$health_tmp_2"
python -m json.tool "$health_tmp_2"

pid_ok="0"
listener_ok="0"

if [[ -f "$ROOT_DIR/.logs/backend_8000.pid" ]]; then
	pid="$(cat "$ROOT_DIR/.logs/backend_8000.pid" 2>/dev/null || true)"
	if [[ -n "$pid" ]] && ps -p "$pid" >/dev/null 2>&1; then
		pid_ok="1"
	fi
fi

listener_pid="$(listener_pid_for_port 8000)"
if [[ -n "$listener_pid" ]]; then
	listener_ok="1"
fi

if [[ "$pid_ok" != "1" && "$listener_ok" != "1" ]]; then
	echo "FAIL backend not running: pid_ok=${pid_ok} listener_ok=${listener_ok}"
	exit 2
fi

echo "PASS backend stays up"
