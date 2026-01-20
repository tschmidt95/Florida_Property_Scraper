#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== restarting =="
bash scripts/up.sh

echo

echo "== ping =="
curl -sS http://127.0.0.1:8000/api/debug/ping | python -m json.tool

echo

head_sha=$(git rev-parse --short HEAD)
ping_sha=$(curl -sS http://127.0.0.1:8000/api/debug/ping | python -c "import sys,json; print(json.load(sys.stdin)['git']['sha'])")

echo "HEAD=${head_sha}"
echo "ping.git.sha=${ping_sha}"

if [[ "${head_sha}" != "${ping_sha}" ]]; then
  echo "MISMATCH: ping SHA != HEAD" >&2
  exit 2
fi

echo "OK: ping SHA matches HEAD"
