#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

python -m compileall -q src

bash -x scripts/dev_reset.sh

curl -sS http://127.0.0.1:8000/api/health | python -m json.tool
curl -sS http://127.0.0.1:8000/api/debug/ping | python -m json.tool

bash -x scripts/smoke_filters.sh

pytest -q
