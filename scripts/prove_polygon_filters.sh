#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
export BASE_URL

build_frontend_if_needed() {
    if [[ ! -f web/dist/index.html ]]; then
        return 0
    fi

    # Rebuild if any source file is newer than the built index.
    if find web/src web/public -type f -newer web/dist/index.html -print -quit 2>/dev/null | grep -q .; then
        return 0
    fi
    if [[ -f web/package.json ]] && [[ web/package.json -nt web/dist/index.html ]]; then
        return 0
    fi
    if [[ -f web/package-lock.json ]] && [[ web/package-lock.json -nt web/dist/index.html ]]; then
        return 0
    fi

    return 1
}

if build_frontend_if_needed; then
    if [[ ! -d web/node_modules ]]; then
        echo "$ (cd web && npm ci)"
        npm --prefix web ci
    fi
    echo "$ (cd web && npm run build)"
    npm --prefix web run build
else
    echo "$ (cd web && npm run build)  # skipped (dist is up-to-date)"
fi

# Backend debug JSONL (used by the Playwright proof to print raw+normalized samples)
export FPS_SEARCH_DEBUG_LOG="${FPS_SEARCH_DEBUG_LOG:-.fps_parcels_search_debug.jsonl}"
: > "$FPS_SEARCH_DEBUG_LOG" || true

# Strict runs prefer correctness over speed; allow more time for inline OCPA.
export INLINE_ENRICH_BUDGET_S="${INLINE_ENRICH_BUDGET_S:-90}"

# Ensure backend is up before running the UI proof.
python -u - <<'PY'
import os
import time
import urllib.request

BASE_URL = os.environ.get('BASE_URL', 'http://127.0.0.1:8000').rstrip('/')

for _ in range(60):
    try:
        with urllib.request.urlopen(f"{BASE_URL}/api/debug/ping", timeout=5) as r:
            _ = r.read()
        print('backend_ok:', BASE_URL)
        raise SystemExit(0)
    except Exception:
        time.sleep(0.25)

raise SystemExit(f'backend not responding at {BASE_URL}')
PY

echo
echo "== UI proof (Playwright): polygon baseline + acceptance filters =="
node web/scripts/smoke_polygon_filters.mjs 2>&1 | tee PROOF_POLYGON_FILTERS.txt
