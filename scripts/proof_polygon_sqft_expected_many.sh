#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

out=$(mktemp)
python scripts/proof_polygon_sqft_expected_many.py | tee "$out"

python - <<'PY' "$out"
import json,sys
path=sys.argv[1]
summary=None
with open(path, 'r', encoding='utf-8') as f:
    for line in f:
        if line.startswith('SUMMARY '):
            summary=json.loads(line[len('SUMMARY '):].strip())
if not summary:
    raise SystemExit('FAIL: missing SUMMARY')
expected=int(summary.get('expected_count') or 0)
api_count=int(summary.get('api_counts', {}).get('sqft_filter') or 0)
coverage=float(summary.get('coverage_pct', {}).get('sqft_filter') or 0)
if expected > 0 and api_count < 0.8 * expected:
    raise SystemExit(f'FAIL: api_count {api_count} < 80% of expected {expected}')
if coverage <= 0:
    raise SystemExit('FAIL: coverage_pct for living_area_sqft is 0')
print('PASS sqft expected-count proof')
PY

rm -f "$out"
