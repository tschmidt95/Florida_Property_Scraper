#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== curl -I http://127.0.0.1:5173/ | head -n 10 =="
curl -I http://127.0.0.1:5173/ | head -n 10

echo

echo "== rg -n \"Legacy map panel disabled\" web/src -S =="
rg -n "Legacy map panel disabled" web/src -S || true

echo

echo "== sed -n 1,60p web/src/pages/MapSearch.tsx =="
sed -n "1,60p" web/src/pages/MapSearch.tsx

echo

echo "== rg -n \"export default function MapSearch|function MapSearch\" web/src/pages/MapSearch.tsx =="
rg -n "export default function MapSearch|function MapSearch" web/src/pages/MapSearch.tsx || true

echo

ret_line=$(rg -n "return \\(\" web/src/pages/MapSearch.tsx | head -n 1 | cut -d: -f1 || true)
if [[ -n "${ret_line}" ]]; then
  start=$((ret_line-20))
  if [[ ${start} -lt 1 ]]; then start=1; fi
  end=$((ret_line+140))
  echo
  echo "== MapSearch return block (~160 lines around return), starting at line ${start} =="
  sed -n "${start},${end}p" web/src/pages/MapSearch.tsx
else
  echo "Could not locate return( in MapSearch.tsx" >&2
fi
