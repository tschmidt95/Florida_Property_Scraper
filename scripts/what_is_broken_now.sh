#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOCALHOST_URL="http://127.0.0.1:8000/"

echo '$ curl -sS -o /dev/null -w "HTTP_STATUS=%{http_code}\n" http://127.0.0.1:8000/api/debug/ping'
curl -sS -o /dev/null -w "HTTP_STATUS=%{http_code}\n" http://127.0.0.1:8000/api/debug/ping

echo

echo '$ gh codespace ports --json sourcePort,browseUrl (filter port 8000)'
echo "CODESPACE_NAME=${CODESPACE_NAME:-}"
echo "GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN=${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-}"

PORTS_JSON="[]"
if command -v gh >/dev/null 2>&1; then
  if [[ -n "${CODESPACE_NAME:-}" ]]; then
    echo "$ gh codespace ports -c ${CODESPACE_NAME} --json sourcePort,browseUrl"
    PORTS_JSON="$(gh codespace ports -c "${CODESPACE_NAME}" --json sourcePort,browseUrl 2>&1 || true)"
    echo "$PORTS_JSON"
  else
    echo '$ gh codespace ports --json sourcePort,browseUrl'
    PORTS_JSON="$(gh codespace ports --json sourcePort,browseUrl 2>&1 || true)"
    echo "$PORTS_JSON"
  fi
else
  echo 'gh not found on PATH'
fi

FORWARDED_URL="$(python - <<'PY'
import json,sys
s=sys.stdin.read() or '[]'
try:
  j=json.loads(s)
except Exception:
  j=[]
url=None
for it in j or []:
  try:
    if int(it.get('sourcePort') or 0)==8000 and it.get('browseUrl'):
      url=it['browseUrl']
      break
  except Exception:
    pass
print(url or '(unavailable)')
PY
<<<"$PORTS_JSON")"

# Fallback: if gh cannot discover ports but Codespaces env is present, compute the
# human-browse URL directly.
if [[ "$FORWARDED_URL" == "(unavailable)" && -n "${CODESPACE_NAME:-}" && -n "${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-}" ]]; then
  FORWARDED_URL="https://${CODESPACE_NAME}-8000.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}/"
fi

echo "LOCALHOST_URL=${LOCALHOST_URL}"
echo "FORWARDED_URL=${FORWARDED_URL}"

echo

FORWARDED_PING_STATUS="(skipped)"
if [[ "$FORWARDED_URL" != "(unavailable)" ]]; then
  echo '$ curl -sS -o /dev/null -w "HTTP_STATUS=%{http_code}\n" "${FORWARDED_URL}api/debug/ping"'
  FORWARDED_PING_STATUS="$(curl -sS -o /dev/null -w "HTTP_STATUS=%{http_code}" "${FORWARDED_URL}api/debug/ping" 2>/dev/null || true)"
  echo "${FORWARDED_PING_STATUS}"
fi

export FORWARDED_PING_STATUS

echo

echo '$ BASE_URL=<localhost> node web/scripts/diag_runtime_report.mjs'
BASE_URL="$LOCALHOST_URL" DIAG_LABEL="localhost" node web/scripts/diag_runtime_report.mjs > /tmp/diag_local.json
python - <<'PY'
import json

def load(p):
  with open(p,'r',encoding='utf-8') as f:
    return json.load(f)

r = load('/tmp/diag_local.json')
print({
  'baseUrl': r.get('baseUrl'),
  'ok': r.get('ok'),
  'errors': (r.get('errors') or [])[:5],
  'polygonRunOk': r.get('polygonRunOk'),
  'polygonResponseStatus': r.get('polygonResponseStatus'),
  'polygonRequestSeen': r.get('polygonRequestSeen'),
  'consoleErrorsCount': len(r.get('consoleErrors') or []),
  'consoleLinesCount': len(r.get('consoleLines') or []),
})
PY

echo

echo '$ BASE_URL=<forwarded> node web/scripts/diag_runtime_report.mjs'
BASE_URL="$FORWARDED_URL" DIAG_LABEL="forwarded" node web/scripts/diag_runtime_report.mjs > /tmp/diag_fwd.json
python - <<'PY'
import json

def load(p):
  with open(p,'r',encoding='utf-8') as f:
    return json.load(f)

r = load('/tmp/diag_fwd.json')
print({
  'baseUrl': r.get('baseUrl'),
  'ok': r.get('ok'),
  'errors': (r.get('errors') or [])[:5],
  'polygonRunOk': r.get('polygonRunOk'),
  'polygonResponseStatus': r.get('polygonResponseStatus'),
  'polygonRequestSeen': r.get('polygonRequestSeen'),
  'consoleErrorsCount': len(r.get('consoleErrors') or []),
  'consoleLinesCount': len(r.get('consoleLines') or []),
})
PY

echo

echo '$ curl -sS http://127.0.0.1:8000/api/debug/ping (stable fields)'
curl -sS http://127.0.0.1:8000/api/debug/ping > /tmp/debug_ping.json
python - <<'PY'
import json

with open('/tmp/debug_ping.json','r',encoding='utf-8') as f:
  d=json.load(f)

out={
  'ok': d.get('ok'),
  'git': d.get('git'),
  'env': {k: (d.get('env') or {}).get(k) for k in ['APP_GIT_SHA','APP_GIT_BRANCH'] if (d.get('env') or {}).get(k) is not None},
}
print(json.dumps(out, sort_keys=True))
PY

echo

echo '$ curl -sS http://127.0.0.1:8000/health'
curl -sS http://127.0.0.1:8000/health

echo

echo '$ curl /api/openapi.json (or /openapi.json) status + first 30 lines'
OPENAPI_URL="http://127.0.0.1:8000/api/openapi.json"
OPENAPI_STATUS="$(curl -sS -o /tmp/openapi.json -w "%{http_code}" "$OPENAPI_URL" || true)"
if [[ "$OPENAPI_STATUS" == "404" ]]; then
  OPENAPI_URL="http://127.0.0.1:8000/openapi.json"
  OPENAPI_STATUS="$(curl -sS -o /tmp/openapi.json -w "%{http_code}" "$OPENAPI_URL" || true)"
fi
echo "OPENAPI_URL=${OPENAPI_URL}"
echo "HTTP_STATUS=${OPENAPI_STATUS}"
if python -m json.tool /tmp/openapi.json >/tmp/openapi_pretty.json 2>/dev/null; then
  head -n 30 /tmp/openapi_pretty.json || true
else
  head -n 30 /tmp/openapi.json || true
fi

echo

echo '$ BASE_URL=http://127.0.0.1:8000 bash scripts/prove_polygon_search.sh'
BASE_URL="http://127.0.0.1:8000" bash scripts/prove_polygon_search.sh

echo

python - <<'PY'
import json
import os

def load(path):
  with open(path,'r',encoding='utf-8') as f:
    return json.load(f)

local = load('/tmp/diag_local.json')
fwd = load('/tmp/diag_fwd.json')

rows = [
  ('LOCALHOST', 'http://127.0.0.1:8000/', local, 'HTTP_STATUS=200'),
  ('FORWARDED', fwd.get('baseUrl') or '(unavailable)', fwd, os.environ.get('FORWARDED_PING_STATUS') or '(unknown)'),
]

def b(x):
  return bool(x) is True

def count(arr):
  return len(arr) if isinstance(arr, list) else 0

def passfail(r):
  page_ok = b(r.get('ok'))
  poly_ok = b(r.get('polygonRunOk'))
  errc = count(r.get('errors'))
  rfc = count(r.get('requestFailed'))
  h4c = count(r.get('http4xx5xx'))
  cec = count(r.get('consoleErrors'))
  ok = page_ok and poly_ok and errc == 0 and rfc == 0 and h4c == 0 and cec == 0
  return ok

print('URL | ping | page load ok | console lines | requestfailed count | 4xx/5xx count | polygon run ok | PASS/FAIL')
for _, url, r, ping in rows:
  page_ok = 'PASS' if b(r.get('ok')) else 'FAIL'
  poly_ok = 'PASS' if b(r.get('polygonRunOk')) else 'FAIL'
  cec = count(r.get('consoleLines'))
  rfc = count(r.get('requestFailed'))
  h4c = count(r.get('http4xx5xx'))
  pf = 'PASS' if passfail(r) else 'FAIL'
  print(f'{url} | {ping} | {page_ok} | {cec} | {rfc} | {h4c} | {poly_ok} | {pf}')

any_fail = any(not passfail(r) for _,_,r,_ in rows)
if any_fail:
  print('\nTOP_ROOT_CAUSES (max 10)')
  items=[]
  def add(s):
    if s and s not in items:
      items.append(s)
  for label, _, r, _ in rows:
    for e in (r.get('errors') or []):
      add(f'[{label}] exception: {e}')
    for ce in (r.get('consoleErrors') or []):
      t = (ce.get('type') if isinstance(ce, dict) else 'error')
      msg = (ce.get('text') if isinstance(ce, dict) else str(ce))
      add(f'[{label}] console {t}: {msg}')
    for rf in (r.get('requestFailed') or []):
      if isinstance(rf, dict):
        add(f"[{label}] requestfailed: {rf.get('method','')} {rf.get('url','')} errorText={rf.get('errorText','')}")
      else:
        add(f'[{label}] requestfailed: {rf}')
    for h in (r.get('http4xx5xx') or []):
      if isinstance(h, dict):
        add(f"[{label}] http{h.get('status')}: {h.get('url')}")
      else:
        add(f'[{label}] http4xx5xx: {h}')

  for line in items[:10]:
    print(line)
PY
