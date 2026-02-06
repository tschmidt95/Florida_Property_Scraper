#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

payload_tmp="$(mktemp)"
resp_tmp="$(mktemp)"
cleanup() {
  rm -f "$payload_tmp" "$resp_tmp"
}
trap cleanup EXIT

cat <<'JSON' > "$payload_tmp"
{
  "county": "seminole",
  "parcel_ids": ["SEM-0001"],
  "provider_keys": ["seminole_official_records"],
  "fixture_mode": true
}
JSON

curl_json_or_fail "http://127.0.0.1:8000/api/enrich" "$resp_tmp" \
  -X POST \
  -H "Content-Type: application/json" \
  --data-binary "@$payload_tmp"

curl_json_or_fail "http://127.0.0.1:8000/api/debug/provider_evidence?county=seminole&parcel_id=SEM-0001" "$resp_tmp"

python -m json.tool "$resp_tmp"

python - <<'PY' "$resp_tmp"
import json
import sys

path = sys.argv[1]
obj = json.load(open(path, "r", encoding="utf-8"))
assert obj.get("ok") is True
count = int(obj.get("count") or 0)
assert count > 0, f"evidence_count={count}"
print({"ok": True, "provider_evidence_persist": "PASS", "count": count})
PY
