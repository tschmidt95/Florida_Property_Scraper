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
  "parcel_ids": ["SEM-0001", "SEM-0002"],
  "provider_keys": ["seminole_official_records"],
  "fixture_mode": true
}
JSON

curl_json_or_fail "http://127.0.0.1:8000/api/enrich" "$resp_tmp" \
  -X POST \
  -H "Content-Type: application/json" \
  --data-binary "@$payload_tmp"

cat <<'JSON' > "$payload_tmp"
{
  "county": "seminole",
  "parcel_ids": ["SEM-0001", "SEM-0002"]
}
JSON

curl_json_or_fail "http://127.0.0.1:8000/api/triggers/evaluate" "$resp_tmp" \
  -X POST \
  -H "Content-Type: application/json" \
  --data-binary "@$payload_tmp"

python -m json.tool "$resp_tmp"

python - <<'PY' "$resp_tmp"
import json
import sys

path = sys.argv[1]
obj = json.load(open(path, "r", encoding="utf-8"))
assert obj.get("ok") is True
results = obj.get("results") or []
fired = [r for r in results if r.get("fired") is True]
assert fired, "no triggers fired"
keys = {str(r.get("trigger_key") or r.get("trigger_id") or "") for r in fired}
assert "deed_transfer_recent" in keys, f"missing deed_transfer_recent: {keys}"
assert "new_recording" in keys, f"missing new_recording: {keys}"
print({"ok": True, "trigger_eval_seminole": "PASS"})
PY
