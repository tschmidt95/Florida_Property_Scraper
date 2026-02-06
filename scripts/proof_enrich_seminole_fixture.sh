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

python -m json.tool "$resp_tmp"

python - <<'PY' "$resp_tmp"
import json
import sys

path = sys.argv[1]
obj = json.load(open(path, "r", encoding="utf-8"))
assert obj.get("ok") is True
assert obj.get("mode") == "evidence_only"
merged = obj.get("merged_fields") or {}
assert "SEM-0001" in merged, "missing merged fields for SEM-0001"
assert "last_sale_date" in merged["SEM-0001"], "missing last_sale_date evidence"
count = len(obj.get("evidence") or [])
assert count >= 2, f"evidence_count={count}"
print({"ok": True, "enrich_seminole_fixture": "PASS"})
PY
