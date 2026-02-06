#!/usr/bin/env bash
set -euo pipefail

cd /workspaces/Florida_Property_Scraper

# shellcheck disable=SC1091
source "/workspaces/Florida_Property_Scraper/scripts/_curl_json_helper.sh"

resp_tmp="$(mktemp)"
cleanup() {
  rm -f "$resp_tmp"
}
trap cleanup EXIT

curl_json_or_fail "http://127.0.0.1:8000/api/providers/catalog?county=seminole" "$resp_tmp"
python -m json.tool "$resp_tmp"

python - <<'PY' "$resp_tmp"
import json
import sys

path = sys.argv[1]
obj = json.load(open(path, "r", encoding="utf-8"))
assert obj.get("ok") is True
providers = obj.get("providers") or []
match = [p for p in providers if p.get("provider_id") == "seminole_official_records"]
assert match, "missing seminole_official_records"
status = str(match[0].get("status") or "").strip().lower()
assert status == "implemented", f"status={status}"
print({"ok": True, "providers_catalog_seminole": "PASS"})
PY
