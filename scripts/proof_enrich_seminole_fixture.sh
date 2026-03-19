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

echo
echo "== bulk data fill + completeness report =="
/workspaces/Florida_Property_Scraper/.venv/bin/python - <<'PY'
import os
import pathlib

os.environ["COUNTY"] = "seminole"
os.environ["MAX_PARCELS"] = "300"
os.environ["OWNER_ENRICH_LIMIT"] = "200"
os.environ["ENRICH_BATCH_SIZE"] = "50"

from scripts.complete_data_fill_and_report import main
import scripts.complete_data_fill_and_report as _m

print({"completeness_module": getattr(_m, "__file__", None)})

rc = int(main())
if rc != 0:
  raise SystemExit(rc)

txt = pathlib.Path("/workspaces/Florida_Property_Scraper/PROOF_DATA_COMPLETENESS_REPORT.txt")
js = pathlib.Path("/workspaces/Florida_Property_Scraper/data/data_completeness_report.json")
print({"report_txt_exists": txt.exists(), "report_json_exists": js.exists()})
PY

echo
echo "== report preview =="
head -n 80 /workspaces/Florida_Property_Scraper/PROOF_DATA_COMPLETENESS_REPORT.txt || true
