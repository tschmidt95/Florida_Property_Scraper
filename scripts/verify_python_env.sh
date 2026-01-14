#!/usr/bin/env bash
set -euo pipefail
PY="/workspaces/Florida_Property_Scraper/.venv/bin/python"
echo "PY=$PY"
"$PY" -c "import sys; print('sys.executable:', sys.executable); print('sys.version:', sys.version)"
"$PY" -c "import florida_property_scraper; print('import ok:', florida_property_scraper.__file__)"
echo "SUCCESS"
