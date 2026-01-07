#!/usr/bin/env bash
set -euo pipefail

PYTHONPATH="$(pwd)/src" python3 -m florida_property_scraper "$@"
