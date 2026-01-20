#!/bin/bash
set -euo pipefail

# Ingest Seminole PA data into leads.sqlite pa_properties
export PYTHONPATH=src
python3 -m florida_property_scraper.pa_adapters.seminole_pa_ingest

# Print pa_properties counts by county
sqlite3 leads.sqlite "SELECT county, COUNT(*) FROM pa_properties GROUP BY county;"
