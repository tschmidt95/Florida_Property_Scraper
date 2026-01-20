#!/bin/bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <SEMINOLE_PARCELS_INPUT>"
  echo "  Input: GeoJSON (preferred) or Shapefile (.shp) for Seminole County parcels."
  exit 1
fi

INPUT="$1"
DB_PATH="data/parcels/parcels.sqlite"

if [[ -f "$DB_PATH" ]]; then
  echo "WARNING: This will DELETE $DB_PATH and re-import ALL parcels for Seminole County."
  read -p "Type YES to continue: " confirm
  if [[ "$confirm" != "YES" ]]; then
    echo "Aborted."
    exit 1
  fi
  rm -f "$DB_PATH"
fi

python3 -m florida_property_scraper.tools.ingest_parcels --county seminole --input "$INPUT"

# Print total rows and bbox for Seminole
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT 'Total parcels' AS label, COUNT(*) AS count FROM parcels WHERE county='seminole';
SELECT 'BBox (min/max)' AS label, MIN(lon), MIN(lat), MAX(lon), MAX(lat) FROM parcels WHERE county='seminole';
SQL
