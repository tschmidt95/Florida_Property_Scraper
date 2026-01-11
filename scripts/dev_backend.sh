#!/usr/bin/env bash
set -euo pipefail

uvicorn florida_property_scraper.api.app:app --reload --port 8000
