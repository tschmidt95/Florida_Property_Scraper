import json
import subprocess
import sys

from florida_property_scraper.county_router import enabled_counties


def test_full_dry_run_all_counties():
    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--dry-run",
        "--log-json",
        "--query",
        "Smith",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.startswith("{")]
    logs = [json.loads(line) for line in lines]
    summary = logs[-1]
    log_entries = [l for l in logs[:-1] if "county" in l]
    assert len(log_entries) == len(enabled_counties())
    assert summary["total_counties"] == len(enabled_counties())
