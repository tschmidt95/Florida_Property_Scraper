import json
import subprocess
import sys


def test_run_summary_output():
    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--demo",
        "--query",
        "Smith",
        "--counties",
        "broward",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.startswith("{")]
    summary = json.loads(lines[-1])
    assert summary["total_counties"] == 1
    assert summary["total_items"] >= 1
