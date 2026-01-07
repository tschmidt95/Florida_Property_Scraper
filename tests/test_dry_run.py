import subprocess
import sys


def test_dry_run_prints_plan():
    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--dry-run",
        "--query",
        "Smith",
        "--counties",
        "broward",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert "broward" in proc.stdout
    assert "broward_spider" in proc.stdout
