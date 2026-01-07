import subprocess
import sys


def test_county_capability_filtering():
    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--dry-run",
        "--query",
        "123 Main St",
        "--counties",
        "broward,orange",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert "orange" in proc.stdout
    assert "broward" not in proc.stdout
