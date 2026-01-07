import os
import subprocess
import sys

import pytest


pytestmark = pytest.mark.skipif(
    os.getenv("LIVE") != "1",
    reason="Live tests disabled; set LIVE=1 to enable.",
)


@pytest.mark.live
def test_live_broward_smoke():
    if os.getenv("LIVE") != "1":
        pytest.skip("Live tests disabled; set LIVE=1 to enable.")
    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--live",
        "--debug-html",
        "--query",
        "Smith",
        "--counties",
        "broward",
        "--max-items",
        "1",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert "Found" in proc.stdout
