import json
import subprocess
import sys
from pathlib import Path


def test_scrapy_runner_respects_max_items():
    sample = Path(__file__).parent / "fixtures" / "broward_sample.html"
    file_url = sample.resolve().as_uri()

    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper.backend.scrapy_runner",
        "--spider-name",
        "broward_spider",
        "--start-urls",
        json.dumps([file_url]),
        "--max-items",
        "1",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    items = json.loads(proc.stdout)
    assert isinstance(items, list)
    assert len(items) == 1
