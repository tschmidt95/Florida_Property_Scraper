import json
import subprocess
import sys
from pathlib import Path

from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter


def test_smoke_import():
    import importlib.util
    spec = importlib.util.find_spec('florida_property_scraper')
    assert spec is not None
    # simple import to ensure module can be imported
    module = __import__('florida_property_scraper')
    assert hasattr(module, '__name__')


def test_adapter_demo_returns_fixture():
    adapter = ScrapyAdapter(demo=True)
    results = adapter.search("irrelevant", start_urls=["file://unused"], spider_name="broward_spider")
    assert isinstance(results, list)
    assert results and "owner" in results[0]


def test_scrapy_runner_on_fixture():
    sample = Path(__file__).parent / "fixtures" / "broward_sample.html"
    assert sample.exists(), "Fixture missing: tests/fixtures/broward_sample.html"
    file_url = sample.resolve().as_uri()

    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper.backend.scrapy_runner",
        "--spider-name",
        "broward_spider",
        "--start-urls",
        json.dumps([file_url]),
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, f"Runner failed: {proc.stderr}\n{proc.stdout}"
    data = json.loads(proc.stdout)
    assert isinstance(data, list)
    assert len(data) >= 2

