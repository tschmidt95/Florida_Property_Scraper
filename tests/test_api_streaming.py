import json
import os
from pathlib import Path

from florida_property_scraper.api.app import stream_search
from florida_property_scraper.cache import cache_clear


def test_api_streaming_fixture_mode():
    cache_clear()
    fixture = Path("tests/fixtures/broward_realistic.html")
    lines = list(
        stream_search(
            state="fl",
            county="broward",
            query="Smith",
            backend="native",
            mode="fixture",
            fixture_path=fixture,
        )
    )
    payloads = [json.loads(line) for line in lines]
    records = [p["record"] for p in payloads if "record" in p]
    summaries = [p["summary"] for p in payloads if "summary" in p]
    assert len(records) >= 2
    assert summaries
