import json
from pathlib import Path

from florida_property_scraper.api.app import stream_search


def test_streaming_respects_max_items():
    fixture = Path("tests/fixtures/broward_realistic.html")
    lines = list(
        stream_search(
            state="fl",
            county="broward",
            query="Smith",
            backend="native",
            mode="fixture",
            max_items=1,
            fixture_path=fixture,
        )
    )
    payloads = [json.loads(line) for line in lines]
    records = [p for p in payloads if "record" in p]
    summaries = [p for p in payloads if "summary" in p]
    assert len(records) == 1
    assert summaries
