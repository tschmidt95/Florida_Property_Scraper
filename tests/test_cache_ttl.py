import json
import os
from pathlib import Path

from florida_property_scraper.api.app import stream_search
from florida_property_scraper.cache import cache_clear, cache_stats


def test_cache_hit_and_miss():
    cache_clear()
    previous = os.environ.get("CACHE")
    os.environ["CACHE"] = "1"
    os.environ["CACHE_STREAM"] = "1"
    fixture = Path("tests/fixtures/broward_realistic.html")
    list(
        stream_search(
            state="fl",
            county="broward",
            query="Smith",
            backend="native",
            mode="fixture",
            fixture_path=fixture,
        )
    )
    stats_after_first = cache_stats()
    list(
        stream_search(
            state="fl",
            county="broward",
            query="Smith",
            backend="native",
            mode="fixture",
            fixture_path=fixture,
        )
    )
    stats_after_second = cache_stats()
    assert stats_after_first["misses"] >= 1
    assert stats_after_second["hits"] >= 1
    if previous is None:
        os.environ.pop("CACHE", None)
    else:
        os.environ["CACHE"] = previous
    os.environ.pop("CACHE_STREAM", None)


def test_cache_bypass():
    cache_clear()
    previous = os.environ.get("CACHE")
    os.environ["CACHE"] = "0"
    fixture = Path("tests/fixtures/broward_realistic.html")
    list(
        stream_search(
            state="fl",
            county="broward",
            query="Smith",
            backend="native",
            mode="fixture",
            fixture_path=fixture,
        )
    )
    stats = cache_stats()
    assert stats["hits"] == 0
    if previous is None:
        os.environ.pop("CACHE", None)
    else:
        os.environ["CACHE"] = previous
