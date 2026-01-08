from florida_property_scraper.cache import cache_clear, cache_set, cache_stats


def test_cache_eviction():
    cache_clear()
    for i in range(3):
        cache_set(f"key-{i}", {"value": i}, ttl=60, max_entries=2)
    stats = cache_stats()
    assert stats["evictions"] >= 1
