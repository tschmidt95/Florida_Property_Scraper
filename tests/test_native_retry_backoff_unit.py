from florida_property_scraper.backend.native.http_client import compute_backoff_delays


def test_native_retry_backoff_unit():
    delays = compute_backoff_delays(3, base_delay=0.5, factor=2.0, jitter=0.0, rand_fn=lambda: 0.2)
    assert delays == [0.5, 1.0, 2.0]
