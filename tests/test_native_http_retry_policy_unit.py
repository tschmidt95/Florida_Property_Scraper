from florida_property_scraper.backend.native.http_client import compute_backoff_delays


def test_native_http_retry_policy_unit():
    delays = compute_backoff_delays(
        3, base_delay=1.0, factor=2.0, jitter=0.0, rand_fn=lambda: 0.5
    )
    assert delays == [1.0, 2.0, 4.0]
