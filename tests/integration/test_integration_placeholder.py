import os
import pytest

@pytest.mark.integration
def test_integration_placeholder():
    """Placeholder integration test that verifies secret is present.

    Replace this with real integration tests that exercise external APIs.
    """
    api_key = os.getenv("SCRAPER_API_KEY")
    assert api_key, "SCRAPER_API_KEY not set (this test runs only in CI when the secret is provided)"
    assert True
