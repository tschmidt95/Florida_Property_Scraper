from florida_property_scraper.backend.native.http_client import HttpClient


def test_native_form_submission_unit():
    client = HttpClient()
    req = client.build_form_request(
        "https://example.local/search", {"owner": "John Smith"}
    )
    assert req["method"] == "POST"
    assert req["url"] == "https://example.local/search"
    assert b"owner=John+Smith" in req["data"]
    assert req["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
