from florida_property_scraper.backend.native.extract import split_result_blocks


def test_extract_max_response_bytes():
    html = '<section class="search-result">Owner: A</section>' * 5000
    blocks = split_result_blocks(html)
    assert blocks
