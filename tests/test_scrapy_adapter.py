from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter


def test_scrapy_adapter_demo_returns_fixture():
    a = ScrapyAdapter(demo=True)
    res = a.search('anything')
    assert isinstance(res, list)
    assert res and res[0]['owner'] == 'Demo Owner'


def test_scrapy_adapter_non_demo_no_start_urls():
    a = ScrapyAdapter(demo=False)
    res = a.search('anything')
    # Without start_urls the adapter currently returns an empty list
    assert res == []
