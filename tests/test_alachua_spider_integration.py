from pathlib import Path
from urllib.request import pathname2url

from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter


def test_alachua_spider_collects_items(tmp_path):
    sample = Path('tests/fixtures/alachua_sample.html').absolute()
    file_url = 'file://' + pathname2url(str(sample))

    a = ScrapyAdapter(demo=False)
    items = a.search('demo', start_urls=[file_url], spider_name='alachua')
    assert isinstance(items, list)
    assert len(items) >= 2
    owners = [it.get('owner') for it in items]
    assert 'Demo Owner A' in owners
    assert 'Demo Owner B' in owners
