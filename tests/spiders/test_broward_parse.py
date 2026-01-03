from pathlib import Path
from scrapy.http import TextResponse

from florida_property_scraper.backend.spiders.broward_spider import BrowardSpider


def test_broward_parse_from_fixture():
    path = Path(
        'tests/fixtures/broward_sample.html'
    ).resolve()
    html = path.read_text()
    response = TextResponse(
        url='file://' + str(path),
        body=html,
        encoding='utf-8',
    )

    spider = BrowardSpider()
    items = list(spider.parse(response))

    # Expect at least one parsed entry with owner and address keys
    assert items, "No items parsed from fixture"
    for item in items:
        assert 'owner' in item
        assert 'address' in item
