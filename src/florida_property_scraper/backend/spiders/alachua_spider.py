from scrapy import Spider


class AlachuaSpider(Spider):
    name = "alachua_spider"

    def __init__(self, start_urls=None, *a, **kw):
        super().__init__(*a, **kw)
        self.start_urls = start_urls or []

    def parse(self, response):
        # Expect a simple table with rows where first td=owner, second=address
        rows = response.css('table tr')
        for row in rows:
            owner = row.css('td:nth-child(1)::text').get()
            address = row.css('td:nth-child(2)::text').get()
            if owner or address:
                yield {
                    'owner': owner.strip() if owner else '',
                    'address': address.strip() if address else ''
                }
