from scrapy import Spider


class BrowardSpider(Spider):
    name = "broward_spider"

    def __init__(self, start_urls=None, *a, **kw):
        super().__init__(*a, **kw)
        self.start_urls = start_urls or []

    def parse(self, response):
        # Expect a simple table with rows where first td=owner, second=address
        rows = response.css('table tr')
        for row in rows:
            owner = row.css('td:nth-child(1)::text').get()
            address = row.css('td:nth-child(2)::text').get()
            land_size = row.css('td:nth-child(3)::text').get()
            building_size = row.css('td:nth-child(4)::text').get()
            bedrooms = row.css('td:nth-child(5)::text').get()
            bathrooms = row.css('td:nth-child(6)::text').get()
            if owner or address:
                yield {
                    'county': 'broward',
                    'owner': owner.strip() if owner else '',
                    'address': address.strip() if address else '',
                    'land_size': land_size.strip() if land_size else '',
                    'building_size': building_size.strip() if building_size else '',
                    'bedrooms': bedrooms.strip() if bedrooms else '',
                    'bathrooms': bathrooms.strip() if bathrooms else '',
                    'zoning': '',
                    'property_class': '',
                    'raw_html': '',
                }
