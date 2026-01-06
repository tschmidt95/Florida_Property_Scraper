import scrapy


class PropertyItem(scrapy.Item):
    county = scrapy.Field()
    search_query = scrapy.Field()
    owner_name = scrapy.Field()
    contact_phones = scrapy.Field()
    contact_emails = scrapy.Field()
    contact_addresses = scrapy.Field()
    mailing_address = scrapy.Field()
    situs_address = scrapy.Field()
    parcel_id = scrapy.Field()
    property_url = scrapy.Field()
    source_url = scrapy.Field()
    mortgage = scrapy.Field()
    purchase_history = scrapy.Field()
    zoning_current = scrapy.Field()
    zoning_future = scrapy.Field()
