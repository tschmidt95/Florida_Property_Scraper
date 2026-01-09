from scrapy import FormRequest, Request, Spider

from florida_property_scraper.schema import normalize_item
from florida_property_scraper.spider_utils import (
    extract_label_items,
    extract_label_items_from_nodes,
    next_page_request,
    truncate_html,
)


class PinellasSpider(Spider):
    name = "pinellas_spider"
    COLUMNS = ["owner", "address", "property_class", "zoning"]

    def __init__(
        self,
        start_urls=None,
        debug_html=False,
        query="",
        pagination="none",
        page_param="",
        form_url="",
        form_fields_template=None,
        max_pages=3,
        *a,
        **kw,
    ):
        super().__init__(*a, **kw)
        self.start_urls = start_urls or []
        self.debug_html = debug_html
        self.query = query or ""
        self.pagination = pagination or "none"
        self.page_param = page_param or ""
        self.form_url = form_url
        self.form_fields_template = form_fields_template or {}
        self.max_pages = max_pages

    def start_requests(self):
        if self.form_url and self.form_fields_template:
            formdata = {
                k: (v.format(query=self.query) if isinstance(v, str) else v)
                for k, v in self.form_fields_template.items()
            }
            yield FormRequest(self.form_url, formdata=formdata, meta={"page": 1})
            return
        for url in self.start_urls:
            yield Request(url, meta={"page": 1})

    async def start(self):
        for request in self.start_requests():
            yield request

    def parse(self, response):
        nodes = response.css(
            ".pinellas-result, .result-card, .search-result, .property-card"
        )
        items = extract_label_items_from_nodes(nodes, "pinellas")
        if not items:
            items = extract_label_items(response, "pinellas")
        if items:
            for item in items:
                item["state"] = "fl"
                item["jurisdiction"] = item.get("county", "pinellas")
                item["raw_html"] = truncate_html(item.get("raw_html"))
                yield normalize_item(item)
        elif self.debug_html:
            yield normalize_item(
                {
                    "state": "fl",
                    "jurisdiction": "pinellas",
                    "county": "pinellas",
                    "raw_html": truncate_html(response.text),
                }
            )
        next_req = next_page_request(
            response, self.pagination, self.page_param, self.max_pages
        )
        if next_req:
            yield next_req
