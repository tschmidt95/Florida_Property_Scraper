# Simple Scrapy adapter scaffold (demo fixture)
class ScrapyAdapter:
    def __init__(self, demo=False, timeout=None):
        self.demo = demo
        self.timeout = timeout

    def search(self, query, **kwargs):
        if self.demo:
            return [{"address": "123 Demo St", "owner": "Demo Owner", "notes": "demo fixture"}]
        # TODO: implement spider runner (CrawlerRunner/CrawlerProcess)
        return []
