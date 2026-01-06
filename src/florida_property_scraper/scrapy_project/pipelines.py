import json
from typing import Any, Dict, List, Optional

from florida_property_scraper.exporters import WebhookExporter, ZohoExporter
from florida_property_scraper.leads import normalize_record
from florida_property_scraper.storage import SQLiteStore
from scrapy.exceptions import NotConfigured

_GLOBAL_COLLECTOR: Optional[List[Dict[str, Any]]] = None


def set_global_collector(collector: List[Dict[str, Any]]) -> None:
    global _GLOBAL_COLLECTOR
    _GLOBAL_COLLECTOR = collector


def _ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


class NormalizePipeline:
    def process_item(self, item: Dict[str, Any], spider):
        item.setdefault("contact_phones", [])
        item.setdefault("contact_emails", [])
        item.setdefault("contact_addresses", [])
        item.setdefault("mortgage", [])
        item.setdefault("purchase_history", [])
        item["contact_phones"] = _ensure_list(item.get("contact_phones"))
        item["contact_emails"] = _ensure_list(item.get("contact_emails"))
        item["contact_addresses"] = _ensure_list(item.get("contact_addresses"))
        item["mortgage"] = _ensure_list(item.get("mortgage"))
        item["purchase_history"] = _ensure_list(item.get("purchase_history"))
        return item


class CollectorPipeline:
    def __init__(self, collector: List[Dict[str, Any]]):
        self.collector = collector

    @classmethod
    def from_crawler(cls, crawler):
        collector = _GLOBAL_COLLECTOR
        if collector is None:
            collector = crawler.settings.get("ITEM_COLLECTOR")
        if collector is None:
            collector = []
        return cls(collector)

    def process_item(self, item: Dict[str, Any], spider):
        self.collector.append(dict(item))
        return item


class AppendJsonlPipeline:
    def __init__(self, path: str):
        self.path = path
        self._handle = None

    @classmethod
    def from_crawler(cls, crawler):
        output_path = crawler.settings.get("OUTPUT_PATH")
        output_format = crawler.settings.get("OUTPUT_FORMAT")
        append_output = crawler.settings.get("APPEND_OUTPUT", True)
        if not output_path or output_format != "jsonl" or not append_output:
            raise NotConfigured("Append JSONL disabled")
        return cls(output_path)

    def open_spider(self, spider):
        self._handle = open(self.path, "a", encoding="utf-8")

    def process_item(self, item: Dict[str, Any], spider):
        if not self._handle:
            return item
        payload = json.dumps(dict(item), ensure_ascii=True)
        self._handle.write(payload + "\n")
        self._handle.flush()
        return item

    def close_spider(self, spider):
        if self._handle:
            self._handle.close()
            self._handle = None


class StoragePipeline:
    def __init__(self, store: SQLiteStore):
        self.store = store

    @classmethod
    def from_crawler(cls, crawler):
        path = crawler.settings.get("STORAGE_PATH")
        if not path:
            raise NotConfigured("STORAGE_PATH not set")
        return cls(SQLiteStore(path))

    def process_item(self, item: Dict[str, Any], spider):
        record = normalize_record(dict(item))
        self.store.upsert_lead(record)
        return item

    def close_spider(self, spider):
        self.store.close()


class ExporterPipeline:
    def __init__(self, exporters: List[Any]):
        self.exporters = exporters

    @classmethod
    def from_crawler(cls, crawler):
        exporters: List[Any] = []
        webhook_url = crawler.settings.get("WEBHOOK_URL")
        if webhook_url:
            exporters.append(WebhookExporter(webhook_url))
        if crawler.settings.get("ZOHO_SYNC"):
            exporters.append(ZohoExporter())
        if not exporters:
            raise NotConfigured("No exporters configured")
        return cls(exporters)

    def process_item(self, item: Dict[str, Any], spider):
        record = normalize_record(dict(item))
        for exporter in self.exporters:
            exporter.export(record)
        return item
