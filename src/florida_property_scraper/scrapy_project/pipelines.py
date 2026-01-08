import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from florida_property_scraper.exporters import WebhookExporter, ZohoExporter
from florida_property_scraper.identity import compute_property_uid
from florida_property_scraper.leads import normalize_record
from florida_property_scraper.signals import generate_events
from florida_property_scraper.storage import SQLiteStore
from scrapy.exceptions import NotConfigured

def _ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


class NormalizePipeline:
    def process_item(self, item: Dict[str, Any], spider=None):
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

    def process_item(self, item: Dict[str, Any], spider=None):
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
    def __init__(self, store: SQLiteStore, run_id: str):
        self.store = store
        self.run_id = run_id

    @classmethod
    def from_crawler(cls, crawler):
        path = crawler.settings.get("STORAGE_PATH")
        if not path:
            raise NotConfigured("STORAGE_PATH not set")
        run_id = crawler.settings.get("RUN_ID") or ""
        return cls(SQLiteStore(path), run_id)

    def process_item(self, item: Dict[str, Any], spider=None):
        record = normalize_record(dict(item))
        self.store.upsert_lead(record)
        property_uid, parcel_id, warnings = compute_property_uid(item)
        if not property_uid:
            return item
        observed_at = datetime.now(timezone.utc).isoformat()
        old_obs = self.store.get_latest_observation(property_uid)
        purchase_history = item.get("purchase_history") or []
        sale_info = _extract_last_sale(purchase_history)
        observation = {
            "property_uid": property_uid,
            "county": item.get("county"),
            "parcel_id": parcel_id,
            "situs_address": item.get("situs_address"),
            "owner_name": item.get("owner_name"),
            "mailing_address": item.get("mailing_address"),
            "last_sale_date": sale_info.get("last_sale_date"),
            "last_sale_price": sale_info.get("last_sale_price"),
            "deed_type": sale_info.get("deed_type"),
            "source_url": item.get("source_url"),
            "raw_json": json.dumps(item, ensure_ascii=True),
            "observed_at": observed_at,
            "run_id": self.run_id,
        }
        self.store.insert_observation(observation)
        events = generate_events(old_obs, observation)
        self.store.insert_events(events)
        return item

    def close_spider(self, spider):
        self.store.close()


def _extract_last_sale(purchase_history: Any) -> Dict[str, Any]:
    if not isinstance(purchase_history, list) or not purchase_history:
        return {}
    best = {}
    for entry in purchase_history:
        if not isinstance(entry, dict):
            continue
        sale_date = entry.get("sale_date") or entry.get("SALE_DATE") or entry.get("Sale Date")
        sale_price = entry.get("sale_price") or entry.get("PRICE") or entry.get("Sale Price")
        deed_type = entry.get("deed_type") or entry.get("DEED_TYPE") or entry.get("Deed Type")
        if sale_date or sale_price or deed_type:
            best = {
                "last_sale_date": sale_date,
                "last_sale_price": sale_price,
                "deed_type": deed_type,
            }
            break
    return best


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

    def process_item(self, item: Dict[str, Any], spider=None):
        record = normalize_record(dict(item))
        for exporter in self.exporters:
            exporter.export(record)
        return item
