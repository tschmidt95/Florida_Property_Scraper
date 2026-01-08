import json
import os
import re

from .http_client import HttpClient
from .extract import ensure_fields, split_result_blocks, truncate_raw_html
from florida_property_scraper.schema.records import normalize_record


class NativeEngine:
    def __init__(self, max_items=None, per_county_limit=None, retry_config=None, max_pages=50):
        self.max_items = max_items
        self.per_county_limit = per_county_limit
        self.http = HttpClient(retry_config=retry_config)
        self.max_pages = max_pages

    def run(self, start_requests, parser, county_slug, allowed_hosts=None, log_fn=None, dry_run=False, fixture_map=None, debug_context=None):
        items = []
        visited = set()
        queue = list(start_requests or [])
        pages_seen = 0
        debug_dir = os.environ.get("NATIVE_DEBUG_DIR")
        first_html = None
        first_blocks = []
        debug_context = debug_context or {}
        while queue:
            if pages_seen >= self.max_pages:
                break
            req = queue.pop(0)
            req_url = req["url"] if isinstance(req, dict) else req
            if req_url in visited:
                continue
            visited.add(req_url)
            response = self.http.request(req, allowed_hosts=allowed_hosts, dry_run=dry_run, fixture_map=fixture_map)
            pages_seen += 1
            if first_html is None:
                first_html = response["text"]
                first_blocks = split_result_blocks(first_html)
            parsed = parser(response["text"], response["final_url"], county_slug)
            normalized = [ensure_fields(item, county_slug, item.get("raw_html", "")) for item in parsed]
            validated = []
            dropped = 0
            for item in normalized:
                try:
                    record = normalize_record(item)
                except ValueError:
                    dropped += 1
                    continue
                validated.append(record.to_dict())
            items.extend(validated)
            if log_fn:
                log_fn(
                    {
                        "county": county_slug,
                        "url": response["final_url"],
                        "items_found": len(validated),
                        "dropped": dropped,
                        "status": "success",
                    }
                )
            if self.per_county_limit and len(items) >= self.per_county_limit:
                items = items[: self.per_county_limit]
                break
            if self.max_items and len(items) >= self.max_items:
                items = items[: self.max_items]
                break
            next_urls = getattr(parser, "get_next_urls", None)
            if callable(next_urls):
                for next_url in next_urls(response["text"], response["final_url"]):
                    if next_url not in visited:
                        queue.append({"url": next_url, "method": "GET"})
        if debug_dir:
            os.makedirs(debug_dir, exist_ok=True)
            query = debug_context.get("query", "")
            safe_query = re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_") or "query"
            prefix = f"{county_slug}_{safe_query}"
            if first_html is not None:
                raw_path = os.path.join(debug_dir, f"{prefix}_raw.html")
                with open(raw_path, "w", encoding="utf-8") as handle:
                    handle.write(first_html[:200000])
            blocks_payload = {
                "count": len(first_blocks),
                "blocks": [truncate_raw_html(block, 2000) for block in first_blocks[:2]],
            }
            blocks_path = os.path.join(debug_dir, f"{prefix}_blocks.json")
            with open(blocks_path, "w", encoding="utf-8") as handle:
                json.dump(blocks_payload, handle)
            parsed_path = os.path.join(debug_dir, f"{prefix}_parsed.json")
            with open(parsed_path, "w", encoding="utf-8") as handle:
                json.dump(items[:5], handle)
        return items
