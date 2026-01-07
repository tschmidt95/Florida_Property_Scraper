import re
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode

from florida_property_scraper.schema import REQUIRED_FIELDS, normalize_item


LABEL_OWNER = ["owner", "owner name", "owner(s)", "property owner"]
LABEL_ADDRESS = [
    "mailing address",
    "situs address",
    "site address",
    "property address",
    "address",
]


def normalize_text(value):
    return " ".join((value or "").split())


def truncate_html(value, limit=50000):
    return (value or "")[:limit]


def extract_table_items(response, columns, county):
    items = []
    rows = response.css("table tr")
    for row in rows:
        cells = [c.get() for c in row.css("td::text")]
        item = {field: "" for field in REQUIRED_FIELDS}
        item["county"] = county
        item["raw_html"] = truncate_html(row.get() or response.text)
        for idx, field in enumerate(columns):
            value = normalize_text(cells[idx]) if idx < len(cells) else ""
            item[field] = value
        if item["owner"] or item["address"]:
            items.append(normalize_item(item))
    return items


def _find_value(lines, labels):
    for idx, line in enumerate(lines):
        lower = line.lower()
        for label in labels:
            if lower == label or lower.startswith(label):
                if ":" in line:
                    after = line.split(":", 1)[1].strip()
                    if after:
                        return after
                if idx + 1 < len(lines):
                    return lines[idx + 1]
    return ""


def _find_address_like(lines):
    for line in lines:
        if re.match(r"\\d+\\s+\\S+", line):
            return line
    return ""


def extract_label_items(response, county):
    items = []
    containers = response.css(
        ".result, .record, .card, .search-result, .result-row, "
        ".result-item, div, li, section, article"
    )
    for container in containers:
        lines = [
            normalize_text(t)
            for t in container.css("::text").getall()
            if normalize_text(t)
        ]
        owner = _find_value(lines, LABEL_OWNER)
        address = _find_value(lines, LABEL_ADDRESS)
        if not address:
            address = _find_address_like(lines)
        if owner and address:
            item = normalize_item(
                {
                    "county": county,
                    "owner": owner,
                    "address": address,
                    "raw_html": truncate_html(container.get() or response.text),
                }
            )
            items.append(item)
    if items:
        return items
    texts = [
        normalize_text(t)
        for t in response.css("body ::text").getall()
        if normalize_text(t)
    ]
    owner = _find_value(texts, LABEL_OWNER)
    address = _find_value(texts, LABEL_ADDRESS)
    if not address:
        address = _find_address_like(texts)
    if owner and address:
        items.append(
            normalize_item(
                {
                    "county": county,
                    "owner": owner,
                    "address": address,
                    "raw_html": truncate_html(response.text),
                }
            )
        )
    return items


def next_page_request(response, pagination, page_param, max_pages):
    request = response.request
    meta = request.meta if request is not None else {}
    page = meta.get("page", 1)
    visited = meta.get("visited_pages", set())
    if page in visited:
        return None
    visited = set(visited)
    visited.add(page)

    if pagination == "next_link" and page < max_pages:
        next_link = response.css("a[rel=next]::attr(href)").get()
        if not next_link:
            next_link = response.xpath("//a[contains(., 'Next')]/@href").get()
        if next_link and next_link != response.url:
            return response.follow(
                next_link, meta={"page": page + 1, "visited_pages": visited}
            )
    if pagination == "page_param" and page_param and page < max_pages:
        parts = urlsplit(response.url)
        query = parse_qs(parts.query)
        query[page_param] = [str(page + 1)]
        next_url = urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(query, doseq=True),
                parts.fragment,
            )
        )
        if next_url != response.url:
            return response.follow(
                next_url, meta={"page": page + 1, "visited_pages": visited}
            )
    return None
