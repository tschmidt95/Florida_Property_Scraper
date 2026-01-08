import html
import re

try:
    from parsel import Selector
except Exception:  # pragma: no cover
    Selector = None


REQUIRED_FIELDS = [
    "state",
    "jurisdiction",
    "county",
    "owner",
    "address",
    "land_size",
    "building_size",
    "bedrooms",
    "bathrooms",
    "zoning",
    "property_class",
    "raw_html",
]


def norm_ws(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def safe_text(value):
    return norm_ws(html.unescape(value or ""))


def truncate_raw_html(text, limit=2000):
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit]


def pick_first_nonempty(*values):
    for value in values:
        if value:
            return value
    return ""


def parse_numeric_like_fields(text):
    cleaned = norm_ws(text)
    match = re.search(r"([0-9]+(\.[0-9]+)?)", cleaned)
    return match.group(1) if match else ""


def _selector_from_html(text):
    if Selector is None:
        return None
    return Selector(text=text)


def find_label_value_pairs(selector):
    pairs = []
    if selector is None:
        return pairs
    for label in selector.css("label, .label, .field-label, .field-name"):
        label_text = safe_text("".join(label.css("::text").getall()))
        if not label_text:
            continue
        value_nodes = label.xpath("following-sibling::*[1]//text()").getall()
        value_text = safe_text(" ".join(value_nodes))
        if value_text:
            pairs.append((label_text.lower(), value_text))
    return pairs


def _extract_owner_address_from_pairs(pairs):
    owner = ""
    address = ""
    for label, value in pairs:
        if "owner" in label and not owner:
            owner = value
        if "address" in label and not address:
            address = value
    return owner, address


def _extract_owner_address_from_text(text):
    combined = safe_text(text)
    match_owner = re.search(r"owner\s*[:\-]\s*([^\|]+)", combined, re.I)
    match_addr = re.search(r"(mailing|site|situs|property)?\s*address\s*[:\-]\s*([^\|]+)", combined, re.I)
    owner = safe_text(match_owner.group(1)) if match_owner else ""
    address = safe_text(match_addr.group(2)) if match_addr else ""
    return owner, address


def blank_item(county_slug, state="fl"):
    return {
        "state": state,
        "jurisdiction": county_slug,
        "county": county_slug,
        "owner": "",
        "address": "",
        "land_size": "",
        "building_size": "",
        "bedrooms": "",
        "bathrooms": "",
        "zoning": "",
        "property_class": "",
        "raw_html": "",
    }


def ensure_fields(item, county_slug, raw_html=""):
    result = blank_item(county_slug)
    result.update(item or {})
    result["raw_html"] = truncate_raw_html(raw_html or result.get("raw_html", ""))
    result["county"] = county_slug
    result["jurisdiction"] = county_slug
    result["state"] = result.get("state") or "fl"
    for key in REQUIRED_FIELDS:
        if key not in result:
            result[key] = ""
    return result


def parse_table_rows(html_text, county_slug):
    selector = _selector_from_html(html_text)
    if selector is None:
        return []
    items = []
    for row in selector.css("table tr"):
        cells = [safe_text("".join(cell.css("::text").getall())) for cell in row.css("td")]
        if not cells:
            continue
        owner = cells[0] if len(cells) > 0 else ""
        address = cells[1] if len(cells) > 1 else ""
        raw_html = truncate_raw_html(row.get() or html_text)
        items.append(ensure_fields({"owner": owner, "address": address}, county_slug, raw_html))
    return items


def find_cards(html_text, selectors):
    selector = _selector_from_html(html_text)
    if selector is None:
        return []
    cards = []
    for css_selector in selectors:
        for node in selector.css(css_selector):
            cards.append(node)
    return cards


def parse_cards(html_text, county_slug, selectors):
    cards = find_cards(html_text, selectors)
    items = []
    for card in cards:
        raw_html = truncate_raw_html(card.get())
        pairs = find_label_value_pairs(card)
        owner, address = _extract_owner_address_from_pairs(pairs)
        if not owner or not address:
            owner_fallback, address_fallback = _extract_owner_address_from_text(" ".join(card.css("::text").getall()))
            owner = pick_first_nonempty(owner, owner_fallback)
            address = pick_first_nonempty(address, address_fallback)
        if owner or address:
            items.append(ensure_fields({"owner": owner, "address": address}, county_slug, raw_html))
    return items


def parse_label_items(html_text, county_slug):
    selector = _selector_from_html(html_text)
    raw_html = truncate_raw_html(html_text)
    pairs = find_label_value_pairs(selector)
    owner, address = _extract_owner_address_from_pairs(pairs)
    if not owner or not address:
        owner_fallback, address_fallback = _extract_owner_address_from_text(html_text)
        owner = pick_first_nonempty(owner, owner_fallback)
        address = pick_first_nonempty(address, address_fallback)
    if owner or address:
        return [ensure_fields({"owner": owner, "address": address}, county_slug, raw_html)]
    return []


def split_result_blocks(html_text):
    start_pattern = re.compile(
        r"<(?:section|article|div)[^>]*class=[\"'][^\"']*(search-result|result-card|property-card)[^\"']*[\"'][^>]*>",
        flags=re.IGNORECASE,
    )
    starts = [match.start() for match in start_pattern.finditer(html_text)]
    if starts:
        blocks = []
        for idx, start in enumerate(starts):
            end = starts[idx + 1] if idx + 1 < len(starts) else len(html_text)
            blocks.append(html_text[start:end])
        return blocks
    label_pattern = re.compile(
        r"(Owner(?: Name)?|Owner\s*\(s\)|Situs Address|Site Address|Property Address)\s*:",
        flags=re.IGNORECASE,
    )
    label_starts = [match.start() for match in label_pattern.finditer(html_text)]
    if len(label_starts) > 1:
        blocks = []
        for idx, start in enumerate(label_starts):
            end = label_starts[idx + 1] if idx + 1 < len(label_starts) else len(html_text)
            blocks.append(html_text[start:end])
        return blocks
    return [html_text]


def grab_label_value(block, label):
    label_pattern = r"\s+".join(re.escape(part) for part in label.split())
    pattern_colon = rf"{label_pattern}\s*:\s*([^<]+)"
    match = re.search(pattern_colon, block, flags=re.IGNORECASE)
    if match:
        return safe_text(match.group(1))
    pattern_tag = rf"{label_pattern}\s*</[^>]+>\s*<[^>]*>\s*([^<]+)"
    match = re.search(pattern_tag, block, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return safe_text(match.group(1))
    return ""


def extract_owner(block):
    labels = ["Owner", "Owner Name", "Owner(s)"]
    for label in labels:
        value = grab_label_value(block, label)
        if value:
            return value
    return ""


def extract_address(block):
    labels = ["Site Address", "Situs Address", "Property Address"]
    for label in labels:
        value = grab_label_value(block, label)
        if value:
            return value
    return ""
