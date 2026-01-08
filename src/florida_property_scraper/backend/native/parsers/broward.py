from ..extract import ensure_fields, grab_label_value, parse_label_items, split_result_blocks, truncate_raw_html


def parse(html, url, county_slug):
    items = parse_results(html, county_slug)
    if items:
        return items
    return parse_label_items(html, county_slug)


def get_next_urls(html, base_url):
    return []


def parse_results(html, county_slug=None):
    county_slug = county_slug or ""
    results = []
    for block in split_result_blocks(html):
        owner = grab_label_value(block, "Owner")
        address = grab_label_value(block, "Site Address")
        if not address:
            address = grab_label_value(block, "Situs Address")
        if not address:
            address = grab_label_value(block, "Property Address")
        if owner or address:
            raw_html = truncate_raw_html(block)
            results.append(ensure_fields({"owner": owner, "address": address}, county_slug, raw_html))
    return results
