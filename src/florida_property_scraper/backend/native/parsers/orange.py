from ..extract import (
    ensure_fields,
    extract_address,
    extract_owner,
    split_result_blocks,
    truncate_raw_html,
)


def parse(html, url, county_slug):
    items = []
    for block in split_result_blocks(html):
        owner = extract_owner(block)
        address = extract_address(block)
        if not owner and not address:
            continue
        raw_html = truncate_raw_html(block)
        items.append(
            ensure_fields({"owner": owner, "address": address}, county_slug, raw_html)
        )
    return items


def parse_results(html):
    results = []
    for block in split_result_blocks(html):
        owner = extract_owner(block)
        address = extract_address(block)
        if not owner and not address:
            continue
        results.append({"owner": owner, "address": address})
    return results
