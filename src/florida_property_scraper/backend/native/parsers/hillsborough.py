from ..extract import parse_cards, parse_label_items, split_result_blocks


CARD_SELECTORS = [
    "section.search-result",
    "article.search-result",
    ".search-result",
    "section.result-card",
    "article.result-card",
    "div.result-card",
    ".result-card",
    ".property-card",
    "table tr",
]


def parse(html, url, county_slug):
    blocks = split_result_blocks(html)
    if blocks:
        items = []
        for block in blocks:
            block_items = parse_cards(block, county_slug, CARD_SELECTORS)
            if not block_items:
                block_items = parse_label_items(block, county_slug)
            items.extend(block_items)
        if items:
            return items

    items = parse_cards(html, county_slug, CARD_SELECTORS)
    if items:
        return items
    return parse_label_items(html, county_slug)
