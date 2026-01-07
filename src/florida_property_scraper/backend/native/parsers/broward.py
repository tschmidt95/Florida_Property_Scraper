from ..extract import parse_cards, parse_label_items


CARD_SELECTORS = [".result", ".record", ".card", "table tr"]


def parse(html, url, county_slug):
    items = parse_cards(html, county_slug, CARD_SELECTORS)
    if items:
        return items
    return parse_label_items(html, county_slug)


def get_next_urls(html, base_url):
    return []
