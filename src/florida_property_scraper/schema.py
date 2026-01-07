REQUIRED_FIELDS = [
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


def normalize_item(item):
    if item is None:
        item = {}
    normalized = {field: item.get(field, "") for field in REQUIRED_FIELDS}
    for key, value in item.items():
        if key not in normalized:
            normalized[key] = value
    return normalized
