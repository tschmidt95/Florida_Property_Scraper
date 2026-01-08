from dataclasses import dataclass, asdict
import re


REQUIRED_FIELDS = ["state", "county", "jurisdiction", "owner", "address", "raw_html"]


def strip_html(value: str) -> str:
    if value is None:
        return ""
    return re.sub(r"<[^>]+>", "", str(value))


def clean_text(value: str) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def is_html_like(text: str) -> bool:
    if text is None:
        return False
    return "<" in text or ">" in text


@dataclass
class PropertyRecord:
    state: str
    county: str
    jurisdiction: str
    owner: str
    address: str
    raw_html: str
    parcel_id: str = ""
    mailing_address: str = ""
    situs_address: str = ""
    sale_date: str = ""
    sale_price: str = ""
    bedrooms: str = ""
    bathrooms: str = ""
    building_size: str = ""
    land_size: str = ""
    zoning: str = ""
    property_class: str = ""

    def to_dict(self):
        return asdict(self)


def normalize_record(data):
    if data is None:
        data = {}
    owner = clean_text(strip_html(data.get("owner", "")))
    address = clean_text(strip_html(data.get("address", "")))
    if not owner and not address:
        raise ValueError("Record missing owner and address")
    if is_html_like(address):
        raise ValueError("Address contains HTML")
    county = clean_text(data.get("county", ""))
    jurisdiction = clean_text(data.get("jurisdiction", "")) or county
    record = PropertyRecord(
        state=clean_text(data.get("state", "fl")) or "fl",
        county=county,
        jurisdiction=jurisdiction,
        owner=owner,
        address=address,
        raw_html=data.get("raw_html", ""),
        parcel_id=clean_text(data.get("parcel_id", "")),
        mailing_address=clean_text(strip_html(data.get("mailing_address", ""))),
        situs_address=clean_text(strip_html(data.get("situs_address", ""))),
        sale_date=clean_text(data.get("sale_date", "")),
        sale_price=clean_text(data.get("sale_price", "")),
        bedrooms=clean_text(data.get("bedrooms", "")),
        bathrooms=clean_text(data.get("bathrooms", "")),
        building_size=clean_text(data.get("building_size", "")),
        land_size=clean_text(data.get("land_size", "")),
        zoning=clean_text(data.get("zoning", "")),
        property_class=clean_text(data.get("property_class", "")),
    )
    return record
