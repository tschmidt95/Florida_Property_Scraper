from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class PAProperty:
    # identifiers
    county: str = ""
    parcel_id: str = ""
    folio: str = ""
    apn: str = ""

    # situs_address fields
    situs_address: str = ""
    situs_city: str = ""
    situs_state: str = ""
    situs_zip: str = ""

    # mailing_address fields
    mailing_address: str = ""
    mailing_city: str = ""
    mailing_state: str = ""
    mailing_zip: str = ""

    # location
    city: str = ""
    zip: str = ""
    subdivision: str = ""
    legal_desc: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # land
    land_sf: float = 0
    land_acres: float = 0
    land_use_code: str = ""
    zoning: str = ""
    flood_zone: str = ""
    lot_dimensions: str = ""

    # improvements/building
    building_sf: float = 0
    living_sf: float = 0
    gross_area: float = 0
    use_type: str = ""
    property_class: str = ""
    year_built: int = 0
    effective_year: int = 0
    stories: float = 0
    units: int = 0
    bedrooms: int = 0
    bathrooms: float = 0
    construction_type: str = ""
    roof: str = ""
    foundation: str = ""
    exterior: str = ""
    hvac: str = ""
    pool_flag: bool = False
    garage_spaces: float = 0
    building_count: int = 0

    # valuation
    just_value: float = 0
    assessed_value: float = 0
    taxable_value: float = 0
    land_value: float = 0
    improvement_value: float = 0
    cap: float = 0
    exemptions: List[str] = field(default_factory=list)

    # sales (PA recorded)
    last_sale_date: Optional[str] = None  # YYYY-MM-DD if provided
    last_sale_price: float = 0
    last_sale_qualifier: str = ""
    prior_sale_date: Optional[str] = None
    prior_sale_price: float = 0

    # ownership
    owner_names: List[str] = field(default_factory=list)
    owner_type: str = ""
    deed_book: str = ""
    deed_page: str = ""

    # metadata
    source_url: str = ""
    extracted_at: str = ""
    parser_version: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
