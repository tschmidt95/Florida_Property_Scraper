from __future__ import annotations

from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class LastSale(BaseModel):
    date: Optional[str] = None
    price: Optional[float] = None


class PermitsSummary(BaseModel):
    last_permit_date: Optional[str] = None
    permits_last_15y_count: int = 0


class Contacts(BaseModel):
    phones: List[str] = Field(default_factory=list)
    emails: List[str] = Field(default_factory=list)
    source: Optional[str] = None
    confidence: Optional[float] = None


class PropertyFields(BaseModel):
    beds: Optional[int] = None
    baths: Optional[float] = None
    sf: Optional[float] = None
    year_built: Optional[int] = None
    zoning: Optional[str] = None
    land_size: Optional[float] = None


class PropertyCard(BaseModel):
    county: str
    address: str
    parcel_id: Optional[str] = None
    owner_name: Optional[str] = None
    owner_mailing_address: Optional[str] = None
    property_fields: PropertyFields = PropertyFields()
    last_sale: LastSale = LastSale()
    permits: PermitsSummary = PermitsSummary()
    contacts: Contacts = Contacts()
    notes: Optional[str] = None


class FieldConfidence(BaseModel):
    """Per-field confidence metadata for `PropertyRecord`.

    This is intentionally lightweight: it's stable for the UI, while allowing
    backends to evolve confidence scoring over time.
    """

    source: Optional[str] = None
    confidence: float = 0.0
    reason: Optional[str] = None


class DataConfidence(BaseModel):
    fields: Dict[str, FieldConfidence] = Field(default_factory=dict)


class PropertyRecord(BaseModel):
    """Unified property record contract for geometry + filters.

    All fields are nullable; missing values must be represented as null.
    Confidence metadata lives in `data_confidence.fields`.
    """

    record_version: int = 1

    parcel_id: Optional[str] = None
    county: Optional[str] = None
    situs_address: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None

    property_type: Optional[str] = None
    living_area_sqft: Optional[float] = None
    beds: Optional[int] = None
    baths: Optional[float] = None
    year_built: Optional[int] = None
    lot_size_sqft: Optional[float] = None

    zoning: Optional[str] = None
    future_land_use: Optional[str] = None

    owner_name: Optional[str] = None
    owner_mailing_address: Optional[str] = None
    homestead_flag: Optional[bool] = None

    last_sale_date: Optional[str] = None
    last_sale_price: Optional[float] = None

    data_confidence: DataConfidence = Field(default_factory=DataConfidence)
