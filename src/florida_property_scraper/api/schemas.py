from __future__ import annotations

from typing import List, Optional
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
