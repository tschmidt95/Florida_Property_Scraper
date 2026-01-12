"""Data models for permit records."""
from typing import Optional
from pydantic import BaseModel


class PermitRecord(BaseModel):
    """Represents a building permit record."""

    county: str
    parcel_id: Optional[str] = None
    address: Optional[str] = None
    permit_number: str
    permit_type: Optional[str] = None
    status: Optional[str] = None
    issue_date: Optional[str] = None
    final_date: Optional[str] = None
    description: Optional[str] = None
    source: str
    raw: Optional[str] = None
