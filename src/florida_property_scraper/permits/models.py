"""Data models for permit records."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class PermitRecord:
    """Represents a single building permit record."""

    county: str
    parcel_id: Optional[str]
    address: Optional[str]
    permit_number: str
    permit_type: Optional[str]
    status: Optional[str]
    issue_date: Optional[str]  # ISO format YYYY-MM-DD
    final_date: Optional[str]  # ISO format YYYY-MM-DD
    description: Optional[str]
    source: str
    raw: Optional[str] = None  # Raw JSON or HTML for debugging

    def to_dict(self):
        """Convert to dictionary for database storage."""
        return {
            "county": self.county,
            "parcel_id": self.parcel_id,
            "address": self.address,
            "permit_number": self.permit_number,
            "permit_type": self.permit_type,
            "status": self.status,
            "issue_date": self.issue_date,
            "final_date": self.final_date,
            "description": self.description,
            "source": self.source,
            "raw": self.raw,
        }
