"""Permit data models."""
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class PermitRecord:
    """Represents a building permit record."""

    county: str
    permit_number: str
    source: str
    parcel_id: Optional[str] = None
    address: Optional[str] = None
    permit_type: Optional[str] = None
    status: Optional[str] = None
    issue_date: Optional[str] = None  # ISO format YYYY-MM-DD
    final_date: Optional[str] = None  # ISO format YYYY-MM-DD
    description: Optional[str] = None
    raw: Optional[str] = None

    def to_dict(self):
        """Convert to dictionary."""
        return asdict(self)

    def with_truncated_raw(self, max_len: int = 5000) -> "PermitRecord":
        """Return a copy with truncated raw data."""
        raw_trunc = None
        if self.raw and len(self.raw) > max_len:
            raw_trunc = self.raw[:max_len] + "..."
        elif self.raw:
            raw_trunc = self.raw
        
        return PermitRecord(
            county=self.county,
            permit_number=self.permit_number,
            source=self.source,
            parcel_id=self.parcel_id,
            address=self.address,
            permit_type=self.permit_type,
            status=self.status,
            issue_date=self.issue_date,
            final_date=self.final_date,
            description=self.description,
            raw=raw_trunc,
        )
