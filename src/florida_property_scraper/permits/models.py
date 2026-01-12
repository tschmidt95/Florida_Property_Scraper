"""Permit data models."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class PermitRecord:
    """Represents a single permit record."""

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
    raw: Optional[str] = None

    def with_truncated_raw(self, max_len: int = 10000) -> "PermitRecord":
        """Return a copy with truncated raw field."""
        if self.raw and len(self.raw) > max_len:
            truncated_raw = self.raw[:max_len] + "..."
        else:
            truncated_raw = self.raw
        return PermitRecord(
            county=self.county,
            parcel_id=self.parcel_id,
            address=self.address,
            permit_number=self.permit_number,
            permit_type=self.permit_type,
            status=self.status,
            issue_date=self.issue_date,
            final_date=self.final_date,
            description=self.description,
            source=self.source,
            raw=truncated_raw,
        )
