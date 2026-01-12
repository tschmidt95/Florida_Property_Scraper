from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PermitRecord:
    county: str
    parcel_id: str | None
    address: str | None
    permit_number: str
    permit_type: str | None
    status: str | None
    issue_date: str | None
    final_date: str | None
    description: str | None
    source: str
    raw: str | None = None

    def with_truncated_raw(self, *, max_chars: int = 4000) -> "PermitRecord":
        if not self.raw:
            return self
        if len(self.raw) <= max_chars:
            return self
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
            raw=self.raw[:max_chars],
        )
