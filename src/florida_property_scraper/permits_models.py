from __future__ import annotations

from pydantic import BaseModel


class PermitRecord(BaseModel):
    county: str
    parcel_id: str
    permit_id: str
    permit_type: str | None = None
    status: str | None = None
    issued_date: str | None = None  # ISO-8601 date: YYYY-MM-DD
    finaled_date: str | None = None  # ISO-8601 date: YYYY-MM-DD
    source: str | None = None


class AdvancedSearchResult(BaseModel):
    owner: str
    address: str
    county: str
    score: int
    parcel_id: str | None = None
    source: str | None = None

    last_permit_date: str | None = None  # ISO-8601 date: YYYY-MM-DD
