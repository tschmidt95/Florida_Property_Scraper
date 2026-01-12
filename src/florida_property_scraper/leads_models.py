from __future__ import annotations

from pydantic import BaseModel


class SearchResult(BaseModel):
    owner: str
    address: str
    county: str
    score: int
    parcel_id: str | None = None
    source: str | None = None
