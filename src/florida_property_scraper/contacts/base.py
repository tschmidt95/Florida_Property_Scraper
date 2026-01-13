from __future__ import annotations

from dataclasses import dataclass
from typing import List, Protocol


@dataclass
class EnrichmentResult:
    phones: List[str]
    emails: List[str]
    source: str | None = None
    confidence: float | None = None


class ContactEnricher(Protocol):
    def enrich(
        self,
        owner_name: str,
        mailing_address: str | None = None,
        parcel_id: str | None = None,
        county: str | None = None,
        state: str | None = None,
    ) -> EnrichmentResult:
        raise NotImplementedError


class NoopContactEnricher:
    def enrich(self, *args, **kwargs) -> EnrichmentResult:  # pragma: no cover - trivial
        return EnrichmentResult(phones=[], emails=[], source=None, confidence=None)
