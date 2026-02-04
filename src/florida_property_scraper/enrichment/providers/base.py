from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol


@dataclass
class OwnerEnrichmentResult:
    status: str
    provider: str
    phones: List[str]
    emails: List[str]
    raw: Dict[str, Any]


class OwnerEnrichmentProvider(Protocol):
    name: str

    def is_configured(self) -> bool: ...

    def enrich(
        self,
        *,
        owner_name: str,
        mailing_address: str,
        county: str,
        parcel_id: str,
    ) -> OwnerEnrichmentResult: ...
