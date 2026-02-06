from __future__ import annotations

import os
from typing import Optional

from .base import OwnerEnrichmentProvider
from .people_data_labs import PeopleDataLabsProvider
from .model import PropertyProvider
from .pa_snapshot_stub import PASnapshotStubProvider
from .permits_stub import PermitsStubProvider
from .tax_collector_stub import TaxCollectorStubProvider
from .code_enforcement_stub import CodeEnforcementStubProvider
from .seminole_official_records import SeminoleOfficialRecordsProvider


def get_owner_enrichment_provider() -> Optional[OwnerEnrichmentProvider]:
    provider_key = str(os.getenv("OWNER_ENRICH_PROVIDER", "")).strip().lower()
    if provider_key in {"pdl", "people_data_labs", "people-data-labs"}:
        return PeopleDataLabsProvider()
    return None


_PROPERTY_PROVIDERS: list[PropertyProvider] = [
    SeminoleOfficialRecordsProvider(),
    PASnapshotStubProvider(),
    PermitsStubProvider(),
    TaxCollectorStubProvider(),
    CodeEnforcementStubProvider(),
]


def get_property_providers(*, county: str) -> list[PropertyProvider]:
    county_key = (county or "").strip().lower()
    return [p for p in _PROPERTY_PROVIDERS if p.supports_county(county_key)]


def get_property_provider(*, provider_key: str) -> Optional[PropertyProvider]:
    key = (provider_key or "").strip().lower()
    for provider in _PROPERTY_PROVIDERS:
        if provider.key == key:
            return provider
    return None
