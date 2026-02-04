from __future__ import annotations

import os
from typing import Optional

from .base import OwnerEnrichmentProvider
from .people_data_labs import PeopleDataLabsProvider


def get_owner_enrichment_provider() -> Optional[OwnerEnrichmentProvider]:
    provider_key = str(os.getenv("OWNER_ENRICH_PROVIDER", "")).strip().lower()
    if provider_key in {"pdl", "people_data_labs", "people-data-labs"}:
        return PeopleDataLabsProvider()
    return None
