from __future__ import annotations

import os
from .base import NoopContactEnricher, ContactEnricher


def get_contact_enricher() -> ContactEnricher:
    provider = os.getenv("CONTACT_PROVIDER")
    api_key = os.getenv("CONTACT_API_KEY")
    if provider and api_key:
        # For now we keep a placeholder: real implementations may provide an HTTP
        # client that calls a templated endpoint. We default to Noop to avoid
        # accidental network calls in tests.
        return NoopContactEnricher()
    return NoopContactEnricher()
