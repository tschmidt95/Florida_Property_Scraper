from __future__ import annotations

from datetime import date
from typing import List, Optional

from .models import ComparableReport, ScoredComparable, SimilarityWeights, SubjectProperty
from .providers.base import ListingProvider
from .providers.mock import FixtureSubjectResolver, MockProvider
from .scoring import similarity_score


def normalize_subject_property(
    *,
    county: str,
    parcel_id: str,
) -> SubjectProperty:
    """Normalize a purchased asset into the subject property schema.

    For now this is fixture-backed (deterministic, offline) and falls back to
    an ID-only subject when unknown.
    """

    resolver = FixtureSubjectResolver()
    return resolver.resolve(county=county, parcel_id=parcel_id)


def find_post_sale_comparables(
    *,
    county: str,
    parcel_id: str,
    sale_date: date,
    sale_price: float,
    provider: Optional[ListingProvider] = None,
    weights: Optional[SimilarityWeights] = None,
    min_comps: int = 3,
    max_comps: int = 10,
) -> ComparableReport:
    if provider is None:
        provider = MockProvider()
    if weights is None:
        weights = SimilarityWeights()

    subject = normalize_subject_property(county=county, parcel_id=parcel_id)
    candidates = provider.search_active_listings(subject, limit=200)

    scored: List[ScoredComparable] = []
    for listing in candidates:
        score, components = similarity_score(
            subject,
            listing,
            sale_price=sale_price,
            weights=weights,
        )
        scored.append(ScoredComparable(listing=listing, score=score, components=components))

    scored.sort(key=lambda c: (-c.score, c.listing.listing_id))

    # Enforce 3–10 comps when possible.
    cap = max(min_comps, min(max_comps, len(scored)))
    selected = scored[:cap]

    return ComparableReport(
        county=county,
        parcel_id=parcel_id,
        sale_date=sale_date,
        sale_price=float(sale_price),
        subject=subject,
        provider=provider.name,
        weights=weights,
        comparables=selected,
    )
