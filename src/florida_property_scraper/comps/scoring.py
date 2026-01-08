from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .models import ComparableListing, RankedComparable, SubjectProperty


# Scoring weights (transparent and documented):
# - property_type: exact match (1.0 or 0.0)
# - distance: haversine distance decay when coords exist
# - size: relative diff on building_sf
# - price_per_sf: relative diff on $/sf (subject sale vs comp asking)
# - cap_rate: only used when both sides have cap rate (subject currently does not)
# - year_built: linear decay by absolute delta up to 80 years
WEIGHTS: Dict[str, float] = {
    "property_type": 0.25,
    "distance": 0.25,
    "size": 0.20,
    "price_per_sf": 0.15,
    "cap_rate": 0.10,
    "year_built": 0.05,
}


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def haversine_miles(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    r_km = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    km = r_km * c
    return km * 0.621371


def _score_exact_match(a: Optional[str], b: Optional[str]) -> Optional[float]:
    if not a or not b:
        return None
    return 1.0 if a.strip().lower() == b.strip().lower() else 0.0


def _score_relative_diff(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Score based on relative difference: 1.0 when equal, down to 0.0."""

    if a is None or b is None:
        return None
    if a <= 0 or b <= 0:
        return None
    rel = abs(a - b) / max(a, b)
    return _clamp01(1.0 - rel)


def _score_distance_miles(
    subject: SubjectProperty,
    comp: ComparableListing,
    *,
    half_life_miles: float = 2.0,
) -> Optional[Tuple[float, float]]:
    if (
        subject.latitude is None
        or subject.longitude is None
        or comp.latitude is None
        or comp.longitude is None
    ):
        return None
    miles = haversine_miles(
        float(subject.latitude),
        float(subject.longitude),
        float(comp.latitude),
        float(comp.longitude),
    )
    score = 0.5 ** (miles / max(half_life_miles, 1e-6))
    return _clamp01(score), miles


def score(subject: SubjectProperty, comp: ComparableListing) -> Tuple[float, Dict[str, Any]]:
    """Compute a similarity score in [0,1] plus a transparent explanation dict."""

    explanation: Dict[str, Any] = {"weights": dict(WEIGHTS)}
    components: Dict[str, float] = {}

    pt = _score_exact_match(subject.property_type, comp.property_type)
    if pt is not None:
        components["property_type"] = pt

    dist = _score_distance_miles(subject, comp)
    if dist is not None:
        dist_score, miles = dist
        components["distance"] = dist_score
        explanation["distance_miles"] = round(miles, 6)

    size = _score_relative_diff(subject.building_sf, comp.building_sf)
    if size is not None:
        components["size"] = size

    subject_ppsf: Optional[float] = None
    if subject.building_sf and subject.building_sf > 0 and subject.sale_price > 0:
        subject_ppsf = float(subject.sale_price) / float(subject.building_sf)
    comp_ppsf = comp.computed_price_per_sf()
    ppsf = _score_relative_diff(subject_ppsf, comp_ppsf)
    if ppsf is not None:
        components["price_per_sf"] = ppsf

    # Cap rate only when both sides have it (subject doesn't currently).
    # Keep hook for future expansion.
    if False:  # pragma: no cover
        pass

    year = None
    if subject.year_built is not None and comp.year_built is not None:
        delta = abs(int(subject.year_built) - int(comp.year_built))
        year = _clamp01(1.0 - min(delta, 80) / 80)
    if year is not None:
        components["year_built"] = year

    explanation["components"] = {k: round(v, 6) for k, v in components.items()}

    active_weights = {k: WEIGHTS[k] for k in components.keys() if WEIGHTS.get(k, 0) > 0}
    if not active_weights:
        return 0.0, explanation

    total_w = sum(active_weights.values())
    weighted = 0.0
    for k, w in active_weights.items():
        weighted += (w / total_w) * components[k]

    score_value = _clamp01(weighted)
    explanation["score"] = round(score_value, 6)
    return score_value, explanation


def rank_comps(
    subject: SubjectProperty,
    comps: Iterable[ComparableListing],
    *,
    top_n: int = 10,
) -> List[RankedComparable]:
    ranked: List[RankedComparable] = []
    for comp in comps:
        s, explanation = score(subject, comp)
        ranked.append(RankedComparable(listing=comp, score=s, explanation=explanation))

    ranked.sort(key=lambda r: (-r.score, r.listing.id))
    return ranked[: max(0, int(top_n))]


def distance_miles(subject: SubjectProperty, comp: ComparableListing) -> Optional[float]:
    scored = _score_distance_miles(subject, comp)
    if scored is None:
        return None
    _score, miles = scored
    return miles
