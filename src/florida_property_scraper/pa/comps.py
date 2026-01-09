from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .schema import PAProperty


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
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


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def _rel_sim(a: float, b: float) -> Optional[float]:
    if a <= 0 or b <= 0:
        return None
    return _clamp01(1.0 - abs(a - b) / max(a, b))


WEIGHTS: Dict[str, float] = {
    "land_use_code": 0.20,
    "property_class": 0.10,
    "distance": 0.20,
    "building_sf": 0.15,
    "year_built": 0.10,
    "land_sf": 0.10,
    "last_sale_price": 0.10,
    "assessed_value": 0.05,
}


@dataclass(frozen=True)
class RankedPAComp:
    county: str
    parcel_id: str
    score: float
    explanation: Dict[str, Any]


def score_similarity(subject: PAProperty, candidate: PAProperty) -> Tuple[float, Dict[str, Any]]:
    explanation: Dict[str, Any] = {"weights": dict(WEIGHTS), "components": {}}
    comps: Dict[str, float] = {}

    if subject.land_use_code and candidate.land_use_code:
        comps["land_use_code"] = 1.0 if subject.land_use_code == candidate.land_use_code else 0.0

    if subject.property_class and candidate.property_class:
        comps["property_class"] = 1.0 if subject.property_class == candidate.property_class else 0.0

    # Distance only if both have coords; otherwise ignore by not adding the component.
    if (
        subject.latitude is not None
        and subject.longitude is not None
        and candidate.latitude is not None
        and candidate.longitude is not None
    ):
        miles = haversine_miles(subject.latitude, subject.longitude, candidate.latitude, candidate.longitude)
        # half-life 2 miles
        comps["distance"] = _clamp01(0.5 ** (miles / 2.0))
        explanation["distance_miles"] = round(miles, 6)

    sim = _rel_sim(subject.building_sf, candidate.building_sf)
    if sim is not None:
        comps["building_sf"] = sim

    if subject.year_built > 0 and candidate.year_built > 0:
        delta = abs(subject.year_built - candidate.year_built)
        comps["year_built"] = _clamp01(1.0 - min(delta, 80) / 80)

    sim = _rel_sim(subject.land_sf, candidate.land_sf)
    if sim is not None:
        comps["land_sf"] = sim

    sim = _rel_sim(subject.last_sale_price, candidate.last_sale_price)
    if sim is not None:
        comps["last_sale_price"] = sim

    sim = _rel_sim(subject.assessed_value, candidate.assessed_value)
    if sim is not None:
        comps["assessed_value"] = sim

    explanation["components"] = {k: round(v, 6) for k, v in comps.items()}

    active_weights = {k: WEIGHTS[k] for k in comps.keys() if WEIGHTS.get(k, 0) > 0}
    if not active_weights:
        return 0.0, explanation

    total = sum(active_weights.values())
    score = 0.0
    for k, w in active_weights.items():
        score += (w / total) * comps[k]

    score = _clamp01(score)
    explanation["score"] = round(score, 6)
    return score, explanation


def rank_comps(
    subject: PAProperty,
    candidates: Iterable[PAProperty],
    *,
    top_n: int,
) -> List[RankedPAComp]:
    ranked: List[RankedPAComp] = []
    for cand in candidates:
        if cand.county != subject.county:
            continue
        if cand.parcel_id == subject.parcel_id:
            continue
        s, exp = score_similarity(subject, cand)
        ranked.append(RankedPAComp(county=cand.county, parcel_id=cand.parcel_id, score=s, explanation=exp))

    ranked.sort(key=lambda r: (-r.score, r.parcel_id))
    return ranked[: max(0, int(top_n))]
