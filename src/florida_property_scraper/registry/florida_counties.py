from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, List, Optional


@dataclass(frozen=True)
class ProviderConfig:
    provider_type: str
    status: str
    notes: str | None = None


@dataclass(frozen=True)
class CountyRecord:
    slug: str
    display_name: str
    geometry_provider: ProviderConfig
    pa_provider: ProviderConfig
    gis_provider: ProviderConfig
    permits_provider: ProviderConfig


def normalize_county_slug(raw: str) -> str:
    s = str(raw or "").strip().lower()
    if not s:
        return ""
    s = s.replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


_COUNTY_NAMES = [
    "Alachua",
    "Baker",
    "Bay",
    "Bradford",
    "Brevard",
    "Broward",
    "Calhoun",
    "Charlotte",
    "Citrus",
    "Clay",
    "Collier",
    "Columbia",
    "DeSoto",
    "Dixie",
    "Duval",
    "Escambia",
    "Flagler",
    "Franklin",
    "Gadsden",
    "Gilchrist",
    "Glades",
    "Gulf",
    "Hamilton",
    "Hardee",
    "Hendry",
    "Hernando",
    "Highlands",
    "Hillsborough",
    "Holmes",
    "Indian River",
    "Jackson",
    "Jefferson",
    "Lafayette",
    "Lake",
    "Lee",
    "Leon",
    "Levy",
    "Liberty",
    "Madison",
    "Manatee",
    "Marion",
    "Martin",
    "Miami-Dade",
    "Monroe",
    "Nassau",
    "Okaloosa",
    "Okeechobee",
    "Orange",
    "Osceola",
    "Palm Beach",
    "Pasco",
    "Pinellas",
    "Polk",
    "Putnam",
    "Santa Rosa",
    "Sarasota",
    "Seminole",
    "St. Johns",
    "St. Lucie",
    "Sumter",
    "Suwannee",
    "Taylor",
    "Union",
    "Volusia",
    "Wakulla",
    "Walton",
    "Washington",
]


def list_counties() -> List[CountyRecord]:
    out: List[CountyRecord] = []
    for name in _COUNTY_NAMES:
        slug = normalize_county_slug(name)
        out.append(
            CountyRecord(
                slug=slug,
                display_name=name,
                geometry_provider=ProviderConfig("parcels_sqlite", "unknown"),
                pa_provider=ProviderConfig("pa_sqlite", "unknown"),
                gis_provider=ProviderConfig("arcgis", "unknown"),
                permits_provider=ProviderConfig("permits", "unknown"),
            )
        )
    return out


def get_county(slug: str) -> Optional[CountyRecord]:
    key = normalize_county_slug(slug)
    for c in list_counties():
        if c.slug == key:
            return c
    return None


def registry_payload() -> List[dict]:
    return [
        {
            "slug": c.slug,
            "display_name": c.display_name,
            "geometry_provider": c.geometry_provider.__dict__,
            "pa_provider": c.pa_provider.__dict__,
            "gis_provider": c.gis_provider.__dict__,
            "permits_provider": c.permits_provider.__dict__,
        }
        for c in list_counties()
    ]
