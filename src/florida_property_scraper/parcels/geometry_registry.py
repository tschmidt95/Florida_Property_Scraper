from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Dict

from florida_property_scraper.parcels.geometry_provider import ParcelGeometryProvider
from florida_property_scraper.parcels.providers.fdor_centroids import FDORCentroidsProvider
from florida_property_scraper.parcels.providers.orange import OrangeProvider
from florida_property_scraper.parcels.providers.seminole import SeminoleProvider


def _default_geojson_dir() -> Path:
    # Optional override for local deployments.
    env = os.getenv("PARCEL_GEOJSON_DIR")
    if env:
        return Path(env)

    repo_root = Path(__file__).resolve().parents[3]
    data_dir = repo_root / "data" / "parcels"
    if data_dir.exists():
        return data_dir

    # Dev/test fallback.
    return repo_root / "tests" / "fixtures" / "parcels"


@lru_cache(maxsize=128)
def get_provider(county: str) -> ParcelGeometryProvider:
    """Return a cached provider instance for the given county.

    Unknown counties return a provider that serves no geometry.
    """

    county_key = (county or "").strip().lower()

    # Optional live geometry provider (network-backed). Kept behind an env flag
    # so tests and offline deployments remain deterministic.
    if os.getenv("FPS_USE_FDOR_CENTROIDS", "").strip() in {"1", "true", "True"}:
        if county_key in {"seminole", "orange"}:
            provider = FDORCentroidsProvider(county=county_key)
            provider.load()
            return provider

    providers: Dict[str, ParcelGeometryProvider] = {
        "seminole": SeminoleProvider(
            geojson_path=_default_geojson_dir() / "seminole.geojson"
        ),
        "orange": OrangeProvider(
            geojson_path=_default_geojson_dir() / "orange.geojson"
        ),
    }

    provider = providers.get(county_key)
    if provider is None:
        # Return SeminoleProvider pointed at a non-existent file to keep the type
        # stable without adding extra provider classes.
        provider = SeminoleProvider(
            geojson_path=_default_geojson_dir() / "__none__.geojson"
        )
        provider.county = county_key.strip()  # type: ignore[attr-defined]
    provider.load()
    return provider
