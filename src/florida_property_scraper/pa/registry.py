from __future__ import annotations

from typing import Protocol


class PAProvider(Protocol):
    county: str

    def fetch_by_address(self, address: str) -> dict:
        """Fetch property data by address. May perform LIVE HTTP only when LIVE=1.

        Must be implemented by county-specific providers.
        """
        raise NotImplementedError


def get_pa_provider(county: str) -> PAProvider:
    county = (county or "").strip().lower()
    if county == "seminole":
        # Seminole provider may be implemented in pa/seminole.py in future.
        try:
            from florida_property_scraper.pa.seminole import SeminolePAProvider

            return SeminolePAProvider()
        except Exception:
            # Missing or incomplete provider implementations should surface as
            # "not implemented" rather than crashing the API tests.
            raise KeyError(f"No PA provider registered for county={county}")
    raise KeyError(f"No PA provider registered for county={county}")
