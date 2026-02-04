from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ProviderKind = Literal["clerk", "permits", "tax", "pa"]


@dataclass(frozen=True)
class ProviderTarget:
    county: str
    kind: ProviderKind
    base_url: str
    notes: str | None = None


def get_targets(county: str) -> list[ProviderTarget]:
    key = (county or "").strip().lower()
    if key != "seminole":
        return []

    return [
        ProviderTarget(
            county="seminole",
            kind="pa",
            base_url="https://www.scpafl.org/",
            notes="Property Appraiser",
        ),
        ProviderTarget(
            county="seminole",
            kind="tax",
            base_url="https://www.seminolecounty.tax/",
            notes="Tax Collector",
        ),
        ProviderTarget(
            county="seminole",
            kind="clerk",
            base_url="https://www.seminoleclerk.org/",
            notes="Clerk of Court",
        ),
        ProviderTarget(
            county="seminole",
            kind="permits",
            base_url="https://aca.seminolecountyfl.gov/",
            notes="Accela permits portal",
        ),
    ]