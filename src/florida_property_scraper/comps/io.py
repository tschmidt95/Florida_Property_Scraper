from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable, List, Tuple

from .models import ComparableReport
from .scoring import distance_miles


def resolve_output_paths(out: str) -> Tuple[Path, Path]:
    out_path = Path(out)
    if out_path.suffix.lower() == ".json":
        return out_path, out_path.with_suffix(".csv")
    if out_path.suffix.lower() == ".csv":
        return out_path.with_suffix(".json"), out_path
    # treat as a base path; create <base>.json and <base>.csv
    return out_path.with_suffix(".json"), out_path.with_suffix(".csv")


def write_report_json(report: ComparableReport, path: Path) -> None:
    payload = report.to_dict()

    def _round(value):
        if isinstance(value, float):
            return round(value, 6)
        if isinstance(value, dict):
            return {k: _round(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_round(v) for v in value]
        return value

    payload = _round(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def write_summary_csv(report: ComparableReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "listing_id",
        "source",
        "status",
        "address",
        "property_type",
        "distance_miles",
        "building_sqft",
        "asking_price",
        "price_per_sqft",
        "cap_rate",
        "year_built",
        "score",
        "url",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for comp in report.comparables:
            listing = comp.listing
            miles = distance_miles(report.subject, listing)
            writer.writerow(
                {
                    "listing_id": listing.listing_id,
                    "source": listing.source,
                    "status": listing.status,
                    "address": listing.address or "",
                    "property_type": listing.property_type or "",
                    "distance_miles": "" if miles is None else f"{miles:.3f}",
                    "building_sqft": "" if listing.building_sqft is None else f"{listing.building_sqft:.0f}",
                    "asking_price": "" if listing.asking_price is None else f"{listing.asking_price:.0f}",
                    "price_per_sqft": "" if listing.price_per_sqft() is None else f"{listing.price_per_sqft():.2f}",
                    "cap_rate": "" if listing.cap_rate is None else f"{listing.cap_rate:.4f}",
                    "year_built": "" if listing.year_built is None else str(listing.year_built),
                    "score": f"{comp.score:.6f}",
                    "url": listing.url or "",
                }
            )
