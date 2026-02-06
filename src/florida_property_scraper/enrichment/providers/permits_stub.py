from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from florida_property_scraper.enrichment.providers.model import (
    EvidenceItem,
    EvidenceSource,
    ProviderResult,
    confidence_score_from_label,
    compute_content_hash,
)


_MAJOR_KEYWORDS = {
    "demolition",
    "structural",
    "roof",
    "hvac",
    "electrical",
    "plumbing",
    "pool",
    "fire",
    "sitework",
    "tenant improvement",
    "tenant",
    "remodel",
    "generator",
}

_MINOR_KEYWORDS = {
    "windows",
    "window",
    "doors",
    "door",
    "solar",
    "fence",
    "sign",
}


def _parse_date(raw: object) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    if not s:
        return None
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        return date.fromisoformat(s)
    except Exception:
        return None


def _classify(text: str) -> str | None:
    if not text:
        return None
    t = text.lower()
    if any(k in t for k in _MAJOR_KEYWORDS):
        return "major"
    if any(k in t for k in _MINOR_KEYWORDS):
        return "minor"
    return None


@dataclass
class PermitsStubProvider:
    key: str = "permits_stub"
    category: str = "permits"
    label: str = "Permits (stub)"
    implemented: bool = True

    def supports_county(self, county: str) -> bool:
        return bool((county or "").strip())

    def fetch(
        self,
        *,
        county: str,
        parcel_id: str,
        dry_run: bool,
        fixture_mode: bool,
        store: Any,
    ) -> ProviderResult:
        county_key = (county or "").strip().lower()
        pid = (parcel_id or "").strip()
        fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        if dry_run:
            return ProviderResult(
                ok=True,
                provider_key=self.key,
                county=county_key,
                parcel_id=pid,
                status="dry_run",
                fetched_at=fetched_at,
            )

        rows = store.conn.execute(
            """
            SELECT permit_type, description, issue_date, final_date
            FROM permits
            WHERE lower(county)=? AND parcel_id=?
            ORDER BY issue_date DESC, final_date DESC
            """,
            (county_key, pid),
        ).fetchall()

        last_major: date | None = None
        last_minor: date | None = None

        for row in rows:
            permit_type = str(row["permit_type"] or "")
            desc = str(row["description"] or "")
            kind = _classify(f"{permit_type} {desc}")
            if not kind:
                continue
            dt = _parse_date(row["issue_date"]) or _parse_date(row["final_date"])
            if dt is None:
                continue
            if kind == "major":
                if last_major is None or dt > last_major:
                    last_major = dt
            if kind == "minor":
                if last_minor is None or dt > last_minor:
                    last_minor = dt

        source = EvidenceSource(source_type="permits", url=None, label=self.label)
        conf_label = "low"
        conf_score = confidence_score_from_label(conf_label)
        evidence: list[EvidenceItem] = []

        def _add(field: str, value: Any) -> None:
            evidence.append(
                EvidenceItem(
                    provider_id=self.key,
                    provider_name=self.label,
                    county=county_key,
                    parcel_id=pid,
                    field=field,
                    value=value,
                    confidence_label=conf_label,
                    confidence_score=conf_score,
                    source=source,
                    fetched_at=fetched_at,
                    retrieved_at=fetched_at,
                    content_hash=compute_content_hash(
                        field=field,
                        value=value,
                        source_url=None,
                    ),
                    extract_method=f"permits_stub:{field}",
                )
            )

        if last_major is not None:
            _add("permit_last_major_date", last_major.isoformat())
        if last_minor is not None:
            _add("permit_last_minor_date", last_minor.isoformat())

        return ProviderResult(
            ok=True,
            provider_key=self.key,
            county=county_key,
            parcel_id=pid,
            status="ok",
            fetched_at=fetched_at,
            evidence=evidence,
        )
