from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from florida_property_scraper.enrichment.providers.model import (
    EvidenceItem,
    EvidenceSource,
    ProviderResult,
    confidence_score_from_label,
    compute_content_hash,
)


def _extract_year(value: object) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(s[:4])
    except Exception:
        return None


@dataclass
class TaxCollectorStubProvider:
    key: str = "tax_collector_stub"
    category: str = "tax"
    label: str = "Tax Collector (stub)"
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
            SELECT event_type, status, event_date, observed_at
            FROM tax_collector_events
            WHERE lower(county)=? AND parcel_id=?
            ORDER BY observed_at DESC
            """,
            (county_key, pid),
        ).fetchall()

        delinquent_years: set[int] = set()
        status_val = ""
        for row in rows:
            event_type = str(row["event_type"] or "").strip().lower()
            status = str(row["status"] or "").strip().lower()
            is_delinquent = "delinquent" in event_type or "delinquent" in status
            if is_delinquent:
                status_val = "delinquent"
                year = _extract_year(row["event_date"]) or _extract_year(row["observed_at"])
                if year:
                    delinquent_years.add(year)
            elif not status_val:
                if "current" in status or "paid" in status:
                    status_val = "current"
                elif status:
                    status_val = status

        evidence: list[EvidenceItem] = []
        source = EvidenceSource(source_type="tax", url=None, label=self.label)
        conf_label = "low"
        conf_score = confidence_score_from_label(conf_label)

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
                    extract_method=f"tax_collector_stub:{field}",
                )
            )

        if status_val:
            _add("tax_status", status_val)
        if rows:
            _add("tax_delinquent_years", len(delinquent_years))

        return ProviderResult(
            ok=True,
            provider_key=self.key,
            county=county_key,
            parcel_id=pid,
            status="ok",
            fetched_at=fetched_at,
            evidence=evidence,
        )
