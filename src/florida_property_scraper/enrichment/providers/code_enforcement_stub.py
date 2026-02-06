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


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        return date.fromisoformat(s)
    except Exception:
        return None


@dataclass
class CodeEnforcementStubProvider:
    key: str = "code_enforcement_stub"
    category: str = "code_enforcement"
    label: str = "Code Enforcement (stub)"
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
            FROM code_enforcement_events
            WHERE lower(county)=? AND parcel_id=?
            ORDER BY observed_at DESC
            """,
            (county_key, pid),
        ).fetchall()

        status_val = ""
        last_open_date: date | None = None
        for row in rows:
            event_type = str(row["event_type"] or "").strip().lower()
            status = str(row["status"] or "").strip().lower()
            if "open" in status or "case_opened" in event_type or "case opened" in event_type:
                status_val = "open"
                dt = _parse_date(row["event_date"]) or _parse_date(row["observed_at"])
                if dt is not None and (last_open_date is None or dt > last_open_date):
                    last_open_date = dt
            elif not status_val and status:
                status_val = status

        evidence: list[EvidenceItem] = []
        source = EvidenceSource(source_type="code_enforcement", url=None, label=self.label)
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
                    extract_method=f"code_enforcement_stub:{field}",
                )
            )

        if status_val:
            _add("code_enforcement_status", status_val)
        if last_open_date is not None:
            _add("code_case_opened_date", last_open_date.isoformat())

        return ProviderResult(
            ok=True,
            provider_key=self.key,
            county=county_key,
            parcel_id=pid,
            status="ok",
            fetched_at=fetched_at,
            evidence=evidence,
        )
