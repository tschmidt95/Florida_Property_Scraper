from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from typing import Any

from florida_property_scraper.enrichment.providers.model import (
    EvidenceItem,
    EvidenceSource,
    ProviderResult,
    confidence_score_from_label,
    compute_content_hash,
)
from florida_property_scraper.pa.storage import PASQLite


@dataclass
class PASnapshotStubProvider:
    key: str = "pa_snapshot_stub"
    category: str = "property_appraiser"
    label: str = "PA Snapshot (stub)"
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

        db_path = (
            str(os.getenv("PA_DB") or "").strip()
            or str(os.getenv("LEADS_SQLITE_PATH") or "").strip()
            or "./leads.sqlite"
        )
        pa_store = PASQLite(db_path)
        try:
            rec = pa_store.get(county=county_key, parcel_id=pid)
        finally:
            pa_store.close()

        if rec is None:
            return ProviderResult(
                ok=True,
                provider_key=self.key,
                county=county_key,
                parcel_id=pid,
                status="ok",
                fetched_at=fetched_at,
            )

        source_url = str(rec.source_url or "").strip() or None
        source = EvidenceSource(source_type="pa", url=source_url, label=self.label)
        conf_label = "low"
        conf_score = confidence_score_from_label(conf_label)

        evidence: list[EvidenceItem] = []

        def _add(field: str, value: Any) -> None:
            if value is None:
                return
            if isinstance(value, str) and not value.strip():
                return
            if isinstance(value, bool):
                return
            if isinstance(value, (int, float)):
                try:
                    if float(value) == 0.0:
                        return
                except Exception:
                    return

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
                        source_url=source_url,
                    ),
                    extract_method=f"pa_snapshot_stub:{field}",
                )
            )

        _add("situs_address", rec.situs_address)
        _add("owner_mailing_address", rec.mailing_address)
        _add("mailing_state", rec.mailing_state)
        _add("last_sale_date", rec.last_sale_date)
        _add("last_sale_price", rec.last_sale_price)
        _add("assessed_value", rec.assessed_value)
        _add("total_value", rec.just_value)
        _add("photo_url", rec.photo_url)
        _add("mortgage_amount", rec.mortgage_amount)
        _add("mortgage_date", rec.mortgage_date)
        _add("mortgage_lender", rec.mortgage_lender)

        return ProviderResult(
            ok=True,
            provider_key=self.key,
            county=county_key,
            parcel_id=pid,
            status="ok",
            fetched_at=fetched_at,
            evidence=evidence,
        )
