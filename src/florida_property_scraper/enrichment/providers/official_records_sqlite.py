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


@dataclass
class OfficialRecordsSQLiteProvider:
    key: str = "official_records_sqlite"
    category: str = "official_records"
    label: str = "Official Records (SQLite)"
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
            SELECT doc_type, rec_date, parties, consideration, source, book_page_or_instrument
            FROM official_records
            WHERE lower(county)=? AND parcel_id=?
            ORDER BY rec_date DESC, id DESC
            """,
            (county_key, pid),
        ).fetchall()

        if not rows:
            return ProviderResult(
                ok=True,
                provider_key=self.key,
                county=county_key,
                parcel_id=pid,
                status="ok",
                fetched_at=fetched_at,
            )

        latest = rows[0]
        source_raw = str(latest["source"] or "").strip()
        source_url = source_raw if source_raw.startswith("http://") or source_raw.startswith("https://") else None

        source = EvidenceSource(source_type="official", url=source_url, label=self.label)
        conf_label = "medium"
        conf_score = confidence_score_from_label(conf_label)
        evidence: list[EvidenceItem] = []

        def _add(field: str, value: Any, *, extract_method: str, raw_reference: str | None = None) -> None:
            if value is None:
                return
            if isinstance(value, str) and not value.strip():
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
                    extract_method=extract_method,
                    raw_reference=raw_reference,
                )
            )

        raw_reference = str(latest["book_page_or_instrument"] or "").strip() or None
        doc_type = str(latest["doc_type"] or "").strip()
        rec_date = str(latest["rec_date"] or "").strip()
        parties = str(latest["parties"] or "").strip()

        _add("official_record_doc_type", doc_type, extract_method="official_records_sqlite:doc_type", raw_reference=raw_reference)
        _add("official_record_rec_date", rec_date, extract_method="official_records_sqlite:rec_date", raw_reference=raw_reference)
        _add("official_record_parties", parties, extract_method="official_records_sqlite:parties", raw_reference=raw_reference)

        doc_lower = doc_type.lower()
        if rec_date and ("deed" in doc_lower or "warranty" in doc_lower or "quitclaim" in doc_lower):
            _add("last_sale_date", rec_date, extract_method="official_records_sqlite:last_sale_date", raw_reference=raw_reference)

        consideration = latest["consideration"]
        if consideration not in (None, ""):
            _add("last_sale_price", consideration, extract_method="official_records_sqlite:last_sale_price", raw_reference=raw_reference)

        return ProviderResult(
            ok=True,
            provider_key=self.key,
            county=county_key,
            parcel_id=pid,
            status="ok",
            fetched_at=fetched_at,
            evidence=evidence,
        )
