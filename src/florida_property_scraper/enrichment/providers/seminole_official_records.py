from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from florida_property_scraper.enrichment.providers.model import (
    ArtifactRef,
    EvidenceItem,
    EvidenceSource,
    ProviderResult,
    confidence_score_from_label,
    compute_content_hash,
)


@dataclass
class SeminoleOfficialRecordsProvider:
    key: str = "seminole_official_records"
    category: str = "official_records"
    label: str = "Seminole Clerk Official Records"
    implemented: bool = True

    def supports_county(self, county: str) -> bool:
        return (county or "").strip().lower() == "seminole"

    def _fixture_path(self) -> Path:
        repo_root = Path(__file__).resolve().parents[4]
        return repo_root / "fixtures" / "providers" / self.key / "records.json"

    def _load_fixture(self) -> dict[str, Any]:
        path = self._fixture_path()
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"records": []}
        return data

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
        fetched_at = store._utc_now_iso()

        if not self.supports_county(county_key):
            return ProviderResult(
                ok=True,
                provider_key=self.key,
                county=county_key,
                parcel_id=pid,
                status="not_implemented",
                fetched_at=fetched_at,
            )

        if dry_run:
            return ProviderResult(
                ok=True,
                provider_key=self.key,
                county=county_key,
                parcel_id=pid,
                status="dry_run",
                fetched_at=fetched_at,
            )

        if not fixture_mode:
            return ProviderResult(
                ok=True,
                provider_key=self.key,
                county=county_key,
                parcel_id=pid,
                status="not_implemented",
                fetched_at=fetched_at,
                warnings=["fixture_mode_required"],
            )

        data = self._load_fixture()
        records = data.get("records") if isinstance(data, dict) else []
        if not isinstance(records, list):
            records = []

        matches = [r for r in records if isinstance(r, dict) and str(r.get("parcel_id") or "").strip() == pid]
        if not matches:
            return ProviderResult(
                ok=True,
                provider_key=self.key,
                county=county_key,
                parcel_id=pid,
                status="ok",
                fetched_at=fetched_at,
            )

        evidence: list[EvidenceItem] = []
        raw_artifacts: list[ArtifactRef] = []

        for rec in matches:
            raw_bytes = json.dumps(rec, ensure_ascii=True, default=str).encode("utf-8")
            source_url = str(rec.get("source_url") or "").strip() or "fixture://seminole_official_records"
            raw_reference = str(rec.get("book_page_or_instrument") or rec.get("instrument_id") or "").strip() or None
            raw_id = store.store_provider_raw(
                provider_key=self.key,
                county=county_key,
                parcel_id=pid,
                url=source_url,
                fetched_at=fetched_at,
                content_type="application/json",
                body=raw_bytes,
            )

            raw_artifacts.append(
                ArtifactRef(
                    raw_id=raw_id,
                    url=source_url,
                    content_type="application/json",
                    sha256=store.conn.execute(
                        "SELECT sha256 FROM provider_raw WHERE id=?",
                        (raw_id,),
                    ).fetchone()["sha256"]
                    if raw_id
                    else "",
                    body_bytes=len(raw_bytes),
                    storage_path=store.conn.execute(
                        "SELECT body_text_or_path FROM provider_raw WHERE id=?",
                        (raw_id,),
                    ).fetchone()["body_text_or_path"]
                    if raw_id
                    else None,
                )
            )

            doc_type = str(rec.get("doc_type") or "").strip()
            rec_date = str(rec.get("rec_date") or "").strip()
            consideration = rec.get("consideration")
            parties = str(rec.get("parties") or "").strip()
            snippet = "; ".join(
                [
                    f"doc_type={doc_type}" if doc_type else "",
                    f"rec_date={rec_date}" if rec_date else "",
                    f"parties={parties}" if parties else "",
                ]
            ).strip("; ")

            source = EvidenceSource(source_type="official", url=source_url, label=self.label)

            if doc_type:
                confidence_label = "high" if pid else "medium"
                evidence.append(
                    EvidenceItem(
                        provider_id=self.key,
                        provider_name=self.label,
                        county=county_key,
                        parcel_id=pid,
                        field="official_record_doc_type",
                        value=doc_type,
                        confidence_label=confidence_label,
                        confidence_score=confidence_score_from_label(confidence_label),
                        source=source,
                        fetched_at=fetched_at,
                        retrieved_at=fetched_at,
                        content_hash=compute_content_hash(
                            field="official_record_doc_type",
                            value=doc_type,
                            source_url=source_url,
                        ),
                        extract_method="fixture_json:doc_type",
                        raw_reference=raw_reference,
                        raw_snippet=snippet or None,
                        raw_ref=raw_id,
                    )
                )
            if rec_date:
                confidence_label = "high" if pid else "medium"
                evidence.append(
                    EvidenceItem(
                        provider_id=self.key,
                        provider_name=self.label,
                        county=county_key,
                        parcel_id=pid,
                        field="official_record_rec_date",
                        value=rec_date,
                        confidence_label=confidence_label,
                        confidence_score=confidence_score_from_label(confidence_label),
                        source=source,
                        fetched_at=fetched_at,
                        retrieved_at=fetched_at,
                        content_hash=compute_content_hash(
                            field="official_record_rec_date",
                            value=rec_date,
                            source_url=source_url,
                        ),
                        extract_method="fixture_json:rec_date",
                        raw_reference=raw_reference,
                        raw_snippet=snippet or None,
                        raw_ref=raw_id,
                    )
                )
            if parties:
                confidence_label = "high" if pid else "medium"
                evidence.append(
                    EvidenceItem(
                        provider_id=self.key,
                        provider_name=self.label,
                        county=county_key,
                        parcel_id=pid,
                        field="official_record_parties",
                        value=parties,
                        confidence_label=confidence_label,
                        confidence_score=confidence_score_from_label(confidence_label),
                        source=source,
                        fetched_at=fetched_at,
                        retrieved_at=fetched_at,
                        content_hash=compute_content_hash(
                            field="official_record_parties",
                            value=parties,
                            source_url=source_url,
                        ),
                        extract_method="fixture_json:parties",
                        raw_reference=raw_reference,
                        raw_snippet=snippet or None,
                        raw_ref=raw_id,
                    )
                )

            doc_lower = doc_type.lower()
            if rec_date and ("deed" in doc_lower or "warranty" in doc_lower or "quitclaim" in doc_lower):
                confidence_label = "high" if pid else "medium"
                evidence.append(
                    EvidenceItem(
                        provider_id=self.key,
                        provider_name=self.label,
                        county=county_key,
                        parcel_id=pid,
                        field="last_sale_date",
                        value=rec_date,
                        confidence_label=confidence_label,
                        confidence_score=confidence_score_from_label(confidence_label),
                        source=source,
                        fetched_at=fetched_at,
                        retrieved_at=fetched_at,
                        content_hash=compute_content_hash(
                            field="last_sale_date",
                            value=rec_date,
                            source_url=source_url,
                        ),
                        extract_method="fixture_json:rec_date",
                        raw_reference=raw_reference,
                        raw_snippet=snippet or None,
                        raw_ref=raw_id,
                    )
                )
            if consideration not in (None, ""):
                confidence_label = "high" if pid else "medium"
                evidence.append(
                    EvidenceItem(
                        provider_id=self.key,
                        provider_name=self.label,
                        county=county_key,
                        parcel_id=pid,
                        field="last_sale_price",
                        value=consideration,
                        confidence_label=confidence_label,
                        confidence_score=confidence_score_from_label(confidence_label),
                        source=source,
                        fetched_at=fetched_at,
                        retrieved_at=fetched_at,
                        content_hash=compute_content_hash(
                            field="last_sale_price",
                            value=consideration,
                            source_url=source_url,
                        ),
                        extract_method="fixture_json:consideration",
                        raw_reference=raw_reference,
                        raw_snippet=snippet or None,
                        raw_ref=raw_id,
                    )
                )

        return ProviderResult(
            ok=True,
            provider_key=self.key,
            county=county_key,
            parcel_id=pid,
            status="ok",
            fetched_at=fetched_at,
            evidence=evidence,
            raw_artifacts=raw_artifacts,
        )
