from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol
import hashlib
import json


CONFIDENCE_SCORES: dict[str, float] = {
    "high": 0.9,
    "medium": 0.6,
    "low": 0.3,
}


def normalize_confidence_label(label: str | None) -> str:
    value = str(label or "").strip().lower()
    if value in CONFIDENCE_SCORES:
        return value
    return "low"


def confidence_score_from_label(label: str | None) -> float:
    return float(CONFIDENCE_SCORES.get(normalize_confidence_label(label), 0.3))


@dataclass(frozen=True)
class EvidenceSource:
    source_type: str
    url: str | None
    label: str | None


@dataclass(frozen=True)
class EvidenceItem:
    provider_id: str
    provider_name: str
    county: str
    parcel_id: str
    field: str
    value: Any
    confidence_label: str
    confidence_score: float
    source: EvidenceSource
    fetched_at: str
    retrieved_at: str
    content_hash: str
    extract_method: str
    raw_reference: str | None = None
    raw_snippet: str | None = None
    raw_ref: int | None = None


@dataclass(frozen=True)
class ArtifactRef:
    raw_id: int | None
    url: str
    content_type: str | None
    sha256: str
    body_bytes: int
    storage_path: str | None = None


@dataclass
class ProviderResult:
    ok: bool
    provider_key: str
    county: str
    parcel_id: str | None
    status: str
    fetched_at: str
    evidence: list[EvidenceItem] = field(default_factory=list)
    raw_artifacts: list[ArtifactRef] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class PropertyProvider(Protocol):
    key: str
    category: str
    label: str
    implemented: bool

    def supports_county(self, county: str) -> bool: ...

    def fetch(
        self,
        *,
        county: str,
        parcel_id: str,
        dry_run: bool,
        fixture_mode: bool,
        store: Any,
    ) -> ProviderResult: ...


def compute_content_hash(*, field: str, value: Any, source_url: str | None) -> str:
    payload = {
        "field": str(field or "").strip().lower(),
        "value": value,
        "source_url": str(source_url or "").strip(),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_request_fingerprint(*, provider_key: str, county: str, parcel_id: str, url: str) -> str:
    payload = {
        "provider_key": str(provider_key or "").strip().lower(),
        "county": str(county or "").strip().lower(),
        "parcel_id": str(parcel_id or "").strip(),
        "url": str(url or "").strip(),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
