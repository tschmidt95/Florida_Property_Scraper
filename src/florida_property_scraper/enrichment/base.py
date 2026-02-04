from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass(frozen=True)
class Evidence:
    field: str
    url: str
    retrieved_at: str
    provider: str


@dataclass
class EnrichmentResult:
    parcel_id: str
    fields: Dict[str, Any] = field(default_factory=dict)
    evidence: List[Evidence] = field(default_factory=list)
    ok: bool = True
    errors: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        allowed = {e.field for e in self.evidence if e.field}
        if not allowed:
            self.fields = {}
            return
        self.fields = {k: v for k, v in self.fields.items() if k in allowed}

    def add_field(self, field_name: str, value: Any, evidence: Evidence) -> None:
        if not field_name:
            return
        if evidence.field != field_name:
            evidence = Evidence(
                field=field_name,
                url=evidence.url,
                retrieved_at=evidence.retrieved_at,
                provider=evidence.provider,
            )
        self.fields[field_name] = value
        self.evidence.append(evidence)