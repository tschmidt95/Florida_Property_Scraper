from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class RawEvent:
    connector_key: str
    county: str
    parcel_id: str
    observed_at: str  # ISO8601
    event_type: str
    payload: Dict[str, Any]

    def payload_json(self) -> str:
        return json.dumps(self.payload or {}, ensure_ascii=True)


@dataclass(frozen=True)
class TriggerEvent:
    county: str
    parcel_id: str
    trigger_key: str
    trigger_at: str  # ISO8601
    severity: int
    source_connector_key: str
    source_event_type: str
    source_event_id: Optional[int] = None
    details: Optional[Dict[str, Any]] = None

    def details_json(self) -> str:
        return json.dumps(self.details or {}, ensure_ascii=True)


@dataclass(frozen=True)
class AlertRecord:
    county: str
    parcel_id: str
    alert_key: str
    severity: int
    first_seen_at: str
    last_seen_at: str
    status: str  # open|closed
    trigger_event_ids: list[int]
    details: Dict[str, Any]

    def details_json(self) -> str:
        return json.dumps(self.details or {}, ensure_ascii=True)

    def trigger_event_ids_json(self) -> str:
        return json.dumps([int(x) for x in (self.trigger_event_ids or [])], ensure_ascii=True)
