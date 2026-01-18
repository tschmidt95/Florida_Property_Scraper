from __future__ import annotations

import os

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..normalization.code_enforcement import normalize_code_enforcement_trigger_key
from ..taxonomy import default_severity_for_trigger


@register_connector
class CodeEnforcementLiveConnector(TriggerConnector):
    """Live-ready code enforcement connector (skeleton).

    IMPORTANT:
    - This connector MUST NOT perform network calls unless explicitly enabled.
    - Tests/proofs should not enable it.

    Enable with:
      FPS_ENABLE_CODE_ENFORCEMENT_LIVE=1
    """

    connector_key = "code_enforcement_live"

    def poll(self, *, county: str, now_iso: str, limit: int) -> list[RawEvent]:
        enabled = os.getenv("FPS_ENABLE_CODE_ENFORCEMENT_LIVE", "0").strip() == "1"
        if not enabled:
            return []

        # Skeleton only: no network calls.
        return []

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        payload = raw.payload or {}
        trigger_key = normalize_code_enforcement_trigger_key(
            event_type=str(raw.event_type or ""),
            status=str(payload.get("status") or ""),
            description=str(payload.get("description") or ""),
        )
        severity = default_severity_for_trigger(str(trigger_key))

        return TriggerEvent(
            county=raw.county,
            parcel_id=raw.parcel_id,
            trigger_key=str(trigger_key),
            trigger_at=raw.observed_at,
            severity=int(severity),
            source_connector_key=raw.connector_key,
            source_event_type=raw.event_type,
            source_event_id=None,
            details={"code_enforcement": payload},
        )
