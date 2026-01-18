from __future__ import annotations

import os

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..normalization.property_appraiser import normalize_property_appraiser_event
from ..taxonomy import default_severity_for_trigger


@register_connector
class PropertyAppraiserLiveConnector(TriggerConnector):
    """Live-ready property appraiser connector (skeleton).

    IMPORTANT:
    - This connector MUST NOT perform network calls unless explicitly enabled.
    - Tests/proofs should not enable it.

    Enable with:
      FPS_ENABLE_PROPERTY_APPRAISER_LIVE=1

    In the future, this can delegate to per-county adapters that fetch PA pages/APIs.
    """

    connector_key = "property_appraiser_live"

    def poll(self, *, county: str, now_iso: str, limit: int) -> list[RawEvent]:
        enabled = os.getenv("FPS_ENABLE_PROPERTY_APPRAISER_LIVE", "0").strip() == "1"
        if not enabled:
            return []

        # Skeleton only: no network calls.
        # TODO: Implement per-county adapter(s) behind additional explicit flags.
        return []

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        norm = normalize_property_appraiser_event(event_type=str(raw.event_type or ""), payload=raw.payload or {})
        trigger_key = norm.trigger_key
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
            details=norm.details,
        )
