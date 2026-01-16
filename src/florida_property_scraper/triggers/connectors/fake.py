from __future__ import annotations

import os
from datetime import datetime, timezone

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..taxonomy import TriggerKey, default_severity_for_trigger


def _default_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@register_connector
class FakeConnector(TriggerConnector):
    """Deterministic, offline connector for tests + local demos."""

    connector_key = "fake"

    def poll(self, *, county: str, now_iso: str, limit: int) -> list[RawEvent]:
        county_key = (county or "").strip().lower()
        now_iso = (now_iso or "").strip() or _default_now_iso()
        try:
            lim = int(limit)
        except Exception:
            lim = 25
        lim = max(1, min(lim, 500))

        parcels_env = (os.environ.get("TRIGGER_FAKE_PARCELS") or "").strip()
        parcel_ids = [p.strip() for p in parcels_env.split(",") if p.strip()] if parcels_env else []
        if not parcel_ids:
            # Keep stable across runs: the point is plumbing, not realism.
            parcel_ids = ["DEMO-001", "DEMO-002", "DEMO-003"]

        out: list[RawEvent] = []
        for i, pid in enumerate(parcel_ids[:lim]):
            if i % 2 == 0:
                event_type = "fake.permit_issued"
                payload = {"permit_number": f"P-{i+1:04d}", "status": "ISSUED"}
            else:
                event_type = "fake.owner_mailing_changed"
                payload = {"from": "OLD ADDRESS", "to": "NEW ADDRESS"}
            out.append(
                RawEvent(
                    connector_key=self.connector_key,
                    county=county_key,
                    parcel_id=pid,
                    observed_at=now_iso,
                    event_type=event_type,
                    payload=payload,
                )
            )
        return out

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        et = (raw.event_type or "").strip().lower()
        if et == "fake.permit_issued":
            trigger_key = TriggerKey.PERMIT_ISSUED
        elif et == "fake.owner_mailing_changed":
            trigger_key = TriggerKey.OWNER_MAILING_CHANGED
        else:
            return None

        severity = default_severity_for_trigger(trigger_key)
        return TriggerEvent(
            county=raw.county,
            parcel_id=raw.parcel_id,
            trigger_key=str(trigger_key),
            trigger_at=raw.observed_at,
            severity=severity,
            source_connector_key=raw.connector_key,
            source_event_type=raw.event_type,
            source_event_id=None,
            details={"payload": raw.payload},
        )
