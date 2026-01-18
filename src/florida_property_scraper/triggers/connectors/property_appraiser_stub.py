from __future__ import annotations

from datetime import datetime, timezone

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..normalization.property_appraiser import normalize_property_appraiser_event
from ..taxonomy import default_severity_for_trigger


def _iso_midnight_utc(iso_date: str) -> str:
    d = (iso_date or "").strip()
    if not d:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return f"{d}T00:00:00+00:00"


@register_connector
class PropertyAppraiserStubConnector(TriggerConnector):
    """Deterministic, offline stub for property appraiser parcel change events.

    Emits events only for county='seminole' and parcel_id='XYZ789' so proofs/tests are stable.
    """

    connector_key = "property_appraiser_stub"

    def poll(self, *, county: str, now_iso: str, limit: int) -> list[RawEvent]:
        county_key = (county or "").strip().lower()
        if county_key != "seminole":
            return []

        events = [
            {
                "event_type": "property_appraiser.owner_mailing_changed",
                "observed_date": "2026-01-10",
                "old_owner": "DOE, JOHN",
                "new_owner": "DOE, JOHN",
                "old_mailing": "123 OLD ST, ORLANDO, FL 32801",
                "new_mailing": "999 NEW RD, SANFORD, FL 32771",
                "source": "stub",
            },
            {
                "event_type": "property_appraiser.owner_name_changed",
                "observed_date": "2026-01-11",
                "old_owner": "DOE, JOHN",
                "new_owner": "DOE FAMILY TRUST",
                "old_mailing": "999 NEW RD, SANFORD, FL 32771",
                "new_mailing": "999 NEW RD, SANFORD, FL 32771",
                "source": "stub",
            },
            {
                "event_type": "property_appraiser.deed_last_sale_updated",
                "observed_date": "2026-01-12",
                "old_owner": "DOE FAMILY TRUST",
                "new_owner": "DOE FAMILY TRUST",
                "old_mailing": "999 NEW RD, SANFORD, FL 32771",
                "new_mailing": "999 NEW RD, SANFORD, FL 32771",
                "last_sale_date": "2026-01-09",
                "last_sale_price": 250000,
                "source": "stub",
            },
        ]

        out: list[RawEvent] = []
        for e in events[: max(1, int(limit or 0))]:
            payload = {
                "old_owner": e.get("old_owner"),
                "new_owner": e.get("new_owner"),
                "old_mailing": e.get("old_mailing"),
                "new_mailing": e.get("new_mailing"),
                "source": e.get("source"),
                "raw": dict(e),
            }
            out.append(
                RawEvent(
                    connector_key=self.connector_key,
                    county=county_key,
                    parcel_id="XYZ789",
                    observed_at=_iso_midnight_utc(str(e.get("observed_date") or "")),
                    event_type=str(e.get("event_type") or ""),
                    payload=payload,
                )
            )

        return out

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
