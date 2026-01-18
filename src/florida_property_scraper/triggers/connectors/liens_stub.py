from __future__ import annotations

from datetime import datetime, timezone

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..normalization.liens import normalize_liens_trigger_key
from ..taxonomy import default_severity_for_trigger


def _iso_midnight_utc(iso_date: str) -> str:
    d = (iso_date or "").strip()
    if not d:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return f"{d}T00:00:00+00:00"


@register_connector
class LiensStubConnector(TriggerConnector):
    """Deterministic, offline stub for lien-related recorder events.

    Emits events only for county='seminole' and parcel_id='XYZ789' so proofs/tests are stable.
    """

    connector_key = "liens_stub"

    def poll(self, *, county: str, now_iso: str, limit: int) -> list[RawEvent]:
        county_key = (county or "").strip().lower()
        if county_key != "seminole":
            return []

        events = [
            {
                "event_type": "liens.mechanics_lien",
                "instrument": "INST-L-1001",
                "rec_date": "2026-01-09",
                "lien_type": "mechanics",
                "amount": 12500.0,
                "parties": "CONTRACTOR LLC / DOE, JOHN",
                "status": "filed",
            },
            {
                "event_type": "liens.hoa_lien",
                "instrument": "INST-L-1002",
                "rec_date": "2026-01-10",
                "lien_type": "hoa",
                "amount": 3450.0,
                "parties": "HOA ASSN / DOE, JOHN",
                "status": "filed",
            },
            {
                "event_type": "liens.irs_tax_lien",
                "instrument": "INST-L-1003",
                "rec_date": "2026-01-11",
                "lien_type": "irs",
                "amount": 9999.0,
                "parties": "IRS / DOE, JOHN",
                "status": "filed",
            },
            {
                "event_type": "liens.judgment_lien",
                "instrument": "INST-L-1004",
                "rec_date": "2026-01-12",
                "lien_type": "judgment",
                "amount": 25000.0,
                "parties": "PLAINTIFF LLC / DOE, JOHN",
                "status": "filed",
            },
            {
                "event_type": "liens.lien_release",
                "instrument": "INST-L-1005",
                "rec_date": "2026-01-13",
                "lien_type": "judgment",
                "amount": None,
                "parties": "PLAINTIFF LLC / DOE, JOHN",
                "status": "released",
            },
        ]

        out: list[RawEvent] = []
        for e in events[: max(1, int(limit or 0))]:
            out.append(
                RawEvent(
                    connector_key=self.connector_key,
                    county=county_key,
                    parcel_id="XYZ789",
                    observed_at=_iso_midnight_utc(str(e["rec_date"])),
                    event_type=str(e["event_type"]),
                    payload={
                        "instrument": e["instrument"],
                        "rec_date": e["rec_date"],
                        "lien_type": e["lien_type"],
                        "amount": e["amount"],
                        "parties": e["parties"],
                        "status": e["status"],
                    },
                )
            )
        return out

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        payload = raw.payload or {}
        trigger_key = normalize_liens_trigger_key(
            event_type=str(raw.event_type or ""),
            lien_type=str(payload.get("lien_type") or ""),
            status=str(payload.get("status") or ""),
            description=str(payload.get("parties") or ""),
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
            details={"liens": payload},
        )
