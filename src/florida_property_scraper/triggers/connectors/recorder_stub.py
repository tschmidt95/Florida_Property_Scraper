from __future__ import annotations

from datetime import datetime, timezone

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..normalization.recorder import normalize_recorder_trigger_key
from ..taxonomy import default_severity_for_trigger


def _iso_midnight_utc(iso_date: str) -> str:
    d = (iso_date or "").strip()
    if not d:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return f"{d}T00:00:00+00:00"


@register_connector
class RecorderStubConnector(TriggerConnector):
    """Deterministic, offline stub for recorder document classification.

    Emits events only for county='seminole' and parcel_id='XYZ789' so proofs/tests are stable.
    """

    connector_key = "recorder_stub"

    def poll(self, *, county: str, now_iso: str, limit: int) -> list[RawEvent]:
        county_key = (county or "").strip().lower()
        if county_key != "seminole":
            return []

        docs = [
            {
                "event_type": "recorder.notice_of_default",
                "instrument": "INST-R-2001",
                "rec_date": "2026-01-14",
                "doc_type": "NOTICE OF DEFAULT",
                "parties": "BANK NA / DOE, JOHN",
                "consideration": None,
            },
            {
                "event_type": "recorder.lis_pendens",
                "instrument": "INST-R-2002",
                "rec_date": "2026-01-15",
                "doc_type": "LIS PENDENS",
                "parties": "PLAINTIFF v DEFENDANT",
                "consideration": None,
            },
            {
                "event_type": "recorder.mortgage_assignment",
                "instrument": "INST-R-2003",
                "rec_date": "2026-01-16",
                "doc_type": "ASSIGNMENT OF MORTGAGE",
                "parties": "LENDER A -> LENDER B",
                "consideration": None,
            },
            {
                "event_type": "recorder.mortgage_satisfaction",
                "instrument": "INST-R-2004",
                "rec_date": "2026-01-17",
                "doc_type": "SATISFACTION OF MORTGAGE",
                "parties": "LENDER / BORROWER",
                "consideration": None,
            },
        ]

        out: list[RawEvent] = []
        for d in docs[: max(1, int(limit or 0))]:
            out.append(
                RawEvent(
                    connector_key=self.connector_key,
                    county=county_key,
                    parcel_id="XYZ789",
                    observed_at=_iso_midnight_utc(str(d["rec_date"])),
                    event_type=str(d["event_type"]),
                    payload={
                        "instrument": d["instrument"],
                        "rec_date": d["rec_date"],
                        "doc_type": d["doc_type"],
                        "parties": d["parties"],
                        "consideration": d["consideration"],
                    },
                )
            )
        return out

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        payload = raw.payload or {}
        trigger_key = normalize_recorder_trigger_key(
            doc_type=str(payload.get("doc_type") or ""),
            instrument=str(payload.get("instrument") or ""),
            description=str(raw.event_type or ""),
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
            details={"recorder": payload},
        )
