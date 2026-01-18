from __future__ import annotations

from datetime import datetime, timezone

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..normalization.courts import normalize_courts_trigger_key
from ..taxonomy import default_severity_for_trigger


def _iso_midnight_utc(iso_date: str) -> str:
    # iso_date expected: YYYY-MM-DD
    d = (iso_date or "").strip()
    if not d:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return f"{d}T00:00:00+00:00"


@register_connector
class CourtsStubConnector(TriggerConnector):
    """Deterministic, offline stub for court docket filings.

    Emits events only for county='seminole' and parcel_id='XYZ789' so proofs/tests are stable.
    """

    connector_key = "courts_stub"

    def poll(self, *, county: str, now_iso: str, limit: int) -> list[RawEvent]:
        county_key = (county or "").strip().lower()
        if county_key != "seminole":
            return []

        events = [
            {
                "event_type": "courts.divorce_filed",
                "case_number": "2026-DR-000123",
                "file_date": "2026-01-05",
                "court": "Seminole County Clerk of Courts",
                "case_type": "Divorce",
                "parties": "DOE, JANE v DOE, JOHN",
            },
            {
                "event_type": "courts.probate_opened",
                "case_number": "2026-PR-000045",
                "file_date": "2026-01-06",
                "court": "Seminole County Clerk of Courts",
                "case_type": "Probate",
                "parties": "ESTATE OF DOE, JOHN",
            },
            {
                "event_type": "courts.eviction_filing",
                "case_number": "2026-CC-000777",
                "file_date": "2026-01-07",
                "court": "Seminole County Clerk of Courts",
                "case_type": "Eviction",
                "parties": "LANDLORD LLC v TENANT, TEST",
            },
            {
                "event_type": "courts.foreclosure_filing",
                "case_number": "2026-CA-000888",
                "file_date": "2026-01-08",
                "court": "Seminole County Clerk of Courts",
                "case_type": "Foreclosure",
                "parties": "BANK NA v DOE, JOHN",
            },
        ]

        out: list[RawEvent] = []
        for e in events[: max(1, int(limit or 0))]:
            out.append(
                RawEvent(
                    connector_key=self.connector_key,
                    county=county_key,
                    parcel_id="XYZ789",
                    observed_at=_iso_midnight_utc(str(e["file_date"])),
                    event_type=str(e["event_type"]),
                    payload={
                        "case_number": e["case_number"],
                        "file_date": e["file_date"],
                        "court": e["court"],
                        "case_type": e["case_type"],
                        "parties": e["parties"],
                    },
                )
            )
        return out

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        payload = raw.payload or {}
        trigger_key = normalize_courts_trigger_key(
            event_type=str(raw.event_type or ""),
            case_type=str(payload.get("case_type") or ""),
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
            details={"courts": payload},
        )
