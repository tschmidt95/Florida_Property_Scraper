from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone

from florida_property_scraper.storage import SQLiteStore

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..normalization.official_records import normalize_official_record_trigger_key
from ..taxonomy import TriggerKey, default_severity_for_trigger


def _parse_date_any(s: str) -> date | None:
    raw = (s or "").strip()
    if not raw:
        return None

    # ISO date: YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    # US date: MM/DD/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except Exception:
            return None

    # ISO timestamp
    if "T" in raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
        except Exception:
            return None

    return None


def _iso_midnight_utc(d: date) -> str:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat()


def _now_date(now_iso: str) -> date:
    try:
        dt = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).date()
    except Exception:
        return datetime.now(timezone.utc).date()


@register_connector
class OfficialRecordsStubConnector(TriggerConnector):
    """Offline stub connector reading from the `official_records` table.

    A future connector can populate `official_records` from a real recorder search.
    """

    connector_key = "official_records_stub"

    def poll(self, *, county: str, now_iso: str, limit: int) -> list[RawEvent]:
        county_key = (county or "").strip().lower()
        now_iso = (now_iso or "").strip()
        try:
            lim = int(limit)
        except Exception:
            lim = 50
        lim = max(1, min(lim, 500))

        db_path = os.getenv("LEADS_SQLITE_PATH", "./leads.sqlite")
        store = SQLiteStore(db_path)
        try:
            row = store.conn.execute(
                """
                SELECT MAX(observed_at) AS max_observed
                FROM trigger_raw_events
                WHERE connector_key=? AND county=?
                """,
                (self.connector_key, county_key),
            ).fetchone()
            watermark_iso = (row["max_observed"] if row else None) or ""

            if watermark_iso:
                since_d = _now_date(watermark_iso)
            else:
                since_d = _now_date(now_iso) - timedelta(days=60)

            rows = store.conn.execute(
                """
                SELECT
                    county,
                    parcel_id,
                    join_key,
                    doc_type,
                    rec_date,
                    parties,
                    book_page_or_instrument,
                    consideration,
                    raw_text,
                    owner_name,
                    address,
                    source
                FROM official_records
                WHERE county=?
                ORDER BY rec_date DESC
                LIMIT ?
                """,
                (county_key, lim),
            ).fetchall()

            out: list[RawEvent] = []
            for r in rows:
                d = _parse_date_any(str(r["rec_date"] or "").strip())
                if d is None:
                    continue
                if d < since_d:
                    continue

                observed_at = _iso_midnight_utc(d)
                pid = (str(r["parcel_id"] or "").strip() or str(r["join_key"] or "").strip())
                if not pid:
                    continue

                payload = {
                    "join_key": r["join_key"],
                    "doc_type": r["doc_type"],
                    "rec_date": r["rec_date"],
                    "parties": r["parties"],
                    "book_page_or_instrument": r["book_page_or_instrument"],
                    "consideration": r["consideration"],
                    "raw_text": r["raw_text"],
                    "owner_name": r["owner_name"],
                    "address": r["address"],
                    "source": r["source"],
                }

                out.append(
                    RawEvent(
                        connector_key=self.connector_key,
                        county=county_key,
                        parcel_id=pid,
                        observed_at=observed_at,
                        event_type="official_records_stub.record",
                        payload=payload,
                    )
                )

            return out
        finally:
            store.close()

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        if (raw.event_type or "").strip().lower() != "official_records_stub.record":
            return None

        payload = raw.payload or {}
        trigger_key = normalize_official_record_trigger_key(
            doc_type=str(payload.get("doc_type") or ""),
            instrument=str(payload.get("book_page_or_instrument") or ""),
            description=str(payload.get("raw_text") or ""),
            parties=str(payload.get("parties") or ""),
            consideration=str(payload.get("consideration") or ""),
        )
        severity = default_severity_for_trigger(str(trigger_key))

        owner_key = str(payload.get("join_key") or "").strip() or None
        return TriggerEvent(
            county=raw.county,
            parcel_id=raw.parcel_id,
            trigger_key=str(trigger_key),
            trigger_at=raw.observed_at,
            severity=int(severity),
            source_connector_key=raw.connector_key,
            source_event_type=raw.event_type,
            source_event_id=None,
            details={"official_record": payload, "owner_key": owner_key},
        )
