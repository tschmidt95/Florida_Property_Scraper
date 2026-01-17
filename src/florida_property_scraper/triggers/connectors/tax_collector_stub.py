from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from florida_property_scraper.storage import SQLiteStore

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..normalization.tax_collector import normalize_tax_collector_trigger_key
from ..taxonomy import default_severity_for_trigger


def _now_date(now_iso: str) -> datetime:
    try:
        dt = datetime.fromisoformat((now_iso or "").replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.astimezone(timezone.utc)


@register_connector
class TaxCollectorStubConnector(TriggerConnector):
    """Offline stub connector reading from `tax_collector_events`.

    This connector is intentionally offline-first: it only reads from SQLite.
    """

    connector_key = "tax_collector_stub"

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

            # If we have a watermark, use a small lookback; otherwise 60 days.
            if watermark_iso:
                since_dt = _now_date(watermark_iso) - timedelta(days=3)
            else:
                since_dt = _now_date(now_iso) - timedelta(days=60)
            since_iso = since_dt.replace(microsecond=0).isoformat()

            rows = store.conn.execute(
                """
                SELECT
                    county,
                    parcel_id,
                    observed_at,
                    event_type,
                    event_date,
                    amount_due,
                    status,
                    description,
                    source,
                    raw_json
                FROM tax_collector_events
                WHERE county=? AND observed_at>=?
                ORDER BY observed_at DESC
                LIMIT ?
                """,
                (county_key, since_iso, lim),
            ).fetchall()

            out: list[RawEvent] = []
            for r in rows:
                pid = str(r["parcel_id"] or "").strip()
                observed_at = str(r["observed_at"] or "").strip()
                et = str(r["event_type"] or "").strip()
                if not pid or not observed_at or not et:
                    continue

                payload = {
                    "event_date": r["event_date"],
                    "amount_due": r["amount_due"],
                    "status": r["status"],
                    "description": r["description"],
                    "source": r["source"],
                    "raw_json": r["raw_json"],
                }

                out.append(
                    RawEvent(
                        connector_key=self.connector_key,
                        county=county_key,
                        parcel_id=pid,
                        observed_at=observed_at,
                        event_type=et,
                        payload=payload,
                    )
                )

            return out
        finally:
            store.close()

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        payload = raw.payload or {}

        trigger_key = normalize_tax_collector_trigger_key(
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
            details={"tax_collector": payload},
        )
