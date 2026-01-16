from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone

from florida_property_scraper.storage import SQLiteStore

from .base import TriggerConnector, register_connector
from ..models import RawEvent, TriggerEvent
from ..taxonomy import TriggerKey, default_severity_for_trigger


def _permit_text(payload: dict) -> str:
    permit_type = str(payload.get("permit_type") or "")
    description = str(payload.get("description") or "")
    status = str(payload.get("status") or "")
    return f"{permit_type} {description} {status}".strip().lower()


def _classify_permit_trigger_key(payload: dict) -> TriggerKey:
    """Best-effort categorization from free-text permit fields.

    If no category matches, fall back to generic PERMIT_ISSUED.
    """

    t = _permit_text(payload)
    if not t:
        return TriggerKey.PERMIT_ISSUED

    # Strong signals
    if re.search(r"\bdemo(lition)?\b|\btear\s*down\b|\bdeconstruction\b", t):
        return TriggerKey.PERMIT_DEMOLITION
    if re.search(
        r"\baddition\b|\bnew\s*construct(ion)?\b|\bbuild(ing)?\b|\bremodel\b|\brenovat(e|ion)\b|\bfoundation\b|\bframing\b",
        t,
    ):
        return TriggerKey.PERMIT_STRUCTURAL
    if re.search(r"\broof\b|\bre-?roof\b|\bshingle\b|\bmetal\s+roof\b|\btile\s+roof\b", t):
        return TriggerKey.PERMIT_ROOF
    if re.search(
        r"\bhvac\b|\bair\s*cond(ition(ing)?)?\b|\bac\s*(unit|system)?\b|\bheat\s*pump\b|\bfurnace\b|\bduct\b",
        t,
    ):
        return TriggerKey.PERMIT_HVAC
    if re.search(r"\belectric(al)?\b|\bpanel\b|\brewire\b|\bwiring\b|\bservice\s*upgrade\b|\bgenerator\b", t):
        return TriggerKey.PERMIT_ELECTRICAL
    if re.search(r"\bplumb(ing)?\b|\brepipe\b|\bwater\s*heater\b|\bsewer\b|\bdrain\b", t):
        return TriggerKey.PERMIT_PLUMBING

    # Support signals
    if re.search(r"\bwindow(s)?\b|\bimpact\s+window(s)?\b", t):
        return TriggerKey.PERMIT_WINDOWS
    if re.search(r"\bdoor(s)?\b|\bgarage\s*door\b", t):
        return TriggerKey.PERMIT_DOORS
    if re.search(r"\bsolar\b|\bphotovoltaic\b|\bpv\b", t):
        return TriggerKey.PERMIT_SOLAR

    return TriggerKey.PERMIT_ISSUED


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
class PermitsDbConnector(TriggerConnector):
    """Reads the existing `permits` table and emits trigger events.

    This connector is offline: it does not scrape. It converts already-ingested permits
    (e.g., from `/api/permits/sync`) into normalized triggers/alerts.
    """

    connector_key = "permits_db"

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
            # Watermark = latest observed_at we have already processed for this connector+county.
            row = store.conn.execute(
                """
                SELECT MAX(observed_at) AS max_observed
                FROM trigger_raw_events
                WHERE connector_key=? AND county=?
                """,
                (self.connector_key, county_key),
            ).fetchone()
            watermark_iso = (row["max_observed"] if row else None) or ""

            # Default lookback if no watermark.
            if watermark_iso:
                since_d = _now_date(watermark_iso)
            else:
                since_d = _now_date(now_iso) - timedelta(days=30)

            rows = store.conn.execute(
                """
                SELECT
                    county,
                    parcel_id,
                    address,
                    permit_number,
                    permit_type,
                    status,
                    issue_date,
                    final_date,
                    description,
                    source
                FROM permits
                WHERE county=?
                ORDER BY COALESCE(issue_date, final_date) DESC
                LIMIT ?
                """,
                (county_key, lim),
            ).fetchall()

            out: list[RawEvent] = []
            for r in rows:
                # Try to derive a stable event timestamp from the permit dates.
                d = _parse_date_any((r["issue_date"] or "").strip())
                if d is None:
                    d = _parse_date_any((r["final_date"] or "").strip())
                if d is None:
                    continue
                if d < since_d:
                    continue

                observed_at = _iso_midnight_utc(d)
                out.append(
                    RawEvent(
                        connector_key=self.connector_key,
                        county=county_key,
                        parcel_id=(r["parcel_id"] or "").strip() or (r["permit_number"] or "").strip(),
                        observed_at=observed_at,
                        event_type="permits_db.permit",
                        payload={
                            "permit_number": r["permit_number"],
                            "permit_type": r["permit_type"],
                            "status": r["status"],
                            "issue_date": r["issue_date"],
                            "final_date": r["final_date"],
                            "description": r["description"],
                            "source": r["source"],
                            "address": r["address"],
                        },
                    )
                )

            return out
        finally:
            store.close()

    def normalize(self, raw: RawEvent, *, now_iso: str) -> TriggerEvent | None:
        if (raw.event_type or "").strip().lower() != "permits_db.permit":
            return None

        trigger_key = _classify_permit_trigger_key(raw.payload or {})
        severity = default_severity_for_trigger(trigger_key)
        return TriggerEvent(
            county=raw.county,
            parcel_id=raw.parcel_id,
            trigger_key=str(trigger_key),
            trigger_at=raw.observed_at,
            severity=int(severity),
            source_connector_key=raw.connector_key,
            source_event_type=raw.event_type,
            source_event_id=None,
            details={"permit": raw.payload},
        )
