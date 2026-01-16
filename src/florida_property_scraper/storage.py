import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from florida_property_scraper.schema import normalize_item
from florida_property_scraper.permits.models import PermitRecord


class SQLiteStorage:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self._create_tables()

    def _create_tables(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS owners (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY,
                state TEXT,
                jurisdiction TEXT,
                county TEXT NOT NULL,
                owner_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                land_size TEXT,
                building_size TEXT,
                bedrooms TEXT,
                bathrooms TEXT,
                zoning TEXT,
                property_class TEXT,
                raw_html TEXT,
                UNIQUE(county, owner_id, address),
                FOREIGN KEY(owner_id) REFERENCES owners(id)
            )
            """
        )
        cur.execute("PRAGMA table_info(properties)")
        columns = {row[1] for row in cur.fetchall()}
        if "state" not in columns:
            cur.execute("ALTER TABLE properties ADD COLUMN state TEXT")
        if "jurisdiction" not in columns:
            cur.execute("ALTER TABLE properties ADD COLUMN jurisdiction TEXT")
        # Add index to speed up lookup by county+address
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_properties_county_address ON properties(county, address)"
        )
        self.conn.commit()

    def save_items(self, items):
        cur = self.conn.cursor()
        for item in items:
            normalized = normalize_item(item)
            owner_name = normalized.get("owner", "")
            cur.execute(
                "INSERT OR IGNORE INTO owners (name) VALUES (?)",
                (owner_name,),
            )
            cur.execute("SELECT id FROM owners WHERE name = ?", (owner_name,))
            row = cur.fetchone()
            if not row:
                continue
            owner_id = row[0]
            cur.execute(
                """
                INSERT OR IGNORE INTO properties (
                    state,
                    jurisdiction,
                    county,
                    owner_id,
                    address,
                    land_size,
                    building_size,
                    bedrooms,
                    bathrooms,
                    zoning,
                    property_class,
                    raw_html
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized.get("state", "fl"),
                    normalized.get("jurisdiction", normalized.get("county", "")),
                    normalized.get("county", ""),
                    owner_id,
                    normalized.get("address", ""),
                    normalized.get("land_size", ""),
                    normalized.get("building_size", ""),
                    normalized.get("bedrooms", ""),
                    normalized.get("bathrooms", ""),
                    normalized.get("zoning", ""),
                    normalized.get("property_class", ""),
                    normalized.get("raw_html", ""),
                ),
            )
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None


class SQLiteStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT UNIQUE,
                county TEXT,
                search_query TEXT,
                owner_name TEXT,
                contact_phones TEXT,
                contact_emails TEXT,
                contact_addresses TEXT,
                mailing_address TEXT,
                situs_address TEXT,
                parcel_id TEXT,
                property_url TEXT,
                source_url TEXT,
                mortgage TEXT,
                purchase_history TEXT,
                zoning_current TEXT,
                zoning_future TEXT,
                lead_score INTEGER,
                captured_at TEXT,
                raw_json TEXT
            )
            """
        )
        # Indexes to make lookups by county+parcel and county+address fast
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(leads)")
        lead_cols = {r[1] for r in cur.fetchall()}
        if "parcel_id" in lead_cols:
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leads_county_parcel ON leads(county, parcel_id)"
            )
        if "situs_address" in lead_cols:
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leads_county_situs ON leads(county, situs_address)"
            )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                run_type TEXT DEFAULT 'manual',
                counties_json TEXT,
                query TEXT,
                items_count INTEGER,
                warnings_json TEXT,
                errors_json TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_uid TEXT NOT NULL,
                county TEXT NOT NULL,
                parcel_id TEXT,
                situs_address TEXT,
                owner_name TEXT,
                mailing_address TEXT,
                last_sale_date TEXT,
                last_sale_price REAL,
                deed_type TEXT,
                source_url TEXT,
                raw_json TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                run_id TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_observations_property_time ON observations(property_uid, observed_at DESC)"
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_uid TEXT NOT NULL,
                county TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_at TEXT NOT NULL,
                run_id TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                details_json TEXT
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, event_at DESC)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_property_time ON events(property_uid, event_at DESC)"
        )

        # Permits (joinable by county + parcel_id when available)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS permits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT NOT NULL,
                parcel_id TEXT,
                address TEXT,
                permit_number TEXT NOT NULL,
                permit_type TEXT,
                status TEXT,
                issue_date TEXT,
                final_date TEXT,
                description TEXT,
                source TEXT,
                raw TEXT
            )
            """
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_permits_county_permit_number ON permits(county, permit_number)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_permits_county_parcel_id ON permits(county, parcel_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_permits_issue_date ON permits(issue_date)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_permits_final_date ON permits(final_date)"
        )

        # Trigger engine tables (separate from polygon search)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trigger_raw_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                connector_key TEXT NOT NULL,
                county TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trigger_raw_county_parcel_time ON trigger_raw_events(county, parcel_id, observed_at DESC)"
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_trigger_raw_dedupe ON trigger_raw_events(connector_key, county, parcel_id, observed_at, event_type)"
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trigger_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                county TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                trigger_key TEXT NOT NULL,
                trigger_at TEXT NOT NULL,
                severity INTEGER NOT NULL,
                source_connector_key TEXT NOT NULL,
                source_event_type TEXT NOT NULL,
                source_event_id INTEGER,
                details_json TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trigger_events_county_parcel_time ON trigger_events(county, parcel_id, trigger_at DESC)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trigger_events_key_time ON trigger_events(trigger_key, trigger_at DESC)"
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trigger_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                alert_key TEXT NOT NULL,
                severity INTEGER NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                status TEXT NOT NULL,
                trigger_event_ids_json TEXT NOT NULL,
                details_json TEXT NOT NULL,
                UNIQUE(county, parcel_id, alert_key)
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trigger_alerts_county_parcel_status_time ON trigger_alerts(county, parcel_id, status, last_seen_at DESC)"
        )
        self.conn.commit()

    def upsert_many_permits(self, records: List[PermitRecord]) -> None:
        if not records:
            return

        payload = [
            (
                r.county,
                r.parcel_id,
                r.address,
                r.permit_number,
                r.permit_type,
                r.status,
                r.issue_date,
                r.final_date,
                r.description,
                r.source,
                r.raw,
            )
            for r in records
        ]

        self.conn.executemany(
            """
            INSERT INTO permits (
                county,
                parcel_id,
                address,
                permit_number,
                permit_type,
                status,
                issue_date,
                final_date,
                description,
                source,
                raw
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(county, permit_number) DO UPDATE SET
                parcel_id=excluded.parcel_id,
                address=excluded.address,
                permit_type=excluded.permit_type,
                status=excluded.status,
                issue_date=excluded.issue_date,
                final_date=excluded.final_date,
                description=excluded.description,
                source=excluded.source,
                raw=excluded.raw
            """,
            payload,
        )
        self.conn.commit()

    def list_permits_for_parcel(
        self,
        *,
        county: str,
        parcel_id: str,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        county_key = (county or "").strip().lower()
        pid = (parcel_id or "").strip()
        try:
            lim = int(limit)
        except Exception:
            lim = 200
        lim = max(1, min(lim, 500))

        if not county_key or not pid:
            return []

        rows = self.conn.execute(
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
            WHERE county=? AND parcel_id=?
            ORDER BY COALESCE(issue_date, final_date) DESC
            LIMIT ?
            """,
            (county_key, pid, lim),
        ).fetchall()
        return [dict(r) for r in rows]

    def record_run_start(
        self,
        run_id: str,
        started_at: str,
        run_type: str,
        counties: Optional[List[str]],
        query: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO runs (
                run_id,
                started_at,
                status,
                run_type,
                counties_json,
                query,
                items_count,
                warnings_json,
                errors_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                started_at=excluded.started_at,
                status=excluded.status,
                run_type=excluded.run_type,
                counties_json=excluded.counties_json,
                query=excluded.query
            """,
            (
                run_id,
                started_at,
                "started",
                run_type,
                json.dumps(counties or [], ensure_ascii=True),
                query,
                0,
                json.dumps([], ensure_ascii=True),
                json.dumps([], ensure_ascii=True),
            ),
        )
        self.conn.commit()

    def record_run_finish(
        self,
        run_id: str,
        finished_at: str,
        status: str,
        items_count: int,
        warnings: List[str],
        errors: List[str],
    ) -> None:
        self.conn.execute(
            """
            UPDATE runs
            SET finished_at = ?,
                status = ?,
                items_count = ?,
                warnings_json = ?,
                errors_json = ?
            WHERE run_id = ?
            """,
            (
                finished_at,
                status,
                items_count,
                json.dumps(warnings, ensure_ascii=True),
                json.dumps(errors, ensure_ascii=True),
                run_id,
            ),
        )
        self.conn.commit()

    def upsert_lead(self, record: Dict[str, Any]) -> None:
        payload = (
            record.get("dedupe_key"),
            record.get("county"),
            record.get("search_query"),
            record.get("owner_name"),
            json.dumps(record.get("contact_phones", []), ensure_ascii=True),
            json.dumps(record.get("contact_emails", []), ensure_ascii=True),
            json.dumps(record.get("contact_addresses", []), ensure_ascii=True),
            record.get("mailing_address"),
            record.get("situs_address"),
            record.get("parcel_id"),
            record.get("property_url"),
            record.get("source_url"),
            json.dumps(record.get("mortgage", []), ensure_ascii=True),
            json.dumps(record.get("purchase_history", []), ensure_ascii=True),
            record.get("zoning_current"),
            record.get("zoning_future"),
            record.get("lead_score"),
            record.get("captured_at"),
            json.dumps(record, ensure_ascii=True),
        )
        self.conn.execute(
            """
            INSERT INTO leads (
                dedupe_key,
                county,
                search_query,
                owner_name,
                contact_phones,
                contact_emails,
                contact_addresses,
                mailing_address,
                situs_address,
                parcel_id,
                property_url,
                source_url,
                mortgage,
                purchase_history,
                zoning_current,
                zoning_future,
                lead_score,
                captured_at,
                raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dedupe_key) DO UPDATE SET
                county=excluded.county,
                search_query=excluded.search_query,
                owner_name=excluded.owner_name,
                contact_phones=excluded.contact_phones,
                contact_emails=excluded.contact_emails,
                contact_addresses=excluded.contact_addresses,
                mailing_address=excluded.mailing_address,
                situs_address=excluded.situs_address,
                parcel_id=excluded.parcel_id,
                property_url=excluded.property_url,
                source_url=excluded.source_url,
                mortgage=excluded.mortgage,
                purchase_history=excluded.purchase_history,
                zoning_current=excluded.zoning_current,
                zoning_future=excluded.zoning_future,
                lead_score=excluded.lead_score,
                captured_at=excluded.captured_at,
                raw_json=excluded.raw_json
            """,
            payload,
        )
        self.conn.commit()

    def get_latest_observation(self, property_uid: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT * FROM observations
            WHERE property_uid = ?
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (property_uid,),
        ).fetchone()
        return dict(row) if row else None

    def insert_observation(self, record: Dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO observations (
                property_uid,
                county,
                parcel_id,
                situs_address,
                owner_name,
                mailing_address,
                last_sale_date,
                last_sale_price,
                deed_type,
                source_url,
                raw_json,
                observed_at,
                run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.get("property_uid"),
                record.get("county"),
                record.get("parcel_id"),
                record.get("situs_address"),
                record.get("owner_name"),
                record.get("mailing_address"),
                record.get("last_sale_date"),
                record.get("last_sale_price"),
                record.get("deed_type"),
                record.get("source_url"),
                record.get("raw_json"),
                record.get("observed_at"),
                record.get("run_id"),
            ),
        )
        self.conn.commit()

    def insert_events(self, events: List[Dict[str, Any]]) -> None:
        if not events:
            return
        self.conn.executemany(
            """
            INSERT INTO events (
                property_uid,
                county,
                event_type,
                event_at,
                run_id,
                old_value,
                new_value,
                details_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    event.get("property_uid"),
                    event.get("county"),
                    event.get("event_type"),
                    event.get("event_at"),
                    event.get("run_id"),
                    event.get("old_value"),
                    event.get("new_value"),
                    event.get("details_json"),
                )
                for event in events
            ],
        )
        self.conn.commit()

    def insert_trigger_raw_events(self, *, raw_events: List[Any], run_id: str) -> List[int]:
        if not raw_events:
            return []
        ids: List[int] = []
        for ev in raw_events:
            connector_key = getattr(ev, "connector_key", None) or (ev.get("connector_key") if isinstance(ev, dict) else None)
            county = getattr(ev, "county", None) or (ev.get("county") if isinstance(ev, dict) else None)
            parcel_id = getattr(ev, "parcel_id", None) or (ev.get("parcel_id") if isinstance(ev, dict) else None)
            observed_at = getattr(ev, "observed_at", None) or (ev.get("observed_at") if isinstance(ev, dict) else None)
            event_type = getattr(ev, "event_type", None) or (ev.get("event_type") if isinstance(ev, dict) else None)
            payload_json = None
            if hasattr(ev, "payload_json"):
                payload_json = ev.payload_json()
            elif isinstance(ev, dict):
                payload_json = json.dumps(ev.get("payload") or {}, ensure_ascii=True)
            if payload_json is None:
                payload_json = "{}"

            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO trigger_raw_events (
                    run_id,
                    connector_key,
                    county,
                    parcel_id,
                    observed_at,
                    event_type,
                    payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    str(connector_key or ""),
                    str(county or ""),
                    str(parcel_id or ""),
                    str(observed_at or ""),
                    str(event_type or ""),
                    str(payload_json or "{}"),
                ),
            )
            rid = int(cur.lastrowid or 0)
            if rid == 0:
                # Dedupe hit; fetch the existing id so callers can link back.
                row = self.conn.execute(
                    """
                    SELECT id FROM trigger_raw_events
                    WHERE connector_key=? AND county=? AND parcel_id=? AND observed_at=? AND event_type=?
                    LIMIT 1
                    """,
                    (
                        str(connector_key or ""),
                        str(county or ""),
                        str(parcel_id or ""),
                        str(observed_at or ""),
                        str(event_type or ""),
                    ),
                ).fetchone()
                rid = int(row["id"]) if row else 0
            ids.append(rid)
        self.conn.commit()
        return ids

    def insert_trigger_events(self, *, trigger_events: List[Any], run_id: str) -> List[int]:
        if not trigger_events:
            return []
        ids: List[int] = []
        for te in trigger_events:
            county = getattr(te, "county", None) or (te.get("county") if isinstance(te, dict) else None)
            parcel_id = getattr(te, "parcel_id", None) or (te.get("parcel_id") if isinstance(te, dict) else None)
            trigger_key = getattr(te, "trigger_key", None) or (te.get("trigger_key") if isinstance(te, dict) else None)
            trigger_at = getattr(te, "trigger_at", None) or (te.get("trigger_at") if isinstance(te, dict) else None)
            severity = getattr(te, "severity", None) or (te.get("severity") if isinstance(te, dict) else None)
            source_connector_key = getattr(te, "source_connector_key", None) or (
                te.get("source_connector_key") if isinstance(te, dict) else None
            )
            source_event_type = getattr(te, "source_event_type", None) or (
                te.get("source_event_type") if isinstance(te, dict) else None
            )
            source_event_id = getattr(te, "source_event_id", None) or (te.get("source_event_id") if isinstance(te, dict) else None)
            details_json = None
            if hasattr(te, "details_json"):
                details_json = te.details_json()
            elif isinstance(te, dict):
                details_json = json.dumps(te.get("details") or {}, ensure_ascii=True)
            if details_json is None:
                details_json = "{}"

            cur = self.conn.execute(
                """
                INSERT INTO trigger_events (
                    run_id,
                    county,
                    parcel_id,
                    trigger_key,
                    trigger_at,
                    severity,
                    source_connector_key,
                    source_event_type,
                    source_event_id,
                    details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    str(county or ""),
                    str(parcel_id or ""),
                    str(trigger_key or ""),
                    str(trigger_at or ""),
                    int(severity or 1),
                    str(source_connector_key or ""),
                    str(source_event_type or ""),
                    int(source_event_id) if source_event_id is not None else None,
                    str(details_json or "{}"),
                ),
            )
            ids.append(int(cur.lastrowid))
        self.conn.commit()
        return ids

    def list_trigger_events_for_parcel(
        self,
        *,
        county: str,
        parcel_id: str,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        county_key = (county or "").strip().lower()
        pid = (parcel_id or "").strip()
        try:
            lim = int(limit)
        except Exception:
            lim = 200
        lim = max(1, min(lim, 500))
        if not county_key or not pid:
            return []
        rows = self.conn.execute(
            """
            SELECT
                id,
                county,
                parcel_id,
                trigger_key,
                trigger_at,
                severity,
                source_connector_key,
                source_event_type,
                source_event_id,
                details_json
            FROM trigger_events
            WHERE county=? AND parcel_id=?
            ORDER BY trigger_at DESC, id DESC
            LIMIT ?
            """,
            (county_key, pid, lim),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_trigger_events_for_county(
        self,
        *,
        county: str,
        since_iso: str,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        county_key = (county or "").strip().lower()
        since = (since_iso or "").strip()
        try:
            lim = int(limit)
        except Exception:
            lim = 5000
        lim = max(1, min(lim, 20000))
        if not county_key or not since:
            return []
        rows = self.conn.execute(
            """
            SELECT
                id,
                county,
                parcel_id,
                trigger_key,
                trigger_at,
                severity
            FROM trigger_events
            WHERE county=? AND trigger_at >= ?
            ORDER BY trigger_at DESC, id DESC
            LIMIT ?
            """,
            (county_key, since, lim),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_trigger_alerts_for_parcel(
        self,
        *,
        county: str,
        parcel_id: str,
        status: str = "open",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        county_key = (county or "").strip().lower()
        pid = (parcel_id or "").strip()
        st = (status or "").strip().lower()
        try:
            lim = int(limit)
        except Exception:
            lim = 50
        lim = max(1, min(lim, 200))
        if not county_key or not pid:
            return []
        rows = self.conn.execute(
            """
            SELECT
                id,
                county,
                parcel_id,
                alert_key,
                severity,
                first_seen_at,
                last_seen_at,
                status,
                trigger_event_ids_json,
                details_json
            FROM trigger_alerts
            WHERE county=? AND parcel_id=? AND status=?
            ORDER BY last_seen_at DESC, id DESC
            LIMIT ?
            """,
            (county_key, pid, st, lim),
        ).fetchall()
        return [dict(r) for r in rows]

    def upsert_trigger_alert(
        self,
        *,
        county: str,
        parcel_id: str,
        alert_key: str,
        severity: int,
        first_seen_at: str,
        last_seen_at: str,
        status: str,
        trigger_event_ids: List[int],
        details: Dict[str, Any],
    ) -> bool:
        county_key = (county or "").strip().lower()
        pid = (parcel_id or "").strip()
        akey = (alert_key or "").strip()
        if not county_key or not pid or not akey:
            return False

        row = self.conn.execute(
            """
            SELECT
                id,
                first_seen_at,
                trigger_event_ids_json,
                severity
            FROM trigger_alerts
            WHERE county=? AND parcel_id=? AND alert_key=?
            LIMIT 1
            """,
            (county_key, pid, akey),
        ).fetchone()

        existing_ids: List[int] = []
        existing_first = None
        existing_sev = None
        if row:
            existing_first = row["first_seen_at"]
            existing_sev = row["severity"]
            try:
                existing_ids = json.loads(row["trigger_event_ids_json"] or "[]")
                if not isinstance(existing_ids, list):
                    existing_ids = []
            except Exception:
                existing_ids = []

        merged_ids = sorted(set([int(x) for x in existing_ids] + [int(x) for x in (trigger_event_ids or [])]))
        if len(merged_ids) > 200:
            merged_ids = merged_ids[:200]

        out_first = existing_first or first_seen_at
        out_sev = int(max(int(existing_sev or 0), int(severity or 1)))
        self.conn.execute(
            """
            INSERT INTO trigger_alerts (
                county,
                parcel_id,
                alert_key,
                severity,
                first_seen_at,
                last_seen_at,
                status,
                trigger_event_ids_json,
                details_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(county, parcel_id, alert_key) DO UPDATE SET
                severity=excluded.severity,
                last_seen_at=excluded.last_seen_at,
                status=excluded.status,
                trigger_event_ids_json=excluded.trigger_event_ids_json,
                details_json=excluded.details_json
            """,
            (
                county_key,
                pid,
                akey,
                out_sev,
                out_first,
                str(last_seen_at or out_first),
                str(status or "open"),
                json.dumps(merged_ids, ensure_ascii=True),
                json.dumps(details or {}, ensure_ascii=True),
            ),
        )
        self.conn.commit()
        return True

    def close(self) -> None:
        self.conn.close()
