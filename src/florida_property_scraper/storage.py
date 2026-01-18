import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
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

        # Official records (offline stub table; future recorder connectors can populate)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS official_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT NOT NULL,
                parcel_id TEXT,
                join_key TEXT,
                doc_type TEXT,
                rec_date TEXT,
                parties TEXT,
                book_page_or_instrument TEXT,
                consideration TEXT,
                raw_text TEXT,
                owner_name TEXT,
                address TEXT,
                source TEXT,
                raw TEXT
            )
            """
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_official_records_county_instrument_doc_date ON official_records(county, book_page_or_instrument, doc_type, rec_date)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_official_records_county_parcel_id ON official_records(county, parcel_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_official_records_county_join_key ON official_records(county, join_key)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_official_records_rec_date ON official_records(rec_date)"
        )

        # Tax collector (offline stub table)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tax_collector_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_date TEXT,
                amount_due REAL,
                status TEXT,
                description TEXT,
                source TEXT,
                raw_json TEXT
            )
            """
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_tax_collector_dedupe ON tax_collector_events(county, parcel_id, observed_at, event_type)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tax_collector_county_parcel_time ON tax_collector_events(county, parcel_id, observed_at DESC)"
        )

        # Code enforcement (offline stub table)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS code_enforcement_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_date TEXT,
                case_number TEXT,
                status TEXT,
                description TEXT,
                fine_amount REAL,
                lien_amount REAL,
                source TEXT,
                raw_json TEXT
            )
            """
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_code_enf_dedupe ON code_enforcement_events(county, parcel_id, observed_at, event_type)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_code_enf_county_parcel_time ON code_enforcement_events(county, parcel_id, observed_at DESC)"
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

        # Precomputed trigger rollups (offline rebuild job; queried by UI filters)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS parcel_trigger_rollups (
                county TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                rebuilt_at TEXT NOT NULL,
                last_seen_any TEXT,
                last_seen_permits TEXT,
                last_seen_tax TEXT,
                last_seen_official_records TEXT,
                last_seen_code_enforcement TEXT,
                last_seen_courts TEXT,
                last_seen_gis_planning TEXT,
                has_permits INTEGER NOT NULL DEFAULT 0,
                has_tax INTEGER NOT NULL DEFAULT 0,
                has_official_records INTEGER NOT NULL DEFAULT 0,
                has_code_enforcement INTEGER NOT NULL DEFAULT 0,
                has_courts INTEGER NOT NULL DEFAULT 0,
                has_gis_planning INTEGER NOT NULL DEFAULT 0,
                count_critical INTEGER NOT NULL DEFAULT 0,
                count_strong INTEGER NOT NULL DEFAULT 0,
                count_support INTEGER NOT NULL DEFAULT 0,
                seller_score INTEGER NOT NULL DEFAULT 0,
                details_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY(county, parcel_id)
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rollups_county_score ON parcel_trigger_rollups(county, seller_score DESC)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rollups_county_flags ON parcel_trigger_rollups(county, has_permits, has_official_records, has_tax, has_code_enforcement, has_courts, has_gis_planning)"
        )

        # Saved searches + watchlists + inbox alerts (offline-first; does not require external services)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlists (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                county TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                is_enabled INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchlists_county_enabled ON watchlists(county, is_enabled)"
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_searches (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                county TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'polygon',
                watchlist_id TEXT,
                polygon_geojson_json TEXT NOT NULL,
                bbox_json TEXT,
                filters_json TEXT NOT NULL DEFAULT '{}',
                enrich INTEGER NOT NULL DEFAULT 0,
                sort TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_run_at TEXT,
                is_enabled INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_saved_searches_county_enabled ON saved_searches(county, is_enabled)"
        )

        # Saved-search membership is tracked separately so it is idempotent and diffable.
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_search_members (
                saved_search_id TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                county TEXT NOT NULL,
                added_at TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'manual',
                removed_at TEXT,
                last_seen_at TEXT,
                PRIMARY KEY(saved_search_id, parcel_id)
            )
            """
        )
        # Backfill/migrate: ensure 'source' exists for older DBs.
        cur.execute("PRAGMA table_info(saved_search_members)")
        ssm_cols = {r[1] for r in cur.fetchall()}
        if "source" not in ssm_cols:
            self.conn.execute("ALTER TABLE saved_search_members ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_saved_search_members_search_active ON saved_search_members(saved_search_id, removed_at, last_seen_at DESC)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_saved_search_members_county_parcel ON saved_search_members(county, parcel_id)"
        )

        # Alert rules drive notifications and how alerts are generated from rollups/triggers.
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_rules (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                severity_threshold INTEGER NOT NULL DEFAULT 1,
                criteria_json TEXT NOT NULL DEFAULT '{}',
                notify_email TEXT,
                notify_webhook_url TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled ON alert_rules(enabled, updated_at DESC)"
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist_members (
                watchlist_id TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                county TEXT NOT NULL,
                added_at TEXT NOT NULL,
                source TEXT NOT NULL,
                last_seen_at TEXT,
                PRIMARY KEY(watchlist_id, parcel_id)
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchlist_members_county_parcel ON watchlist_members(county, parcel_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchlist_members_watchlist_seen ON watchlist_members(watchlist_id, last_seen_at DESC)"
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist_runs (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                saved_search_id TEXT,
                watchlist_id TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                ok INTEGER NOT NULL DEFAULT 0,
                stats_json TEXT NOT NULL DEFAULT '{}',
                error_text TEXT
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchlist_runs_watchlist_time ON watchlist_runs(watchlist_id, started_at DESC)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchlist_runs_saved_search_time ON watchlist_runs(saved_search_id, started_at DESC)"
        )

        # Migrate legacy alerts_inbox schema (watchlist-based) to alerts_inbox_legacy.
        try:
            cols = {
                str(r["name"])
                for r in self.conn.execute("PRAGMA table_info(alerts_inbox)").fetchall()
            }
        except sqlite3.OperationalError:
            cols = set()
        if cols and ("watchlist_id" in cols) and ("saved_search_id" not in cols):
            self.conn.execute("ALTER TABLE alerts_inbox RENAME TO alerts_inbox_legacy")

        # New inbox schema (saved-search scoped; deterministic + offline-first).
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts_inbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                saved_search_id TEXT,
                parcel_id TEXT NOT NULL,
                county TEXT NOT NULL,
                alert_key TEXT NOT NULL,
                severity INTEGER NOT NULL,
                title TEXT NOT NULL,
                body_json TEXT NOT NULL DEFAULT '{}',
                why_json TEXT NOT NULL DEFAULT '{}',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new'
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_inbox_status_time ON alerts_inbox(status, last_seen_at DESC)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_inbox_search_time ON alerts_inbox(saved_search_id, last_seen_at DESC)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_inbox_county_time2 ON alerts_inbox(county, last_seen_at DESC)"
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_alerts_inbox_dedupe2 ON alerts_inbox(saved_search_id, parcel_id, alert_key)"
        )

        # Idempotent notification delivery ledger. Do not resend unless the alert fingerprint changes.
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_inbox_id INTEGER NOT NULL,
                channel TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                recipient TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                sent_at TEXT,
                error_text TEXT,
                UNIQUE(alert_inbox_id, channel, fingerprint)
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_alert_channel ON alert_deliveries(alert_inbox_id, channel, created_at DESC)"
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                recipient TEXT,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                sent_at TEXT,
                error_text TEXT
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notifications_status_time ON notifications(status, created_at DESC)"
        )

        # SQLite-backed scheduler locks (single-runner enforcement).
        # Heartbeat timestamps are stored as unix seconds for reliable TTL logic.
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_locks (
                lock_name TEXT PRIMARY KEY,
                acquired_at TEXT NOT NULL,
                pid INTEGER NOT NULL,
                heartbeat_at TEXT NOT NULL,
                heartbeat_ts INTEGER NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scheduler_locks_heartbeat ON scheduler_locks(heartbeat_ts)"
        )

        self.conn.commit()

    @staticmethod
    def _iso_to_epoch_seconds(iso: str) -> int:
        raw = (iso or "").strip()
        if not raw:
            return 0
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0

    def acquire_scheduler_lock(
        self,
        *,
        lock_name: str,
        now_iso: str | None = None,
        ttl_seconds: int = 7200,
        pid: int | None = None,
    ) -> Dict[str, Any]:
        """Acquire a named scheduler lock (with TTL-based stale takeover).

        Returns a dict with:
        - ok: bool
        - acquired: bool
        - stolen: bool
        - held_by_pid: int | None
        """

        now = (now_iso or "").strip() or self._utc_now_iso()
        name = (lock_name or "").strip() or "scheduler"
        ttl = max(10, int(ttl_seconds or 0))
        my_pid = int(pid if pid is not None else os.getpid())
        now_ts = int(self._iso_to_epoch_seconds(now) or 0)

        try:
            self.conn.execute("BEGIN IMMEDIATE")
            row = self.conn.execute(
                "SELECT lock_name, pid, heartbeat_ts, heartbeat_at FROM scheduler_locks WHERE lock_name=? LIMIT 1",
                (name,),
            ).fetchone()

            if not row:
                self.conn.execute(
                    """
                    INSERT INTO scheduler_locks(lock_name, acquired_at, pid, heartbeat_at, heartbeat_ts)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (name, now, my_pid, now, now_ts),
                )
                self.conn.commit()
                return {"ok": True, "acquired": True, "stolen": False, "held_by_pid": my_pid}

            held_pid = int(row["pid"] or 0)
            held_ts = int(row["heartbeat_ts"] or 0)
            stale = bool(now_ts and held_ts and (held_ts < (now_ts - ttl)))

            if stale:
                self.conn.execute(
                    """
                    UPDATE scheduler_locks
                    SET acquired_at=?, pid=?, heartbeat_at=?, heartbeat_ts=?
                    WHERE lock_name=?
                    """,
                    (now, my_pid, now, now_ts, name),
                )
                self.conn.commit()
                return {
                    "ok": True,
                    "acquired": True,
                    "stolen": True,
                    "held_by_pid": my_pid,
                    "previous_pid": held_pid,
                }

            self.conn.rollback()
            return {
                "ok": True,
                "acquired": False,
                "stolen": False,
                "held_by_pid": held_pid or None,
                "heartbeat_at": str(row["heartbeat_at"] or "") or None,
            }
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            return {"ok": False, "acquired": False, "error": str(e)}

    def refresh_scheduler_lock(
        self,
        *,
        lock_name: str,
        now_iso: str | None = None,
        pid: int | None = None,
    ) -> bool:
        now = (now_iso or "").strip() or self._utc_now_iso()
        name = (lock_name or "").strip() or "scheduler"
        my_pid = int(pid if pid is not None else os.getpid())
        now_ts = int(self._iso_to_epoch_seconds(now) or 0)
        cur = self.conn.execute(
            """
            UPDATE scheduler_locks
            SET heartbeat_at=?, heartbeat_ts=?
            WHERE lock_name=? AND pid=?
            """,
            (now, now_ts, name, my_pid),
        )
        self.conn.commit()
        return bool(cur.rowcount and int(cur.rowcount) > 0)

    def release_scheduler_lock(self, *, lock_name: str, pid: int | None = None) -> bool:
        name = (lock_name or "").strip() or "scheduler"
        my_pid = int(pid if pid is not None else os.getpid())
        cur = self.conn.execute(
            "DELETE FROM scheduler_locks WHERE lock_name=? AND pid=?",
            (name, my_pid),
        )
        self.conn.commit()
        return bool(cur.rowcount and int(cur.rowcount) > 0)

    def list_active_saved_searches(
        self,
        *,
        counties: list[str] | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        try:
            lim = int(limit)
        except Exception:
            lim = 200
        lim = max(1, min(lim, 5000))

        county_keys = [str(c or "").strip().lower() for c in (counties or [])]
        county_keys = [c for c in county_keys if c]

        sql = "SELECT * FROM saved_searches WHERE is_enabled=1"
        params: list[Any] = []
        if county_keys:
            placeholders = ",".join(["?"] * len(county_keys))
            sql += f" AND county IN ({placeholders})"
            params.extend(county_keys)
        sql += " ORDER BY updated_at DESC, created_at DESC LIMIT ?"
        params.append(lim)
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def _uuid() -> str:
        return uuid.uuid4().hex

    @staticmethod
    def _clean_json(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=True, default=str)

    def create_watchlist(
        self,
        *,
        name: str,
        county: str,
        is_enabled: bool = True,
        now_iso: str | None = None,
    ) -> Dict[str, Any]:
        now = (now_iso or "").strip() or self._utc_now_iso()
        wid = self._uuid()[:12]
        nm = str(name or "").strip() or "Watchlist"
        county_key = (county or "").strip().lower()
        self.conn.execute(
            """
            INSERT INTO watchlists (id, name, county, created_at, updated_at, is_enabled)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (wid, nm, county_key, now, now, 1 if is_enabled else 0),
        )
        self.conn.commit()
        return {
            "id": wid,
            "name": nm,
            "county": county_key,
            "created_at": now,
            "updated_at": now,
            "is_enabled": 1 if is_enabled else 0,
        }

    def list_watchlists(self, *, county: str | None = None) -> List[Dict[str, Any]]:
        where = []
        params: list[Any] = []
        if county:
            where.append("county=?")
            params.append((county or "").strip().lower())
        sql = "SELECT * FROM watchlists"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at DESC, created_at DESC"
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def get_watchlist(self, *, watchlist_id: str) -> Dict[str, Any] | None:
        wid = str(watchlist_id or "").strip()
        if not wid:
            return None
        r = self.conn.execute("SELECT * FROM watchlists WHERE id=? LIMIT 1", (wid,)).fetchone()
        return dict(r) if r else None

    def add_parcel_to_watchlist(
        self,
        *,
        watchlist_id: str,
        county: str,
        parcel_id: str,
        source: str = "manual",
        now_iso: str | None = None,
    ) -> bool:
        now = (now_iso or "").strip() or self._utc_now_iso()
        wid = str(watchlist_id or "").strip()
        pid = str(parcel_id or "").strip()
        county_key = (county or "").strip().lower()
        src = str(source or "manual").strip().lower() or "manual"
        if not wid or not pid or not county_key:
            return False

        self.conn.execute(
            """
            INSERT INTO watchlist_members (watchlist_id, parcel_id, county, added_at, source, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(watchlist_id, parcel_id) DO UPDATE SET
                county=excluded.county,
                last_seen_at=excluded.last_seen_at
            """,
            (wid, pid, county_key, now, src, now),
        )
        self.conn.execute(
            "UPDATE watchlists SET updated_at=? WHERE id=?",
            (now, wid),
        )
        self.conn.commit()
        return True

    def list_watchlist_members(
        self,
        *,
        watchlist_id: str,
        limit: int = 2000,
    ) -> List[Dict[str, Any]]:
        wid = str(watchlist_id or "").strip()
        if not wid:
            return []
        try:
            lim = int(limit)
        except Exception:
            lim = 2000
        lim = max(1, min(lim, 5000))
        rows = self.conn.execute(
            """
            SELECT * FROM watchlist_members
            WHERE watchlist_id=?
            ORDER BY (last_seen_at IS NULL) ASC, last_seen_at DESC, added_at DESC
            LIMIT ?
            """,
            (wid, lim),
        ).fetchall()
        return [dict(r) for r in rows]

    def create_saved_search(
        self,
        *,
        name: str,
        county: str,
        polygon_geojson: Dict[str, Any],
        filters: Dict[str, Any] | None = None,
        enrich: bool = False,
        sort: str | None = None,
        watchlist_id: str | None = None,
        is_enabled: bool = True,
        now_iso: str | None = None,
    ) -> Dict[str, Any]:
        now = (now_iso or "").strip() or self._utc_now_iso()
        sid = self._uuid()[:12]
        nm = str(name or "").strip() or "Saved Search"
        county_key = (county or "").strip().lower()

        wl_id = (watchlist_id or "").strip() or None
        if wl_id is None:
            wl = self.create_watchlist(name=nm, county=county_key, now_iso=now)
            wl_id = str(wl.get("id") or "").strip() or None

        # Normalize filters: drop blanks so "optional" filters never restrict when unset.
        f_in = filters or {}
        f_out: Dict[str, Any] = {}
        if isinstance(f_in, dict):
            for k, v in f_in.items():
                if v is None:
                    continue
                if isinstance(v, str) and not v.strip():
                    continue
                if isinstance(v, list) and len(v) == 0:
                    continue
                f_out[str(k)] = v

        self.conn.execute(
            """
            INSERT INTO saved_searches (
                id, name, county, watchlist_id, polygon_geojson_json, filters_json, enrich, sort,
                created_at, updated_at, last_run_at, is_enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sid,
                nm,
                county_key,
                wl_id,
                self._clean_json(polygon_geojson or {}),
                self._clean_json(f_out),
                1 if bool(enrich) else 0,
                str(sort or ""),
                now,
                now,
                None,
                1 if is_enabled else 0,
            ),
        )
        self.conn.commit()
        return self.get_saved_search(saved_search_id=sid) or {"id": sid}

    def list_saved_searches(self, *, county: str | None = None) -> List[Dict[str, Any]]:
        where = []
        params: list[Any] = []
        if county:
            where.append("county=?")
            params.append((county or "").strip().lower())
        sql = "SELECT * FROM saved_searches"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at DESC, created_at DESC"
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def get_saved_search(self, *, saved_search_id: str) -> Dict[str, Any] | None:
        sid = str(saved_search_id or "").strip()
        if not sid:
            return None
        r = self.conn.execute("SELECT * FROM saved_searches WHERE id=? LIMIT 1", (sid,)).fetchone()
        return dict(r) if r else None

    def add_member_to_saved_search(
        self,
        *,
        saved_search_id: str,
        county: str,
        parcel_id: str,
        source: str = "manual",
        now_iso: str | None = None,
    ) -> bool:
        now = (now_iso or "").strip() or self._utc_now_iso()
        sid = str(saved_search_id or "").strip()
        pid = str(parcel_id or "").strip()
        county_key = str(county or "").strip().lower()
        src = str(source or "manual").strip().lower() or "manual"
        if not sid or not pid or not county_key:
            return False
        if not self.get_saved_search(saved_search_id=sid):
            return False
        self.conn.execute(
            """
            INSERT INTO saved_search_members (saved_search_id, parcel_id, county, added_at, source, removed_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, NULL, ?)
            ON CONFLICT(saved_search_id, parcel_id) DO UPDATE SET
                county=excluded.county,
                removed_at=NULL,
                last_seen_at=excluded.last_seen_at,
                source=excluded.source
            """,
            (sid, pid, county_key, now, src, now),
        )
        self.conn.commit()
        return True

    def list_saved_search_members(
        self,
        *,
        saved_search_id: str,
        active_only: bool = True,
        limit: int = 2000,
    ) -> List[Dict[str, Any]]:
        sid = str(saved_search_id or "").strip()
        if not sid:
            return []
        try:
            lim = int(limit)
        except Exception:
            lim = 2000
        lim = max(1, min(lim, 5000))
        sql = "SELECT * FROM saved_search_members WHERE saved_search_id=?"
        params: list[Any] = [sid]
        if active_only:
            sql += " AND removed_at IS NULL"
        sql += " ORDER BY (last_seen_at IS NULL) ASC, last_seen_at DESC, added_at DESC LIMIT ?"
        params.append(lim)
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def record_watchlist_run_start(
        self,
        *,
        kind: str,
        watchlist_id: str | None,
        saved_search_id: str | None,
        started_at: str,
    ) -> str:
        rid = self._uuid()[:12]
        self.conn.execute(
            """
            INSERT INTO watchlist_runs (
                id, kind, saved_search_id, watchlist_id, started_at, ok, stats_json
            )
            VALUES (?, ?, ?, ?, ?, 0, '{}')
            """,
            (rid, str(kind or "").strip() or "unknown", saved_search_id, watchlist_id, started_at),
        )
        self.conn.commit()
        return rid

    def record_watchlist_run_finish(
        self,
        *,
        run_id: str,
        finished_at: str,
        ok: bool,
        stats: Dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE watchlist_runs
            SET finished_at=?, ok=?, stats_json=?, error_text=?
            WHERE id=?
            """,
            (
                str(finished_at or ""),
                1 if ok else 0,
                self._clean_json(stats or {}),
                (str(error_text or "") if error_text else None),
                str(run_id or ""),
            ),
        )
        self.conn.commit()

    def get_last_watchlist_run(
        self,
        *,
        watchlist_id: str,
        kind: str,
    ) -> Dict[str, Any] | None:
        wid = str(watchlist_id or "").strip()
        kd = str(kind or "").strip().lower() or "unknown"
        if not wid:
            return None
        r = self.conn.execute(
            """
            SELECT * FROM watchlist_runs
            WHERE watchlist_id=? AND kind=? AND ok=1 AND finished_at IS NOT NULL
            ORDER BY finished_at DESC
            LIMIT 1
            """,
            (wid, kd),
        ).fetchone()
        return dict(r) if r else None

    def run_saved_search(
        self,
        *,
        saved_search_id: str,
        now_iso: str | None = None,
        limit: int = 2000,
    ) -> Dict[str, Any]:
        now = (now_iso or "").strip() or self._utc_now_iso()
        ss = self.get_saved_search(saved_search_id=saved_search_id)
        if not ss:
            return {"ok": False, "error": "saved_search not found"}

        county_key = str(ss.get("county") or "").strip().lower()
        watchlist_id = str(ss.get("watchlist_id") or "").strip()
        if not watchlist_id:
            return {"ok": False, "error": "saved_search has no watchlist_id"}

        run_id = self.record_watchlist_run_start(
            kind="saved_search",
            watchlist_id=watchlist_id,
            saved_search_id=str(ss.get("id") or ""),
            started_at=now,
        )

        try:
            try:
                polygon = json.loads(ss.get("polygon_geojson_json") or "{}")
            except Exception:
                polygon = {}
            try:
                filters = json.loads(ss.get("filters_json") or "{}")
            except Exception:
                filters = {}

            enrich = bool(int(ss.get("enrich") or 0))
            sort = str(ss.get("sort") or "")

            # Reuse the authoritative parcel search implementation.
            from florida_property_scraper.api import app as api_app

            resp = api_app.api_parcels_search(
                {
                    "county": county_key,
                    "geometry": polygon,
                    "filters": filters,
                    "enrich": bool(enrich),
                    "sort": sort,
                    "limit": int(limit),
                    "include_geometry": False,
                }
            )

            payload: Dict[str, Any] = {}
            try:
                if hasattr(resp, "body") and isinstance(resp.body, (bytes, bytearray)):
                    payload = json.loads(resp.body.decode("utf-8"))
                elif isinstance(resp, dict):
                    payload = resp
            except Exception:
                payload = {}

            records = payload.get("records") if isinstance(payload.get("records"), list) else []
            next_ids = [str(r.get("parcel_id") or "").strip() for r in (records or []) if isinstance(r, dict)]
            next_ids = [p for p in next_ids if p]
            next_set = set(next_ids)

            cur_members = self.list_watchlist_members(watchlist_id=watchlist_id, limit=5000)
            cur_set = set(str(m.get("parcel_id") or "").strip() for m in cur_members)

            # Only auto-remove members that came from this saved search.
            removable = set(
                str(m.get("parcel_id") or "").strip()
                for m in cur_members
                if str(m.get("source") or "").strip().lower() == "saved_search"
            )

            added = sorted(next_set - cur_set)
            removed = sorted(removable - next_set)

            # Maintain saved_search_members separately so scheduler/alerts can be keyed
            # directly to a saved search (and manual membership does not get auto-removed).
            cur_rows = self.conn.execute(
                """
                SELECT parcel_id, source, removed_at
                FROM saved_search_members
                WHERE saved_search_id=?
                """,
                (str(ss.get("id") or ""),),
            ).fetchall()
            active_ssm = {
                str(r["parcel_id"] or "").strip()
                for r in cur_rows
                if str(r["parcel_id"] or "").strip() and (r["removed_at"] is None)
            }
            removable_ssm = {
                str(r["parcel_id"] or "").strip()
                for r in cur_rows
                if str(r["parcel_id"] or "").strip()
                and (r["removed_at"] is None)
                and (str(r["source"] or "").strip().lower() == "saved_search")
            }
            added_ssm = sorted(next_set - active_ssm)
            removed_ssm = sorted(removable_ssm - next_set)

            for pid in added:
                self.add_parcel_to_watchlist(
                    watchlist_id=watchlist_id,
                    county=county_key,
                    parcel_id=pid,
                    source="saved_search",
                    now_iso=now,
                )

            for pid in added_ssm:
                self.conn.execute(
                    """
                    INSERT INTO saved_search_members (saved_search_id, parcel_id, county, added_at, source, removed_at, last_seen_at)
                    VALUES (?, ?, ?, ?, 'saved_search', NULL, ?)
                    ON CONFLICT(saved_search_id, parcel_id) DO UPDATE SET
                        county=excluded.county,
                        removed_at=NULL,
                        last_seen_at=excluded.last_seen_at,
                        source=CASE
                            WHEN saved_search_members.source IS NULL OR saved_search_members.source='' THEN 'saved_search'
                            ELSE saved_search_members.source
                        END
                    """,
                    (str(ss.get("id") or ""), pid, county_key, now, now),
                )

            # Update last_seen for existing saved_search members we still match.
            keep = sorted((next_set & cur_set))
            if keep:
                placeholders = ",".join(["?"] * len(keep))
                self.conn.execute(
                    f"""
                    UPDATE watchlist_members
                    SET last_seen_at=?
                    WHERE watchlist_id=? AND parcel_id IN ({placeholders})
                    """,
                    [now, watchlist_id] + keep,
                )

                # Mirror last_seen into saved_search_members for any still-active membership.
                self.conn.execute(
                    f"""
                    UPDATE saved_search_members
                    SET last_seen_at=?
                    WHERE saved_search_id=? AND removed_at IS NULL AND parcel_id IN ({placeholders})
                    """,
                    [now, str(ss.get("id") or "")] + keep,
                )

            for pid in removed:
                self.conn.execute(
                    "DELETE FROM watchlist_members WHERE watchlist_id=? AND parcel_id=? AND source='saved_search'",
                    (watchlist_id, pid),
                )

            for pid in removed_ssm:
                self.conn.execute(
                    """
                    UPDATE saved_search_members
                    SET removed_at=?
                    WHERE saved_search_id=? AND parcel_id=? AND removed_at IS NULL AND source='saved_search'
                    """,
                    (now, str(ss.get("id") or ""), pid),
                )

            self.conn.execute(
                "UPDATE saved_searches SET updated_at=?, last_run_at=? WHERE id=?",
                (now, now, str(ss.get("id") or "")),
            )
            self.conn.execute(
                "UPDATE watchlists SET updated_at=? WHERE id=?",
                (now, watchlist_id),
            )
            self.conn.commit()

            stats = {
                "added": len(added),
                "removed": len(removed),
                "matched": len(next_set),
                "candidate_count": int(payload.get("summary", {}).get("candidate_count") or 0) if isinstance(payload.get("summary"), dict) else 0,
                "filtered_count": int(payload.get("summary", {}).get("filtered_count") or 0) if isinstance(payload.get("summary"), dict) else 0,
            }
            self.record_watchlist_run_finish(run_id=run_id, finished_at=now, ok=True, stats=stats)
            return {
                "ok": True,
                "run_id": run_id,
                "saved_search_id": str(ss.get("id") or ""),
                "watchlist_id": watchlist_id,
                "county": county_key,
                "added": added,
                "removed": removed,
                "stats": stats,
            }
        except Exception as e:
            self.record_watchlist_run_finish(
                run_id=run_id,
                finished_at=now,
                ok=False,
                stats={"error": type(e).__name__},
                error_text=str(e),
            )
            return {"ok": False, "error": str(e), "run_id": run_id}

    def list_alerts(
        self,
        *,
        saved_search_id: str | None = None,
        county: str | None = None,
        status: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if saved_search_id:
            where.append("saved_search_id=?")
            params.append(str(saved_search_id))
        if county:
            where.append("county=?")
            params.append(str(county).strip().lower())
        if status:
            where.append("status=?")
            params.append(str(status).strip().lower())
        try:
            lim = int(limit)
        except Exception:
            lim = 200
        lim = max(1, min(lim, 1000))

        try:
            off = int(offset)
        except Exception:
            off = 0
        off = max(0, min(off, 100000))

        sql = "SELECT * FROM alerts_inbox"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY last_seen_at DESC, id DESC LIMIT ? OFFSET ?"
        params.append(lim)
        params.append(off)
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def mark_alert_read(self, *, alert_id: int) -> bool:
        try:
            aid = int(alert_id)
        except Exception:
            return False
        if aid <= 0:
            return False
        cur = self.conn.execute("UPDATE alerts_inbox SET status='read' WHERE id=?", (aid,))
        self.conn.commit()
        return bool(cur.rowcount and int(cur.rowcount) > 0)

    def _alert_fingerprint(self, alert_row: Dict[str, Any]) -> str:
        """Stable fingerprint used for delivery dedupe."""

        parts = [
            str(alert_row.get("saved_search_id") or ""),
            str(alert_row.get("parcel_id") or ""),
            str(alert_row.get("alert_key") or ""),
            str(alert_row.get("last_seen_at") or ""),
            str(alert_row.get("severity") or ""),
            str(alert_row.get("title") or ""),
            str(alert_row.get("body_json") or ""),
        ]
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def list_undelivered_alerts(
        self,
        *,
        saved_search_ids: list[str] | None = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ids = [str(s or "").strip() for s in (saved_search_ids or [])]
        ids = [s for s in ids if s]
        try:
            lim = int(limit)
        except Exception:
            lim = 200
        lim = max(1, min(lim, 2000))

        where = ["status='new'"]
        params: list[Any] = []
        if ids:
            placeholders = ",".join(["?"] * len(ids))
            where.append(f"saved_search_id IN ({placeholders})")
            params.extend(ids)

        sql = "SELECT * FROM alerts_inbox WHERE " + " AND ".join(where)
        sql += " ORDER BY last_seen_at DESC, id DESC LIMIT ?"
        params.append(lim)
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def mark_alert_delivered(
        self,
        *,
        alert_inbox_id: int,
        channel: str,
        fingerprint: str,
        recipient: str | None,
        now_iso: str,
    ) -> bool:
        ch = (channel or "").strip().lower() or "console"
        fp = (fingerprint or "").strip()
        if not fp:
            return False
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO alert_deliveries(
                alert_inbox_id, channel, fingerprint, recipient, status, created_at, sent_at, error_text
            )
            VALUES (?, ?, ?, ?, 'sent', ?, ?, NULL)
            """,
            (int(alert_inbox_id), ch, fp, recipient, now_iso, now_iso),
        )
        self.conn.commit()
        return bool(cur.rowcount and int(cur.rowcount) > 0)

    def mark_alert_delivery_failed(
        self,
        *,
        alert_inbox_id: int,
        channel: str,
        fingerprint: str,
        recipient: str | None,
        now_iso: str,
        error_text: str,
    ) -> bool:
        ch = (channel or "").strip().lower() or "console"
        fp = (fingerprint or "").strip()
        if not fp:
            return False
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO alert_deliveries(
                alert_inbox_id, channel, fingerprint, recipient, status, created_at, sent_at, error_text
            )
            VALUES (?, ?, ?, ?, 'failed', ?, NULL, ?)
            """,
            (int(alert_inbox_id), ch, fp, recipient, now_iso, str(error_text or "")[:2000]),
        )
        self.conn.commit()
        return bool(cur.rowcount and int(cur.rowcount) > 0)

    def deliver_new_alerts(
        self,
        *,
        saved_search_ids: list[str] | None = None,
        now_iso: str | None = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        """Deliver 'new' inbox alerts with idempotent dedupe via alert_deliveries."""

        now = (now_iso or "").strip() or self._utc_now_iso()
        alerts = self.list_undelivered_alerts(saved_search_ids=saved_search_ids, limit=limit)

        enable_email = str(os.getenv("ALERTS_ENABLE_EMAIL", "")).strip() in {"1", "true", "yes"}
        enable_sms = str(os.getenv("ALERTS_ENABLE_SMS", "")).strip() in {"1", "true", "yes"}
        email_to = (os.getenv("ALERTS_EMAIL_TO", "") or "").strip() or None
        sms_to = (os.getenv("ALERTS_SMS_TO", "") or "").strip() or None

        channels: list[tuple[str, str | None]] = [("console", None)]
        if enable_email:
            channels.append(("email", email_to))
        if enable_sms:
            channels.append(("sms", sms_to))

        delivered = 0
        per_channel: Dict[str, int] = {c: 0 for (c, _) in channels}
        attempted = 0

        for a in alerts:
            fp = self._alert_fingerprint(a)
            for ch, recipient in channels:
                attempted += 1
                if ch in {"email", "sms"} and not recipient:
                    # Record a single failure per alert+channel+fingerprint.
                    self.mark_alert_delivery_failed(
                        alert_inbox_id=int(a.get("id") or 0),
                        channel=ch,
                        fingerprint=fp,
                        recipient=None,
                        now_iso=now,
                        error_text=f"{ch} enabled but recipient missing",
                    )
                    continue

                ok = self.mark_alert_delivered(
                    alert_inbox_id=int(a.get("id") or 0),
                    channel=ch,
                    fingerprint=fp,
                    recipient=recipient,
                    now_iso=now,
                )
                if not ok:
                    continue

                per_channel[ch] = int(per_channel.get(ch) or 0) + 1
                delivered += 1

                payload = {
                    "saved_search_id": a.get("saved_search_id"),
                    "parcel_id": a.get("parcel_id"),
                    "county": a.get("county"),
                    "alert_key": a.get("alert_key"),
                    "title": a.get("title"),
                    "body_json": a.get("body_json"),
                    "why_json": a.get("why_json"),
                    "last_seen_at": a.get("last_seen_at"),
                    "severity": a.get("severity"),
                }

                # Persist a lightweight notification ledger (even console delivery) for debugging.
                self.conn.execute(
                    """
                    INSERT INTO notifications(channel, recipient, payload_json, status, created_at, sent_at, error_text)
                    VALUES (?, ?, ?, 'sent', ?, ?, NULL)
                    """,
                    (ch, recipient, self._clean_json(payload), now, now),
                )
                self.conn.commit()

                if ch == "console":
                    print(
                        f"ALERT[{a.get('id')}] saved_search={a.get('saved_search_id')} parcel={a.get('parcel_id')} key={a.get('alert_key')} sev={a.get('severity')}"
                    )

        return {
            "ok": True,
            "attempted": attempted,
            "delivered": delivered,
            "by_channel": per_channel,
        }

    def _format_inbox_alert(self, *, alert_key: str, details: Dict[str, Any] | None) -> tuple[str, str, int, list[str]]:
        d = details or {}
        sev = int(d.get("severity") or 0)
        trigger_keys = d.get("trigger_keys") if isinstance(d.get("trigger_keys"), list) else []
        trigger_keys = [str(k or "").strip() for k in trigger_keys]
        trigger_keys = [k for k in trigger_keys if k]

        seller_score = None
        try:
            seller_score = int(d.get("seller_score") or 0)
        except Exception:
            seller_score = None
        rule = str(d.get("rule") or "").strip() or None

        title = str(alert_key)
        body_parts: list[str] = []

        if alert_key == "permit_activity":
            title = "Permit activity"
            body_parts.append("Recent permit activity detected.")
        elif alert_key == "owner_moved":
            title = "Owner mailing changed"
            body_parts.append("Owner mailing address change detected.")
        elif alert_key == "redevelopment_signal":
            title = "Redevelopment signal"
            body_parts.append("Permit activity + owner mailing change within window.")
        elif alert_key == "seller_intent_critical":
            title = "Seller intent (critical)"
        elif alert_key == "seller_intent_strong_stack":
            title = "Seller intent (strong stack)"
        elif alert_key == "seller_intent_mixed_stack":
            title = "Seller intent (mixed stack)"

        if seller_score is not None and seller_score > 0:
            body_parts.append(f"seller_score={seller_score}")
        if rule:
            body_parts.append(f"rule={rule}")
        if trigger_keys:
            body_parts.append("trigger_keys=" + ",".join(trigger_keys[:10]))

        body = " ".join(body_parts).strip() or title
        if sev <= 0:
            # Severity is stored on trigger_alerts; default to a conservative mapping.
            if alert_key in {"seller_intent_critical"}:
                sev = 5
            elif alert_key in {"seller_intent_strong_stack", "redevelopment_signal"}:
                sev = 4
            elif alert_key in {"seller_intent_mixed_stack", "owner_moved"}:
                sev = 3
            else:
                sev = 2
        return title, body, int(sev), trigger_keys

    def sync_saved_search_inbox_from_trigger_alerts(
        self,
        *,
        saved_search_id: str,
        now_iso: str | None = None,
        since_iso: str | None = None,
        max_parcels: int = 500,
    ) -> Dict[str, Any]:
        now = (now_iso or "").strip() or self._utc_now_iso()
        sid = str(saved_search_id or "").strip()
        if not sid:
            return {"ok": False, "error": "saved_search_id required"}

        ss = self.get_saved_search(saved_search_id=sid)
        if not ss:
            return {"ok": False, "error": "saved_search not found"}

        county_key = str(ss.get("county") or "").strip().lower()
        watchlist_id = str(ss.get("watchlist_id") or "").strip() or None

        if since_iso is None:
            r = self.conn.execute(
                """
                SELECT finished_at
                FROM watchlist_runs
                WHERE saved_search_id=? AND kind='alerts' AND ok=1 AND finished_at IS NOT NULL
                ORDER BY finished_at DESC
                LIMIT 1
                """,
                (sid,),
            ).fetchone()
            since_iso = str(r["finished_at"] or "").strip() if r else None

        if not since_iso:
            since_iso = "1970-01-01T00:00:00+00:00"

        run_id = self.record_watchlist_run_start(
            kind="alerts",
            watchlist_id=watchlist_id,
            saved_search_id=sid,
            started_at=now,
        )

        try:
            # Prefer saved_search_members (diffable, source-aware). Fallback to watchlist_members.
            members = self.conn.execute(
                """
                SELECT parcel_id
                FROM saved_search_members
                WHERE saved_search_id=? AND removed_at IS NULL
                ORDER BY (last_seen_at IS NULL) ASC, last_seen_at DESC, added_at DESC
                LIMIT ?
                """,
                (sid, max(1, int(max_parcels))),
            ).fetchall()
            parcel_ids = [str(r["parcel_id"] or "").strip() for r in members]
            parcel_ids = [p for p in parcel_ids if p]

            if (not parcel_ids) and watchlist_id:
                wl_members = self.list_watchlist_members(watchlist_id=watchlist_id, limit=max(1, int(max_parcels)))
                parcel_ids = [str(m.get("parcel_id") or "").strip() for m in wl_members]
                parcel_ids = [p for p in parcel_ids if p]

            if not parcel_ids:
                self.record_watchlist_run_finish(
                    run_id=run_id,
                    finished_at=now,
                    ok=True,
                    stats={"inserted": 0, "updated": 0, "watched": 0, "since": since_iso},
                )
                return {"ok": True, "run_id": run_id, "inserted": 0, "updated": 0, "watched": 0, "since": since_iso}

            placeholders = ",".join(["?"] * len(parcel_ids))
            rows = self.conn.execute(
                f"""
                SELECT county, parcel_id, alert_key, severity, first_seen_at, last_seen_at, details_json
                FROM trigger_alerts
                WHERE county=?
                  AND status='open'
                  AND last_seen_at > ?
                  AND parcel_id IN ({placeholders})
                ORDER BY last_seen_at DESC
                """,
                [county_key, since_iso] + parcel_ids,
            ).fetchall()

            inserted = 0
            updated = 0
            for r in rows:
                try:
                    details = json.loads(str(r["details_json"] or "{}"))
                except Exception:
                    details = {}
                if isinstance(details, dict):
                    details.setdefault("severity", int(r["severity"] or 1))

                title, body, sev, trigger_keys = self._format_inbox_alert(
                    alert_key=str(r["alert_key"] or ""),
                    details=details,
                )
                first_seen_at = str(r["first_seen_at"] or "").strip() or now
                last_seen_at = str(r["last_seen_at"] or "").strip() or now

                body_json = {"text": body, "details": details}
                why_json = {"trigger_keys": trigger_keys}

                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO alerts_inbox (
                        saved_search_id, parcel_id, county, alert_key, severity,
                        title, body_json, why_json,
                        first_seen_at, last_seen_at, status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')
                    """,
                    (
                        sid,
                        str(r["parcel_id"] or ""),
                        county_key,
                        str(r["alert_key"] or ""),
                        int(sev),
                        str(title or ""),
                        self._clean_json(body_json),
                        self._clean_json(why_json),
                        first_seen_at,
                        last_seen_at,
                    ),
                )
                inserted += int(cur.rowcount or 0)

                # If this alert already existed, only bump it when trigger_alerts moved forward.
                cur2 = self.conn.execute(
                    """
                    UPDATE alerts_inbox
                    SET
                        severity=?,
                        title=?,
                        body_json=?,
                        why_json=?,
                        last_seen_at=?,
                        status='new'
                    WHERE saved_search_id=? AND parcel_id=? AND alert_key=? AND last_seen_at < ?
                    """,
                    (
                        int(sev),
                        str(title or ""),
                        self._clean_json(body_json),
                        self._clean_json(why_json),
                        last_seen_at,
                        sid,
                        str(r["parcel_id"] or ""),
                        str(r["alert_key"] or ""),
                        last_seen_at,
                    ),
                )
                updated += int(cur2.rowcount or 0)

            self.conn.commit()
            self.record_watchlist_run_finish(
                run_id=run_id,
                finished_at=now,
                ok=True,
                stats={"inserted": inserted, "updated": updated, "watched": len(parcel_ids), "since": since_iso},
            )
            return {
                "ok": True,
                "run_id": run_id,
                "inserted": inserted,
                "updated": updated,
                "watched": len(parcel_ids),
                "since": since_iso,
            }
        except Exception as e:
            self.record_watchlist_run_finish(
                run_id=run_id,
                finished_at=now,
                ok=False,
                stats={"error": type(e).__name__},
                error_text=str(e),
            )
            return {"ok": False, "error": str(e), "run_id": run_id}

    def sync_watchlist_inbox_from_trigger_alerts(
        self,
        *,
        watchlist_id: str,
        now_iso: str | None = None,
        since_iso: str | None = None,
        max_parcels: int = 500,
    ) -> Dict[str, Any]:
        """Backward-compatible wrapper: sync all saved searches attached to a watchlist."""
        wid = str(watchlist_id or "").strip()
        if not wid:
            return {"ok": False, "error": "watchlist_id required"}
        rows = self.conn.execute(
            "SELECT id FROM saved_searches WHERE watchlist_id=? ORDER BY updated_at DESC",
            (wid,),
        ).fetchall()
        if not rows:
            return {"ok": True, "inserted": 0, "updated": 0, "saved_searches": 0}
        inserted = 0
        updated = 0
        for r in rows[:50]:
            sid = str(r["id"] or "").strip()
            if not sid:
                continue
            res = self.sync_saved_search_inbox_from_trigger_alerts(
                saved_search_id=sid,
                now_iso=now_iso,
                since_iso=since_iso,
                max_parcels=max_parcels,
            )
            inserted += int(res.get("inserted") or 0)
            updated += int(res.get("updated") or 0)
        return {"ok": True, "inserted": inserted, "updated": updated, "saved_searches": len(rows)}

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

    def upsert_many_official_records(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return

        payload = [
            (
                (r.get("county") or "").strip().lower(),
                r.get("parcel_id"),
                r.get("join_key"),
                r.get("doc_type"),
                r.get("rec_date"),
                r.get("parties"),
                r.get("book_page_or_instrument"),
                r.get("consideration"),
                r.get("raw_text"),
                r.get("owner_name"),
                r.get("address"),
                r.get("source"),
                r.get("raw"),
            )
            for r in records
        ]

        self.conn.executemany(
            """
            INSERT INTO official_records (
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
                source,
                raw
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(county, book_page_or_instrument, doc_type, rec_date) DO UPDATE SET
                parcel_id=excluded.parcel_id,
                join_key=excluded.join_key,
                parties=excluded.parties,
                consideration=excluded.consideration,
                raw_text=excluded.raw_text,
                owner_name=excluded.owner_name,
                address=excluded.address,
                source=excluded.source,
                raw=excluded.raw
            """,
            payload,
        )
        self.conn.commit()

    def upsert_many_tax_collector_events(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return

        payload = [
            (
                (r.get("county") or "").strip().lower(),
                str(r.get("parcel_id") or "").strip(),
                str(r.get("observed_at") or "").strip(),
                str(r.get("event_type") or "").strip(),
                r.get("event_date"),
                r.get("amount_due"),
                r.get("status"),
                r.get("description"),
                r.get("source"),
                json.dumps(r.get("raw") if ("raw" in r) else r, ensure_ascii=True),
            )
            for r in records
            if str(r.get("parcel_id") or "").strip() and str(r.get("observed_at") or "").strip() and str(r.get("event_type") or "").strip()
        ]
        if not payload:
            return

        self.conn.executemany(
            """
            INSERT INTO tax_collector_events (
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
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(county, parcel_id, observed_at, event_type) DO UPDATE SET
                event_date=excluded.event_date,
                amount_due=excluded.amount_due,
                status=excluded.status,
                description=excluded.description,
                source=excluded.source,
                raw_json=excluded.raw_json
            """,
            payload,
        )
        self.conn.commit()

    def upsert_many_code_enforcement_events(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return

        payload = [
            (
                (r.get("county") or "").strip().lower(),
                str(r.get("parcel_id") or "").strip(),
                str(r.get("observed_at") or "").strip(),
                str(r.get("event_type") or "").strip(),
                r.get("event_date"),
                r.get("case_number"),
                r.get("status"),
                r.get("description"),
                r.get("fine_amount"),
                r.get("lien_amount"),
                r.get("source"),
                json.dumps(r.get("raw") if ("raw" in r) else r, ensure_ascii=True),
            )
            for r in records
            if str(r.get("parcel_id") or "").strip() and str(r.get("observed_at") or "").strip() and str(r.get("event_type") or "").strip()
        ]
        if not payload:
            return

        self.conn.executemany(
            """
            INSERT INTO code_enforcement_events (
                county,
                parcel_id,
                observed_at,
                event_type,
                event_date,
                case_number,
                status,
                description,
                fine_amount,
                lien_amount,
                source,
                raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(county, parcel_id, observed_at, event_type) DO UPDATE SET
                event_date=excluded.event_date,
                case_number=excluded.case_number,
                status=excluded.status,
                description=excluded.description,
                fine_amount=excluded.fine_amount,
                lien_amount=excluded.lien_amount,
                source=excluded.source,
                raw_json=excluded.raw_json
            """,
            payload,
        )
        self.conn.commit()

    @staticmethod
    def _severity_to_tier(severity: int) -> str:
        try:
            s = int(severity)
        except Exception:
            s = 1
        if s >= 5:
            return "critical"
        if s >= 4:
            return "strong"
        if s >= 2:
            return "support"
        return "low"

    @staticmethod
    def _compute_seller_score(*, critical: int, strong: int, support: int) -> int:
        """Compute a 0-100 seller score from stacking rules.

        Rules (requested):
          - 1 critical OR 2 strong OR 4 mixed -> high score
        """

        c = max(0, int(critical))
        s = max(0, int(strong))
        p = max(0, int(support))

        if c >= 1:
            return 100
        if s >= 2:
            return 85
        if (s + p) >= 4:
            return 70
        if s == 1:
            return 45
        if p >= 2:
            return 25
        if p == 1:
            return 15
        return 0

    @staticmethod
    def _seller_intent_rule(*, critical: int, strong: int, support: int) -> str | None:
        """Explain the stacking rule that produced a high seller intent signal."""

        c = max(0, int(critical))
        s = max(0, int(strong))
        p = max(0, int(support))
        if c >= 1:
            return "critical>=1"
        if s >= 2:
            return "strong>=2"
        if (s + p) >= 4:
            return "mixed>=4"
        return None

    def rebuild_parcel_trigger_rollups(
        self,
        *,
        county: str,
        rebuilt_at: str,
    ) -> Dict[str, Any]:
        """Idempotently rebuild rollups for a county from trigger_events.

        This does not call any network providers; it only reads/writes SQLite.
        """

        county_key = (county or "").strip().lower()
        if not county_key:
            return {"ok": False, "error": "county is required"}

        rows = self.conn.execute(
            """
            SELECT county, parcel_id, trigger_key, trigger_at, severity
            FROM trigger_events
            WHERE county=?
            ORDER BY parcel_id, trigger_at DESC
            """,
            (county_key,),
        ).fetchall()

        # Aggregate in Python to keep trigger-key mapping flexible.
        agg: Dict[str, Dict[str, Any]] = {}

        def _group_for_trigger_key(k: str) -> str:
            kk = (k or "").strip().lower()
            if kk.startswith("permit_"):
                return "permits"
            if kk.startswith("owner_"):
                return "property_appraiser"
            if kk.startswith("deed_"):
                return "official_records"
            if kk.startswith("mortgage") or kk.startswith("heloc"):
                return "official_records"
            if kk.startswith("foreclosure_"):
                return "official_records"
            if kk.startswith("lien_") or kk in {"lis_pendens", "foreclosure", "probate", "divorce", "satisfaction", "release"}:
                return "official_records"
            # Reserved groups for future connectors
            if kk.startswith("tax_") or kk in {"delinquent_tax", "payment_plan_started", "payment_plan_defaulted"}:
                return "tax"
            if kk.startswith("code_") or kk in {
                "unsafe_structure",
                "condemnation",
                "demolition_order",
                "abatement_order",
                "board_hearing_set",
                "reinspection_failed",
                "lien_recorded",
                "fines_imposed",
                "lien_released",
                "compliance_achieved",
                "repeat_violation",
            }:
                return "code_enforcement"
            if kk in {"probate_opened", "divorce_filed", "eviction_filing"}:
                return "courts"
            if kk.startswith("court_"):
                return "courts"
            if kk.startswith("gis_"):
                return "gis_planning"
            return "other"

        for r in rows:
            pid = str(r["parcel_id"] or "").strip()
            if not pid:
                continue

            d = agg.get(pid)
            if d is None:
                d = {
                    "last_seen_any": None,
                    "last_seen_permits": None,
                    "last_seen_tax": None,
                    "last_seen_official_records": None,
                    "last_seen_code_enforcement": None,
                    "last_seen_courts": None,
                    "last_seen_gis_planning": None,
                    "has_permits": 0,
                    "has_tax": 0,
                    "has_official_records": 0,
                    "has_code_enforcement": 0,
                    "has_courts": 0,
                    "has_gis_planning": 0,
                    "count_critical": 0,
                    "count_strong": 0,
                    "count_support": 0,
                    "trigger_keys": set(),
                    "groups": set(),
                }
                agg[pid] = d

            trigger_at = str(r["trigger_at"] or "").strip() or None
            if trigger_at:
                if d["last_seen_any"] is None or trigger_at > d["last_seen_any"]:
                    d["last_seen_any"] = trigger_at

            group = _group_for_trigger_key(str(r["trigger_key"] or ""))

            try:
                d["trigger_keys"].add(str(r["trigger_key"] or "").strip())
            except Exception:
                pass
            if group in {"permits", "official_records", "tax", "code_enforcement", "courts", "gis_planning", "property_appraiser"}:
                try:
                    d["groups"].add(group)
                except Exception:
                    pass
            if group == "permits":
                d["has_permits"] = 1
                if trigger_at and (d["last_seen_permits"] is None or trigger_at > d["last_seen_permits"]):
                    d["last_seen_permits"] = trigger_at
            elif group == "official_records":
                d["has_official_records"] = 1
                if trigger_at and (
                    d["last_seen_official_records"] is None
                    or trigger_at > d["last_seen_official_records"]
                ):
                    d["last_seen_official_records"] = trigger_at
            elif group == "tax":
                d["has_tax"] = 1
                if trigger_at and (d["last_seen_tax"] is None or trigger_at > d["last_seen_tax"]):
                    d["last_seen_tax"] = trigger_at
            elif group == "code_enforcement":
                d["has_code_enforcement"] = 1
                if trigger_at and (
                    d["last_seen_code_enforcement"] is None
                    or trigger_at > d["last_seen_code_enforcement"]
                ):
                    d["last_seen_code_enforcement"] = trigger_at
            elif group == "courts":
                d["has_courts"] = 1
                if trigger_at and (d["last_seen_courts"] is None or trigger_at > d["last_seen_courts"]):
                    d["last_seen_courts"] = trigger_at
            elif group == "gis_planning":
                d["has_gis_planning"] = 1
                if trigger_at and (
                    d["last_seen_gis_planning"] is None
                    or trigger_at > d["last_seen_gis_planning"]
                ):
                    d["last_seen_gis_planning"] = trigger_at

            tier = self._severity_to_tier(int(r["severity"] or 1))
            if tier == "critical":
                d["count_critical"] += 1
            elif tier == "strong":
                d["count_strong"] += 1
            elif tier == "support":
                d["count_support"] += 1

        # Idempotent: replace all rollups for the county.
        self.conn.execute("DELETE FROM parcel_trigger_rollups WHERE county=?", (county_key,))

        out_rows = []
        for pid, d in agg.items():
            score = self._compute_seller_score(
                critical=int(d["count_critical"]),
                strong=int(d["count_strong"]),
                support=int(d["count_support"]),
            )

            rule = self._seller_intent_rule(
                critical=int(d["count_critical"]),
                strong=int(d["count_strong"]),
                support=int(d["count_support"]),
            )
            details = {
                "seller_intent": {
                    "rule": rule,
                    "seller_score": int(score),
                    "counts": {
                        "critical": int(d["count_critical"]),
                        "strong": int(d["count_strong"]),
                        "support": int(d["count_support"]),
                    },
                }
            }
            try:
                details["trigger_keys"] = sorted(str(x) for x in (d.get("trigger_keys") or set()) if str(x).strip())
                details["groups"] = sorted(str(x) for x in (d.get("groups") or set()) if str(x).strip())
            except Exception:
                pass
            out_rows.append(
                (
                    county_key,
                    pid,
                    str(rebuilt_at),
                    d["last_seen_any"],
                    d["last_seen_permits"],
                    d["last_seen_tax"],
                    d["last_seen_official_records"],
                    d["last_seen_code_enforcement"],
                    d["last_seen_courts"],
                    d["last_seen_gis_planning"],
                    int(d["has_permits"]),
                    int(d["has_tax"]),
                    int(d["has_official_records"]),
                    int(d["has_code_enforcement"]),
                    int(d["has_courts"]),
                    int(d["has_gis_planning"]),
                    int(d["count_critical"]),
                    int(d["count_strong"]),
                    int(d["count_support"]),
                    int(score),
                    json.dumps(details, ensure_ascii=True),
                )
            )

        self.conn.executemany(
            """
            INSERT INTO parcel_trigger_rollups (
                county,
                parcel_id,
                rebuilt_at,
                last_seen_any,
                last_seen_permits,
                last_seen_tax,
                last_seen_official_records,
                last_seen_code_enforcement,
                last_seen_courts,
                last_seen_gis_planning,
                has_permits,
                has_tax,
                has_official_records,
                has_code_enforcement,
                has_courts,
                has_gis_planning,
                count_critical,
                count_strong,
                count_support,
                seller_score,
                details_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            out_rows,
        )
        self.conn.commit()
        return {"ok": True, "county": county_key, "rows": len(out_rows)}

    def get_rollup_for_parcel(self, *, county: str, parcel_id: str) -> Dict[str, Any] | None:
        county_key = (county or "").strip().lower()
        pid = (parcel_id or "").strip()
        if not county_key or not pid:
            return None
        r = self.conn.execute(
            """
            SELECT * FROM parcel_trigger_rollups
            WHERE county=? AND parcel_id=?
            """,
            (county_key, pid),
        ).fetchone()
        return dict(r) if r else None

    def search_rollups(
        self,
        *,
        county: str,
        parcel_ids: List[str] | None = None,
        min_score: int | None = None,
        require_any_groups: List[str] | None = None,
        require_trigger_keys: List[str] | None = None,
        require_tiers: List[str] | None = None,
        limit: int = 250,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        county_key = (county or "").strip().lower()
        if not county_key:
            return []

        try:
            lim = int(limit)
        except Exception:
            lim = 250
        lim = max(1, min(lim, 2000))
        try:
            off = int(offset)
        except Exception:
            off = 0
        off = max(0, min(off, 50_000))

        where = ["county=?"]
        params: list[Any] = [county_key]

        if parcel_ids:
            ids = [str(x or "").strip() for x in parcel_ids]
            ids = [x for x in ids if x]
            if ids:
                ids = ids[:2000]
                placeholders = ",".join(["?"] * len(ids))
                where.append(f"parcel_id IN ({placeholders})")
                params.extend(ids)

        if min_score is not None:
            try:
                ms = int(min_score)
            except Exception:
                ms = 0
            where.append("seller_score >= ?")
            params.append(ms)

        groups = set((require_any_groups or []))
        # Any-of semantics
        group_clauses: list[str] = []
        for g in groups:
            gg = str(g or "").strip().lower()
            if gg == "permits":
                group_clauses.append("has_permits=1")
            elif gg == "tax":
                group_clauses.append("has_tax=1")
            elif gg in {"official_records", "records"}:
                group_clauses.append("has_official_records=1")
            elif gg in {"code", "code_enforcement"}:
                group_clauses.append("has_code_enforcement=1")
            elif gg == "courts":
                group_clauses.append("has_courts=1")
            elif gg in {"gis", "gis_planning"}:
                group_clauses.append("has_gis_planning=1")
            else:
                # Backstop for groups without dedicated columns (e.g., property_appraiser)
                group_clauses.append("details_json LIKE ?")
                params.append(f"%\"{gg}\"%")
        if group_clauses:
            where.append("(" + " OR ".join(group_clauses) + ")")

        keys = set((require_trigger_keys or []))
        key_clauses: list[str] = []
        for k in keys:
            kk = str(k or "").strip().lower()
            if not kk:
                continue
            # JSON string containment (deterministic + SQLite-friendly)
            key_clauses.append("details_json LIKE ?")
            params.append(f"%\"{kk}\"%")
        if key_clauses:
            where.append("(" + " OR ".join(key_clauses) + ")")

        tiers = set((require_tiers or []))
        tier_clauses: list[str] = []
        for t in tiers:
            tt = str(t or "").strip().lower()
            if tt == "critical":
                tier_clauses.append("count_critical > 0")
            elif tt == "strong":
                tier_clauses.append("count_strong > 0")
            elif tt == "support":
                tier_clauses.append("count_support > 0")
        if tier_clauses:
            where.append("(" + " OR ".join(tier_clauses) + ")")

        sql = (
            "SELECT * FROM parcel_trigger_rollups WHERE "
            + " AND ".join(where)
            + " ORDER BY seller_score DESC, (last_seen_any IS NULL) ASC, last_seen_any DESC LIMIT ? OFFSET ?"
        )
        params.extend([lim, off])
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

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
