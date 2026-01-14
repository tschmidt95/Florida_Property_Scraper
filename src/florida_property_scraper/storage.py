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

    def close(self) -> None:
        self.conn.close()
