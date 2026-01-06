import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


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

    def close(self) -> None:
        self.conn.close()
