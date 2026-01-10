from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class UserMeta:
    county: str
    parcel_id: str
    starred: bool
    tags: List[str]
    notes: str
    lists: List[str]
    updated_at: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "county": self.county,
            "parcel_id": self.parcel_id,
            "starred": bool(self.starred),
            "tags": list(self.tags),
            "notes": self.notes,
            "lists": list(self.lists),
            "updated_at": float(self.updated_at),
        }


class UserMetaSQLite:
    """SQLite persistence for user-managed parcel metadata."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_meta (
                county TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                starred INTEGER NOT NULL DEFAULT 0,
                tags_json TEXT NOT NULL DEFAULT '[]',
                notes TEXT NOT NULL DEFAULT '',
                lists_json TEXT NOT NULL DEFAULT '[]',
                updated_at REAL NOT NULL,
                PRIMARY KEY (county, parcel_id)
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_meta_county ON user_meta(county)"
        )
        self.conn.commit()

    @staticmethod
    def _clean_list(values: Any) -> List[str]:
        if values is None:
            return []
        if isinstance(values, str):
            # allow comma-separated strings
            return [v.strip() for v in values.split(",") if v.strip()]
        if isinstance(values, list):
            out: List[str] = []
            for v in values:
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    out.append(s)
            return out
        return [str(values).strip()] if str(values).strip() else []

    def get(self, *, county: str, parcel_id: str) -> Optional[UserMeta]:
        row = self.conn.execute(
            "SELECT county, parcel_id, starred, tags_json, notes, lists_json, updated_at FROM user_meta WHERE county=? AND parcel_id=?",
            (county, parcel_id),
        ).fetchone()
        if not row:
            return None
        try:
            tags = json.loads(row["tags_json"]) if row["tags_json"] else []
        except Exception:
            tags = []
        try:
            lists_v = json.loads(row["lists_json"]) if row["lists_json"] else []
        except Exception:
            lists_v = []

        return UserMeta(
            county=str(row["county"]),
            parcel_id=str(row["parcel_id"]),
            starred=bool(int(row["starred"])),
            tags=self._clean_list(tags),
            notes=str(row["notes"] or ""),
            lists=self._clean_list(lists_v),
            updated_at=float(row["updated_at"]),
        )

    def upsert(
        self,
        *,
        county: str,
        parcel_id: str,
        starred: bool,
        tags: Any,
        notes: str,
        lists: Any,
    ) -> UserMeta:
        now = time.time()
        tags_list = self._clean_list(tags)
        lists_list = self._clean_list(lists)
        self.conn.execute(
            """
            INSERT INTO user_meta (county, parcel_id, starred, tags_json, notes, lists_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(county, parcel_id) DO UPDATE SET
                starred=excluded.starred,
                tags_json=excluded.tags_json,
                notes=excluded.notes,
                lists_json=excluded.lists_json,
                updated_at=excluded.updated_at
            """,
            (
                county,
                parcel_id,
                1 if starred else 0,
                json.dumps(tags_list, sort_keys=True),
                notes or "",
                json.dumps(lists_list, sort_keys=True),
                float(now),
            ),
        )
        self.conn.commit()
        return UserMeta(
            county=county,
            parcel_id=parcel_id,
            starred=bool(starred),
            tags=tags_list,
            notes=notes or "",
            lists=lists_list,
            updated_at=float(now),
        )


def empty_user_meta(*, county: str, parcel_id: str) -> Dict[str, Any]:
    return {
        "county": county,
        "parcel_id": parcel_id,
        "starred": False,
        "tags": [],
        "notes": "",
        "lists": [],
        "updated_at": 0.0,
    }
