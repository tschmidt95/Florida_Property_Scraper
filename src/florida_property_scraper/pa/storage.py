from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .normalize import apply_defaults
from .schema import PAProperty


class PASQLite:
    """SQLite persistence for PAProperty records."""

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
            CREATE TABLE IF NOT EXISTS pa_properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                zip TEXT NOT NULL,
                land_use_code TEXT NOT NULL,
                year_built INTEGER NOT NULL,
                building_sf REAL NOT NULL,
                living_sf REAL NOT NULL,
                bedrooms INTEGER NOT NULL,
                bathrooms REAL NOT NULL,
                zoning TEXT NOT NULL,
                use_type TEXT NOT NULL,
                land_value REAL NOT NULL,
                improvement_value REAL NOT NULL,
                just_value REAL NOT NULL,
                last_sale_date TEXT,
                last_sale_price REAL NOT NULL,
                assessed_value REAL NOT NULL,
                latitude REAL,
                longitude REAL,
                record_json TEXT NOT NULL,
                UNIQUE(county, parcel_id)
            )
            """
        )

        # Backwards-compatible migrations for older DBs.
        # SQLite doesn't support ADD COLUMN IF NOT EXISTS, so we inspect and add.
        cols = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(pa_properties)").fetchall()
        }
        to_add = [
            ("living_sf", "REAL NOT NULL DEFAULT 0"),
            ("bedrooms", "INTEGER NOT NULL DEFAULT 0"),
            ("bathrooms", "REAL NOT NULL DEFAULT 0"),
            ("zoning", "TEXT NOT NULL DEFAULT ''"),
            ("use_type", "TEXT NOT NULL DEFAULT ''"),
            ("land_value", "REAL NOT NULL DEFAULT 0"),
            ("improvement_value", "REAL NOT NULL DEFAULT 0"),
            ("just_value", "REAL NOT NULL DEFAULT 0"),
        ]
        for name, ddl in to_add:
            if name in cols:
                continue
            self.conn.execute(f"ALTER TABLE pa_properties ADD COLUMN {name} {ddl}")

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_county ON pa_properties(county)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_parcel ON pa_properties(parcel_id)"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pa_zip ON pa_properties(zip)")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_land_use ON pa_properties(land_use_code)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_year_built ON pa_properties(year_built)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_building_sf ON pa_properties(building_sf)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_living_sf ON pa_properties(living_sf)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_bedrooms ON pa_properties(bedrooms)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_bathrooms ON pa_properties(bathrooms)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_zoning ON pa_properties(zoning)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_use_type ON pa_properties(use_type)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_land_value ON pa_properties(land_value)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_improvement_value ON pa_properties(improvement_value)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_just_value ON pa_properties(just_value)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_last_sale_date ON pa_properties(last_sale_date)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_last_sale_price ON pa_properties(last_sale_price)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pa_assessed ON pa_properties(assessed_value)"
        )
        self.conn.commit()

    def upsert(self, record: PAProperty) -> None:
        payload = record.to_dict()
        self.conn.execute(
            """
            INSERT INTO pa_properties (
                county, parcel_id, zip, land_use_code, year_built, building_sf,
                living_sf, bedrooms, bathrooms, zoning, use_type,
                land_value, improvement_value, just_value,
                last_sale_date, last_sale_price, assessed_value, latitude, longitude, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(county, parcel_id) DO UPDATE SET
                zip=excluded.zip,
                land_use_code=excluded.land_use_code,
                year_built=excluded.year_built,
                building_sf=excluded.building_sf,
                living_sf=excluded.living_sf,
                bedrooms=excluded.bedrooms,
                bathrooms=excluded.bathrooms,
                zoning=excluded.zoning,
                use_type=excluded.use_type,
                land_value=excluded.land_value,
                improvement_value=excluded.improvement_value,
                just_value=excluded.just_value,
                last_sale_date=excluded.last_sale_date,
                last_sale_price=excluded.last_sale_price,
                assessed_value=excluded.assessed_value,
                latitude=excluded.latitude,
                longitude=excluded.longitude,
                record_json=excluded.record_json
            """,
            (
                record.county,
                record.parcel_id,
                record.zip,
                record.land_use_code,
                int(record.year_built),
                float(record.building_sf),
                float(record.living_sf),
                int(record.bedrooms),
                float(record.bathrooms),
                str(record.zoning or ""),
                str(record.use_type or ""),
                float(record.land_value),
                float(record.improvement_value),
                float(record.just_value),
                record.last_sale_date,
                float(record.last_sale_price),
                float(record.assessed_value),
                record.latitude,
                record.longitude,
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()

    def filter_cached_ids(
        self,
        *,
        county: str,
        parcel_ids: Sequence[str],
        where_sql: str,
        params: Sequence[Any],
        limit: int,
    ) -> List[str]:
        """Return parcel_ids that match SQL constraints.

        Intended for geometry search: intersecting parcel_ids are computed in Python,
        then we apply attribute filters cheaply in SQL against the PA cache.
        """

        ids = [str(pid) for pid in parcel_ids if pid]
        if not ids:
            return []

        placeholders = ",".join(["?"] * len(ids))
        sql = (
            f"SELECT parcel_id FROM pa_properties WHERE county=? AND parcel_id IN ({placeholders})"
        )
        args: List[Any] = [county, *ids]
        if where_sql:
            sql += " AND (" + where_sql + ")"
            args.extend(list(params))
        sql += " LIMIT ?"
        args.append(int(limit))

        rows = self.conn.execute(sql, tuple(args)).fetchall()
        return [str(r["parcel_id"]) for r in rows]

    def upsert_many(self, records: Iterable[PAProperty]) -> None:
        for r in records:
            self.upsert(r)

    def get(self, *, county: str, parcel_id: str) -> Optional[PAProperty]:
        row = self.conn.execute(
            "SELECT record_json FROM pa_properties WHERE county=? AND parcel_id=?",
            (county, parcel_id),
        ).fetchone()
        if not row:
            return None
        raw = json.loads(row["record_json"])
        return apply_defaults(raw)

    def get_many(
        self,
        *,
        county: str,
        parcel_ids: Sequence[str],
    ) -> Dict[str, PAProperty]:
        ids = [str(pid) for pid in parcel_ids if pid]
        if not ids:
            return {}

        placeholders = ",".join(["?"] * len(ids))
        rows = self.conn.execute(
            f"SELECT parcel_id, record_json FROM pa_properties WHERE county=? AND parcel_id IN ({placeholders})",
            (county, *ids),
        ).fetchall()

        out: Dict[str, PAProperty] = {}
        for row in rows:
            try:
                raw = json.loads(row["record_json"])
            except Exception:
                continue
            out[str(row["parcel_id"])] = apply_defaults(raw)
        return out

    def query(
        self,
        *,
        where_sql: str,
        params: Sequence[Any],
        limit: int,
    ) -> List[Tuple[int, PAProperty]]:
        sql = "SELECT id, record_json FROM pa_properties"
        if where_sql:
            sql += " WHERE " + where_sql
        sql += " ORDER BY county, parcel_id LIMIT ?"
        rows = self.conn.execute(sql, tuple(params) + (int(limit),)).fetchall()
        results: List[Tuple[int, PAProperty]] = []
        for row in rows:
            raw = json.loads(row["record_json"])
            results.append((int(row["id"]), apply_defaults(raw)))
        return results

    def get_hover_fields_many(
        self,
        *,
        county: str,
        parcel_ids: Sequence[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Return hover-safe fields keyed by parcel_id.

        Contract fields:
        - situs_address
        - owner_name
        - last_sale_date
        - last_sale_price
        - mortgage_amount

        Mortgage fields remain 0 unless PA explicitly provides them.
        """

        ids = [str(pid) for pid in parcel_ids if pid]
        if not ids:
            return {}

        placeholders = ",".join(["?"] * len(ids))
        rows = self.conn.execute(
            f"SELECT parcel_id, record_json FROM pa_properties WHERE county=? AND parcel_id IN ({placeholders})",
            (county, *ids),
        ).fetchall()

        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            try:
                raw = json.loads(row["record_json"])
            except Exception:
                continue
            rec = apply_defaults(raw)
            owner_name = "; ".join([n for n in (rec.owner_names or []) if n])
            out[str(row["parcel_id"])] = {
                "situs_address": rec.situs_address or "",
                "owner_name": owner_name,
                "last_sale_date": rec.last_sale_date,
                "last_sale_price": float(rec.last_sale_price or 0),
                # PA-only: mortgage fields are unknown unless PA explicitly provides them.
                "mortgage_amount": None,
            }

        return out
