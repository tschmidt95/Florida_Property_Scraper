import sqlite3

from florida_property_scraper.schema import normalize_item


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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
