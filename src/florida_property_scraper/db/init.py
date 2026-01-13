from __future__ import annotations

import os
from pathlib import Path

from florida_property_scraper.storage import SQLiteStore


def init_db(db_path: str | None = None) -> None:
    db_path = db_path or os.getenv("LEADS_SQLITE_PATH", "./leads.sqlite")
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    store = SQLiteStore(str(p))
    store.close()
