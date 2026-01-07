#!/usr/bin/env python3
import argparse
import sqlite3


def main() -> None:
    parser = argparse.ArgumentParser(description="Show recent events from the SQLite store.")
    parser.add_argument("--db", default="./leads.sqlite", help="SQLite database path")
    parser.add_argument("--limit", type=int, default=20, help="Number of events to display")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT event_at, event_type, county, property_uid, old_value, new_value
        FROM events
        ORDER BY event_at DESC
        LIMIT ?
        """,
        (args.limit,),
    ).fetchall()
    for row in rows:
        print(
            f"{row['event_at']} | {row['event_type']} | {row['county']} | "
            f"{row['property_uid']} | {row['old_value']} -> {row['new_value']}"
        )
    conn.close()


if __name__ == "__main__":
    main()
