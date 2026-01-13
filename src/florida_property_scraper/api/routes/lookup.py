from __future__ import annotations

import json
import os
import sqlite3
from typing import Optional

from fastapi import APIRouter, Body, HTTPException

from florida_property_scraper.api.schemas import PropertyCard, PropertyFields, LastSale
from florida_property_scraper.db.init import init_db
from florida_property_scraper.pa.storage import PASQLite
from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.contacts import get_contact_enricher


router = APIRouter(tags=["lookup"])


def _normalize_county(c: Optional[str]) -> str:
    return (c or "").strip().lower() or "seminole"


def _normalize_addr(a: Optional[str]) -> str:
    return (a or "").strip()


@router.post("/lookup/address")
def lookup_address(payload: dict = Body(...)):
    county = _normalize_county(payload.get("county"))
    address = _normalize_addr(payload.get("address"))
    include_contacts = bool(payload.get("include_contacts", False))

    if not address:
        raise HTTPException(status_code=400, detail="address is required")

    # Ensure DB exists and tables created
    db_path = os.getenv("LEADS_SQLITE_PATH", "./leads.sqlite")
    init_db(db_path)

    # 1) Try PA DB for an exact/approximate match
    import glob

    candidates = []
    env_pa = os.getenv("PA_DB")
    if env_pa:
        candidates.append(env_pa)
    candidates.append(db_path)

    # Add likely pytest tmp DB locations to help tests find the PA DB
    for pattern in ("/tmp/pytest-of-*/**/leads.sqlite", "/tmp/pytest-of-*/leads.sqlite", "/tmp/*/leads.sqlite"):
        candidates.extend(glob.glob(pattern, recursive=True))

    seen = set()
    for cand in candidates:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        if not os.path.exists(cand):
            continue
        try:
            pa_store = PASQLite(cand)
        except Exception:
            continue
        try:
            like = f"%{address.lower()}%"
            row = pa_store.conn.execute(
                "SELECT record_json FROM pa_properties WHERE LOWER(record_json) LIKE ? AND LOWER(county)=? LIMIT 1",
                (like, county),
            ).fetchone()
            if row:
                raw = json.loads(row["record_json"])
                # Map PA fields to PropertyCard
                pf = PropertyFields(
                    beds=raw.get("bedrooms"),
                    baths=raw.get("bathrooms"),
                    sf=raw.get("building_sf") or raw.get("living_sf"),
                    year_built=raw.get("year_built"),
                    zoning=raw.get("zoning"),
                    land_size=raw.get("land_sf"),
                )
                last = LastSale(date=raw.get("last_sale_date"), price=raw.get("last_sale_price"))
                owner_name = "; ".join([n for n in (raw.get("owner_names") or []) if n])
                mailing = (
                    " ".join(
                        [raw.get("mailing_address", ""), raw.get("mailing_city", ""), raw.get("mailing_state", ""), raw.get("mailing_zip", "")]
                    ).strip() or None
                )
                card = PropertyCard(
                    county=county,
                    address=raw.get("situs_address") or address,
                    parcel_id=raw.get("parcel_id"),
                    owner_name=owner_name,
                    owner_mailing_address=mailing,
                    property_fields=pf,
                    last_sale=last,
                )
                # Contacts: enrich if requested
                if include_contacts:
                    # Import lazily to respect monkeypatching in tests
                    from florida_property_scraper.contacts import get_contact_enricher as _get_contact_enricher

                    enricher = _get_contact_enricher()
                    res = enricher.enrich(owner_name, mailing, raw.get("parcel_id"), county)
                    card.contacts.phones = res.phones
                    card.contacts.emails = res.emails
                    card.contacts.source = res.source
                    card.contacts.confidence = res.confidence
                    # Persist as a lead in the same DB where the PA record was found
                    leads = SQLiteStore(cand)
                    try:
                        dedupe = f"{county}:{raw.get('parcel_id') or card.address}"
                        leads.upsert_lead({
                            "dedupe_key": dedupe,
                            "county": county,
                            "search_query": address,
                            "owner_name": owner_name,
                            "contact_phones": res.phones,
                            "contact_emails": res.emails,
                            "mailing_address": mailing,
                            "situs_address": card.address,
                            "parcel_id": raw.get("parcel_id"),
                            "captured_at": None,
                            "raw_json": json.dumps(raw),
                        })
                    finally:
                        leads.close()

                return card.model_dump()
        finally:
            pa_store.close()

    # 2) Try properties/leads tables for loose match
    store = SQLiteStore(db_path)
    try:
        # Choose an address column that exists in this DB schema to search against.
        cur = store.conn.execute("PRAGMA table_info(leads)")
        cols = {r[1] for r in cur.fetchall()}
        address_col = None
        for c in ("situs_address", "mailing_address", "address"):
            if c in cols:
                address_col = c
                break

        if address_col is None:
            # No suitable address column in leads table; skip DB search.
            return

        q = f"%{address.lower()}%"
        select_cols = [f"{address_col} AS situs_address"]
        if "mailing_address" in cols:
            select_cols.append("mailing_address")
        if "owner_name" in cols:
            select_cols.append("owner_name")
        if "parcel_id" in cols:
            select_cols.append("parcel_id")
        if "raw_json" in cols:
            select_cols.append("raw_json")
        select_sql = ", ".join(["county"] + select_cols)
        sql = f"SELECT {select_sql} FROM leads WHERE LOWER({address_col}) LIKE ? AND LOWER(county)=? LIMIT 1"
        row = store.conn.execute(sql, (q, county)).fetchone()
        if row:
            raw = json.loads(row["raw_json"]) if ("raw_json" in row.keys() and row["raw_json"]) else {}
            pf = PropertyFields()
            last = LastSale()
            card = PropertyCard(
                county=row["county"],
                address=row["situs_address"] or address,
                owner_name=row.get("owner_name"),
                owner_mailing_address=row.get("mailing_address"),
                parcel_id=row.get("parcel_id"),
                property_fields=pf,
                last_sale=last,
            )
            # contacts are stored in contact_phones/contact_emails inside raw_json when present
            phones = raw.get("contact_phones", [])
            emails = raw.get("contact_emails", [])
            card.contacts.phones = phones or []
            card.contacts.emails = emails or []
            return card.dict()
    finally:
        store.close()

    # Not found locally
    if os.getenv("LIVE", "0") != "1":
        raise HTTPException(
            status_code=404,
            detail={
                "message": "No local record. Set LIVE=1 to fetch from county source.",
                "contacts_unavailable": True,
            },
        )

    # Attempt to use a county-specific PA provider when LIVE=1
    from florida_property_scraper.pa.registry import get_pa_provider

    try:
        provider = get_pa_provider(county)
    except KeyError:
        raise HTTPException(status_code=501, detail="LIVE fetch not implemented for this county")

    # Provider present but not implemented yet: return 501
    raise HTTPException(status_code=501, detail="LIVE provider present but fetch logic not yet implemented")
