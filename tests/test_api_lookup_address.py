from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from florida_property_scraper.api.app import app
from florida_property_scraper.db.init import init_db
from florida_property_scraper.pa.schema import PAProperty
from florida_property_scraper.pa.storage import PASQLite
from florida_property_scraper.storage import SQLiteStore


@pytest.fixture()
def tmp_db(tmp_path):
    p = tmp_path / "leads.sqlite"
    return str(p)


def test_init_db_creates_tables(tmp_db):
    init_db(tmp_db)
    store = SQLiteStore(tmp_db)
    try:
        cur = store.conn.execute("PRAGMA table_info(leads)")
        cols = {r[1] for r in cur.fetchall()}
        assert "id" in cols
        assert "raw_json" in cols
        cur = store.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='permits'")
        assert cur.fetchone() is not None
    finally:
        store.close()


def test_lookup_finds_from_pa(tmp_db):
    # seed PA DB
    pa = PASQLite(tmp_db)
    try:
        rec = PAProperty(
            county="seminole",
            parcel_id="ABC123",
            situs_address="105 Pineapple Lane",
            owner_names=["John Doe"],
            mailing_address="123 Mail St",
            mailing_city="Oviedo",
            mailing_state="FL",
            mailing_zip="32765",
            building_sf=1500,
            bedrooms=3,
            bathrooms=2.0,
            year_built=1990,
            last_sale_date="2020-01-01",
            last_sale_price=250000,
            zoning="R-1",
        )
        pa.upsert(rec)
    finally:
        pa.close()

    client = TestClient(app)
    resp = client.post(
        "/api/lookup/address",
        json={"county": "seminole", "address": "105 Pineapple Lane", "include_contacts": False},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["owner_name"] == "John Doe"
    assert data["parcel_id"] == "ABC123"
    assert data["property_fields"]["sf"] == 1500
    assert data["last_sale"]["price"] == 250000
    # contacts are unavailable by default
    assert data["contacts"]["phones"] == []
    assert data["contacts"]["emails"] == []


def test_lookup_not_found_live_disabled(tmp_db):
    client = TestClient(app)
    resp = client.post(
        "/api/lookup/address",
        json={"county": "seminole", "address": "Nonexistent Lane"},
    )
    assert resp.status_code == 404
    assert "Set LIVE=1" in json.loads(resp.content.decode())["detail"]["message"]

    # When LIVE=1 but no provider exists, we return 501
    os.environ["LIVE"] = "1"
    try:
        resp2 = client.post(
            "/api/lookup/address",
            json={"county": "seminole", "address": "Nonexistent Lane"},
        )
        assert resp2.status_code == 501
    finally:
        os.environ.pop("LIVE", None)


def test_contacts_enrichment_and_persistence(tmp_db, monkeypatch):
    # seed PA record
    pa = PASQLite(tmp_db)
    try:
        rec = PAProperty(
            county="seminole",
            parcel_id="XYZ789",
            situs_address="42 Example Ave",
            owner_names=["Jane Agent"],
        )
        pa.upsert(rec)
    finally:
        pa.close()

    class StubEnricher:
        def enrich(self, owner_name, mailing_address=None, parcel_id=None, county=None, state=None):
            return type("R", (), {"phones": ["555-1234"], "emails": ["jane@example.com"], "source": "stub", "confidence": 0.9})()

    monkeypatch.setenv("CONTACT_PROVIDER", "stub")
    monkeypatch.setenv("CONTACT_API_KEY", "key")

    # monkeypatch get_contact_enricher to return our stub
    import florida_property_scraper.contacts as contacts_mod

    monkeypatch.setattr(contacts_mod, "get_contact_enricher", lambda: StubEnricher())

    client = TestClient(app)
    resp = client.post(
        "/api/lookup/address",
        json={"county": "seminole", "address": "42 Example Ave", "include_contacts": True},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["contacts"]["phones"] == ["555-1234"]

    # Ensure persisted to leads table
    store = SQLiteStore(tmp_db)
    try:
        row = store.conn.execute("SELECT contact_phones, contact_emails FROM leads WHERE parcel_id=?", ("XYZ789",)).fetchone()
        assert row is not None
        phones = json.loads(row["contact_phones"])
        emails = json.loads(row["contact_emails"])
        assert phones == ["555-1234"]
        assert emails == ["jane@example.com"]
    finally:
        store.close()
