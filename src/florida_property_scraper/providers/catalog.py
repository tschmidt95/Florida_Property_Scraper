from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from florida_property_scraper.registry import get_county as get_registry_county

ProviderKind = Literal["clerk", "permits", "tax", "pa"]


@dataclass(frozen=True)
class ProviderCatalogEntry:
    county: str
    provider_id: str
    category: str
    method: str
    supports_counties: list[str]
    target_ids: list[str]
    base_url: str
    supported_fields: list[str]
    rate_limit: dict[str, int] | None
    status: str
    notes: str | None = None


@dataclass(frozen=True)
class ProviderTarget:
    county: str
    kind: ProviderKind
    base_url: str
    notes: str | None = None


def _county_display_or_slug(county: str) -> str:
    key = (county or "").strip().lower()
    rec = get_registry_county(key)
    if rec is None:
        return key
    return rec.display_name


def get_targets(county: str) -> list[ProviderTarget]:
    key = (county or "").strip().lower()
    if not key:
        return []

    if key != "seminole":
        display = _county_display_or_slug(key)
        return [
            ProviderTarget(
                county=key,
                kind="pa",
                base_url="",
                notes=f"{display} public property appraiser data source",
            ),
            ProviderTarget(
                county=key,
                kind="tax",
                base_url="",
                notes=f"{display} public tax collector data source",
            ),
            ProviderTarget(
                county=key,
                kind="clerk",
                base_url="",
                notes=f"{display} public clerk/official records data source",
            ),
            ProviderTarget(
                county=key,
                kind="permits",
                base_url="",
                notes=f"{display} public permits data source",
            ),
        ]

    return [
        ProviderTarget(
            county="seminole",
            kind="pa",
            base_url="https://www.scpafl.org/",
            notes="Property Appraiser",
        ),
        ProviderTarget(
            county="seminole",
            kind="tax",
            base_url="https://www.seminolecounty.tax/",
            notes="Tax Collector",
        ),
        ProviderTarget(
            county="seminole",
            kind="clerk",
            base_url="https://www.seminoleclerk.org/",
            notes="Clerk of Court",
        ),
        ProviderTarget(
            county="seminole",
            kind="permits",
            base_url="https://aca.seminolecountyfl.gov/",
            notes="Accela permits portal",
        ),
    ]


def get_provider_catalog(county: str) -> list[ProviderCatalogEntry]:
    key = (county or "").strip().lower()
    if not key:
        return []

    supports = [key]
    entries: list[ProviderCatalogEntry] = [
        ProviderCatalogEntry(
            county=key,
            provider_id="official_records_sqlite",
            category="official_records",
            method="sqlite",
            supports_counties=supports,
            target_ids=["clerk"],
            base_url="",
            supported_fields=[
                "official_record_doc_type",
                "official_record_rec_date",
                "official_record_parties",
                "last_sale_date",
                "last_sale_price",
            ],
            rate_limit=None,
            status="implemented",
            notes="Public official-records evidence from ingested county records table",
        ),
        ProviderCatalogEntry(
            county=key,
            provider_id="pa_snapshot_stub",
            category="property_appraiser",
            method="sqlite",
            supports_counties=supports,
            target_ids=["pa"],
            base_url="",
            supported_fields=[
                "situs_address",
                "owner_mailing_address",
                "mailing_state",
                "last_sale_date",
                "last_sale_price",
                "assessed_value",
                "total_value",
                "mortgage_amount",
                "mortgage_date",
                "mortgage_lender",
            ],
            rate_limit=None,
            status="implemented",
            notes="Public property appraiser snapshot evidence from local PA table",
        ),
        ProviderCatalogEntry(
            county=key,
            provider_id="permits_stub",
            category="permits",
            method="sqlite",
            supports_counties=supports,
            target_ids=["permits"],
            base_url="",
            supported_fields=["permit_last_major_date", "permit_last_minor_date"],
            rate_limit=None,
            status="implemented",
            notes="Public permit evidence from ingested permits table",
        ),
        ProviderCatalogEntry(
            county=key,
            provider_id="tax_collector_stub",
            category="tax",
            method="sqlite",
            supports_counties=supports,
            target_ids=["tax"],
            base_url="",
            supported_fields=["tax_status", "tax_delinquent_years"],
            rate_limit=None,
            status="implemented",
            notes="Public tax collector evidence from ingested tax events table",
        ),
        ProviderCatalogEntry(
            county=key,
            provider_id="code_enforcement_stub",
            category="code_enforcement",
            method="sqlite",
            supports_counties=supports,
            target_ids=[],
            base_url="",
            supported_fields=["code_enforcement_status", "code_case_opened_date"],
            rate_limit=None,
            status="implemented",
            notes="Public code enforcement evidence from ingested county events table",
        ),
        ProviderCatalogEntry(
            county=key,
            provider_id="manual_ingest",
            category="manual",
            method="manual",
            supports_counties=supports,
            target_ids=[],
            base_url="",
            supported_fields=[
                "official_record_doc_type",
                "official_record_rec_date",
                "last_sale_date",
                "last_sale_price",
                "tax_status",
                "tax_delinquent_years",
                "code_enforcement_status",
                "code_case_opened_date",
                "permit_last_major_date",
                "permit_last_minor_date",
                "owner_mailing_address",
                "situs_address",
                "mailing_state",
                "mortgage_amount",
                "mortgage_date",
                "assessed_value",
                "total_value",
            ],
            rate_limit=None,
            status="implemented",
            notes="Manual public-record evidence fallback",
        ),
        ProviderCatalogEntry(
            county=key,
            provider_id=f"{key}_courts",
            category="courts",
            method="not_supported",
            supports_counties=supports,
            target_ids=["clerk"],
            base_url="",
            supported_fields=["case_type", "filed_date", "parties"],
            rate_limit={"requests_per_min": 30},
            status="not_implemented",
            notes="Planned public courts connector",
        ),
        ProviderCatalogEntry(
            county=key,
            provider_id=f"{key}_liens",
            category="liens",
            method="not_supported",
            supports_counties=supports,
            target_ids=["clerk"],
            base_url="",
            supported_fields=["lien_type", "recorded_date", "amount"],
            rate_limit={"requests_per_min": 30},
            status="not_implemented",
            notes="Planned public liens connector",
        ),
        ProviderCatalogEntry(
            county=key,
            provider_id=f"{key}_utilities",
            category="utilities",
            method="not_supported",
            supports_counties=supports,
            target_ids=[],
            base_url="",
            supported_fields=["account_status", "service_address"],
            rate_limit={"requests_per_min": 30},
            status="not_implemented",
            notes="Planned public utilities connector",
        ),
    ]

    if key != "seminole":
        return entries

    entries.extend([
        ProviderCatalogEntry(
            county="seminole",
            provider_id="seminole_clerk_official_records",
            category="official_records",
            method="fixture",
            supports_counties=["seminole"],
            target_ids=["clerk"],
            base_url="https://www.seminoleclerk.org/",
            supported_fields=[
                "doc_type",
                "rec_date",
                "parties",
                "book_page_or_instrument",
                "consideration",
                "owner_name",
                "address",
            ],
            rate_limit={"requests_per_min": 60},
            status="implemented",
            notes="Clerk of Court (official records) - fixture only",
        ),
        ProviderCatalogEntry(
            county="seminole",
            provider_id="manual_ingest",
            category="manual",
            method="manual",
            supports_counties=["seminole"],
            target_ids=[],
            base_url="",
            supported_fields=[
                "official_record_doc_type",
                "official_record_rec_date",
                "last_sale_date",
                "last_sale_price",
                "tax_status",
                "tax_delinquent_years",
                "code_enforcement_status",
                "code_case_opened_date",
            ],
            rate_limit=None,
            status="implemented",
            notes="Manual evidence ingestion (official-first fallback)",
        ),
        ProviderCatalogEntry(
            county="seminole",
            provider_id="seminole_tax_collector",
            category="tax",
            method="not_supported",
            supports_counties=["seminole"],
            target_ids=["tax"],
            base_url="https://www.seminolecounty.tax/",
            supported_fields=[
                "event_type",
                "event_date",
                "amount_due",
                "status",
                "description",
            ],
            rate_limit={"requests_per_min": 60},
            status="not_implemented",
            notes="Tax Collector",
        ),
        ProviderCatalogEntry(
            county="seminole",
            provider_id="seminole_permits_accela",
            category="permits",
            method="not_supported",
            supports_counties=["seminole"],
            target_ids=["permits"],
            base_url="https://aca.seminolecountyfl.gov/",
            supported_fields=[
                "permit_number",
                "permit_type",
                "status",
                "issue_date",
                "final_date",
                "description",
                "address",
                "parcel_id",
            ],
            rate_limit={"requests_per_min": 30},
            status="not_implemented",
            notes="Accela permits portal",
        ),
        ProviderCatalogEntry(
            county="seminole",
            provider_id="seminole_code_enforcement",
            category="code_enforcement",
            method="not_supported",
            supports_counties=["seminole"],
            target_ids=["code_enforcement"],
            base_url="https://www.seminolecountyfl.gov/",
            supported_fields=["event_type", "event_date", "case_number", "status", "description"],
            rate_limit={"requests_per_min": 30},
            status="not_implemented",
            notes="Code Enforcement (placeholder)",
        ),
        ProviderCatalogEntry(
            county="seminole",
            provider_id="seminole_courts",
            category="courts",
            method="not_supported",
            supports_counties=["seminole"],
            target_ids=["courts"],
            base_url="https://www.seminoleclerk.org/",
            supported_fields=["case_type", "filed_date", "parties"],
            rate_limit={"requests_per_min": 30},
            status="not_implemented",
            notes="Courts (placeholder)",
        ),
        ProviderCatalogEntry(
            county="seminole",
            provider_id="seminole_liens",
            category="liens",
            method="not_supported",
            supports_counties=["seminole"],
            target_ids=["liens"],
            base_url="https://www.seminoleclerk.org/",
            supported_fields=["lien_type", "recorded_date", "amount"],
            rate_limit={"requests_per_min": 30},
            status="not_implemented",
            notes="Liens (placeholder)",
        ),
        ProviderCatalogEntry(
            county="seminole",
            provider_id="seminole_utilities",
            category="utilities",
            method="not_supported",
            supports_counties=["seminole"],
            target_ids=["utilities"],
            base_url="https://www.seminolecountyfl.gov/",
            supported_fields=["account_status", "service_address"],
            rate_limit={"requests_per_min": 30},
            status="not_implemented",
            notes="Utilities (placeholder)",
        ),
    ])
    return entries