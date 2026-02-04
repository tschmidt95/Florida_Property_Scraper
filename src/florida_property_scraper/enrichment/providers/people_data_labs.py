from __future__ import annotations

import os
from typing import Any, Dict, List

import requests

from .base import OwnerEnrichmentResult


class PeopleDataLabsProvider:
    name = "people_data_labs"

    def __init__(self) -> None:
        self.api_key = str(os.getenv("PDL_API_KEY", "")).strip()
        self.base_url = str(os.getenv("PDL_BASE_URL", "https://api.peopledatalabs.com/v5/person/enrich")).strip()

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def enrich(
        self,
        *,
        owner_name: str,
        mailing_address: str,
        county: str,
        parcel_id: str,
    ) -> OwnerEnrichmentResult:
        if not self.is_configured():
            return OwnerEnrichmentResult(
                status="not_configured",
                provider=self.name,
                phones=[],
                emails=[],
                raw={},
            )

        params = {
            "api_key": self.api_key,
            "name": owner_name,
            "location": mailing_address,
        }

        try:
            resp = requests.get(self.base_url, params=params, timeout=15)
        except Exception as exc:
            return OwnerEnrichmentResult(
                status=f"error:{exc.__class__.__name__}",
                provider=self.name,
                phones=[],
                emails=[],
                raw={"error": str(exc)},
            )

        if resp.status_code >= 400:
            return OwnerEnrichmentResult(
                status=f"error:http_{resp.status_code}",
                provider=self.name,
                phones=[],
                emails=[],
                raw={"body": resp.text[:1000]},
            )

        data: Dict[str, Any] = {}
        try:
            data = resp.json() if resp.content else {}
        except Exception:
            data = {}

        phones: List[str] = []
        emails: List[str] = []

        try:
            for phone in data.get("phone_numbers", []) or []:
                if isinstance(phone, dict):
                    val = phone.get("number") or phone.get("value")
                else:
                    val = phone
                if isinstance(val, str) and val.strip():
                    phones.append(val.strip())
        except Exception:
            phones = []

        try:
            for email in data.get("emails", []) or []:
                if isinstance(email, dict):
                    val = email.get("address") or email.get("value")
                else:
                    val = email
                if isinstance(val, str) and val.strip():
                    emails.append(val.strip())
        except Exception:
            emails = []

        return OwnerEnrichmentResult(
            status="ok",
            provider=self.name,
            phones=phones,
            emails=emails,
            raw=data,
        )
