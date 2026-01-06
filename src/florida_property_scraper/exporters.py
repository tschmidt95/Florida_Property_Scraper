import json
import os
import urllib.request
from typing import Any, Dict, Optional


class Exporter:
    def export(self, record: Dict[str, Any]) -> None:
        raise NotImplementedError


class WebhookExporter(Exporter):
    def __init__(self, url: str, timeout: int = 10):
        self.url = url
        self.timeout = timeout

    def export(self, record: Dict[str, Any]) -> None:
        data = json.dumps(record, ensure_ascii=True).encode("utf-8")
        req = urllib.request.Request(
            self.url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout):
            return None


class ZohoExporter(Exporter):
    def __init__(
        self,
        access_token: Optional[str] = None,
        api_domain: Optional[str] = None,
        timeout: int = 10,
    ):
        self.access_token = access_token or os.environ.get("ZOHO_ACCESS_TOKEN")
        self.api_domain = api_domain or os.environ.get("ZOHO_API_DOMAIN", "https://www.zohoapis.com")
        self.timeout = timeout
        if not self.access_token:
            raise ValueError("ZOHO_ACCESS_TOKEN is required for Zoho sync")

    def export(self, record: Dict[str, Any]) -> None:
        payload = {"data": [self._map_record(record)]}
        data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        req = urllib.request.Request(
            f"{self.api_domain}/crm/v2/Leads",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Zoho-oauthtoken {self.access_token}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout):
            return None

    def _map_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        owner_name = record.get("owner_name") or "Unknown"
        phone = (record.get("contact_phones") or [None])[0]
        email = (record.get("contact_emails") or [None])[0]
        address = record.get("situs_address") or record.get("mailing_address") or ""
        return {
            "Last_Name": owner_name,
            "Company": "Florida Property Lead",
            "Phone": phone or "",
            "Email": email or "",
            "Street": address,
            "City": "",
            "State": "FL",
            "Lead_Source": "Florida Property Scraper",
            "Description": json.dumps(record, ensure_ascii=True),
        }
