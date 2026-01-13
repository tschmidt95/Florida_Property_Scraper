from __future__ import annotations

import os
import time
from dataclasses import dataclass

from bs4 import BeautifulSoup

from florida_property_scraper.permits.models import PermitRecord


"""Seminole County (FL) public permits portal.

Phase 1 portal discovery result (target base URL):
- https://semc-egov.aspgov.com/Click2GovBP/

Notes:
- This module is CI-safe: `parse_permits()` is pure and can be fixture-tested.
- LIVE HTTP is only allowed when `LIVE=1`.
- The exact search endpoint can vary; configure the live search URL template with:
  - `SEMINOLE_PERMITS_SEARCH_URL_TEMPLATE` (must include `{query}`)

Alternate public endpoints observed (not currently targeted):
- https://scccap01.seminolecountyfl.gov/BuildingPermitWebInquiry/
"""


_BASE_URL = "https://semc-egov.aspgov.com/Click2GovBP/"


def _is_live_enabled() -> bool:
    return os.environ.get("LIVE", "0") == "1"


def _rate_limit_sleep():
    time.sleep(1.05)  # <= 1 req/sec


def parse_permits(content: str, source_url: str) -> list[PermitRecord]:
    """Parse permit search results from HTML.

    The real portal HTML can vary; this parser targets the common pattern of an
    HTML table or list containing permit rows.

    Fixture contract: tests provide an HTML file containing a table with columns
    that include Permit #/Number, Address, Type, Status, and at least one date.
    """

    soup = BeautifulSoup(content, "html.parser")

    # Prefer a table with a header containing "Permit".
    tables = soup.find_all("table")
    chosen = None
    for t in tables:
        header_text = " ".join(
            (th.get_text(" ", strip=True) for th in t.find_all(["th", "caption"]))
        ).lower()
        if "permit" in header_text and (
            "address" in header_text or "location" in header_text
        ):
            chosen = t
            break

    if chosen is None and tables:
        chosen = tables[0]

    if chosen is None:
        return []

    # Map columns by header name.
    headers: list[str] = []
    header_row = chosen.find("tr")
    if header_row:
        headers = [
            " ".join(th.get_text(" ", strip=True).split())
            for th in header_row.find_all(["th", "td"])
        ]

    def col_idx(*names: str) -> int | None:
        if not headers:
            return None
        lowered = [h.lower() for h in headers]
        for name in names:
            n = name.lower()
            for i, h in enumerate(lowered):
                if n in h:
                    return i
        return None

    idx_permit = col_idx("permit", "permit #", "permit number")
    idx_addr = col_idx("address", "location")
    idx_type = col_idx("type")
    idx_status = col_idx("status")
    idx_issue = col_idx("issue", "issued")
    idx_final = col_idx("final", "closed", "completed")
    idx_desc = col_idx("description", "work")

    out: list[PermitRecord] = []
    for tr in chosen.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue

        def cell(i: int | None) -> str:
            if i is None or i < 0 or i >= len(cells):
                return ""
            return " ".join(cells[i].get_text(" ", strip=True).split())

        permit_number = cell(idx_permit) if idx_permit is not None else cell(0)
        permit_number = permit_number.strip()
        if not permit_number or permit_number.lower().startswith("permit"):
            continue

        address = cell(idx_addr).strip() or None
        permit_type = cell(idx_type).strip() or None
        status = cell(idx_status).strip() or None
        issue_date = cell(idx_issue).strip() or None
        final_date = cell(idx_final).strip() or None
        description = cell(idx_desc).strip() or None

        raw = " ".join(tr.get_text(" ", strip=True).split())

        out.append(
            PermitRecord(
                county="seminole",
                parcel_id=None,
                address=address,
                permit_number=permit_number,
                permit_type=permit_type,
                status=status,
                issue_date=issue_date,
                final_date=final_date,
                description=description,
                source=source_url,
                raw=raw,
            ).with_truncated_raw()
        )

    return out


@dataclass(frozen=True, slots=True)
class SeminolePermitsScraper:
    county: str = "seminole"

    def search_permits(self, query: str, limit: int) -> list[PermitRecord]:
        if not _is_live_enabled():
            raise RuntimeError(
                "Live permits search is disabled. Re-run with LIVE=1 to enable network access."
            )

        q = (query or "").strip()
        if not q:
            return []

        try:
            lim = int(limit)
        except Exception:
            lim = 25
        lim = max(1, min(lim, 200))

        # NOTE: This portal's search workflow may require form state, cookies, or
        # additional parameters. We provide a configurable URL template so that
        # live operation can be tuned without code changes.
        tpl = os.environ.get(
            "SEMINOLE_PERMITS_SEARCH_URL_TEMPLATE",
            _BASE_URL + "search?query={query}",
        )
        if "{query}" not in tpl:
            raise RuntimeError(
                "SEMINOLE_PERMITS_SEARCH_URL_TEMPLATE must include {query}"
            )

        import urllib.parse

        search_url = tpl.format(query=urllib.parse.quote_plus(q), limit=str(lim))

        # Hard safety: do not allow live probing outside Click2GovBP.
        if not search_url.startswith(_BASE_URL):
            raise RuntimeError(
                "Refusing to fetch outside Click2GovBP base URL; "
                "check SEMINOLE_PERMITS_SEARCH_URL_TEMPLATE"
            )

        # Best-effort robots.txt check.
        _rate_limit_sleep()
        robots_allowed = _best_effort_robots_allowed(base_url=_BASE_URL, url=search_url)
        if robots_allowed is False:
            raise RuntimeError("robots.txt disallows this fetch (best-effort check)")

        # Live fetch with retries/backoff.
        _rate_limit_sleep()
        content = _fetch_text_with_retries(search_url)
        return parse_permits(content, source_url=search_url)[:lim]


def _best_effort_robots_allowed(*, base_url: str, url: str) -> bool | None:
    try:
        import urllib.robotparser
        import urllib.parse
        import httpx

        rp = urllib.robotparser.RobotFileParser()
        robots_url = urllib.parse.urljoin(base_url, "robots.txt")
        r = httpx.get(
            robots_url,
            headers={
                "User-Agent": "FloridaPropertyScraperBot/1.0 (+github.com/tschmidt95/Florida_Property_Scraper)"
            },
            follow_redirects=True,
            timeout=30.0,
        )
        if r.status_code >= 400:
            return None
        rp.parse(r.text.splitlines())
        return bool(rp.can_fetch("*", url))
    except Exception:
        return None


def _fetch_text_with_retries(url: str) -> str:
    import httpx

    ua = "FloridaPropertyScraperBot/1.0 (+github.com/tschmidt95/Florida_Property_Scraper)"

    last_err: Exception | None = None
    for attempt in range(1, 6):
        try:
            resp = httpx.get(
                url,
                headers={"User-Agent": ua, "Accept": "text/html,application/json"},
                follow_redirects=True,
                timeout=30.0,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}")
            return resp.text
        except Exception as e:
            last_err = e
            # Exponential backoff capped ~16s.
            sleep_s = min(2**attempt, 16)
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch {url!r}: {last_err!r}")
