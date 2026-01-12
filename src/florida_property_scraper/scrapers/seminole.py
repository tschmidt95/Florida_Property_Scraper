from __future__ import annotations

import time
import urllib.parse
import urllib.robotparser
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

from florida_property_scraper.leads_models import SearchResult


_DEFAULT_UA = "FloridaPropertyScraper/1.0 (+https://github.com/tschmidt95/Florida_Property_Scraper)"


def _now() -> float:
    return time.monotonic()


@dataclass
class _RateLimiter:
    min_interval_s: float = 1.0
    _last_at: float = 0.0

    def wait(self) -> None:
        elapsed = _now() - self._last_at
        remaining = self.min_interval_s - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_at = _now()


def _get_robot_parser(base_url: str, *, session: requests.Session, timeout: float) -> urllib.robotparser.RobotFileParser:
    parsed = urllib.parse.urlparse(base_url)
    robots_url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, "/robots.txt", "", "", ""))

    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    try:
        resp = session.get(robots_url, timeout=timeout)
        if resp.status_code == 200:
            rp.parse(resp.text.splitlines())
        else:
            # If robots is missing/unavailable, default allow.
            rp.parse([])
    except Exception:
        rp.parse([])
    return rp


def _score(query: str, owner: str, address: str) -> int:
    q = (query or "").strip().lower()
    if not q:
        return 50
    o = (owner or "").lower()
    a = (address or "").lower()
    if q == o or q == a:
        return 95
    if q in o or q in a:
        return 80
    return 60


def parse_results(html: str, *, county: str, query: str, base_url: str) -> list[SearchResult]:
    """Parse Seminole HTML into SearchResults.

    This is intentionally conservative and fixture-friendly: it looks for repeated "card"/"result" containers
    and extracts Owner + Address labels.
    """

    soup = BeautifulSoup(html or "", "html.parser")

    results: list[SearchResult] = []

    # Prefer explicit container patterns.
    containers = soup.select(
        ".seminole-result, .result-card, article.search-result, .search-result, .result-row, .property-card"
    )
    if not containers:
        containers = soup.select("article, section, div")

    def text_lines(node) -> list[str]:
        lines = []
        for t in node.stripped_strings:
            s = " ".join(str(t).split())
            if s:
                lines.append(s)
        return lines

    owner_labels = {"owner", "owner name", "owner(s)", "property owner"}
    address_labels = {
        "situs address",
        "site address",
        "property address",
        "address",
        "mailing address",
    }

    def find_value(lines: list[str], labels: set[str]) -> str:
        for idx, line in enumerate(lines):
            lower = line.lower().strip()
            if lower in labels or any(lower.startswith(l + ":") for l in labels):
                if ":" in line:
                    after = line.split(":", 1)[1].strip()
                    if after:
                        return after
                if idx + 1 < len(lines):
                    return lines[idx + 1]
        return ""

    def find_address_like(lines: list[str]) -> str:
        for line in lines:
            # naive street-ish heuristic
            if any(ch.isdigit() for ch in line) and any(tok in line.upper() for tok in [" ST", " AVE", " RD", " DR", " BLVD", " WAY", " LN", " CT", " PL"]):
                return line
        for line in lines:
            if line[:1].isdigit():
                return line
        return ""

    for c in containers:
        lines = text_lines(c)
        owner = find_value(lines, owner_labels)
        address = find_value(lines, address_labels) or find_address_like(lines)

        if not owner or not address:
            continue

        results.append(
            SearchResult(
                owner=owner,
                address=address,
                county=county,
                parcel_id=None,
                source=base_url,
                score=_score(query, owner, address),
            )
        )

    # Deduplicate exact pairs.
    seen: set[tuple[str, str]] = set()
    out: list[SearchResult] = []
    for r in results:
        key = (r.owner.strip().lower(), r.address.strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


class SeminoleScraper:
    county = "Seminole"

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": _DEFAULT_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self._limiter = _RateLimiter(min_interval_s=1.0)

    def _request(self, method: str, url: str, *, data: dict[str, str] | None = None, timeout: float = 20.0) -> requests.Response:
        # Rate limit: 1 req/sec.
        self._limiter.wait()

        # Simple retries/backoff for transient failures.
        backoffs = [0.0, 1.0, 2.0]
        last_exc: Exception | None = None
        for delay in backoffs:
            if delay:
                time.sleep(delay)
            try:
                resp = self._session.request(method, url, data=data, timeout=timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    continue
                return resp
            except Exception as e:
                last_exc = e
                continue
        if last_exc:
            raise last_exc
        raise RuntimeError("Request failed")

    def search(self, query: str, limit: int) -> list[SearchResult]:
        q = (query or "").strip()
        if not q:
            return []

        limit = max(1, min(int(limit or 50), 200))

        base_url = "https://www.seminolecountyfl.gov/property-search"

        # robots.txt best-effort.
        rp = _get_robot_parser(base_url, session=self._session, timeout=10.0)
        if not rp.can_fetch(self._session.headers.get("User-Agent", _DEFAULT_UA), base_url):
            return []

        # This endpoint is configured as a form POST in existing coverage metadata.
        resp = self._request("POST", base_url, data={"owner": q})
        if resp.status_code != 200:
            return []

        results = parse_results(resp.text, county=self.county, query=q, base_url=base_url)
        return results[:limit]
