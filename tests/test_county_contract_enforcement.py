from __future__ import annotations

from pathlib import Path

import pytest
from parsel import Selector


class _FakeRequest:
    def __init__(self, meta: dict[str, object] | None = None) -> None:
        self.meta = meta or {}


class _FakeResponse:
    def __init__(
        self, *, url: str, text: str, request_meta: dict[str, object] | None = None
    ) -> None:
        self.url = url
        self.text = text
        self.request = _FakeRequest(request_meta)
        self._sel = Selector(text=text)

    def css(self, query: str):
        return self._sel.css(query)

    def xpath(self, query: str):
        return self._sel.xpath(query)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _live_counties() -> list[str]:
    from florida_property_scraper.routers.fl_coverage import FL_COUNTIES

    return sorted([c["slug"] for c in FL_COUNTIES if c.get("status") == "live"])


def _assert_capabilities(slug: str) -> None:
    from florida_property_scraper.routers.fl import get_entry

    entry = get_entry(slug)
    if not entry or entry.get("slug") != slug:
        raise AssertionError(
            f"Router entry missing for county '{slug}' in src/florida_property_scraper/routers/fl_coverage.py"
        )

    caps = entry.get("capabilities")
    if not isinstance(caps, dict):
        raise AssertionError(
            f"Capability metadata missing for county '{slug}'. Expected capabilities={{...}} in src/florida_property_scraper/routers/fl_coverage.py"
        )

    required = {
        "supports_query_param",
        "needs_form_post",
        "needs_pagination",
        "needs_js",
        "supports_owner_search",
        "supports_address_search",
    }
    missing = sorted([k for k in required if k not in caps])
    if missing:
        raise AssertionError(
            f"Capability metadata incomplete for '{slug}'. Missing keys: {missing}. Update src/florida_property_scraper/routers/fl_coverage.py"
        )


def _assert_realistic_fixture(slug: str) -> Path:
    fixture = _repo_root() / "tests" / "fixtures" / f"{slug}_realistic.html"
    if not fixture.exists():
        raise AssertionError(
            f"Missing realistic fixture for '{slug}': {fixture}. Add tests/fixtures/{slug}_realistic.html"
        )
    return fixture


def _assert_test_coverage(slug: str) -> None:
    tests_dir = _repo_root() / "tests"
    integration = tests_dir / f"test_{slug}_spider_integration.py"
    if integration.exists():
        return

    # Allow central parametrized tests to count as coverage.
    central_candidates = [
        tests_dir / "test_backend_parity_realistic_fixtures.py",
        tests_dir / "test_native_parsers_realistic_fixtures.py",
    ]
    for p in central_candidates:
        if p.exists() and f'"{slug}"' in p.read_text(encoding="utf-8"):
            return

    raise AssertionError(
        f"County test coverage missing for '{slug}'. "
        f"Expected either {integration} OR include '{slug}' in one of: "
        f"{', '.join(str(p) for p in central_candidates)}"
    )


def _assert_schema_validation(slug: str, fixture: Path) -> None:
    from florida_property_scraper.backend import spiders as spiders_pkg
    from florida_property_scraper.schema.records import normalize_record

    spider_cls = spiders_pkg.SPIDERS.get(slug) or spiders_pkg.SPIDERS.get(
        f"{slug}_spider"
    )
    if spider_cls is None:
        raise AssertionError(
            f"Missing spider registration for '{slug}' in src/florida_property_scraper/backend/spiders/__init__.py"
        )

    html = fixture.read_text(encoding="utf-8")
    resp = _FakeResponse(url="file://fixture", text=html)
    spider = spider_cls(start_urls=["file://fixture"])
    items = list(spider.parse(resp))
    if not items:
        raise AssertionError(
            f"Realistic fixture produced no items for '{slug}': {fixture}"
        )

    # Validate records against canonical schema.
    for i, item in enumerate(items[:25]):
        try:
            normalize_record(item)
        except Exception as e:
            raise AssertionError(
                f"Schema validation failed for '{slug}' on item index {i} from {fixture}: {e}"
            )


@pytest.mark.parametrize("slug", _live_counties())
def test_county_contract_enforcement(slug: str):
    _assert_capabilities(slug)
    fixture = _assert_realistic_fixture(slug)
    _assert_test_coverage(slug)
    _assert_schema_validation(slug, fixture)
