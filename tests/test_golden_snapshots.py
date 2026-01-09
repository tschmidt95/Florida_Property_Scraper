from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import pytest
from parsel import Selector


class _FakeRequest:
    def __init__(self, meta: Dict[str, object] | None = None) -> None:
        self.meta = meta or {}


class _FakeResponse:
    def __init__(self, *, url: str, text: str, request_meta: Dict[str, object] | None = None) -> None:
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


def _golden_dir(county: str) -> Path:
    return _repo_root() / "tests" / "golden" / county


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def _write_or_assert(path: Path, payload: Any) -> None:
    update = os.getenv("UPDATE_GOLDENS", "0") == "1"
    rendered = _canonical_json(payload)

    if update:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
        return

    if not path.exists():
        raise AssertionError(
            f"Missing golden snapshot: {path}. Run: UPDATE_GOLDENS=1 pytest -q {Path(__file__).name}"
        )

    expected = path.read_text(encoding="utf-8")
    # String compare gives the most readable diffs under pytest.
    assert rendered == expected


def _scrape_fixture_records(county: str, *, max_items: int = 5) -> List[Dict[str, Any]]:
    from florida_property_scraper.backend import spiders as spiders_pkg
    from florida_property_scraper.schema.records import normalize_record

    fixture = _repo_root() / "tests" / "fixtures" / f"{county}_realistic.html"
    html = fixture.read_text(encoding="utf-8")
    resp = _FakeResponse(url="file://fixture", text=html)

    spider_cls = spiders_pkg.SPIDERS.get(county) or spiders_pkg.SPIDERS.get(
        f"{county}_spider"
    )
    if spider_cls is None:
        raise AssertionError(f"No spider registered for {county}")

    spider = spider_cls(start_urls=["file://fixture"])
    items = list(spider.parse(resp))
    if not items:
        raise AssertionError(f"No items parsed for {county} from {fixture}")

    # Normalize to the canonical record schema to keep snapshots stable.
    records = []
    for it in items[: max_items]:
        rec = normalize_record(it).to_dict()
        raw_html = rec.get("raw_html") or ""
        rec.pop("raw_html", None)
        rec["raw_html_len"] = len(raw_html)
        records.append(rec)

    records.sort(
        key=lambda r: (
            str(r.get("owner", "")),
            str(r.get("address", "")),
            str(r.get("parcel_id", "")),
        )
    )
    return records


def _geometry_bbox_features(county: str, bbox_raw: str) -> List[Dict[str, Any]]:
    from florida_property_scraper.parcels.geometry_provider import parse_bbox
    from florida_property_scraper.parcels import geometry_registry
    from florida_property_scraper.parcels.geometry_search import geometry_bbox

    # Ensure tests are fixture-only without leaking env to other tests.
    old = os.environ.get("PARCEL_GEOJSON_DIR")
    try:
        os.environ["PARCEL_GEOJSON_DIR"] = str(
            (_repo_root() / "tests" / "fixtures" / "parcels").resolve()
        )
        geometry_registry.get_provider.cache_clear()

        provider = geometry_registry.get_provider(county)
        feats = provider.query(parse_bbox(bbox_raw))
    finally:
        if old is None:
            os.environ.pop("PARCEL_GEOJSON_DIR", None)
        else:
            os.environ["PARCEL_GEOJSON_DIR"] = old
        geometry_registry.get_provider.cache_clear()

    out: List[Dict[str, Any]] = []
    for f in feats:
        bb = geometry_bbox(f.geometry)
        out.append(
            {
                "feature_id": f.feature_id,
                "parcel_id": f.parcel_id,
                "geometry_type": (f.geometry or {}).get("type"),
                "bbox": [round(x, 6) for x in bb] if bb is not None else None,
            }
        )
    out.sort(key=lambda x: str(x.get("parcel_id", "")))
    return out


@pytest.mark.parametrize(
    "county",
    ["orange", "seminole"],
)
def test_golden_native_scrape_realistic_fixture(county: str):
    payload = {
        "county": county,
        "source": "native_adapter.iter_records",
        "fixture": f"tests/fixtures/{county}_realistic.html",
        "records": _scrape_fixture_records(county, max_items=5),
    }
    _write_or_assert(_golden_dir(county) / "native_scrape_realistic.json", payload)


@pytest.mark.parametrize(
    "county,bbox",
    [
        ("seminole", "-81.38,28.64,-81.36,28.66"),
        ("orange", "-81.312,28.535,-81.301,28.543"),
    ],
)
def test_golden_geometry_provider_bbox(county: str, bbox: str):
    payload = {
        "county": county,
        "source": "parcels.geometry_registry",
        "bbox": bbox,
        "features": _geometry_bbox_features(county, bbox),
    }
    _write_or_assert(_golden_dir(county) / "geometry_bbox_features.json", payload)
