from pathlib import Path

from scripts import add_county


def test_add_county_defaults_to_fl_router(tmp_path):
    base = tmp_path / "repo"
    routers_dir = base / "src" / "florida_property_scraper" / "routers"
    spiders_dir = base / "src" / "florida_property_scraper" / "backend" / "spiders"
    tests_dir = base / "tests"
    fixtures_dir = tests_dir / "fixtures"
    routers_dir.mkdir(parents=True, exist_ok=True)
    spiders_dir.mkdir(parents=True, exist_ok=True)
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    tests_dir.mkdir(parents=True, exist_ok=True)

    (routers_dir / "fl.py").write_text(
        "import re\nfrom urllib.parse import quote_plus\n\n_ENTRIES = {}\n\n"
        "def canonicalize_jurisdiction_name(name: str) -> str:\n    return name\n\n"
        "def get_entry(jurisdiction: str) -> dict:\n    return _ENTRIES.get(jurisdiction, {})\n\n",
        encoding="utf-8",
    )
    (spiders_dir / "__init__.py").write_text("SPIDERS = {}\n", encoding="utf-8")

    add_county.scaffold_county(
        base_dir=base,
        slug="sample",
        columns=["owner", "address"],
        url_template="https://example.gov/search?owner={query}",
        pagination="none",
        needs_form_post=False,
        needs_js=False,
        state="fl",
        dry_run=False,
        force=False,
    )

    fl_router = (routers_dir / "fl.py").read_text(encoding="utf-8")
    assert "\"sample\"" in fl_router
