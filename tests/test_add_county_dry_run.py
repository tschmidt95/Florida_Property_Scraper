from pathlib import Path

from scripts import add_county


def test_add_county_dry_run_no_writes(tmp_path):
    base = tmp_path / "repo"
    routers_dir = base / "src" / "florida_property_scraper" / "routers"
    spiders_dir = base / "src" / "florida_property_scraper" / "backend" / "spiders"
    tests_dir = base / "tests"
    fixtures_dir = tests_dir / "fixtures"
    routers_dir.mkdir(parents=True, exist_ok=True)
    spiders_dir.mkdir(parents=True, exist_ok=True)
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    tests_dir.mkdir(parents=True, exist_ok=True)
    (routers_dir / "fl.py").write_text("_ENTRIES = {}\n", encoding="utf-8")
    (spiders_dir / "__init__.py").write_text("SPIDERS = {}\n", encoding="utf-8")

    result = add_county.scaffold_county(
        base_dir=base,
        slug="dryrun",
        columns=["owner", "address"],
        url_template="https://example.gov/search?owner={query}",
        pagination="none",
        needs_form_post=False,
        needs_js=False,
        state="fl",
        status="live",
        dry_run=True,
        force=False,
    )

    assert result["planned"]
    assert not (spiders_dir / "dryrun_spider.py").exists()
