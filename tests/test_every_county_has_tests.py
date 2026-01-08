from pathlib import Path

from florida_property_scraper.routers.fl_coverage import FL_COUNTIES


def test_every_county_has_tests():
    root = Path(__file__).resolve().parents[1]
    live_slugs = [c["slug"] for c in FL_COUNTIES if c.get("status") == "live"]
    for slug in live_slugs:
        fixture = root / "tests" / "fixtures" / f"{slug}_sample.html"
        test_path = root / "tests" / f"test_{slug}_spider_integration.py"
        spider_path = (
            root
            / "src"
            / "florida_property_scraper"
            / "backend"
            / "spiders"
            / f"{slug}_spider.py"
        )
        assert fixture.exists()
        assert test_path.exists()
        assert spider_path.exists()
