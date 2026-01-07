from pathlib import Path

from florida_property_scraper.county_router import enabled_counties


def test_every_county_has_tests():
    root = Path(__file__).resolve().parents[1]
    for slug in enabled_counties():
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
