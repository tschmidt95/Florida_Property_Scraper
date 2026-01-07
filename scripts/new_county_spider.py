import argparse
from pathlib import Path


DEFAULT_COLUMNS = [
    "owner",
    "address",
]


SPIDER_TEMPLATE = """from scrapy import Spider

from florida_property_scraper.schema import REQUIRED_FIELDS, normalize_item


class {class_name}(Spider):
    name = "{spider_name}"

    def __init__(self, start_urls=None, *a, **kw):
        super().__init__(*a, **kw)
        self.start_urls = start_urls or []

    def parse(self, response):
        # Expect a simple table with rows where tds map to configured columns
        rows = response.css('table tr')
        for row in rows:
            cells = [c.get() for c in row.css('td::text')]
            item = {{field: "" for field in REQUIRED_FIELDS}}
            item["county"] = "{county}"
            item["raw_html"] = row.get() or response.text[:2000]
            for idx, field in enumerate({columns}):
                value = cells[idx].strip() if idx < len(cells) and cells[idx] else ""
                item[field] = value
            if item["owner"] or item["address"]:
                yield normalize_item(item)
"""


FIXTURE_TEMPLATE = """<html>
  <body>
    <table>
      <tr>{row_one}</tr>
      <tr>{row_two}</tr>
    </table>
  </body>
</html>
"""


TEST_TEMPLATE = """from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from florida_property_scraper.backend import spiders as spiders_pkg
from florida_property_scraper.schema import REQUIRED_FIELDS

{class_name} = spiders_pkg.{module_name}.{class_name}


def test_{county}_spider_collects_items():
    sample = Path('tests/fixtures/{county}_sample.html').absolute()
    file_url = 'file://' + pathname2url(str(sample))

    html = sample.read_bytes()
    resp = TextResponse(url=file_url, body=html)

    spider = {class_name}(start_urls=[file_url])
    items = list(spider.parse(resp))

    assert isinstance(items, list)
    assert len(items) >= 2
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
        assert item.get("county") == "{county}"
        assert item.get("owner")
        assert item.get("address")
"""


def _update_registry(registry_path: Path, class_name: str, county: str) -> None:
    content = registry_path.read_text(encoding="utf-8")
    import_line = f"from .{county}_spider import {class_name}"
    if import_line not in content:
        lines = content.splitlines()
        insert_at = 0
        for idx, line in enumerate(lines):
            if line.startswith("from .") and "spider" in line:
                insert_at = idx + 1
        lines.insert(insert_at, import_line)
        content = "\n".join(lines) + "\n"
    if f"\"{county}\"" not in content:
        content = content.replace(
            "SPIDERS = {",
            "SPIDERS = {\n"
            f"    \"{county}\": {class_name},\n"
            f"    \"{county}_spider\": {class_name},",
            1,
        )
    registry_path.write_text(content, encoding="utf-8")


def _update_registry_test(test_path: Path, county: str) -> None:
    content = test_path.read_text(encoding="utf-8")
    test_name = f"test_spider_registry_contains_{county}"
    if test_name in content:
        return
    addition = (
        f"\n\ndef {test_name}():\n"
        f"    assert '{county}' in SPIDERS\n"
        f"    assert callable(SPIDERS['{county}'])\n"
        f"    assert '{county}_spider' in SPIDERS\n"
        f"    assert callable(SPIDERS['{county}_spider'])\n"
    )
    test_path.write_text(content.rstrip() + addition, encoding="utf-8")


def _fixture_cell(field: str, county_title: str, row_index: int) -> str:
    values = {
        "owner": f"{county_title} Owner {row_index}",
        "address": f"{100 + row_index} Main St",
        "land_size": f"{1500 + row_index * 100} sq ft",
        "building_size": f"{900 + row_index * 50} sq ft",
        "bedrooms": str(2 + row_index),
        "bathrooms": str(1 + row_index),
        "zoning": "R-1",
        "property_class": "Residential",
    }
    return values.get(field, f"{field} {row_index}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("county")
    parser.add_argument(
        "--columns",
        default=",".join(DEFAULT_COLUMNS),
        help="Comma-separated list of columns",
    )
    args = parser.parse_args()

    county = args.county.strip().lower()
    if not county:
        print("County name required.")
        return 1

    columns = [c.strip() for c in args.columns.split(",") if c.strip()]
    if not columns:
        columns = DEFAULT_COLUMNS

    base = Path(__file__).resolve().parents[1]
    spiders_dir = base / "src" / "florida_property_scraper" / "backend" / "spiders"
    tests_dir = base / "tests"
    fixtures_dir = tests_dir / "fixtures"

    class_name = "".join(part.capitalize() for part in county.split("_")) + "Spider"
    spider_path = spiders_dir / f"{county}_spider.py"
    fixture_path = fixtures_dir / f"{county}_sample.html"
    test_path = tests_dir / f"test_{county}_spider_integration.py"

    spider_path.write_text(
        SPIDER_TEMPLATE.format(
            class_name=class_name,
            spider_name=f"{county}_spider",
            county=county,
            columns=columns,
        ),
        encoding="utf-8",
    )

    county_title = county.replace("_", " ").title()
    row_one = "".join(
        f"<td>{_fixture_cell(field, county_title, 1)}</td>" for field in columns
    )
    row_two = "".join(
        f"<td>{_fixture_cell(field, county_title, 2)}</td>" for field in columns
    )
    fixture_path.write_text(
        FIXTURE_TEMPLATE.format(row_one=row_one, row_two=row_two),
        encoding="utf-8",
    )

    test_path.write_text(
        TEST_TEMPLATE.format(
            class_name=class_name,
            module_name=f"{county}_spider",
            county=county,
        ),
        encoding="utf-8",
    )

    registry_path = spiders_dir / "__init__.py"
    _update_registry(registry_path, class_name, county)

    registry_test_path = tests_dir / "test_spider_registry.py"
    if registry_test_path.exists():
        _update_registry_test(registry_test_path, county)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
