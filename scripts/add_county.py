import argparse
from pathlib import Path


SPIDER_TEMPLATE = """from scrapy import FormRequest, Request, Spider

from florida_property_scraper.schema import REQUIRED_FIELDS, normalize_item
from florida_property_scraper.spider_utils import (
    extract_label_items,
    extract_table_items,
    next_page_request,
    truncate_html,
)


class {class_name}(Spider):
    name = "{slug}_spider"
    COLUMNS = {columns}

    def __init__(
        self,
        start_urls=None,
        debug_html=False,
        query="",
        pagination="none",
        page_param="",
        form_url="",
        form_fields_template=None,
        max_pages=3,
        *a,
        **kw,
    ):
        super().__init__(*a, **kw)
        self.start_urls = start_urls or []
        self.debug_html = debug_html
        self.query = query or ""
        self.pagination = pagination or "none"
        self.page_param = page_param or ""
        self.form_url = form_url
        self.form_fields_template = form_fields_template or {{}}
        self.max_pages = max_pages

    def start_requests(self):
        if self.form_url and self.form_fields_template:
            formdata = {{
                k: (v.format(query=self.query) if isinstance(v, str) else v)
                for k, v in self.form_fields_template.items()
            }}
            yield FormRequest(self.form_url, formdata=formdata, meta={{"page": 1}})
            return
        for url in self.start_urls:
            yield Request(url, meta={{"page": 1}})

    def parse(self, response):
        items = extract_table_items(response, self.COLUMNS, "{slug}")
        if not items:
            items = extract_label_items(response, "{slug}")
        if items:
            for item in items:
                item["raw_html"] = truncate_html(item.get("raw_html"))
                yield normalize_item(item)
        elif self.debug_html:
            yield normalize_item(
                {{
                    "county": "{slug}",
                    "raw_html": response.text[:50000],
                }}
            )
        next_req = next_page_request(
            response, self.pagination, self.page_param, self.max_pages
        )
        if next_req:
            yield next_req
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


def test_{slug}_spider_collects_items():
    sample = Path('tests/fixtures/{slug}_sample.html').absolute()
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
        assert item.get("county") == "{slug}"
        assert item.get("owner")
        assert item.get("address")
"""


def _fixture_cell(field: str, slug_title: str, row_index: int) -> str:
    values = {
        "owner": f"{slug_title} Owner {row_index}",
        "address": f"{100 + row_index} Main St",
        "land_size": f"{1500 + row_index * 100} sq ft",
        "building_size": f"{900 + row_index * 50} sq ft",
        "bedrooms": str(2 + row_index),
        "bathrooms": str(1 + row_index),
        "zoning": "R-1",
        "property_class": "Residential",
    }
    return values.get(field, f"{field} {row_index}")


def _update_registry(registry_path: Path, class_name: str, slug: str) -> None:
    content = registry_path.read_text(encoding="utf-8")
    import_line = f"from .{slug}_spider import {class_name}"
    if import_line not in content:
        lines = content.splitlines()
        insert_at = 0
        for idx, line in enumerate(lines):
            if line.startswith("from .") and "spider" in line:
                insert_at = idx + 1
        lines.insert(insert_at, import_line)
        content = "\n".join(lines) + "\n"
    if f"\"{slug}\"" not in content:
        content = content.replace(
            "SPIDERS = {",
            "SPIDERS = {\n"
            f"    \"{slug}\": {class_name},\n"
            f"    \"{slug}_spider\": {class_name},",
            1,
        )
    registry_path.write_text(content, encoding="utf-8")


def _update_router(router_path: Path, slug: str, search_style: str, pagination: str) -> None:
    content = router_path.read_text(encoding="utf-8")
    if f"\"{slug}\":" in content:
        return
    if search_style == "template":
        entry = (
            f"    \"{slug}\": {{\n"
            f"        \"slug\": \"{slug}\",\n"
            f"        \"spider_key\": \"{slug}_spider\",\n"
            f"        \"url_template\": \"https://{slug}.example.gov/search?owner={{query}}\",\n"
            f"        \"query_param_style\": \"template\",\n"
            f"        \"pagination\": \"{pagination}\",\n"
            f"        \"page_param\": \"page\" if \"{pagination}\" == \"page_param\" else \"\",\n"
            f"        \"supports_owner_search\": True,\n"
            f"        \"supports_address_search\": True,\n"
            f"        \"notes\": \"Generated entry.\",\n"
            f"    }},\n"
        )
    else:
        entry = (
            f"    \"{slug}\": {{\n"
            f"        \"slug\": \"{slug}\",\n"
            f"        \"spider_key\": \"{slug}_spider\",\n"
            f"        \"url_template\": \"\",\n"
            f"        \"query_param_style\": \"form\",\n"
            f"        \"form_url\": \"https://{slug}.example.gov/search\",\n"
            f"        \"form_fields_template\": {{\"owner\": \"{{query}}\"}},\n"
            f"        \"pagination\": \"{pagination}\",\n"
            f"        \"page_param\": \"page\" if \"{pagination}\" == \"page_param\" else \"\",\n"
            f"        \"supports_owner_search\": True,\n"
            f"        \"supports_address_search\": True,\n"
            f"        \"notes\": \"Generated entry.\",\n"
            f"    }},\n"
        )
    marker = "_COUNTY_ENTRIES = {"
    if marker in content:
        content = content.replace(marker, marker + "\n" + entry, 1)
        router_path.write_text(content, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug", required=True)
    parser.add_argument("--columns", required=True)
    parser.add_argument("--search-style", required=True, choices=["template", "form"])
    parser.add_argument(
        "--pagination", required=True, choices=["none", "page_param", "next_link"]
    )
    args = parser.parse_args()

    slug = args.slug.strip().lower()
    columns = [c.strip() for c in args.columns.split(",") if c.strip()]
    if not slug or not columns:
        raise SystemExit(1)

    base = Path(__file__).resolve().parents[1]
    spiders_dir = base / "src" / "florida_property_scraper" / "backend" / "spiders"
    tests_dir = base / "tests"
    fixtures_dir = tests_dir / "fixtures"

    class_name = "".join(part.capitalize() for part in slug.split("_")) + "Spider"
    spider_path = spiders_dir / f"{slug}_spider.py"
    fixture_path = fixtures_dir / f"{slug}_sample.html"
    test_path = tests_dir / f"test_{slug}_spider_integration.py"

    spider_path.write_text(
        SPIDER_TEMPLATE.format(
            class_name=class_name,
            slug=slug,
            columns=columns,
        ),
        encoding="utf-8",
    )

    slug_title = slug.replace("_", " ").title()
    row_one = "".join(
        f"<td>{_fixture_cell(field, slug_title, 1)}</td>" for field in columns
    )
    row_two = "".join(
        f"<td>{_fixture_cell(field, slug_title, 2)}</td>" for field in columns
    )
    fixture_path.write_text(
        FIXTURE_TEMPLATE.format(row_one=row_one, row_two=row_two),
        encoding="utf-8",
    )

    test_path.write_text(
        TEST_TEMPLATE.format(
            class_name=class_name,
            module_name=f"{slug}_spider",
            slug=slug,
        ),
        encoding="utf-8",
    )

    _update_registry(spiders_dir / "__init__.py", class_name, slug)
    _update_router(base / "src" / "florida_property_scraper" / "county_router.py", slug, args.search_style, args.pagination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
