import argparse
from pathlib import Path


SPIDER_TEMPLATE = """from scrapy import FormRequest, Request, Spider

from florida_property_scraper.schema import normalize_item
from florida_property_scraper.spider_utils import (
    extract_label_items,
    extract_label_items_from_nodes,
    next_page_request,
    truncate_html,
)


class {class_name}(Spider):
    name = "{slug}_spider"

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
        nodes = response.css(
            ".{slug}-result, .result-card, .search-result, .property-card"
        )
        items = extract_label_items_from_nodes(nodes, "{slug}")
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
                    "raw_html": truncate_html(response.text),
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
    <div class="{slug}-result">
{row_one}
    </div>
    <div class="{slug}-result">
{row_two}
    </div>
  </body>
</html>
"""


REALISTIC_TEMPLATE = """<html>
  <body>
    <section class="search-result">
{row_one}
    </section>
    <section class="search-result">
{row_two}
    </section>
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


REALISTIC_TEST_TEMPLATE = """from pathlib import Path
from urllib.request import pathname2url

from scrapy.http import TextResponse
from florida_property_scraper.backend import spiders as spiders_pkg
from florida_property_scraper.schema import REQUIRED_FIELDS

{class_name} = spiders_pkg.{module_name}.{class_name}


def test_{slug}_spider_realistic_fixture():
    sample = Path('tests/fixtures/{slug}_realistic.html').absolute()
    file_url = 'file://' + pathname2url(str(sample))

    html = sample.read_bytes()
    resp = TextResponse(url=file_url, body=html)

    spider = {class_name}(start_urls=[file_url])
    items = list(spider.parse(resp))

    assert len(items) >= 2
    for item in items:
        for field in REQUIRED_FIELDS:
            assert field in item
        assert item.get("county") == "{slug}"
        assert item.get("owner")
        assert item.get("address")
"""


def _label_for_field(field: str) -> str:
    label_map = {
        "owner": "Owner",
        "address": "Property Address",
        "land_size": "Land Size",
        "building_size": "Building Size",
        "bedrooms": "Bedrooms",
        "bathrooms": "Bathrooms",
        "zoning": "Zoning",
        "property_class": "Property Class",
    }
    return label_map.get(field, field.replace("_", " ").title())


def _fixture_value(field: str, slug_title: str, row_index: int) -> str:
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


def _render_row(fields, slug_title: str, row_index: int) -> str:
    parts = []
    for field in fields:
        label = _label_for_field(field)
        value = _fixture_value(field, slug_title, row_index)
        parts.append(f"      <div>{label}</div>")
        parts.append(f"      <div>{value}</div>")
    return "\n".join(parts)


def _render_realistic_row(fields, slug_title: str, row_index: int) -> str:
    parts = []
    for field in fields:
        label = _label_for_field(field)
        value = _fixture_value(field, slug_title, row_index)
        parts.append(f"      <div>{label}: {value}</div>")
    return "\n".join(parts)


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


def _update_router(
    router_path: Path,
    slug: str,
    url_template: str,
    pagination: str,
    needs_form_post: bool,
    needs_js: bool,
) -> None:
    content = router_path.read_text(encoding="utf-8")
    if f"\"{slug}\":" in content:
        return
    supports_query_param = not needs_form_post
    needs_pagination = pagination != "none"
    query_style = "form" if needs_form_post else "template"
    form_url = url_template if needs_form_post else ""
    form_fields = {"owner": "{query}"} if needs_form_post else {}
    entry = (
        f"    \"{slug}\": {{\n"
        f"        \"slug\": \"{slug}\",\n"
        f"        \"spider_key\": \"{slug}_spider\",\n"
        f"        \"url_template\": \"{url_template}\",\n"
        f"        \"query_param_style\": \"{query_style}\",\n"
        f"        \"form_url\": \"{form_url}\",\n"
        f"        \"form_fields_template\": {form_fields},\n"
        f"        \"pagination\": \"{pagination}\",\n"
        f"        \"page_param\": \"page\" if \"{pagination}\" == \"page_param\" else \"\",\n"
        f"        \"supports_query_param\": {str(supports_query_param)},\n"
        f"        \"needs_form_post\": {str(needs_form_post)},\n"
        f"        \"needs_pagination\": {str(needs_pagination)},\n"
        f"        \"needs_js\": {str(needs_js)},\n"
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
    parser.add_argument("--url-template", required=True)
    parser.add_argument("--columns", required=True)
    parser.add_argument(
        "--pagination", default="none", choices=["none", "page_param", "next_link"]
    )
    parser.add_argument("--needs-form-post", action="store_true")
    parser.add_argument("--needs-js", action="store_true")
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
    realistic_fixture_path = fixtures_dir / f"{slug}_realistic.html"
    test_path = tests_dir / f"test_{slug}_spider_integration.py"
    realistic_test_path = tests_dir / f"test_{slug}_spider_realistic_fixture.py"

    spider_path.write_text(
        SPIDER_TEMPLATE.format(
            class_name=class_name,
            slug=slug,
        ),
        encoding="utf-8",
    )

    slug_title = slug.replace("_", " ").title()
    row_one = _render_row(columns, slug_title, 1)
    row_two = _render_row(columns, slug_title, 2)
    fixture_path.write_text(
        FIXTURE_TEMPLATE.format(slug=slug, row_one=row_one, row_two=row_two),
        encoding="utf-8",
    )
    realistic_row_one = _render_realistic_row(columns, slug_title, 1)
    realistic_row_two = _render_realistic_row(columns, slug_title, 2)
    realistic_fixture_path.write_text(
        REALISTIC_TEMPLATE.format(row_one=realistic_row_one, row_two=realistic_row_two),
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
    realistic_test_path.write_text(
        REALISTIC_TEST_TEMPLATE.format(
            class_name=class_name,
            module_name=f"{slug}_spider",
            slug=slug,
        ),
        encoding="utf-8",
    )

    _update_registry(spiders_dir / "__init__.py", class_name, slug)
    _update_router(
        base / "src" / "florida_property_scraper" / "county_router.py",
        slug,
        args.url_template,
        args.pagination,
        args.needs_form_post,
        args.needs_js,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
