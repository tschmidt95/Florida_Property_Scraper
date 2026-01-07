import argparse
from pathlib import Path


ROUTER_TEMPLATE = """import re
from urllib.parse import quote_plus


_ENTRIES = {{
}}


def canonicalize_jurisdiction_name(name: str) -> str:
    if not name:
        return ""
    cleaned = name.strip().lower()
    cleaned = re.sub(r"[\\s\\-]+", "_", cleaned)
    cleaned = re.sub(r"[^a-z0-9_]", "", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def get_entry(jurisdiction: str) -> dict:
    slug = canonicalize_jurisdiction_name(jurisdiction)
    entry = _ENTRIES.get(slug)
    if entry:
        return dict(entry)
    return {{
        "slug": slug,
        "spider_key": f"{{slug}}_spider" if slug else "",
        "url_template": "",
        "query_param_style": "none",
        "pagination": "none",
        "page_param": "",
        "supports_query_param": False,
        "needs_form_post": False,
        "needs_pagination": False,
        "needs_js": False,
        "supports_owner_search": False,
        "supports_address_search": False,
        "notes": "No start url configured.",
    }}


def build_request_plan(jurisdiction: str, query: str) -> dict:
    entry = get_entry(jurisdiction)
    if entry.get("needs_js"):
        return {{
            "start_urls": [],
            "spider_key": entry.get("spider_key", ""),
            "needs_form_post": entry.get("needs_form_post", False),
            "pagination": entry.get("pagination", "none"),
            "page_param": entry.get("page_param", ""),
        }}
    if entry.get("needs_form_post"):
        form_url = entry.get("form_url", "")
        return {{
            "start_urls": [form_url] if form_url else [],
            "spider_key": entry.get("spider_key", ""),
            "needs_form_post": True,
            "pagination": entry.get("pagination", "none"),
            "page_param": entry.get("page_param", ""),
        }}
    if entry.get("supports_query_param"):
        template = entry.get("url_template", "")
        if not template:
            return {{
                "start_urls": [],
                "spider_key": entry.get("spider_key", ""),
                "needs_form_post": False,
                "pagination": entry.get("pagination", "none"),
                "page_param": entry.get("page_param", ""),
            }}
        encoded = quote_plus(query or "")
        return {{
            "start_urls": [template.format(query=encoded)],
            "spider_key": entry.get("spider_key", ""),
            "needs_form_post": False,
            "pagination": entry.get("pagination", "none"),
            "page_param": entry.get("page_param", ""),
        }}
    return {{
        "start_urls": [],
        "spider_key": entry.get("spider_key", ""),
        "needs_form_post": entry.get("needs_form_post", False),
        "pagination": entry.get("pagination", "none"),
        "page_param": entry.get("page_param", ""),
    }}


def build_start_urls(jurisdiction: str, query: str) -> list:
    return build_request_plan(jurisdiction, query)["start_urls"]


def enabled_jurisdictions() -> list:
    return sorted(_ENTRIES.keys())


def enabled_counties() -> list:
    return enabled_jurisdictions()
"""


def _ensure_module_export(init_path: Path, state: str) -> None:
    content = init_path.read_text(encoding="utf-8") if init_path.exists() else ""
    imports = []
    for line in content.splitlines():
        if line.startswith("from . import "):
            imports.append(line.split("from . import ", 1)[1].strip())
    if state not in imports:
        imports.append(state)
    imports = sorted(set(imports))
    lines = [f"from . import {name}" for name in imports]
    if imports:
        lines.append("")
        lines.append("__all__ = [" + ", ".join([f'\"{name}\"' for name in imports]) + "]")
    init_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _register_router(registry_path: Path, state: str) -> None:
    content = registry_path.read_text(encoding="utf-8")
    import_lines = []
    router_entries = []
    in_routers = False
    for line in content.splitlines():
        if line.startswith("from florida_property_scraper.routers import"):
            import_lines.append(line)
        if line.strip().startswith("_ROUTERS = {"):
            in_routers = True
            continue
        if in_routers:
            if line.strip().startswith("}"):
                in_routers = False
                continue
            router_entries.append(line.strip())
    imports = sorted(
        {
            line.split("from florida_property_scraper.routers import ", 1)[1].strip()
            for line in import_lines
        }
        | {state}
    )
    entry_keys = []
    for entry in router_entries:
        if ":" in entry:
            key = entry.split(":", 1)[0].strip().strip("\"")
            entry_keys.append(key)
    entry_keys = sorted(set(entry_keys + [state]))
    if "fl" in entry_keys:
        entry_keys.remove("fl")
        entry_keys.insert(0, "fl")
    lines = [f"from florida_property_scraper.routers import {name}" for name in imports]
    lines.append("")
    lines.append("_ROUTERS = {")
    for key in entry_keys:
        lines.append(f"    \"{key}\": {key},")
    lines.append("}")
    lines.append("")
    lines.append("def get_router(state: str):")
    lines.append("    state_key = (state or \"\").lower()")
    lines.append("    return _ROUTERS.get(state_key)")
    lines.append("")
    lines.append("def enabled_jurisdictions(state: str) -> list:")
    lines.append("    router = get_router(state)")
    lines.append("    if not router:")
    lines.append("        return []")
    lines.append("    return router.enabled_jurisdictions()")
    lines.append("")
    lines.append("def build_start_urls(state: str, jurisdiction: str, query: str) -> list:")
    lines.append("    router = get_router(state)")
    lines.append("    if not router:")
    lines.append("        return []")
    lines.append("    return router.build_start_urls(jurisdiction, query)")
    lines.append("")
    lines.append("def get_entry(state: str, jurisdiction: str) -> dict:")
    lines.append("    router = get_router(state)")
    lines.append("    if not router:")
    lines.append("        return {")
    lines.append("            \"slug\": \"\",")
    lines.append("            \"spider_key\": \"\",")
    lines.append("            \"url_template\": \"\",")
    lines.append("            \"query_param_style\": \"none\",")
    lines.append("            \"pagination\": \"none\",")
    lines.append("            \"page_param\": \"\",")
    lines.append("            \"supports_query_param\": False,")
    lines.append("            \"needs_form_post\": False,")
    lines.append("            \"needs_pagination\": False,")
    lines.append("            \"needs_js\": False,")
    lines.append("            \"supports_owner_search\": False,")
    lines.append("            \"supports_address_search\": False,")
    lines.append("            \"notes\": \"No router configured.\",")
    lines.append("        }")
    lines.append("    return router.get_entry(jurisdiction)")
    lines.append("")
    lines.append("def build_request_plan(state: str, jurisdiction: str, query: str) -> dict:")
    lines.append("    router = get_router(state)")
    lines.append("    if not router:")
    lines.append("        return {\"start_urls\": []}")
    lines.append("    return router.build_request_plan(jurisdiction, query)")
    registry_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def validate_state(state: str) -> bool:
    return bool(state and len(state) == 2 and state.isalpha() and state == state.lower())


def scaffold_state(base_dir: Path, state: str, name: str, dry_run: bool = False) -> dict:
    if not validate_state(state):
        raise SystemExit(1)
    routers_dir = base_dir / "src" / "florida_property_scraper" / "routers"
    routers_dir.mkdir(parents=True, exist_ok=True)

    router_path = routers_dir / f"{state}.py"
    created = []
    if not router_path.exists():
        if not dry_run:
            router_path.write_text(ROUTER_TEMPLATE, encoding="utf-8")
        created.append(str(router_path))

    init_path = routers_dir / "__init__.py"
    registry_path = routers_dir / "registry.py"
    if not dry_run:
        _ensure_module_export(init_path, state)
        _register_router(registry_path, state)
    return {"router_path": str(router_path), "created": created}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    state = args.state.strip().lower()
    if not validate_state(state):
        raise SystemExit(1)
    base = Path(__file__).resolve().parents[1]
    scaffold_state(base, state, args.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
