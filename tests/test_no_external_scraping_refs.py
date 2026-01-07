from pathlib import Path


def test_no_external_scraping_refs():
    root = Path(__file__).resolve().parents[1]
    scan_roots = [root / "src", root / "tests"]
    tokens = [
        "scraping" + "bee",
        "scraping" + "-" + "bee",
        "app." + "scraping" + "bee" + ".com",
        "SCRAPING" + "BEE",
    ]
    self_name = Path(__file__).name
    for scan_root in scan_roots:
        if not scan_root.exists():
            continue
        for path in scan_root.rglob("*.py"):
            if path.name == self_name:
                continue
            content = path.read_text(encoding="utf-8", errors="ignore")
            lower = content.lower()
            for token in tokens:
                if token.lower() in lower:
                    raise AssertionError(f"Found disallowed reference in {path}")
