import os
import socket
import sys
import urllib.request
from pathlib import Path
import re

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"

# Prefer repo sources over any installed package.
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    if os.getenv("LIVE") == "1":
        return

    real_connect = socket.socket.connect

    def guarded_connect(sock, address):
        host = address[0]
        if host not in ("127.0.0.1", "localhost"):
            raise RuntimeError("Network access blocked in tests")
        return real_connect(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("Network access blocked in tests")
        ),
    )


def pytest_collection_modifyitems(config, items):
    # Skip integration placeholder unless secret is present.
    if not os.getenv("SCRAPER_API_KEY"):
        skip_integration = pytest.mark.skip(reason="SCRAPER_API_KEY missing")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    # Skip web UI template tests if web_app.py is missing.
    if not (REPO_ROOT / "web_app.py").exists():
        skip_web = pytest.mark.skip(reason="web_app.py missing")
        for item in items:
            if str(item.fspath).endswith("test_web_ui.py"):
                item.add_marker(skip_web)

    # Skip fixture-dependent tests when fixtures are missing.
    for item in items:
        try:
            text = Path(item.fspath).read_text(encoding="utf-8")
        except Exception:
            continue
        if "tests/fixtures/" not in text:
            continue
        missing = []
        for m in re.findall(r"tests/fixtures/([A-Za-z0-9_\-\.]+)", text):
            if not (REPO_ROOT / "tests" / "fixtures" / m).exists():
                missing.append(m)
        if missing:
            item.add_marker(
                pytest.mark.skip(reason=f"fixture missing: {', '.join(sorted(set(missing)))}")
            )
