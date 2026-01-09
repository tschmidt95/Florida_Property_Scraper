import os
import socket
import sys
import urllib.request
from pathlib import Path

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
