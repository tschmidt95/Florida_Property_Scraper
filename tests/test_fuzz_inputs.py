import json
import random
import string
import subprocess
import sys

from florida_property_scraper.county_router import canonicalize_county_name
from florida_property_scraper.security import sanitize_path
from pathlib import Path


def _rand_str():
    chars = string.printable + "\u2603\u202e"
    return "".join(random.choice(chars) for _ in range(16))


def test_fuzz_inputs_no_crash(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    for _ in range(120):
        s = _rand_str()
        canonicalize_county_name(s)
        try:
            sanitize_path(str(tmp_path / s), project_root)
        except Exception:
            pass

    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--demo",
        "--query",
        _rand_str(),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode in (0, 1, 2)
    assert "Traceback" not in proc.stdout
    if proc.stdout.strip().startswith("{"):
        json.loads(proc.stdout.splitlines()[-1])
