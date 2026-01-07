import json
import subprocess
import sys


def test_json_logging_per_county():
    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--demo",
        "--query",
        "SecretQuery",
        "--counties",
        "broward",
        "--log-json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.startswith("{")]
    payloads = [json.loads(line) for line in lines]
    assert any(p.get("county") == "broward" for p in payloads)
    assert "SecretQuery" not in proc.stdout
