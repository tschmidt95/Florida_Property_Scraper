import json
import subprocess
import sys


def test_cli_json_output_appends(tmp_path):
    output_path = tmp_path / "out.json"
    base_cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--demo",
        "--query",
        "Smith",
        "--output",
        str(output_path),
        "--format",
        "json",
        "--append-output",
        "--no-store",
    ]
    proc = subprocess.run(base_cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    proc = subprocess.run(base_cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    assert len(payload) == 2
