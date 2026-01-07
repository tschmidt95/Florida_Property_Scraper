import json
import subprocess
import sys


def test_cli_writes_jsonl(tmp_path):
    output_path = tmp_path / "out.jsonl"
    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "--demo",
        "--query",
        "Smith",
        "--output",
        str(output_path),
        "--format",
        "jsonl",
        "--no-store",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert output_path.exists()
    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert lines
    item = json.loads(lines[0])
    assert item.get("owner") == "Demo Owner"
