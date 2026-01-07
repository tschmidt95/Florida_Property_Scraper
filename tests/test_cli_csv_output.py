import subprocess
import sys


def test_cli_csv_output_header_once(tmp_path):
    output_path = tmp_path / "out.csv"
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
        "csv",
        "--append-output",
        "--no-store",
    ]
    proc = subprocess.run(base_cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    proc = subprocess.run(base_cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert lines
    header_count = sum(1 for line in lines if line.startswith("county,"))
    assert header_count == 1
