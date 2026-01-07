import json
import subprocess
import sys
from pathlib import Path


def test_path_traversal_output_rejected(tmp_path):
    bad_paths = [
        "../outside.jsonl",
        "..\\outside.jsonl",
        "/etc/passwd",
        "safe/..\u2215outside.jsonl",
    ]
    for path in bad_paths:
        cmd = [
            sys.executable,
            "-m",
            "florida_property_scraper",
            "--demo",
            "--query",
            "Smith",
            "--output",
            path,
            "--format",
            "jsonl",
            "--no-store",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        assert proc.returncode != 0
        if proc.stdout.strip():
            payload = json.loads(proc.stdout.splitlines()[-1])
            assert "error" in payload


def test_path_traversal_store_rejected(tmp_path):
    bad_paths = [
        "../outside.sqlite",
        "..\\outside.sqlite",
        "/etc/passwd",
        "safe/..\u2215outside.sqlite",
    ]
    for path in bad_paths:
        cmd = [
            sys.executable,
            "-m",
            "florida_property_scraper",
            "--demo",
            "--query",
            "Smith",
            "--store",
            path,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        assert proc.returncode != 0
        if proc.stdout.strip():
            payload = json.loads(proc.stdout.splitlines()[-1])
            assert "error" in payload
