import json
import subprocess
import sys

from florida_property_scraper.pa.normalize import apply_defaults
from florida_property_scraper.pa.storage import PASQLite


def _load_sample_records():
    import pathlib

    path = pathlib.Path("tests/fixtures/pa/sample_records.json")
    return json.loads(path.read_text(encoding="utf-8"))


def _build_db(tmp_path):
    db_path = tmp_path / "pa.sqlite"
    store = PASQLite(str(db_path))
    try:
        records = [apply_defaults(r) for r in _load_sample_records()]
        store.upsert_many(records)
    finally:
        store.close()
    return db_path


def test_pa_search_writes_json(tmp_path):
    db_path = _build_db(tmp_path)
    out_path = tmp_path / "out.json"

    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "pa-search",
        "--db",
        str(db_path),
        "--county",
        "broward",
        "--where",
        "land_use_code=OFF",
        "--select",
        "year_built",
        "--out",
        str(out_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert out_path.exists()

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    assert payload
    assert all(r["county"] == "broward" for r in payload)
    assert all("parcel_id" in r for r in payload)
    assert all("year_built" in r for r in payload)


def test_pa_search_writes_csv(tmp_path):
    db_path = _build_db(tmp_path)
    out_path = tmp_path / "out.csv"

    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "pa-search",
        "--db",
        str(db_path),
        "--county",
        "broward",
        "--where",
        "land_use_code=OFF",
        "--select",
        "year_built",
        "--out",
        str(out_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert out_path.exists()

    text = out_path.read_text(encoding="utf-8").strip().splitlines()
    assert text
    header = text[0].split(",")
    # when --select is used, we expect id/county/parcel_id plus selected fields
    assert header[:3] == ["id", "county", "parcel_id"]
    assert "year_built" in header


def test_pa_comps_ranks_deterministically(tmp_path):
    db_path = _build_db(tmp_path)
    out_path = tmp_path / "comps.json"

    cmd = [
        sys.executable,
        "-m",
        "florida_property_scraper",
        "pa-comps",
        "--db",
        str(db_path),
        "--county",
        "broward",
        "--parcel",
        "SUBJECT-001",
        "--top",
        "2",
        "--out",
        str(out_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    report = json.loads(out_path.read_text(encoding="utf-8"))

    assert report["subject"]["parcel_id"] == "SUBJECT-001"
    assert len(report["comps"]) == 2
    assert report["comps"][0]["parcel_id"] == "COMP-002"
