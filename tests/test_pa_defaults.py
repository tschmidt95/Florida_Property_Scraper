import json
from dataclasses import fields

from florida_property_scraper.pa.normalize import apply_defaults
from florida_property_scraper.pa.schema import PAProperty


def test_apply_defaults_populates_all_fields_and_coerces_none():
    partial = {
        "county": "broward",
        "parcel_id": "X",
        "assessed_value": None,
        "owner_names": None,
        "exemptions": None,
    }
    rec = apply_defaults(partial)
    assert isinstance(rec, PAProperty)

    d = rec.to_dict()
    expected_keys = {f.name for f in fields(PAProperty)}
    assert set(d.keys()) == expected_keys

    assert rec.county == "broward"
    assert rec.parcel_id == "X"
    assert rec.assessed_value == 0
    assert rec.owner_names == []
    assert rec.exemptions == []


def test_apply_defaults_ignores_extra_keys():
    rec = apply_defaults({"county": "broward", "parcel_id": "Y", "extra": 123})
    assert rec.county == "broward"
    assert rec.parcel_id == "Y"
