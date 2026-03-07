from florida_property_scraper.triggers.evidence_rules import evaluate_triggers_from_evidence


def _result_by_key(results, key: str):
    for item in results:
        if str(item.trigger_key) == key:
            return item
    return None


def test_doc_type_rules_fire_for_divorce_probate_tax_deed():
    rows = [
        {
            "id": 101,
            "county": "seminole",
            "parcel_id": "SEM-001",
            "field": "official_record_doc_type",
            "value": "Final Judgment of Dissolution of Marriage",
            "confidence": 0.9,
            "retrieved_at": "2026-03-06T00:00:00+00:00",
        },
        {
            "id": 102,
            "county": "seminole",
            "parcel_id": "SEM-001",
            "field": "official_record_doc_type",
            "value": "Probate Administration",
            "confidence": 0.8,
            "retrieved_at": "2026-03-05T00:00:00+00:00",
        },
        {
            "id": 103,
            "county": "seminole",
            "parcel_id": "SEM-001",
            "field": "official_record_doc_type",
            "value": "Tax Deed Application",
            "confidence": 0.7,
            "retrieved_at": "2026-03-04T00:00:00+00:00",
        },
    ]

    results = evaluate_triggers_from_evidence(
        county="seminole",
        evidence_rows=rows,
        parcel_ids=["SEM-001"],
        now_iso="2026-03-06T12:00:00+00:00",
    )

    divorce = _result_by_key(results, "divorce_filed")
    probate = _result_by_key(results, "probate_opened")
    tax_deed = _result_by_key(results, "tax_deed_application")

    assert divorce is not None and divorce.fired is True
    assert probate is not None and probate.fired is True
    assert tax_deed is not None and tax_deed.fired is True
