from florida_property_scraper.api.rules import Condition, apply_filters, apply_filters_explain


def test_missing_included_by_default():
    fields = {"living_area_sqft": None, "__missing_ok_fields": ["living_area_sqft"]}
    filters = [Condition(field="living_area_sqft", op=">=", value=1000)]
    assert apply_filters(fields, filters) is True
    passed, reason = apply_filters_explain(fields, filters)
    assert passed is True
    assert reason is None


def test_missing_excluded_when_no_missing_ok():
    fields = {"year_built": None}
    filters = [Condition(field="year_built", op=">=", value=2000)]
    assert apply_filters(fields, filters) is False
    passed, reason = apply_filters_explain(fields, filters)
    assert passed is False
    assert reason == "year_built:missing"


def test_failed_condition_reason():
    fields = {"beds": 2}
    filters = [Condition(field="beds", op=">=", value=4)]
    passed, reason = apply_filters_explain(fields, filters)
    assert passed is False
    assert reason == "beds:>="
