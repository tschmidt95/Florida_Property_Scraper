import pytest

from florida_property_scraper.search.query import build_where


def test_build_where_supports_ops_and_is_parameterized():
    q = build_where(
        [
            "county=broward",
            "year_built>=2000",
            "zip in [\"33301\",\"33302\"]",
            "parcel_id contains \"SUBJECT\"",
            "last_sale_price between 1000000 and 3000000",
        ]
    )

    assert q.where_sql == (
        "county = ? AND year_built >= ? AND zip IN (?,?) AND parcel_id LIKE ? "
        "AND last_sale_price BETWEEN ? AND ?"
    )
    assert q.params == [
        "broward",
        2000,
        "33301",
        "33302",
        "%SUBJECT%",
        1000000.0,
        3000000.0,
    ]


def test_build_where_rejects_unknown_field():
    with pytest.raises(ValueError):
        build_where(["not_a_field=1"])
