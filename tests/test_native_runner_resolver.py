import pytest

from florida_property_scraper.backend.native.native_runner import resolve_parser


@pytest.mark.parametrize(
    "name,slug",
    [
        ("alachua", "alachua"),
        ("alachua_spider", "alachua"),
        ("broward", "broward"),
        ("broward_spider", "broward"),
        ("seminole", "seminole"),
        ("seminole_spider", "seminole"),
        ("orange", "orange"),
        ("orange_spider", "orange"),
        ("palm_beach", "palm_beach"),
        ("palm_beach_spider", "palm_beach"),
        ("miami_dade", "miami_dade"),
        ("miami_dade_spider", "miami_dade"),
        ("hillsborough", "hillsborough"),
        ("hillsborough_spider", "hillsborough"),
        ("pinellas", "pinellas"),
        ("pinellas_spider", "pinellas"),
    ],
)
def test_native_runner_resolver(name, slug):
    parser, resolved = resolve_parser(name)
    assert resolved == slug
    assert callable(parser)
