from florida_property_scraper.security import neutralize_csv_field


def test_csv_injection_neutralized():
    values = [
        '=CMD("calc")',
        '=HYPERLINK("http://evil")',
        '+SUM(1,2)',
        '@IMPORT("evil")',
    ]
    for value in values:
        safe = neutralize_csv_field(value)
        assert safe.startswith("'")
