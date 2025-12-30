def test_smoke_import():
    import importlib.util
    spec = importlib.util.find_spec('florida_property_scraper')
    assert spec is not None
    # simple import to ensure module can be imported
    module = __import__('florida_property_scraper')
    assert hasattr(module, '__name__')
