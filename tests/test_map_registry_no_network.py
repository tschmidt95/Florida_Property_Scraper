from florida_property_scraper.map_layer import registry


def test_map_registry_import():
    provider = registry.get_provider("fl", "broward")
    assert provider is not None
