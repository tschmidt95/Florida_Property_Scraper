from florida_property_scraper.api.geojson import to_featurecollection


def test_geojson_contract():
    features = [
        {
            "geometry": {"type": "Point", "coordinates": [-81.5, 27.8]},
            "properties": {"parcel_id": "P-1", "address": "123 Demo"},
        },
        {
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[-81, 27], [-81, 28], [-80, 28], [-80, 27], [-81, 27]]
                ],
            },
            "properties": {"parcel_id": "P-2", "address": ""},
        },
    ]
    data = to_featurecollection(features, "broward")
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 2
    for feature in data["features"]:
        assert feature["type"] == "Feature"
        assert "geometry" in feature
        props = feature["properties"]
        assert "parcel_id" in props
        assert "county" in props
        assert "address" in props
