# Statewide County Ingestion Plan

## Overview
This project is designed to scale parcel geometry and Property Appraiser (PA) attribute coverage to all 67 Florida counties. Each county is handled via a registry/config system, with a uniform ingestion and PA adapter interface.

## Adding a New County

1. **Obtain Parcel Geometry**
   - Download authoritative parcel polygons (GeoJSON or Shapefile) from the county GIS or Property Appraiser.
   - Ensure the file includes a unique parcel identifier (e.g., parcel_id, strap, or pid).

2. **Ingest Geometry**
   - Run:
     ```
     python -m florida_property_scraper.tools.ingest_parcels --county <county_name> --input <path-to-geojson>
     ```
   - This loads all polygons and bboxes into the parcels DB and RTree.

3. **Implement/Configure PA Adapter**
   - Add a PA adapter module for the county in `florida_property_scraper/pa_adapters/`.
   - Adapter must provide: `fetch_pa_fields(county, parcel_id) -> dict` with at least owner_name, situs_address, living_area_sqft, last_sale_date, last_sale_price.
   - Register the adapter in the county registry.

4. **Test Proofs**
   - Use the proof script:
     ```
     bash scripts/prove_seminole_ingest_and_polygon_search.sh
     ```
   - Replace `seminole` with the new county as needed.

## Data Sources
- **Seminole:** [Seminole County GIS](https://www.seminolecountyfl.gov/departments-services/information-technology/gis-data/)
- **Orange:** [Orange County OCPA](https://www.ocpafl.org/)
- **Other counties:** See Florida Department of Revenue or each county's GIS/PA portal.

## Registry Example
```python
COUNTY_REGISTRY = {
    'seminole': {
        'geometry_source': 'data/parcels/seminole.geojson',
        'pa_adapter': 'seminole_pa',
    },
    'orange': {
        'geometry_source': 'data/parcels/orange.geojson',
        'pa_adapter': 'orange_pa',
    },
    # ...
}
```

## Uniform Ingest Command
```
python -m florida_property_scraper.tools.ingest_parcels --county <county> --input <path>
```

## Uniform PA Adapter Interface
```
def fetch_pa_fields(county, parcel_id) -> dict:
    # returns: {owner_name, situs_address, living_area_sqft, ...}
```

## Notes
- All counties must use the same DB schema and API contract.
- Adapters should cache results in `parcels_pa` for performance.
- Fallback to demo GeoJSON only if no DB rows exist for a county.
