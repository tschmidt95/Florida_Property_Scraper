from typing import Dict, List, Optional


RAW_COUNTY_URLS: List[Dict[str, str]] = [
    {"name": "Alachua", "url": "https://www.alachuaclerk.com/propertysearch/search.aspx?owner="},
    {"name": "Baker", "url": "https://www.bakercountyfl.org/239/Property-Search"},
    {"name": "Bay", "url": "https://www.baycountyfl.gov/property-search/"},
    {"name": "Bradford", "url": "https://www.bradfordcountyfl.gov/departments/clerk_of_court/property_records/index.php"},
    {"name": "Brevard", "url": "https://www.brevardcounty.us/propertysearch"},
    {"name": "Broward", "url": "https://bcpa.net/RecMenu.asp"},
    {"name": "Calhoun", "url": "https://www.calhouncountyfl.com/departments/property_appraiser"},
    {"name": "Charlotte", "url": "https://www.charlottecountyfl.gov/agencies-departments/property-appraiser/property-search"},
    {"name": "Citrus", "url": "https://www.citruscounty-fl.gov/property-search/"},
    {"name": "Clay", "url": "https://www.claycountyfl.gov/property-search"},
    {"name": "Collier", "url": "https://www.colliercountyfl.gov/property-search"},
    {"name": "Columbia", "url": "https://www.columbiacountyfl.org/property-search/"},
    {"name": "Dade (Miami-Dade)", "url": "https://www.miamidadepa.gov/pa/real-estate/property-search.page"},
    {"name": "DeSoto", "url": "https://www.desotocountyfl.gov/property-search/"},
    {"name": "Dixie", "url": "https://www.dixiecountyfl.com/property-search"},
    {"name": "Duval", "url": "https://www.duvalclerk.com/property-search"},
    {"name": "Escambia", "url": "https://www.escambiacounty.org/property-search/"},
    {"name": "Flagler", "url": "https://www.flaglercounty.org/property-search"},
    {"name": "Franklin", "url": "https://www.franklincountyflorida.com/property-search/"},
    {"name": "Gadsden", "url": "https://www.gadsdencountyfl.gov/property-search"},
    {"name": "Gilchrist", "url": "https://www.gilchristcountyfl.com/property-search/"},
    {"name": "Glades", "url": "https://www.gladescountyfl.com/property-search"},
    {"name": "Gulf", "url": "https://www.gulfcountyfl.com/property-search/"},
    {"name": "Hamilton", "url": "https://www.hamiltoncountyfl.gov/property-search"},
    {"name": "Hardee", "url": "https://www.hardeecounty.net/property-search/"},
    {"name": "Hendry", "url": "https://www.hendryfla.net/property-search"},
    {"name": "Hernando", "url": "https://www.hernandocounty.us/property-search/"},
    {"name": "Highlands", "url": "https://www.highlandscountyfl.gov/property-search"},
    {"name": "Hillsborough", "url": "https://gis.hcpafl.org/propertysearch/"},
    {"name": "Holmes", "url": "https://www.holmescountyfl.com/property-search/"},
    {"name": "Indian River", "url": "https://www.ircpa.org/property-search"},
    {"name": "Jackson", "url": "https://www.jacksoncountyfl.net/property-search/"},
    {"name": "Jefferson", "url": "https://www.jeffersoncountyfl.com/property-search"},
    {"name": "Lafayette", "url": "https://www.lafayettecountyfl.com/property-search/"},
    {"name": "Lake", "url": "https://www.lakecountyfl.gov/property-search"},
    {"name": "Lee", "url": "https://www.leeclerk.org/property-search"},
    {"name": "Leon", "url": "https://www.leoncountyfl.gov/property-search/"},
    {"name": "Levy", "url": "https://www.levycounty.org/property-search"},
    {"name": "Liberty", "url": "https://www.libertycountyflorida.com/property-search/"},
    {"name": "Madison", "url": "https://www.madisoncountyfl.com/property-search"},
    {"name": "Manatee", "url": "https://www.manateepao.gov/property-search"},
    {"name": "Marion", "url": "https://www.marioncountyfl.org/property-search/"},
    {"name": "Martin", "url": "https://www.martin.fl.us/property-search"},
    {"name": "Monroe", "url": "https://www.monroecounty-fl.gov/property-search/"},
    {"name": "Nassau", "url": "https://www.nassaucountyfl.com/property-search"},
    {"name": "Okaloosa", "url": "https://www.okaloosaclerk.com/property-search/"},
    {"name": "Okeechobee", "url": "https://www.okeechobeecountyfl.com/property-search"},
    {"name": "Orange", "url": "https://ocpaservices.ocpafl.org/Searches/ParcelSearch.aspx"},
    {"name": "Osceola", "url": "https://www.osceola.org/property-search/"},
    {"name": "Palm Beach", "url": "https://pbcpao.gov/index.htm"},
    {"name": "Pasco", "url": "https://search.pascopa.com"},
    {"name": "Pinellas", "url": "https://www.pinellascounty.org/property-search"},
    {"name": "Polk", "url": "https://www.polkpa.org/property-search/"},
    {"name": "Putnam", "url": "https://www.putnam-fl.com/property-search"},
    {"name": "St. Johns", "url": "https://www.stjohnsclerk.com/property-search"},
    {"name": "St. Lucie", "url": "https://www.stlucieco.gov/property-search"},
    {"name": "Santa Rosa", "url": "https://www.santarosa.fl.gov/property-search/"},
    {"name": "Sarasota", "url": "https://www.sc-pa.com/property-search"},
    {"name": "Seminole", "url": "https://www.seminolecountyfl.gov/property-search"},
    {"name": "Sumter", "url": "https://www.sumtercountyfl.gov/property-search/"},
    {"name": "Suwannee", "url": "https://www.suwanneecountyfl.com/property-search"},
    {"name": "Taylor", "url": "https://www.taylorcountyfl.com/property-search/"},
    {"name": "Union", "url": "https://www.unioncountyfl.com/property-search"},
    {"name": "Volusia", "url": "https://www.volusia.org/property-search"},
    {"name": "Wakulla", "url": "https://www.wakullacounty.gov/property-search/"},
    {"name": "Walton", "url": "https://www.waltoncountyfl.org/property-search"},
    {"name": "Washington", "url": "https://www.washingtoncountyfl.com/property-search/"},
]

ARCGIS_CONFIGS: Dict[str, Dict[str, object]] = {
    "Dade (Miami-Dade)": {
        "search_layer_url": "https://gisfs.miamidade.gov/mdarcgis/rest/services/MD_PA_PropertySearch/MapServer/6",
        "address_field": "TRUE_SITE_ADDR",
        "parcel_field": "FOLIO",
        "out_fields": ["TRUE_SITE_ADDR", "FOLIO"],
        "zoning_layers": [
            {
                "url": "https://gisfs.miamidade.gov/mdarcgis/rest/services/MD_PA_PropertySearch/MapServer/16",
                "fields": ["DESCR"],
                "target": "zoning_current",
            }
        ],
    },
    "Pinellas": {
        "search_layer_url": "https://egis.pinellas.gov/pcpagis/rest/services/Pcpaoorg_b/PropertySearch/MapServer/0",
        "search_field": "SEARCH_RESULTS",
        "address_field": "SITE_ADDRESS",
        "situs_fields": ["SITE_ADDRESS", "SITE_CITYZIP"],
        "parcel_field": "DISPLAY_STRAP",
        "owner_fields": ["OWNER1", "OWNER2"],
        "out_fields": [
            "OWNER1",
            "OWNER2",
            "SITE_ADDRESS",
            "SITE_CITYZIP",
            "DISPLAY_STRAP",
            "SEARCH_RESULTS",
            "ADDRESS_ZIP_CITY",
        ],
    },
    "Manatee": {
        "search_layer_url": "https://gis.manateepao.com/arcgis/rest/services/Website/WebLayers/MapServer/0",
        "address_field": "SITUS_ADDRESS",
        "situs_fields": [
            "SITUS_ADDRESS",
            "SITUS_POSTAL_CITY",
            "SITUS_STATE",
            "SITUS_POSTAL_ZIP",
        ],
        "parcel_field": "PARID",
        "owner_fields": ["PAR_OWNER_NAME1", "PAR_OWNER_NAME2"],
        "mailing_fields": [
            "PAR_MAIL_LABEL1",
            "PAR_MAIL_LABEL2",
            "PAR_MAIL_LABEL3",
            "PAR_MAIL_LABEL4",
            "PAR_MAIL_LABEL5",
            "PAR_MAIL_LABEL6",
        ],
        "zoning_current_field": "PAR_ZONING",
        "zoning_future_field": "PAR_FUTURE_LNDUSE",
        "out_fields": [
            "PARID",
            "PAR_OWNER_NAME1",
            "PAR_OWNER_NAME2",
            "PAR_MAIL_LABEL1",
            "PAR_MAIL_LABEL2",
            "PAR_MAIL_LABEL3",
            "PAR_MAIL_LABEL4",
            "PAR_MAIL_LABEL5",
            "PAR_MAIL_LABEL6",
            "SITUS_ADDRESS",
            "SITUS_POSTAL_CITY",
            "SITUS_STATE",
            "SITUS_POSTAL_ZIP",
            "PAR_ZONING",
            "PAR_FUTURE_LNDUSE",
        ],
    },
}

HCPA_CONFIGS: Dict[str, Dict[str, str]] = {
    "Hillsborough": {
        "base_url": "https://gis.hcpafl.org/CommonServices/property/search",
    }
}

BCPA_CONFIGS: Dict[str, Dict[str, str]] = {
    "Broward": {
        "base_url": "https://web.bcpa.net/BcpaClient/search.aspx",
    }
}

PBCPA_CONFIGS: Dict[str, Dict[str, str]] = {
    "Palm Beach": {
        "api_base": "https://maps.pbc.gov/giswebapi",
        "origin": "https://gis.pbcgov.org",
        "referer": "https://gis.pbcgov.org/papagis",
        "uid": "89540f28-8b9a-4aed-b609-72529f86a3ca",
    }
}

VCPA_CONFIGS: Dict[str, Dict[str, str]] = {
    "Volusia": {
        "api_base": "https://vcpa.vcgov.org/api",
        "detail_base": "https://vcpa.vcgov.org/parcel/summary/",
        "disclaimer_cookie": "acceptedNewDisclaimer",
    }
}

LAKE_CONFIGS: Dict[str, Dict[str, str]] = {
    "Lake": {
        "base_url": "https://lakecopropappr.com",
        "disclaimer_property": "https://lakecopropappr.com/property-disclaimer.aspx?to=%2fproperty-search.aspx%3f",
        "disclaimer_address": "https://lakecopropappr.com/property-disclaimer.aspx?to=%2faddress-search.aspx%3f",
        "property_search_url": "https://lakecopropappr.com/property-search.aspx",
        "address_search_url": "https://lakecopropappr.com/address-search.aspx",
    }
}

OCPA_CONFIGS: Dict[str, Dict[str, str]] = {
    "Orange": {
        "landing_url": "https://ocpaservices.ocpafl.org/Searches/ParcelSearch.aspx",
    }
}

PASCO_CONFIGS: Dict[str, Dict[str, str]] = {
    "Pasco": {
        "base_url": "https://search.pascopa.com",
    }
}

SARASOTA_CONFIGS: Dict[str, Dict[str, str]] = {
    "Sarasota": {
        "landing_url": "https://www.sc-pa.com/",
        "search_url": "https://www.sc-pa.com/propertysearch/result",
    }
}

LEE_CONFIGS: Dict[str, Dict[str, str]] = {
    "Lee": {
        "landing_url": "https://www.leepa.org/Search/PropertySearch.aspx",
    }
}


def build_county_sources() -> List[Dict[str, Optional[str]]]:
    sources: List[Dict[str, Optional[str]]] = []
    for entry in RAW_COUNTY_URLS:
        url = entry["url"]
        arcgis_config = ARCGIS_CONFIGS.get(entry["name"])
        hcpafl_config = HCPA_CONFIGS.get(entry["name"])
        bcpa_config = BCPA_CONFIGS.get(entry["name"])
        pbcpa_config = PBCPA_CONFIGS.get(entry["name"])
        vcpa_config = VCPA_CONFIGS.get(entry["name"])
        lake_config = LAKE_CONFIGS.get(entry["name"])
        ocpa_config = OCPA_CONFIGS.get(entry["name"])
        pasco_config = PASCO_CONFIGS.get(entry["name"])
        sarasota_config = SARASOTA_CONFIGS.get(entry["name"])
        lee_config = LEE_CONFIGS.get(entry["name"])
        if "owner=" in url:
            sources.append(
                {
                    "name": entry["name"],
                    "search_url_template": f"{url}{{query}}",
                    "landing_url": None,
                    "arcgis": arcgis_config,
                    "hcpafl": hcpafl_config,
                    "bcpa": bcpa_config,
                    "pbcpa": pbcpa_config,
                    "vcpa": vcpa_config,
                    "lake": lake_config,
                    "ocpa": ocpa_config,
                    "pasco": pasco_config,
                    "sarasota": sarasota_config,
                    "lee": lee_config,
                }
            )
        else:
            sources.append(
                {
                    "name": entry["name"],
                    "search_url_template": None,
                    "landing_url": url,
                    "arcgis": arcgis_config,
                    "hcpafl": hcpafl_config,
                    "bcpa": bcpa_config,
                    "pbcpa": pbcpa_config,
                    "vcpa": vcpa_config,
                    "lake": lake_config,
                    "ocpa": ocpa_config,
                    "pasco": pasco_config,
                    "sarasota": sarasota_config,
                    "lee": lee_config,
                }
            )
    return sources


COUNTY_SOURCES = build_county_sources()
