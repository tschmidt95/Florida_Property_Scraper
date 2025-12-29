class FloridaPropertyScraper:
    def __init__(self, scrapingbee_api_key: str):
        self.scrapingbee_api_key = scrapingbee_api_key
        self.base_url = "https://app.scrapingbee.com/api/v1/"
        self.counties = [
            {"name": "Alachua", "url": "https://www.alachuaclerk.com/propertysearch/search.aspx?owner="},
            {"name": "Baker", "url": "https://www.bakercountyfl.org/239/Property-Search"},
            {"name": "Bay", "url": "https://www.baycountyfl.gov/property-search/"},
            {"name": "Bradford", "url": "https://www.bradfordcountyfl.gov/departments/clerk_of_court/property_records/index.php"},
            {"name": "Brevard", "url": "https://www.brevardcounty.us/propertysearch"},
            {"name": "Broward", "url": "https://www.broward.org/propertysearch/Pages/OwnerSearch.aspx?owner="},
            {"name": "Calhoun", "url": "https://www.calhouncountyfl.com/departments/property_appraiser"},
            {"name": "Charlotte", "url": "https://www.charlottecountyfl.gov/agencies-departments/property-appraiser/property-search"},
            {"name": "Citrus", "url": "https://www.citruscounty-fl.gov/property-search/"},
            {"name": "Clay", "url": "https://www.claycountyfl.gov/property-search"},
            {"name": "Collier", "url": "https://www.colliercountyfl.gov/property-search"},
            {"name": "Columbia", "url": "https://www.columbiacountyfl.org/property-search/"},
            {"name": "Dade (Miami-Dade)", "url": "https://www.miami-dadeclerk.com/ocs/Search.aspx"},
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
            {"name": "Hillsborough", "url": "https://www.hillsboroughcounty.org/property-search"},
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
            {"name": "Orange", "url": "https://www.orangecountyfl.net/property-search"},
            {"name": "Osceola", "url": "https://www.osceola.org/property-search/"},
            {"name": "Palm Beach", "url": "https://www.pbcgov.org/papa/searchproperty.aspx?owner="},
            {"name": "Pasco", "url": "https://www.pascopa.com/property-search"},
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
            {"name": "Washington", "url": "https://www.washingtoncountyfl.com/property-search/"}
        ]

    def scrape_county(self, county: Dict, query: str) -> List[Dict]:
        search_url = county["url"] + query.replace(" ", "+")
        params = {
            "api_key": self.scrapingbee_api_key,
            "url": search_url,
            "render_js": "false",
            "extract_rules": json.dumps({
                "properties": {
                    "selector": "table tr",
                    "type": "list",
                    "output": {
                        "owner": "td:nth-child(1)",
                        "address": "td:nth-child(2)",
                        "value": "td:nth-child(3)",
                        "phone": "td:nth-child(4)",
                        "email": "td:nth-child(5)",
                        "property_id": "td:nth-child(6) a@href"
                    }
                }
            })
        }
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            properties = data.get("properties", [])
            for prop in properties:
                prop["county"] = county["name"]
            return properties
        except requests.RequestException as e:
            print(f"Error scraping {county['name']}: {e}")
            return []

    def get_ownership_details(self, county: Dict, property_id: str) -> Dict:
        detail_url = county["url"].replace("search.aspx?owner=", "details.aspx?id=") + property_id
        params = {
            "api_key": self.scrapingbee_api_key,
            "url": detail_url,
            "render_js": "false",
            "extract_rules": json.dumps({
                "details": {
                    "owner": "#owner",
                    "phone": "#phone",
                    "email": "#email",
                    "address": "#address",
                    "value": "#value",
                    "mobile": "#mobile"
                }
            })
        }
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("details", {})
        except requests.RequestException as e:
            print(f"Error getting details for {county['name']}: {e}")
            return {}

    def search_all_counties(self, query: str) -> List[Dict]:
        all_results = []
        for county in self.counties:
            print(f"Searching {county['name']}...")
            results = self.scrape_county(county, query)
            all_results.extend(results)
            time.sleep(1)
        return all_results

    def get_detailed_info(self, county_name: str, property_id: str) -> Dict:
        county = next((c for c in self.counties if c["name"] == county_name), None)
        if not county:
            return {}
        return self.get_ownership_details(county, property_id)

def main():
    scraper = FloridaPropertyScraper("your-scrapingbee-api-key-here")
    query = input("Enter owner name or address to search: ")
    results = scraper.search_all_counties(query)
    print(f"Found {len(results)} properties:")
    for i, result in enumerate(results):
        print(f"{i+1}. {result['county']}: {result['owner']} - {result['address']} - Value: {result['value']}")
        if 'property_id' in result:
            details = scraper.get_detailed_info(result['county'], result['property_id'])
            if details:
                print(f"   Details: Phone: {details.get('phone', 'N/A')}, Mobile: {details.get('mobile', 'N/A')}, Email: {details.get('email', 'N/A')}")

if __name__ == "__main__":
    main()