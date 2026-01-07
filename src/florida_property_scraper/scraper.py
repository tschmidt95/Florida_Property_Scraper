from typing import Dict, List, Optional

from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter


class FloridaPropertyScraper:
    def __init__(
        self,
        timeout: int = 10,
        stop_after_first: bool = True,
        log_level: Optional[str] = None,
        demo: bool = False,
        counties: Optional[List[str]] = None,
        max_items: Optional[int] = None,
    ):
        """Create a scraper using the Scrapy backend only."""
        self.timeout = timeout
        self.stop_after_first = stop_after_first
        self.log_level = log_level
        self.demo = demo
        self.counties_filter = counties
        self.max_items = max_items
        self.adapter = ScrapyAdapter(demo=demo, timeout=timeout)

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
            {"name": "Taylor", "url": "https://www.taylorcountyfl.com/property-search"},
            {"name": "Union", "url": "https://www.unioncountyfl.com/property-search"},
            {"name": "Volusia", "url": "https://www.volusia.org/property-search"},
            {"name": "Wakulla", "url": "https://www.wakullacounty.gov/property-search/"},
            {"name": "Walton", "url": "https://www.waltoncountyfl.org/property-search"},
            {"name": "Washington", "url": "https://www.washingtoncountyfl.com/property-search/"}
        ]

    def search_all_counties(
        self,
        query: str,
        stop_after_first: Optional[bool] = None,
        counties: Optional[List[str]] = None,
        max_items: Optional[int] = None,
    ) -> List[Dict]:
        if stop_after_first is None:
            stop_after_first = self.stop_after_first
        if counties is None:
            counties = self.counties_filter
        if max_items is None:
            max_items = self.max_items
        all_results: List[Dict] = []
        if self.demo:
            demo_results = self.adapter.search(
                query,
                start_urls=["file://demo"],
                spider_name="broward_spider",
                max_items=max_items,
            )
            return demo_results
        allowed = None
        if counties:
            allowed = {c.strip().lower() for c in counties if c.strip()}
        for county in self.counties:
            if allowed:
                if county["name"].strip().lower() not in allowed:
                    continue
            results = self.adapter.search(
                query,
                start_urls=[county["url"]],
                spider_name=f"{county['name'].strip().lower().replace(' ', '_')}_spider",
                max_items=max_items,
            )
            if results:
                all_results.extend(results)
                if stop_after_first:
                    break
        return all_results
