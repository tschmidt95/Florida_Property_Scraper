import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus, urlencode

import scrapy
from scrapy.http import FormRequest

from florida_property_scraper.arcgis import (
    build_geometry_query_url,
    build_query_url,
    build_where_clause,
    extract_first_field,
)
from florida_property_scraper.county_sources import COUNTY_SOURCES
from florida_property_scraper.scrapy_project.items import PropertyItem

LABEL_PATTERNS = {
    "owner_name": ["owner", "owner name", "property owner"],
    "mailing_address": ["mailing address", "mailing"],
    "situs_address": ["situs", "property address", "site address", "physical address"],
    "parcel_id": ["parcel", "parcel id", "account", "folio"],
    "contact_phones": ["phone", "telephone", "mobile", "cell"],
    "contact_emails": ["email", "e-mail"],
    "zoning_current": ["zoning", "current zoning"],
    "zoning_future": ["future zoning", "future land use"],
}

PURCHASE_HEADERS = {"sale", "price", "buyer", "seller", "deed", "book", "page", "instrument", "date"}
MORTGAGE_HEADERS = {"mortgage", "lender", "loan", "principal", "amount", "instrument", "book", "page", "date"}


class CountySpider(scrapy.Spider):
    name = "county"

    def __init__(
        self,
        query: str,
        counties: Optional[str] = None,
        max_items: Optional[int] = None,
        allow_forms: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.max_items = int(max_items) if max_items else None
        self.allow_forms = allow_forms
        self.items_seen = 0
        self.counties = self._filter_counties(counties)

    def _filter_counties(self, counties: Optional[str]) -> List[Dict[str, Optional[str]]]:
        if not counties:
            return list(COUNTY_SOURCES)
        requested = {c.strip().lower() for c in counties.split(",") if c.strip()}
        filtered = [c for c in COUNTY_SOURCES if c["name"].lower() in requested]
        return filtered or list(COUNTY_SOURCES)

    async def start(self) -> Iterable[scrapy.Request]:
        for request in self.start_requests():
            yield request

    def start_requests(self) -> Iterable[scrapy.Request]:
        query = quote_plus(self.query)
        for county in self.counties:
            ocpa_config = county.get("ocpa")
            if ocpa_config and ocpa_config.get("landing_url"):
                yield scrapy.Request(
                    ocpa_config["landing_url"],
                    callback=self.parse_ocpa_landing,
                    meta={"county": county},
                )
                continue
            pbcpa_config = county.get("pbcpa")
            if pbcpa_config:
                request = self._build_pbcpa_anysearch_request(county, pbcpa_config)
                if request:
                    yield request
                    continue
            vcpa_config = county.get("vcpa")
            if vcpa_config:
                request = self._build_vcpa_search_request(county, vcpa_config)
                if request:
                    yield request
                    continue
            bcpa_config = county.get("bcpa")
            if bcpa_config:
                request = self._build_bcpa_search_request(county, bcpa_config)
                if request:
                    yield request
                    continue
            hcpafl_config = county.get("hcpafl")
            if hcpafl_config:
                url = self._build_hcpafl_search_url(hcpafl_config)
                if url:
                    yield scrapy.Request(
                        url,
                        callback=self.parse_hcpafl_search,
                        meta={"county": county, "hcpafl": hcpafl_config},
                    )
                    continue
            arcgis_config = county.get("arcgis")
            if arcgis_config:
                url = self._build_arcgis_search_url(arcgis_config)
                if url:
                    yield scrapy.Request(
                        url,
                        callback=self.parse_arcgis_search,
                        meta={"county": county, "arcgis": arcgis_config},
                    )
                    continue
            pasco_config = county.get("pasco")
            if pasco_config:
                url = self._build_pasco_search_url(pasco_config)
                if url:
                    yield scrapy.Request(
                        url,
                        callback=self.parse_pasco_search,
                        meta={"county": county, "pasco": pasco_config},
                    )
                    continue
            sarasota_config = county.get("sarasota")
            if sarasota_config and sarasota_config.get("landing_url"):
                yield scrapy.Request(
                    sarasota_config["landing_url"],
                    callback=self.parse_sarasota_landing,
                    meta={"county": county, "sarasota": sarasota_config},
                )
                continue
            lee_config = county.get("lee")
            if lee_config and lee_config.get("landing_url"):
                yield scrapy.Request(
                    lee_config["landing_url"],
                    callback=self.parse_lee_landing,
                    meta={"county": county, "lee": lee_config},
                )
                continue
            if county.get("search_url_template"):
                url = county["search_url_template"].format(query=query)
                yield scrapy.Request(url, callback=self.parse_search_results, meta={"county": county})
            elif county.get("landing_url"):
                yield scrapy.Request(county["landing_url"], callback=self.parse_landing, meta={"county": county})

    def parse_landing(self, response: scrapy.http.Response):
        if not self.allow_forms:
            self.logger.info("Skipping form discovery for %s", response.meta["county"]["name"])
            return
        form_choice = self._choose_form(response)
        if not form_choice:
            self.logger.info("No suitable form found for %s", response.meta["county"]["name"])
            return
        form_index, formdata = form_choice
        yield FormRequest.from_response(
            response,
            formnumber=form_index,
            formdata=formdata,
            callback=self.parse_search_results,
            meta=response.meta,
        )

    def parse_ocpa_landing(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        if not self.allow_forms:
            self.logger.info("Skipping OCPA form submission for %s", county.get("name"))
            return
        hidden_fields = self._extract_hidden_fields(response)
        query = self.query.strip()
        if not query:
            return
        if any(ch.isdigit() for ch in query):
            input_name = self._find_input_name(response, "CompositAddressSearch1$ctl00$Address")
            submit_name = self._find_input_name(response, "CompositAddressSearch1$ctl00$ActionButton1")
        else:
            input_name = self._find_input_name(response, "OwnerNameSearch1$ctl00$OwnerName")
            submit_name = self._find_input_name(response, "OwnerNameSearch1$ctl00$ActionButton1")
        if not input_name or not submit_name:
            self.logger.info("OCPA form fields not found for %s", county.get("name"))
            return
        formdata = dict(hidden_fields)
        formdata[input_name] = query
        formdata[submit_name] = ""
        if "__EVENTTARGET" in formdata:
            formdata["__EVENTTARGET"] = submit_name
            formdata.setdefault("__EVENTARGUMENT", "")
        yield FormRequest(
            response.url,
            formdata=formdata,
            callback=self.parse_lee_results,
            meta=response.meta,
        )

    def parse_lee_results(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        for row in response.css("table tr"):
            cells = row.css("td")
            if len(cells) < 3:
                continue
            folio_link = row.css("a[href*='DisplayParcel.aspx']::attr(href)").get()
            if not folio_link:
                continue
            lines_0 = self._split_lines(cells[0])
            lines_1 = self._split_lines(cells[1])
            lines_2 = self._split_lines(cells[2])
            strap = lines_0[0] if lines_0 else ""
            folio = lines_0[1] if len(lines_0) > 1 else ""
            owner_lines, mailing_lines = self._split_owner_mailing(lines_1)
            situs_lines = self._split_situs(lines_2)
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            item["parcel_id"] = folio or strap
            if owner_lines:
                item["owner_name"] = " ".join(p for p in owner_lines if p)
            mailing_address = ", ".join(p for p in mailing_lines if p)
            if mailing_address:
                item["mailing_address"] = mailing_address
                item.setdefault("contact_addresses", []).append(mailing_address)
            situs_address = ", ".join(p for p in situs_lines if p)
            if situs_address:
                item["situs_address"] = situs_address
            item["property_url"] = response.urljoin(folio_link)
            yield response.follow(folio_link, callback=self.parse_detail, meta={"item": item})
            if self.max_items and self.items_seen >= self.max_items:
                return

    def parse_search_results(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        rows = self._extract_table_rows(response)
        if not rows:
            self.logger.info("No table rows found for %s", county.get("name"))
        for row in rows:
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            item.update(row.get("mapped", {}))
            property_url = row.get("property_url")
            if property_url and property_url.lower().startswith("javascript"):
                property_url = None
            if property_url:
                item["property_url"] = property_url
                yield response.follow(property_url, callback=self.parse_detail, meta={"item": item})
            else:
                yield self._finalize_item(item)
            if self.max_items and self.items_seen >= self.max_items:
                return

    def parse_detail(self, response: scrapy.http.Response):
        item = response.meta.get("item")
        if (
            item
            and item.get("county") == "Volusia"
            and "Owner(s):" not in response.text
            and not response.meta.get("vcpa_cookie_retry")
        ):
            retry_headers = {"Cookie": "acceptedNewDisclaimer=true"}
            retry_request = response.request.replace(
                cookies={"acceptedNewDisclaimer": "true"},
                headers=retry_headers,
            )
            retry_request.meta["item"] = item
            retry_request.meta["vcpa_cookie_retry"] = True
            retry_request.meta["dont_merge_cookies"] = True
            yield retry_request
            return
        item["property_url"] = response.url
        label_pairs = self._extract_label_value_pairs(response)
        for label, value in label_pairs:
            self._apply_label(item, label, value)
        self._extract_history_tables(response, item)
        yield self._finalize_item(item)

    def _finalize_item(self, item: PropertyItem) -> PropertyItem:
        item.setdefault("contact_phones", [])
        item.setdefault("contact_emails", [])
        item.setdefault("contact_addresses", [])
        item.setdefault("mortgage", [])
        item.setdefault("purchase_history", [])
        self.items_seen += 1
        return item

    def _choose_form(self, response: scrapy.http.Response) -> Optional[Tuple[int, Dict[str, str]]]:
        best_score = 0
        best_index = None
        best_formdata = None
        forms = response.css("form")
        query_has_digits = any(ch.isdigit() for ch in self.query)
        for idx, form in enumerate(forms):
            inputs = form.css("input, select")
            fields = {}
            score = 0
            hidden_fields: Dict[str, str] = {}
            submit_name: Optional[str] = None
            for input_tag in inputs:
                name = input_tag.attrib.get("name")
                if not name:
                    continue
                input_type = input_tag.attrib.get("type", "").lower()
                value = input_tag.attrib.get("value", "")
                if input_type == "hidden":
                    hidden_fields[name] = value
                    continue
                if input_type in {"submit", "button", "image"}:
                    if value or input_tag.attrib.get("name"):
                        hidden_fields[name] = value
                        submit_name = submit_name or name
                    continue
                fields[name] = ""
                lower = name.lower()
                if "owner" in lower:
                    score += 3 if not query_has_digits else 1
                if "name" in lower or lower in {"nam", "nams"}:
                    score += 3 if not query_has_digits else 1
                if "address" in lower or lower in {"add", "add1", "add2", "addr"}:
                    score += 3 if query_has_digits else 1
                if "search" in lower:
                    score += 1
            if not fields:
                continue
            query_field = self._best_query_field(fields)
            if not query_field:
                continue
            fields[query_field] = self.query
            if "__EVENTTARGET" in hidden_fields and submit_name:
                hidden_fields["__EVENTTARGET"] = submit_name
                hidden_fields.setdefault("__EVENTARGUMENT", "")
            fields.update(hidden_fields)
            if score > best_score:
                best_score = score
                best_index = idx
                best_formdata = fields
        if best_index is None:
            return None
        return best_index, best_formdata or {}

    def parse_arcgis_search(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        arcgis = response.meta.get("arcgis", {})
        data = response.json()
        features = data.get("features", [])
        zoning_layers = arcgis.get("zoning_layers") or []
        for feature in features:
            attrs = feature.get("attributes", {})
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            situs_fields = arcgis.get("situs_fields") or []
            if situs_fields:
                item["situs_address"] = self._join_fields(
                    [attrs.get(field) for field in situs_fields],
                    separator=", ",
                )
            else:
                item["situs_address"] = attrs.get(arcgis.get("address_field"), "")
            item["parcel_id"] = attrs.get(arcgis.get("parcel_field"), "")
            owner_fields = arcgis.get("owner_fields") or []
            if owner_fields:
                owner_name = self._join_fields(
                    [attrs.get(field) for field in owner_fields],
                    separator="; ",
                )
                if owner_name:
                    item["owner_name"] = owner_name
            mailing_fields = arcgis.get("mailing_fields") or []
            if mailing_fields:
                mailing_address = self._join_fields(
                    [attrs.get(field) for field in mailing_fields],
                    separator=", ",
                )
                if mailing_address:
                    item["mailing_address"] = mailing_address
                    item.setdefault("contact_addresses", []).append(mailing_address)
            zoning_current_field = arcgis.get("zoning_current_field")
            if zoning_current_field:
                zoning_value = attrs.get(zoning_current_field)
                if zoning_value:
                    item["zoning_current"] = zoning_value
            zoning_future_field = arcgis.get("zoning_future_field")
            if zoning_future_field:
                zoning_value = attrs.get(zoning_future_field)
                if zoning_value:
                    item["zoning_future"] = zoning_value
            geometry = feature.get("geometry")
            if zoning_layers and geometry:
                yield from self._chain_zoning_layers(item, geometry, zoning_layers, 0)
            else:
                yield self._finalize_item(item)
            if self.max_items and self.items_seen >= self.max_items:
                return

    def parse_arcgis_zoning(self, response: scrapy.http.Response):
        item = response.meta.get("item")
        zoning_layers = response.meta.get("zoning_layers") or []
        index = response.meta.get("zoning_index", 0)
        layer = zoning_layers[index]
        data = response.json()
        value = extract_first_field(data.get("features", []), layer.get("fields", []))
        if value:
            item[layer.get("target")] = value
        next_index = index + 1
        if next_index < len(zoning_layers):
            geometry = response.meta.get("geometry")
            yield from self._chain_zoning_layers(item, geometry, zoning_layers, next_index)
        else:
            yield self._finalize_item(item)

    def parse_pasco_search(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        seen: set = set()
        for anchor in response.css("a[href*='parcel.aspx']"):
            href = anchor.attrib.get("href")
            if not href:
                continue
            parcel_id = self._clean_text(anchor.css("::text").get() or "")
            if not parcel_id or parcel_id in seen:
                continue
            seen.add(parcel_id)
            row = anchor.xpath("ancestor::tr[1]")
            cells = [self._clean_text(c) for c in row.css("td ::text").getall()]
            if not cells:
                continue
            try:
                idx = cells.index(parcel_id)
            except ValueError:
                idx = 0
            owner_cells = [c for c in cells[idx + 1 : -1] if c and c.lower() != "map"]
            situs_address = cells[-1] if len(cells) > idx + 1 else ""
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            item["parcel_id"] = parcel_id
            if owner_cells:
                item["owner_name"] = " ".join(owner_cells).strip()
            if situs_address:
                item["situs_address"] = situs_address
            item["property_url"] = response.urljoin(href)
            yield self._finalize_item(item)
            if self.max_items and self.items_seen >= self.max_items:
                return

    def parse_sarasota_landing(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        sarasota = response.meta.get("sarasota", {})
        if not self.allow_forms:
            self.logger.info("Skipping Sarasota form submission for %s", county.get("name"))
            return
        form = response.css("form[action*='/propertysearch/result']")
        if not form:
            self.logger.info("No Sarasota search form found for %s", county.get("name"))
            return
        fields = {}
        for input_tag in form.css("input"):
            name = input_tag.attrib.get("name")
            if not name:
                continue
            value = input_tag.attrib.get("value", "")
            input_type = input_tag.attrib.get("type", "").lower()
            if input_type in {"submit", "button"}:
                fields[name] = value
                continue
            if input_type in {"hidden", "text"}:
                fields[name] = value
        query = self.query.strip()
        if not query:
            return
        if any(ch.isdigit() for ch in query):
            fields["AddressKeywords"] = query
        else:
            fields["OwnerKeywords"] = query
        search_url = sarasota.get("search_url") or response.url
        yield FormRequest(
            search_url,
            formdata=fields,
            callback=self.parse_sarasota_results,
            meta={"county": county, "sarasota": sarasota},
        )

    def parse_sarasota_results(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        links = response.css("a[href*='/propertysearch/parcel/details/']::attr(href)").getall()
        if not links:
            self.logger.info("No Sarasota results found for %s", county.get("name"))
            return
        seen: set = set()
        for href in links:
            if href in seen:
                continue
            seen.add(href)
            parcel_id = href.rstrip("/").split("/")[-1]
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            item["parcel_id"] = parcel_id
            yield response.follow(href, callback=self.parse_detail, meta={"item": item})
            if self.max_items and self.items_seen >= self.max_items:
                return

    def parse_lee_landing(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        if not self.allow_forms:
            self.logger.info("Skipping Lee form submission for %s", county.get("name"))
            return
        hidden_fields = self._extract_hidden_fields(response)
        query = self.query.strip()
        if not query:
            return
        formdata = dict(hidden_fields)
        owner_field = "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$OwnerNameTextBox"
        address_field = "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$AddressTextBox"
        search_source = "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SearchSouceGroup"
        submit_target = "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SubmitPropertySearch"
        submit_field = "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SubmitPropertySearch"
        if any(ch.isdigit() for ch in query):
            formdata[owner_field] = ""
            formdata[address_field] = query
            formdata[search_source] = "SiteRadioButton"
        else:
            formdata[owner_field] = query
            formdata[address_field] = ""
            formdata[search_source] = "OwnerRadioButton"
        formdata["__EVENTTARGET"] = submit_target
        formdata.setdefault("__EVENTARGUMENT", "")
        formdata[submit_field] = "Search"
        yield FormRequest(
            response.url,
            formdata=formdata,
            callback=self.parse_lee_results,
            meta=response.meta,
        )

    def parse_hcpafl_search(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        hcpafl = response.meta.get("hcpafl", {})
        data = response.json()
        for row in data or []:
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            item["owner_name"] = row.get("owner", "")
            item["situs_address"] = row.get("address", "")
            item["parcel_id"] = row.get("folio") or row.get("pin") or ""
            pin = row.get("pin")
            parcel_url = self._build_hcpafl_parcel_url(hcpafl, pin)
            if pin and parcel_url:
                yield scrapy.Request(
                    parcel_url,
                    callback=self.parse_hcpafl_parcel,
                    meta={"item": item, "pin": pin},
                )
            else:
                yield self._finalize_item(item)
            if self.max_items and self.items_seen >= self.max_items:
                return

    def parse_hcpafl_parcel(self, response: scrapy.http.Response):
        item = response.meta.get("item")
        pin = response.meta.get("pin", "")
        data = response.json()
        item["parcel_id"] = item.get("parcel_id") or data.get("pin", "") or pin
        owner = data.get("owner")
        if owner:
            item["owner_name"] = owner
        site_address = data.get("siteAddress")
        if site_address:
            item["situs_address"] = site_address
        mailing_address = self._format_hcpafl_mailing_address(data.get("mailingAddress") or {})
        if mailing_address:
            item["mailing_address"] = mailing_address
            item.setdefault("contact_addresses", []).append(mailing_address)
        land_use = data.get("landUse") or {}
        zoning = land_use.get("description")
        if zoning:
            item["zoning_current"] = zoning
        sales_history = data.get("salesHistory") or []
        if sales_history:
            item.setdefault("purchase_history", []).extend(self._map_hcpafl_sales(sales_history))
        yield self._finalize_item(item)

    def parse_bcpa_search(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        bcpa = response.meta.get("bcpa", {})
        data = response.json()
        results = (data.get("d") or {}).get("resultListk__BackingField") or []
        for row in results:
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            item["owner_name"] = self._join_owner_names(
                row.get("ownerName1"),
                row.get("ownerName2"),
            )
            item["situs_address"] = self._join_address(
                row.get("siteAddress1"),
                row.get("siteAddress2"),
            )
            item["parcel_id"] = row.get("folioNumber", "")
            folio = row.get("folioNumber")
            detail_request = self._build_bcpa_parcel_request(bcpa, folio)
            if detail_request:
                detail_request.meta["item"] = item
                yield detail_request
            else:
                yield self._finalize_item(item)
            if self.max_items and self.items_seen >= self.max_items:
                return

    def parse_bcpa_parcel(self, response: scrapy.http.Response):
        item = response.meta.get("item")
        data = response.json().get("d") or {}
        parcels = data.get("parcelInfok__BackingField") or []
        if parcels:
            parcel = parcels[0]
            item["owner_name"] = self._join_owner_names(
                parcel.get("ownerName1"),
                parcel.get("ownerName2"),
            ) or item.get("owner_name", "")
            item["mailing_address"] = self._join_address(
                parcel.get("mailingAddress1"),
                parcel.get("mailingAddress2"),
            )
            if item.get("mailing_address"):
                item.setdefault("contact_addresses", []).append(item["mailing_address"])
            item["situs_address"] = self._join_address(
                parcel.get("situsAddress1") or parcel.get("situsNoUnit"),
                self._join_address(parcel.get("situsCity"), parcel.get("situsZipCode")),
            ) or item.get("situs_address", "")
            zoning = parcel.get("landCalcZoning") or parcel.get("useCode")
            if zoning:
                item["zoning_current"] = zoning
            item.setdefault("purchase_history", []).extend(self._map_bcpa_sales(parcel))
        yield self._finalize_item(item)

    def parse_pbcpa_anysearch(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        pbcpa = response.meta.get("pbcpa", {})
        data = response.json()
        pcns = []
        for row in data or []:
            pcn = row.get("PCN")
            if pcn:
                pcns.append(str(pcn))
        if not pcns:
            return
        request = self._build_pbcpa_details_request(pbcpa, pcns)
        if request:
            request.meta["county"] = county
            yield request

    def parse_pbcpa_details(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        data = response.json()
        for row in data or []:
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            item["parcel_id"] = row.get("PCN", "")
            item["owner_name"] = self._join_owner_names(
                row.get("OWNER_NAME_1"),
                row.get("OWNER_NAME_2"),
            )
            item["situs_address"] = (row.get("SITE_ADDR") or "").strip()
            item["mailing_address"] = self._format_pbcpa_mailing(row)
            if item.get("mailing_address"):
                item.setdefault("contact_addresses", []).append(item["mailing_address"])
            zoning = row.get("ZONING") or row.get("USE_CODE")
            if zoning:
                item["zoning_current"] = zoning
            sale = self._map_pbcpa_sale(row)
            if sale:
                item.setdefault("purchase_history", []).append(sale)
            yield self._finalize_item(item)
            if self.max_items and self.items_seen >= self.max_items:
                return

    def parse_vcpa_search(self, response: scrapy.http.Response):
        county = response.meta.get("county", {})
        vcpa = response.meta.get("vcpa", {})
        data = response.json()
        rows = data.get("data") or []
        for row in rows:
            item = PropertyItem()
            item["county"] = county.get("name")
            item["search_query"] = self.query
            item["source_url"] = response.url
            item["parcel_id"] = row.get("parcel", "")
            owner = row.get("owner")
            if owner:
                item["owner_name"] = owner
            street = row.get("street")
            if street:
                item["situs_address"] = street
            altkey = row.get("altkey") or row.get("DT_RowId")
            detail_request = self._build_vcpa_detail_request(vcpa, altkey)
            if detail_request:
                detail_request.meta["item"] = item
                detail_request.meta["dont_merge_cookies"] = True
                yield detail_request
            else:
                yield self._finalize_item(item)
            if self.max_items and self.items_seen >= self.max_items:
                return

    def _chain_zoning_layers(
        self,
        item: PropertyItem,
        geometry: Dict[str, Any],
        zoning_layers: List[Dict[str, Any]],
        index: int,
    ):
        layer = zoning_layers[index]
        url = build_geometry_query_url(layer["url"], geometry, out_fields=layer.get("fields"))
        yield scrapy.Request(
            url,
            callback=self.parse_arcgis_zoning,
            meta={
                "item": item,
                "geometry": geometry,
                "zoning_layers": zoning_layers,
                "zoning_index": index,
            },
        )

    def _build_arcgis_search_url(self, arcgis: Dict[str, Any]) -> Optional[str]:
        address_field = arcgis.get("address_field")
        parcel_field = arcgis.get("parcel_field")
        search_field = arcgis.get("search_field") or address_field
        out_fields = arcgis.get("out_fields") or []
        if not (search_field and parcel_field and out_fields):
            return None
        where = build_where_clause(self.query, search_field, parcel_field)
        return build_query_url(
            arcgis["search_layer_url"],
            where=where,
            out_fields=out_fields,
            return_geometry=bool(arcgis.get("zoning_layers")),
            limit=10,
        )

    def _build_hcpafl_search_url(self, hcpafl: Dict[str, Any]) -> Optional[str]:
        base = hcpafl.get("base_url")
        if not base:
            return None
        query = self._build_hcpafl_query()
        if not query:
            return None
        return f"{base}/BasicSearch?{query}"

    def _build_pasco_search_url(self, pasco: Dict[str, Any]) -> Optional[str]:
        base = pasco.get("base_url")
        if not base:
            return None
        params = self._build_pasco_query_params()
        if not params:
            return None
        return f"{base}/default.aspx?{urlencode(params)}"

    def _build_hcpafl_parcel_url(self, hcpafl: Dict[str, Any], pin: str) -> Optional[str]:
        base = hcpafl.get("base_url")
        if not base or not pin:
            return None
        return f"{base}/ParcelData?pin={quote_plus(str(pin))}"

    def _build_bcpa_search_request(
        self,
        county: Dict[str, Any],
        bcpa: Dict[str, Any],
    ) -> Optional[scrapy.Request]:
        base = bcpa.get("base_url")
        if not base:
            return None
        payload = self._build_bcpa_payload()
        if not payload:
            return None
        return scrapy.Request(
            f"{base}/GetData",
            method="POST",
            body=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            callback=self.parse_bcpa_search,
            meta={"county": county, "bcpa": bcpa},
        )

    def _build_bcpa_parcel_request(
        self,
        bcpa: Dict[str, Any],
        folio: Optional[str],
    ) -> Optional[scrapy.Request]:
        base = bcpa.get("base_url")
        if not base or not folio:
            return None
        payload = self._build_bcpa_parcel_payload(folio)
        return scrapy.Request(
            f"{base}/getParcelInformation",
            method="POST",
            body=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            callback=self.parse_bcpa_parcel,
        )

    def _build_bcpa_payload(self) -> str:
        query = self.query.strip()
        if not query:
            return ""
        payload = {
            "value": query,
            "cities": "",
            "orderBy": "",
            "pageNumber": "1",
            "pageCount": "10",
            "arrayOfValues": "",
            "selectedFromList": "false",
            "totalCount": "0",
        }
        return json.dumps(payload)

    def _build_bcpa_parcel_payload(self, folio: str) -> str:
        year = str(datetime.utcnow().year)
        payload = {
            "folioNumber": folio,
            "taxyear": year,
            "action": "CURRENT",
            "use": "",
        }
        return json.dumps(payload)

    def _build_pbcpa_anysearch_request(
        self,
        county: Dict[str, Any],
        pbcpa: Dict[str, Any],
    ) -> Optional[scrapy.Request]:
        base = pbcpa.get("api_base")
        uid = pbcpa.get("uid")
        if not base or not uid:
            return None
        query = self.query.strip()
        if not query:
            return None
        payload = {
            "searchText": query,
            "searchType": "address,pcn",
            "searchLimit": 10,
            "uID": uid,
            "version": 2,
        }
        headers = self._build_pbcpa_headers(pbcpa)
        return scrapy.Request(
            f"{base}/anysearch",
            method="POST",
            body=json.dumps(payload),
            headers=headers,
            callback=self.parse_pbcpa_anysearch,
            meta={"county": county, "pbcpa": pbcpa},
        )

    def _build_pbcpa_details_request(
        self,
        pbcpa: Dict[str, Any],
        pcns: List[str],
    ) -> Optional[scrapy.Request]:
        base = pbcpa.get("api_base")
        if not base or not pcns:
            return None
        payload = {
            "functionName": "getPapaPropInfoUpdate",
            "parameters": {"pcnList": pcns},
        }
        headers = self._build_pbcpa_headers(pbcpa)
        return scrapy.Request(
            f"{base}/gisdatajson",
            method="POST",
            body=json.dumps(payload),
            headers=headers,
            callback=self.parse_pbcpa_details,
        )

    def _build_pbcpa_headers(self, pbcpa: Dict[str, Any]) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }
        origin = pbcpa.get("origin")
        referer = pbcpa.get("referer")
        if origin:
            headers["Origin"] = origin
        if referer:
            headers["Referer"] = referer
        return headers

    def _build_vcpa_search_request(
        self,
        county: Dict[str, Any],
        vcpa: Dict[str, Any],
    ) -> Optional[scrapy.Request]:
        base = vcpa.get("api_base")
        if not base:
            return None
        payload = self._build_vcpa_payload()
        if not payload:
            return None
        return scrapy.Request(
            f"{base}/search/real-property",
            method="POST",
            body=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
            callback=self.parse_vcpa_search,
            meta={"county": county, "vcpa": vcpa},
        )

    def _build_vcpa_detail_request(
        self,
        vcpa: Dict[str, Any],
        altkey: Optional[str],
    ) -> Optional[scrapy.Request]:
        detail_base = vcpa.get("detail_base")
        if not detail_base or not altkey:
            return None
        cookie_name = vcpa.get("disclaimer_cookie")
        cookies = {cookie_name: "true"} if cookie_name else None
        headers = {"Cookie": f"{cookie_name}=true"} if cookie_name else None
        return scrapy.Request(
            f"{detail_base}?altkey={quote_plus(str(altkey))}",
            cookies=cookies,
            headers=headers,
            callback=self.parse_detail,
        )

    def _build_hcpafl_query(self) -> str:
        query = self.query.strip()
        if not query:
            return ""
        if query.isdigit():
            return f"folio={quote_plus(query)}"
        if any(ch.isdigit() for ch in query):
            return f"address={quote_plus(query)}"
        return f"owner={quote_plus(query)}"

    def _build_pasco_query_params(self) -> Dict[str, str]:
        query = self.query.strip()
        if not query:
            return {}
        if any(ch.isdigit() for ch in query):
            tokens = query.split()
            street_num = tokens[0] if tokens else query
            street_name = " ".join(tokens[1:]) if len(tokens) > 1 else ""
            return {
                "pid": "add",
                "key": "BHI",
                "add1": street_num,
                "add2": street_name,
                "add": "Submit",
            }
        return {
            "pid": "nam",
            "key": "BHI",
            "nam": query,
            "nams": "Submit",
        }

    def _build_vcpa_payload(self) -> str:
        query = self.query.strip()
        if not query:
            return ""
        payload = {
            "draw": "1",
            "columns[0][data]": "altkey",
            "columns[0][name]": "",
            "columns[0][searchable]": "true",
            "columns[0][orderable]": "true",
            "columns[0][search][value]": "",
            "columns[0][search][regex]": "false",
            "columns[1][data]": "parcel",
            "columns[1][name]": "",
            "columns[1][searchable]": "true",
            "columns[1][orderable]": "true",
            "columns[1][search][value]": "",
            "columns[1][search][regex]": "false",
            "columns[2][data]": "owner",
            "columns[2][name]": "",
            "columns[2][searchable]": "true",
            "columns[2][orderable]": "true",
            "columns[2][search][value]": "",
            "columns[2][search][regex]": "false",
            "columns[3][data]": "street",
            "columns[3][name]": "",
            "columns[3][searchable]": "true",
            "columns[3][orderable]": "true",
            "columns[3][search][value]": "",
            "columns[3][search][regex]": "false",
            "columns[4][data]": "pc",
            "columns[4][name]": "",
            "columns[4][searchable]": "true",
            "columns[4][orderable]": "true",
            "columns[4][search][value]": "",
            "columns[4][search][regex]": "false",
            "order[0][column]": "0",
            "order[0][dir]": "asc",
            "start": "0",
            "length": "10",
            "search[value]": query,
            "search[regex]": "false",
        }
        return urlencode(payload)

    def _best_query_field(self, fields: Dict[str, str]) -> Optional[str]:
        priorities = ["owner", "name", "address", "search", "query", "nam", "nams", "add", "addr"]
        for key in fields:
            if key.lower() == "q":
                return key
        for key in fields:
            lower = key.lower()
            if lower in {"nam", "nams"}:
                return key
            if lower in {"add1", "add2", "addr", "address"}:
                return key
            if any(p in lower for p in priorities):
                return key
        return next(iter(fields.keys()), None)

    def _extract_hidden_fields(self, response: scrapy.http.Response) -> Dict[str, str]:
        fields: Dict[str, str] = {}
        for input_tag in response.css("input[type=hidden]"):
            name = input_tag.attrib.get("name")
            if not name:
                continue
            fields[name] = input_tag.attrib.get("value", "")
        return fields

    def _find_input_name(self, response: scrapy.http.Response, contains: str) -> Optional[str]:
        for input_tag in response.css("input"):
            name = input_tag.attrib.get("name")
            if name and contains in name:
                return name
        return None

    def _extract_table_rows(self, response: scrapy.http.Response) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for table in response.css("table"):
            headers = [self._clean_text(h) for h in table.css("th ::text").getall()]
            header_map = [self._map_header(h) for h in headers] if headers else []
            header_row_index: Optional[int] = None
            if not headers:
                for idx, row in enumerate(table.css("tr")):
                    header_cells = row.css("td")
                    classes = " ".join(cell.attrib.get("class", "") for cell in header_cells).lower()
                    if "hdr" in classes or "header" in classes:
                        headers = [self._clean_text(c) for c in row.css("td ::text").getall()]
                        header_map = [self._map_header(h) for h in headers] if headers else []
                        header_row_index = idx
                        break
            if header_map and not any(header_map):
                continue
            if not header_map:
                link_hint = table.css("a::attr(href)").re_first(
                    r"(parcel|folio|account|detail|property)",
                )
                if not link_hint:
                    continue
            for idx, row in enumerate(table.css("tr")):
                if header_row_index is not None and idx == header_row_index:
                    continue
                cells = [self._clean_text(c) for c in row.css("td ::text").getall()]
                if not cells:
                    continue
                if header_map and len(cells) > len(header_map) and header_map[0] == "owner_name":
                    extra = len(cells) - len(header_map) + 1
                    owner_cells = [c for c in cells[:extra] if c]
                    merged = [" ".join(owner_cells).strip()] + cells[extra:]
                    cells = merged
                mapped: Dict[str, Any] = {}
                if header_map:
                    for key, value in zip(header_map, cells):
                        if key and value:
                            if key in {"contact_phones", "contact_emails", "contact_addresses"}:
                                mapped.setdefault(key, []).append(value)
                            else:
                                mapped[key] = value
                property_url = row.css("a::attr(href)").get()
                has_parcel_link = bool(row.css("a::attr(href)").re_first(r"parcel\\.aspx"))
                if property_url in {"/", "#"}:
                    property_url = None
                if not header_map and (not property_url or "parcel.aspx" not in property_url):
                    continue
                if not mapped and property_url and "parcel.aspx" in property_url and len(cells) >= 3:
                    start_index = 1 if cells and cells[0].lower() == "map" else 0
                    if len(cells) - start_index >= 3:
                        mapped["parcel_id"] = cells[start_index]
                        mapped["owner_name"] = " ".join(cells[start_index + 1 : -1]).strip()
                        mapped["situs_address"] = cells[-1]
                if header_map and not has_parcel_link and (not property_url or "parcel.aspx" not in property_url):
                    continue
                if not mapped and (not property_url or "parcel.aspx" not in property_url):
                    continue
                results.append({"mapped": mapped, "property_url": property_url})
        return results

    def _extract_label_value_pairs(self, response: scrapy.http.Response) -> List[Tuple[str, str]]:
        pairs: List[Tuple[str, str]] = []
        for row in response.css("tr"):
            cells = row.css("th, td")
            if len(cells) < 2:
                continue
            label = self._clean_text(cells[0].css("::text").getall())
            value = self._clean_text(cells[1].css("::text").getall())
            if label and value:
                pairs.append((label, value))
        for dt in response.css("dt"):
            label = self._clean_text(dt.css("::text").getall())
            dd = dt.xpath("following-sibling::dd[1]")
            value = self._clean_text(dd.css("::text").getall())
            if label and value:
                pairs.append((label, value))
        for label_node in response.css("li.med.bold"):
            label = self._clean_text(label_node.css("::text").getall())
            value = self._clean_text(label_node.xpath("following-sibling::li[1]//text()").getall())
            if label and value:
                pairs.append((label, value))
        for label_node in response.css("div.col-sm-5 strong"):
            label = self._clean_text(label_node.css("::text").getall())
            value_node = label_node.xpath(
                "ancestor::div[contains(@class,'col-sm-5')]/following-sibling::div[contains(@class,'col-sm-7')][1]"
            )
            value = self._clean_text(value_node.css("::text").getall())
            if label and value:
                pairs.append((label, value))
        return pairs

    def _extract_history_tables(self, response: scrapy.http.Response, item: PropertyItem) -> None:
        for table in response.css("table"):
            headers = [self._clean_text(h) for h in table.css("th::text").getall()]
            if not headers:
                continue
            lower_headers = {h.lower() for h in headers if h}
            if lower_headers & PURCHASE_HEADERS:
                for row in table.css("tr"):
                    cells = [self._clean_text(c) for c in row.css("td::text").getall()]
                    if len(cells) != len(headers):
                        continue
                    entry = {self._clean_text(h): v for h, v in zip(headers, cells) if v}
                    if entry:
                        item.setdefault("purchase_history", []).append(entry)
            if lower_headers & MORTGAGE_HEADERS:
                for row in table.css("tr"):
                    cells = [self._clean_text(c) for c in row.css("td::text").getall()]
                    if len(cells) != len(headers):
                        continue
                    entry = {self._clean_text(h): v for h, v in zip(headers, cells) if v}
                    if entry:
                        item.setdefault("mortgage", []).append(entry)

    def _apply_label(self, item: PropertyItem, label: str, value: str) -> None:
        norm = self._normalize_label(label)
        for field, patterns in LABEL_PATTERNS.items():
            if any(p in norm for p in patterns):
                if field == "contact_phones":
                    item.setdefault(field, []).extend(self._split_values(value))
                elif field == "contact_emails":
                    item.setdefault(field, []).extend(self._split_values(value))
                elif field == "contact_addresses":
                    item.setdefault(field, []).append(value)
                elif field == "mailing_address":
                    cleaned = value.replace("Update Mailing Address", "").replace("Update Physical Address", "")
                    cleaned = self._clean_text(cleaned)
                    if cleaned:
                        item[field] = cleaned
                        item.setdefault("contact_addresses", []).append(cleaned)
                    return
                else:
                    item[field] = value
                return
        if "future" in norm and "zoning" in norm:
            item["zoning_future"] = value
        elif "zoning" in norm and "future" not in norm:
            item["zoning_current"] = value

    def _map_header(self, header: str) -> Optional[str]:
        norm = self._normalize_label(header)
        for field, patterns in LABEL_PATTERNS.items():
            if any(p in norm for p in patterns):
                return field
        if "address" in norm:
            return "situs_address"
        if "parcel" in norm or "folio" in norm:
            return "parcel_id"
        return None

    def _normalize_label(self, text: str) -> str:
        return re.sub(r"\s+", " ", text.strip().lower())

    def _clean_text(self, value: Any) -> str:
        if isinstance(value, list):
            value = " ".join(value)
        return re.sub(r"\s+", " ", str(value)).strip()

    def _split_values(self, value: str) -> List[str]:
        if not value:
            return []
        parts = re.split(r"[,;/]|\s{2,}", value)
        return [p.strip() for p in parts if p.strip()]

    def _format_hcpafl_mailing_address(self, mailing: Dict[str, Any]) -> str:
        if not mailing:
            return ""
        line1 = mailing.get("addr1", "") or ""
        line2 = mailing.get("addr2", "") or ""
        city = mailing.get("city", "") or ""
        state = mailing.get("state", "") or ""
        zip_code = mailing.get("zip", "") or ""
        street = " ".join(part for part in [line1, line2] if part)
        locality = ", ".join(part for part in [city, state] if part)
        if zip_code:
            locality = f"{locality} {zip_code}".strip()
        return ", ".join(part for part in [street, locality] if part)

    def _map_hcpafl_sales(self, sales: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        history: List[Dict[str, Any]] = []
        for entry in sales:
            record = {
                "sale_date": entry.get("saleDate"),
                "sale_price": entry.get("salePrice"),
                "deed_type": entry.get("deedType"),
                "docnum": entry.get("docnum"),
                "book": entry.get("book"),
                "page": entry.get("page"),
                "qualified": entry.get("qualified"),
            }
            cleaned = {k: v for k, v in record.items() if v not in (None, "", [])}
            if cleaned:
                history.append(cleaned)
        return history

    def _map_bcpa_sales(self, parcel: Dict[str, Any]) -> List[Dict[str, Any]]:
        history: List[Dict[str, Any]] = []
        for idx in range(1, 6):
            date = parcel.get(f"saleDate{idx}")
            price = parcel.get(f"stampAmount{idx}")
            deed = parcel.get(f"deedType{idx}")
            book_page = parcel.get(f"bookAndPageOrCin{idx}")
            record = {
                "sale_date": date,
                "sale_price": price,
                "deed_type": deed,
                "docnum": book_page,
            }
            cleaned = {k: v for k, v in record.items() if v not in (None, "", [])}
            if cleaned:
                history.append(cleaned)
        return history

    def _map_pbcpa_sale(self, row: Dict[str, Any]) -> Dict[str, Any]:
        record = {
            "sale_date": row.get("SALE_DATE"),
            "sale_price": row.get("PRICE"),
            "book": row.get("BOOK"),
            "page": row.get("PAGE"),
        }
        return {k: v for k, v in record.items() if v not in (None, "", [])}

    def _format_pbcpa_mailing(self, row: Dict[str, Any]) -> str:
        line1 = row.get("MAILING_ADDRESS1") or ""
        city = row.get("MAILING_CITY") or ""
        state = row.get("MAILING_STATE") or ""
        zip_code = row.get("MAILING_ZIP") or ""
        locality = ", ".join(part for part in [city, state] if part)
        if zip_code:
            locality = f"{locality} {zip_code}".strip()
        return ", ".join(part for part in [line1.strip(), locality] if part)

    def _join_owner_names(self, name1: Optional[str], name2: Optional[str]) -> str:
        names = [n.strip() for n in [name1, name2] if n and str(n).strip()]
        return "; ".join(names)

    def _join_address(self, line1: Optional[str], line2: Optional[str]) -> str:
        parts = [p.strip() for p in [line1, line2] if p and str(p).strip()]
        return ", ".join(parts)

    def _join_fields(self, values: List[Optional[str]], separator: str = " ") -> str:
        cleaned = [str(v).strip() for v in values if v not in (None, "") and str(v).strip()]
        return separator.join(cleaned)

    def _looks_like_address(self, value: str) -> bool:
        if not value:
            return False
        if " " not in value:
            return False
        has_digit = any(ch.isdigit() for ch in value)
        has_letter = any(ch.isalpha() for ch in value)
        return has_digit and has_letter

    def _split_lines(self, cell: scrapy.selector.unified.Selector) -> List[str]:
        lines: List[str] = []
        for text in cell.css("::text").getall():
            cleaned = self._clean_text(text)
            if cleaned:
                lines.append(cleaned)
        return lines

    def _split_owner_mailing(self, lines: List[str]) -> Tuple[List[str], List[str]]:
        addr_index = None
        for idx, line in enumerate(lines):
            if self._looks_like_address(line):
                addr_index = idx
                break
        if addr_index is None:
            return lines, []
        owner_lines = lines[:addr_index]
        mailing_lines = lines[addr_index:addr_index + 2]
        return owner_lines, mailing_lines

    def _split_situs(self, lines: List[str]) -> List[str]:
        for idx, line in enumerate(lines):
            if self._looks_like_address(line):
                return lines[idx:idx + 2]
        return lines[:2]
