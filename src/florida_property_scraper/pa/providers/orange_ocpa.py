from __future__ import annotations

import re
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode, urljoin
from urllib.request import HTTPCookieProcessor, Request, build_opener

import http.cookiejar

from florida_property_scraper.parcels.live.fdor_centroids import FDORCentroidClient


OCPA_LANDING_URL = "https://ocpaservices.ocpafl.org/Searches/ParcelSearch.aspx"


_BLOCK_TEXT_NEEDLES = [
    # Keep these fairly specific to avoid false positives (the OCPA HTML can contain
    # random base64-ish blobs where the substring "robot" may appear).
    "captcha",
    "g-recaptcha",
    "hcaptcha",
    "cloudflare",
    "access denied",
    "unusual traffic",
]


def _ua() -> str:
    # Match typical browser UA to reduce trivial blocking.
    return "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _strip_tags(html: str) -> str:
    # Very lightweight HTML -> text for label/value extraction.
    txt = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    txt = re.sub(r"<style[\s\S]*?</style>", " ", txt, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    return _norm_ws(unescape(txt))


def _as_float(v: object) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v)
    s = s.replace(",", "").replace("$", "").strip()
    if not s:
        return None
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _as_int(v: object) -> Optional[int]:
    f = _as_float(v)
    if f is None:
        return None
    i = int(round(f))
    return i if i != 0 else None


def _as_date_iso(v: str) -> Optional[str]:
    s = _norm_ws(v)
    if not s:
        return None

    # Common PA format: MM/DD/YYYY
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    # ISO already
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return m.group(0)

    return None


@dataclass(frozen=True)
class FieldProv:
    source_url: str
    raw_label: str


def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", (s or "").strip())


def _parcel_variants(parcel_id: str) -> List[str]:
    raw = (parcel_id or "").strip()
    digits = _digits_only(raw)
    out: List[str] = []

    def add(v: str) -> None:
        v = (v or "").strip()
        if v and v not in out:
            out.append(v)

    add(raw)
    add(digits)
    add(digits.lstrip("0"))

    # Some UIs show 18-digit straps with separators.
    if len(digits) == 18:
        add(f"{digits[:2]}-{digits[2:5]}-{digits[5:8]}-{digits[8:12]}-{digits[12:]}")
        add(f"{digits[:2]} {digits[2:5]} {digits[5:8]} {digits[8:12]} {digits[12:]}")
        add(f"{digits[:2]}-{digits[2:8]}-{digits[8:]}"
            )

    return [v for v in out if v]


def _extract_hidden_inputs(html: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for m in re.finditer(r"<input[^>]+\btype=(['\"])hidden\1[^>]*>", html, flags=re.I):
        tag = m.group(0)
        name_m = re.search(r"\bname=(['\"])([^'\"]+)\1", tag, flags=re.I)
        if not name_m:
            continue
        val_m = re.search(r"\bvalue=(['\"])([^'\"]*)\1", tag, flags=re.I)
        out[name_m.group(2)] = unescape(val_m.group(2)) if val_m else ""
    return out


def _find_input_name(html: str, contains: str) -> Optional[str]:
    # Find the ASP.NET generated input name that contains a stable substring.
    needle = (contains or "").lower()
    for m in re.finditer(r"<input[^>]+>", html, flags=re.I):
        tag = m.group(0)
        name_m = re.search(r"\bname=(['\"])([^'\"]+)\1", tag, flags=re.I)
        if not name_m:
            continue
        name = name_m.group(2)
        if needle and needle in name.lower():
            return name
    return None


def _extract_label_value_pairs(html: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []

    # <tr><th>Label</th><td>Value</td></tr> or <tr><td>Label</td><td>Value</td></tr>
    for m in re.finditer(
        r"<tr[^>]*>\s*<(?:th|td)[^>]*>(?P<label>[\s\S]*?)</(?:th|td)>\s*<(?:th|td)[^>]*>(?P<value>[\s\S]*?)</(?:th|td)>[\s\S]*?</tr>",
        html,
        flags=re.I,
    ):
        label = _strip_tags(m.group("label"))
        value = _strip_tags(m.group("value"))
        if label and value:
            pairs.append((label, value))

    # <dt>Label</dt><dd>Value</dd>
    dts = list(re.finditer(r"<dt[^>]*>([\s\S]*?)</dt>", html, flags=re.I))
    for dt in dts:
        label = _strip_tags(dt.group(1))
        after = html[dt.end() :]
        dd_m = re.search(r"<dd[^>]*>([\s\S]*?)</dd>", after, flags=re.I)
        if not dd_m:
            continue
        value = _strip_tags(dd_m.group(1))
        if label and value:
            pairs.append((label, value))

    # De-dupe by keeping first occurrence.
    seen: set[Tuple[str, str]] = set()
    out: List[Tuple[str, str]] = []
    for l, v in pairs:
        key = (l.lower(), v)
        if key in seen:
            continue
        seen.add(key)
        out.append((l, v))
    return out


def _extract_div_label_pairs(html: str) -> List[Tuple[str, str]]:
    """Best-effort extraction for div/span label-value layouts."""
    pairs: List[Tuple[str, str]] = []

    # Common pattern: <span class="...Label...">X</span><span class="...Value...">Y</span>
    for m in re.finditer(
        r"<(?:div|span)[^>]*class=\"[^\"]*(?:Label|label)[^\"]*\"[^>]*>(?P<label>[\s\S]*?)</(?:div|span)>\s*"
        r"<(?:div|span)[^>]*class=\"[^\"]*(?:Value|value)[^\"]*\"[^>]*>(?P<value>[\s\S]*?)</(?:div|span)>",
        html,
        flags=re.I,
    ):
        label = _strip_tags(m.group("label"))
        value = _strip_tags(m.group("value"))
        if label and value:
            pairs.append((label, value))

    return pairs


def _label_norm(label: str) -> str:
    s = (label or "").lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _pairs_map(pairs: List[Tuple[str, str]]) -> Dict[str, Tuple[str, str]]:
    out: Dict[str, Tuple[str, str]] = {}
    for label, value in pairs:
        ln = _label_norm(label)
        if not ln:
            continue
        if ln not in out:
            out[ln] = (label, value)
    return out


def _is_blocked(status: int, html: str) -> Tuple[bool, str]:
    if int(status) in {403, 429, 503}:
        return True, f"http_status={status}"
    lower = (html or "").lower()
    for needle in _BLOCK_TEXT_NEEDLES:
        if needle in lower:
            return True, f"html_contains={needle}"
    # Heuristic phrases for bot checks.
    if re.search(r"\bnot\s+a\s+robot\b", lower):
        return True, "html_contains=not_a_robot"
    return False, ""


def _extract_photo_url(detail_html: str, base_url: str) -> Optional[str]:
    """Best-effort extraction of a parcel photo URL from OCPA detail HTML."""

    candidates: List[str] = []
    for m in re.finditer(r"<img[^>]+src=(['\"])([^'\"]+)\1", detail_html or "", flags=re.I):
        src = (m.group(2) or "").strip()
        if not src:
            continue
        if src.startswith("data:"):
            continue
        lower = src.lower()
        if any(x in lower for x in ("logo", "icon", "sprite", "spacer", "captcha")):
            continue
        candidates.append(src)

    if not candidates:
        return None

    def score(src: str) -> int:
        s = src.lower()
        sc = 0
        if any(x in s for x in ("photo", "picture", "image")):
            sc += 4
        if any(x in s for x in ("parcel", "property", "recordcard", "record", "card")):
            sc += 2
        if any(s.endswith(ext) for ext in (".jpg", ".jpeg", ".png")):
            sc += 2
        if any(x in s for x in ("transparent", "blank", "pixel")):
            sc -= 5
        return sc

    best = max(candidates, key=score)
    if score(best) <= 0:
        return None
    try:
        return urljoin(base_url or OCPA_LANDING_URL, best)
    except Exception:
        return None


def _extract_ocpa_result_links(html: str) -> List[Tuple[str, str]]:
    """Return a list of (href, row_text) for OCPA detail candidates.

    OCPA frequently returns detail pages as `/Searches/ParcelSearch.aspx/PID/<digits>`
    instead of the older `DisplayParcel.aspx` result links.
    """
    out: List[Tuple[str, str]] = []
    for m in re.finditer(r"<tr[^>]*>([\s\S]*?)</tr>", html, flags=re.I):
        row_html = m.group(1)
        href_m = re.search(
            r"href=\"([^\"]*(?:DisplayParcel\.aspx[^\"]*|/Searches/ParcelSearch\.aspx/PID/\d+)[^\"]*)\"",
            row_html,
            flags=re.I,
        )
        if not href_m:
            continue
        href = href_m.group(1)
        row_text = _strip_tags(row_html)
        if href:
            out.append((href, row_text))
    # Fallback: if rows weren't captured, try any hrefs.
    if not out:
        hrefs = re.findall(
            r"href=\"([^\"]*(?:DisplayParcel\.aspx[^\"]*|/Searches/ParcelSearch\.aspx/PID/\d+)[^\"]*)\"",
            html,
            flags=re.I,
        )
        out.extend([(h, "") for h in hrefs])
    return out


def _extract_land_use_and_zoning(html: str) -> tuple[Optional[str], Optional[str]]:
    # Land grid table headers include: Land Use Code | Zoning | ...
    m = re.search(
        r"<th[^>]*>\s*Land\s+Use\s+Code\s*</th>[\s\S]*?"
        r"<th[^>]*>\s*Zoning\s*</th>[\s\S]*?"
        r"<tr[^>]*class=\"DataRow\"[^>]*>\s*"
        r"<td[^>]*>(?P<land>.*?)</td>\s*<td[^>]*>(?P<zoning>.*?)</td>",
        html,
        flags=re.I,
    )
    if not m:
        return None, None
    land_use = _strip_tags(m.group("land"))
    zoning = _strip_tags(m.group("zoning"))
    return land_use or None, zoning or None


def _extract_tax_year_values(html: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (land_value, building_value, market_value) for the latest year."""
    lower = (html or "").lower()
    idx = lower.find("tax year values")
    if idx < 0:
        return None, None, None

    # The OCPA markup nests tables heavily; avoid trying to regex-match a whole
    # BenefitsGrid table (it will stop at an inner </table>). Instead, grab a
    # reasonably-large slice and extract the ValueTable blocks in order.
    chunk = html[idx : idx + 60000]
    value_tables = re.findall(
        r"<table[^>]*class=\"[^\"]*ValueTable[^\"]*\"[^>]*>[\s\S]*?</table>",
        chunk,
        flags=re.I,
    )
    # Expect: Land | Building(s) | Feature(s) | Market Value | Assessed Value
    if len(value_tables) < 4:
        return None, None, None

    def first_money(cell_html: str) -> Optional[float]:
        m = re.search(r"\$\s*\d[\d,]*", cell_html)
        return _as_float(m.group(0)) if m else None

    land_v = first_money(value_tables[0])
    bldg_v = first_money(value_tables[1])
    market_v = first_money(value_tables[3])
    return land_v, bldg_v, market_v


def _choose_detail_href(parcel_id: str, links: List[Tuple[str, str]]) -> Optional[str]:
    pid = (parcel_id or "").strip()
    if not links:
        return None

    # Prefer exact match in href or row text.
    for href, txt in links:
        if pid and (pid in href or pid in txt):
            return href

    # Prefer numeric-only match when formatting differs.
    pid_digits = re.sub(r"\D+", "", pid)
    if pid_digits:
        for href, txt in links:
            if pid_digits in re.sub(r"\D+", "", href) or pid_digits in re.sub(
                r"\D+", "", txt
            ):
                return href

    # Otherwise first result.
    return links[0][0]


def _pick(pairs: List[Tuple[str, str]], patterns: Iterable[str]) -> Tuple[Optional[str], Optional[str]]:
    pats = [p.lower() for p in patterns]
    for label, value in pairs:
        norm = label.strip().lower()
        if any(p in norm for p in pats):
            return value, label
    return None, None


def enrich_parcel(parcel_id: str) -> Dict[str, Any]:
    """Enrich an Orange County parcel using the Orange County Property Appraiser (OCPA).

    This function must fetch real OCPA data and return normalized fields + per-field provenance.

    Returns:
      {
        owner_name, situs_address, mailing_address, land_use, year_built,
        living_area_sqft, land_value, building_value, total_value,
        last_sale_date, last_sale_price,
        source_url,
        field_provenance: { field: {source_url, raw_label} }
      }
    """

    pid = str(parcel_id or "").strip()
    if not pid:
        raise ValueError("parcel_id is required")

    # Use FDOR ONLY to obtain a search hint (address). Output values must come from OCPA.
    addr_query: Optional[str] = None
    try:
        fdor = FDORCentroidClient()
        hint_rows = fdor.fetch_parcels([pid], include_geometry=False)
        hint = hint_rows.get(pid)
        if hint is not None and (hint.situs_address or "").strip():
            addr_hint = (hint.situs_address or "").strip()
            # OCPA address search typically prefers street portion.
            addr_query = addr_hint.split(",")[0].strip() or addr_hint
    except Exception:
        addr_query = None

    cj = http.cookiejar.CookieJar()
    opener = build_opener(HTTPCookieProcessor(cj))

    def _open(
        url: str,
        *,
        method: str = "GET",
        data: Optional[bytes] = None,
        referer: Optional[str] = None,
        timeout_s: int = 30,
    ) -> tuple[int, str, str]:
        headers = {
            "User-Agent": _ua(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "close",
        }
        if referer:
            headers["Referer"] = referer
        if method == "POST":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        req = Request(url, data=data, headers=headers, method=method)
        try:
            with opener.open(req, timeout=int(timeout_s)) as resp:
                status = getattr(resp, "status", None) or resp.getcode()
                final_url = getattr(resp, "geturl", lambda: url)()
                raw = resp.read().decode("utf-8", errors="replace")
                return int(status or 0), str(final_url or url), raw
        except Exception as e:
            # Best-effort: expose failure as blocked-like, the caller will surface.
            return 0, url, f"__request_failed__ {e}"

    status, landing_final, landing_html = _open(OCPA_LANDING_URL)
    blocked, hint_msg = _is_blocked(status, landing_html)
    if blocked:
        # One retry max with small backoff.
        time.sleep(1.5)
        status, landing_final, landing_html = _open(OCPA_LANDING_URL)
        blocked, hint_msg = _is_blocked(status, landing_html)
        if blocked:
            return {
                "error_reason": "blocked",
                "http_status": status,
                "hint": hint_msg,
                "source_url": landing_final,
            }

    hidden = _extract_hidden_inputs(landing_html)

    # Avoid hammering the site: small jitter between fetches.
    time.sleep(0.25)

    attempts: List[Tuple[str, str]] = []
    results_html: Optional[str] = None
    results_url: Optional[str] = None

    # Try parcel-id search variants first (if form fields exist).
    # Current OCPA landing uses QuickSearches > ParcelIDSearch1 > FullParcel.
    strap_input = (
        _find_input_name(landing_html, "ParcelIDSearch1$ctl00$FullParcel")
        or _find_input_name(landing_html, "ParcelIDSearch1$FullParcel")
        or _find_input_name(landing_html, "FullParcel")
    )
    strap_submit = (
        _find_input_name(landing_html, "ParcelIDSearch1$ctl00$ActionButton1")
        or _find_input_name(landing_html, "ParcelIDSearch1$ActionButton1")
        or _find_input_name(landing_html, "ParcelIDSearch1")
    )
    if strap_input and strap_submit:
        for v in _parcel_variants(pid):
            attempts.append(("strap", v))

    # Fallback: address search.
    # Composite address search (QuickSearches > CompositAddressSearch1 > AddressSearch1).
    addr_input = (
        _find_input_name(
            landing_html, "QuickSearches$CompositAddressSearch1$AddressSearch1$ctl00$Address"
        )
        or _find_input_name(landing_html, "CompositAddressSearch1$AddressSearch1$ctl00$Address")
        or _find_input_name(landing_html, "CompositAddressSearch1$Address")
    )
    addr_submit = (
        _find_input_name(
            landing_html,
            "QuickSearches$CompositAddressSearch1$AddressSearch1$ctl00$ActionButton1",
        )
        or _find_input_name(landing_html, "CompositAddressSearch1$AddressSearch1$ctl00$ActionButton1")
        or _find_input_name(landing_html, "CompositAddressSearch1$ActionButton1")
    )
    if addr_input and addr_submit and addr_query:
        attempts.append(("address", addr_query))

    if not attempts:
        return {
            "error_reason": "form_fields_not_found",
            "http_status": status,
            "hint": "OCPA search form fields not found",
            "source_url": landing_final,
        }

    chosen_href: Optional[str] = None
    chosen_links: List[Tuple[str, str]] = []

    for mode, q in attempts:
        form = dict(hidden)
        if mode == "strap" and strap_input and strap_submit:
            form[strap_input] = q
            form[strap_submit] = ""
            submit_name = strap_submit
        else:
            if not addr_input or not addr_submit:
                continue
            form[addr_input] = q
            form[addr_submit] = ""
            submit_name = addr_submit

        if "__EVENTTARGET" in form:
            form["__EVENTTARGET"] = submit_name
            form.setdefault("__EVENTARGUMENT", "")

        post_body = urlencode(form).encode("utf-8")
        st, res_final, res_html = _open(
            OCPA_LANDING_URL, method="POST", data=post_body, referer=OCPA_LANDING_URL
        )
        blocked, hint_msg = _is_blocked(st, res_html)
        if blocked:
            # One retry max with small backoff.
            time.sleep(1.5)
            st, res_final, res_html = _open(
                OCPA_LANDING_URL,
                method="POST",
                data=post_body,
                referer=OCPA_LANDING_URL,
            )
            blocked, hint_msg = _is_blocked(st, res_html)
            if blocked:
                return {
                    "error_reason": "blocked",
                    "http_status": st,
                    "hint": hint_msg,
                    "source_url": res_final,
                }

        links = _extract_ocpa_result_links(res_html)
        href = _choose_detail_href(pid, links)
        if href:
            results_html = res_html
            results_url = res_final
            chosen_links = links
            chosen_href = href
            break

        # Keep last response for debugging.
        results_html = res_html
        results_url = res_final

    if not chosen_href:
        # Debug artifact only when failing.
        safe = _digits_only(pid) or "unknown"
        p = Path(f"/tmp/ocpa_search_{safe}.html")
        try:
            p.write_text((results_html or "")[:3000], encoding="utf-8")
        except Exception:
            pass

        return {
            "error_reason": "no_search_results",
            "http_status": 200,
            "hint": f"attempts={attempts}",
            "source_url": results_url or OCPA_LANDING_URL,
        }

    detail_url = urljoin(OCPA_LANDING_URL, chosen_href)

    # Optimization: when the POST response already returns the PID detail view,
    # reuse it instead of issuing an extra GET.
    d_status: int
    detail_final: str
    detail_html: str
    if (
        results_html
        and "/Searches/ParcelSearch.aspx/PID/" in (chosen_href or "")
        and (
            re.search(r"View\s+\d{4}\s+Property\s+Record\s+Card", results_html, flags=re.I)
            or re.search(r"View\s+Property\s+Record\s+Card", results_html, flags=re.I)
            or "DetailsTab" in results_html
        )
        # Only reuse if the valuation grid is present; otherwise we tend to miss values.
        and ("Tax Year Values" in results_html or "BenefitsGrid" in results_html)
    ):
        d_status = 200
        detail_final = results_url or detail_url
        detail_html = results_html
    else:
        d_status, detail_final, detail_html = _open(detail_url, referer=OCPA_LANDING_URL)
        blocked, hint_msg = _is_blocked(d_status, detail_html)
        if blocked:
            time.sleep(1.5)
            d_status, detail_final, detail_html = _open(detail_url, referer=OCPA_LANDING_URL)
            blocked, hint_msg = _is_blocked(d_status, detail_html)
            if blocked:
                return {
                    "error_reason": "blocked",
                    "http_status": d_status,
                    "hint": hint_msg,
                    "source_url": detail_final,
                }

    pairs = _extract_label_value_pairs(detail_html)
    pairs.extend(_extract_div_label_pairs(detail_html))

    # Stable provenance URL: prefer the PID detail URL (even if the server returns
    # a different final URL).
    source_url = detail_url

    out: Dict[str, Any] = {
        "owner_name": None,
        "situs_address": None,
        "mailing_address": None,
        "land_use": None,
        "property_type": None,
        "zoning": None,
        "future_land_use": None,
        "beds": None,
        "baths": None,
        "year_built": None,
        "living_area_sqft": None,
        "land_value": None,
        "building_value": None,
        "total_value": None,
        "last_sale_date": None,
        "last_sale_price": None,
        "photo_url": None,
        "mortgage_lender": None,
        "mortgage_amount": None,
        "mortgage_date": None,
        "source_url": source_url,
        "field_provenance": {},
    }

    prov: Dict[str, FieldProv] = {}

    def _set(field: str, value: Any, raw_label: Optional[str]) -> None:
        if value in (None, ""):
            return
        out[field] = value
        if raw_label:
            prov[field] = FieldProv(source_url=source_url, raw_label=raw_label)

    # Prefer exact normalized labels when present.
    pm = _pairs_map(pairs)
    def _set_from_norm(field: str, norms: List[str], conv) -> None:
        for n in norms:
            if n in pm:
                raw_label, raw_value = pm[n]
                try:
                    _set(field, conv(raw_value), raw_label)
                except Exception:
                    _set(field, raw_value, raw_label)
                return

    _set_from_norm("owner_name", ["ownername", "owner"], lambda x: _norm_ws(x))
    if not out.get("owner_name"):
        v, lab = _pick(pairs, ["owner", "owner name"])
        _set("owner_name", v, lab)

    _set_from_norm(
        "situs_address",
        ["situsaddress", "physicaladdress", "siteaddress", "propertyaddress"],
        lambda x: _norm_ws(x),
    )
    if not out.get("situs_address"):
        v, lab = _pick(pairs, ["situs", "physical address", "site address", "property address"])
        _set("situs_address", v, lab)

    _set_from_norm("mailing_address", ["mailingaddress"], lambda x: _norm_ws(x))
    if not out.get("mailing_address"):
        v, lab = _pick(pairs, ["mailing address", "mailing"])
        _set("mailing_address", v, lab)

    _set_from_norm(
        "land_use",
        ["landuse", "usecode", "doruc", "dorusecode"],
        lambda x: _norm_ws(x),
    )
    if not out.get("land_use"):
        v, lab = _pick(pairs, ["land use", "use code", "dor", "d.o.r", "uc"])
        _set("land_use", v, lab)

    # Property/use type (often a longer description than land_use).
    _set_from_norm(
        "property_type",
        [
            "propertyuse",
            "usetypedescription",
            "usetype",
            "usedescription",
            "use",
        ],
        lambda x: _norm_ws(x),
    )
    if not out.get("property_type"):
        v, lab = _pick(pairs, ["property use", "use type", "use description", "property type", "use"])
        _set("property_type", v, lab)

    # Zoning
    _set_from_norm("zoning", ["zoning", "zoningcode"], lambda x: _norm_ws(x))
    if not out.get("zoning"):
        v, lab = _pick(pairs, ["zoning"])
        _set("zoning", v, lab)

    # Future Land Use (FLU) (best-effort; not always present on OCPA)
    _set_from_norm(
        "future_land_use",
        [
            "futurelanduse",
            "futurelandusecode",
            "futurelandusecategory",
            "futurelanduseclassification",
            "futurelanduseclass",
            "futurelandusemap",
            "futurelandusedesignation",
        ],
        lambda x: _norm_ws(x),
    )
    if not out.get("future_land_use"):
        v, lab = _pick(
            pairs,
            [
                "future land use",
                "future land-use",
                "future landuse",
                "flu",
            ],
        )
        _set("future_land_use", v, lab)

    # Fallback: land_use + zoning appear in the Land grid.
    land_use, zoning = _extract_land_use_and_zoning(detail_html)
    # Prefer authoritative grid values over noisy label-pair extractions.
    if land_use:
        _set("land_use", land_use, "Land Use Code")
        # The land use code description is a good, stable proxy for property type.
        if not out.get("property_type") or str(out.get("property_type") or "").strip().lower() in {"min", "max"}:
            _set("property_type", land_use, "Land Use Code")
    if zoning:
        _set("zoning", zoning, "Zoning")

    # Beds/Baths
    _set_from_norm("beds", ["bedrooms", "beds"], lambda x: _as_int(x))
    if not out.get("beds"):
        v, lab = _pick(pairs, ["bedrooms", "beds"])
        _set("beds", _as_int(v) if v is not None else None, lab)

    _set_from_norm("baths", ["bathrooms", "baths"], lambda x: _as_float(x))
    if not out.get("baths"):
        v, lab = _pick(pairs, ["bathrooms", "baths"])
        _set("baths", _as_float(v) if v is not None else None, lab)

    _set_from_norm("year_built", ["yearbuilt", "yrbuilt"], lambda x: _as_int(x))
    if not out.get("year_built"):
        v, lab = _pick(pairs, ["year built", "yr built", "built"])
        _set("year_built", _as_int(v) if v is not None else None, lab)

    _set_from_norm(
        "living_area_sqft",
        ["livingarea", "heatedarea", "livingareasf", "livingareasqft"],
        lambda x: _as_float(x),
    )
    if not out.get("living_area_sqft"):
        v, lab = _pick(pairs, ["living area", "heated area", "living sq", "gross living"])
        _set("living_area_sqft", _as_float(v) if v is not None else None, lab)

    _set_from_norm("land_value", ["landvalue"], lambda x: _as_float(x))
    if not out.get("land_value"):
        v, lab = _pick(pairs, ["land value"])
        _set("land_value", _as_float(v) if v is not None else None, lab)

    _set_from_norm(
        "building_value",
        ["buildingvalue", "improvementvalue", "improvementsvalue"],
        lambda x: _as_float(x),
    )
    if not out.get("building_value"):
        v, lab = _pick(pairs, ["building value", "improvement value", "improvements"])
        _set("building_value", _as_float(v) if v is not None else None, lab)

    _set_from_norm(
        "total_value",
        ["totalvalue", "justvalue", "marketvalue"],
        lambda x: _as_float(x),
    )
    if not out.get("total_value"):
        v, lab = _pick(pairs, ["total value", "just value", "market value"])
        _set("total_value", _as_float(v) if v is not None else None, lab)

    # Fallback: valuation grid has reliable Land/Building/Market Value.
    land_v, bldg_v, market_v = _extract_tax_year_values(detail_html)
    if land_v is not None:
        _set("land_value", land_v, "Tax Year Values (Land)")
    if bldg_v is not None:
        _set("building_value", bldg_v, "Tax Year Values (Building)")
    if market_v is not None:
        _set("total_value", market_v, "Tax Year Values (Market Value)")

    # Targeted sales extraction: find a Sale Date + Sale Amount pair.
    sale_m = re.search(
        r"(\d{1,2}/\d{1,2}/\d{4})[\s\S]{0,140}?(\$\s*\d[\d,]*)",
        detail_html,
        flags=re.I,
    )
    if sale_m:
        _set("last_sale_date", _as_date_iso(sale_m.group(1)), "Sale Date")
        _set("last_sale_price", _as_float(sale_m.group(2)), "Sale Amount")

    _set_from_norm(
        "last_sale_date",
        ["lastsaledate", "saledate", "dateofsale"],
        lambda x: _as_date_iso(x),
    )
    if not out.get("last_sale_date"):
        v, lab = _pick(pairs, ["last sale date", "sale date", "date of sale"])
        _set("last_sale_date", _as_date_iso(v or "") if v is not None else None, lab)

    _set_from_norm(
        "last_sale_price",
        ["lastsaleprice", "saleprice"],
        lambda x: _as_float(x),
    )
    if not out.get("last_sale_price"):
        v, lab = _pick(pairs, ["last sale price", "sale price", "price"])
        _set("last_sale_price", _as_float(v) if v is not None else None, lab)

    # Photo (if present)
    photo = _extract_photo_url(detail_html, source_url)
    if photo:
        _set("photo_url", photo, "Property Photo")

    # Mortgage (best-effort; not always present in OCPA HTML)
    def _pick_mortgage(tokens: List[str]) -> Tuple[Optional[str], Optional[str]]:
        toks = [t.lower() for t in tokens if t]
        for label, value in pairs:
            lower = (label or "").lower()
            if all(t in lower for t in toks):
                return value, label
        return None, None

    v, lab = _pick_mortgage(["mortg", "lend"])  # mortgage lender
    if v:
        _set("mortgage_lender", _norm_ws(v), lab)

    v, lab = _pick_mortgage(["mortg", "amount"])  # mortgage amount
    if v:
        _set("mortgage_amount", _as_float(v), lab)

    v, lab = _pick_mortgage(["mortg", "date"])  # mortgage date
    if v:
        _set("mortgage_date", _as_date_iso(v) or _norm_ws(v), lab)

    # Emit provenance in required shape
    out["field_provenance"] = {
        k: {"source_url": p.source_url, "raw_label": p.raw_label} for k, p in prov.items()
    }

    # If required fields are missing, return a structured error.
    # Many parcels legitimately have no improvement data (or are special parcels),
    # so distinguish that case from genuine parse failures.
    required = ["year_built", "living_area_sqft", "total_value"]
    missing = [f for f in required if not out.get(f)]
    if missing:
        missing_set = set(missing)
        if missing_set.issubset({"year_built", "living_area_sqft"}) and out.get(
            "total_value"
        ):
            return {
                "error_reason": "no_improvement_data",
                "http_status": int(d_status or 0),
                "hint": f"missing={missing}",
                "source_url": source_url,
            }

        # Save detail HTML for debugging and return a parse_failed error.
        safe = _digits_only(pid) or "unknown"
        p = Path(f"/tmp/ocpa_detail_{safe}.html")
        try:
            p.write_text(detail_html, encoding="utf-8")
        except Exception:
            pass
        return {
            "error_reason": "parse_failed",
            "http_status": int(d_status or 0),
            "hint": f"missing={missing}",
            "source_url": source_url,
        }

    return out
