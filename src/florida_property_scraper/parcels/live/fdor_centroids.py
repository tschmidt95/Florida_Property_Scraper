from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BBox = Tuple[float, float, float, float]


DEFAULT_LAYER_URL = (
    "https://services9.arcgis.com/Gh9awoU677aKree0/arcgis/rest/services/"
    "Florida_Statewide_Parcel_Centroid_Version/FeatureServer/0"
)


def _ua() -> str:
    return os.getenv("FPS_HTTP_USER_AGENT") or "Mozilla/5.0 (FloridaPropertyScraper)"


def _request_json(url: str, timeout_s: int = 30) -> Dict[str, Any]:
    req = Request(url, headers={"User-Agent": _ua()})
    with urlopen(req, timeout=int(timeout_s)) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _chunked(seq: Sequence[str], n: int) -> Iterable[List[str]]:
    n = max(int(n), 1)
    buf: List[str] = []
    for it in seq:
        if not it:
            continue
        buf.append(str(it))
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def _escape_sql_string(s: str) -> str:
    return (s or "").replace("'", "''")


def _as_str(v: object) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _as_int(v: object) -> int:
    try:
        if v is None:
            return 0
        if isinstance(v, bool):
            return 1 if v else 0
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return 0
        return int(float(s))
    except Exception:
        return 0


def _as_float(v: object) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace(",", "")
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _sale_date_from_parts(year: object, month: object) -> Optional[str]:
    y = _as_int(year)
    if y <= 0:
        return None
    m_raw = _as_str(month)
    m = _as_int(m_raw)
    if m <= 0 or m > 12:
        # Sometimes month is blank/space.
        return f"{y:04d}-01-01"
    return f"{y:04d}-{m:02d}-01"


@dataclass(frozen=True)
class FDORCentroidRow:
    parcel_id: str
    lon: Optional[float]
    lat: Optional[float]
    owner_name: str
    situs_address: str
    situs_city: str
    situs_zip: str
    land_use_code: str
    land_sqft: float
    year_built: int
    last_sale_price: float
    last_sale_date: Optional[str]
    raw_source_url: str


class FDORCentroidClient:
    """Best-effort live parcel+attribute provider using FDOR statewide centroids.

    Notes:
    - Geometry is POINT (centroid), not parcel boundary.
    - Fields are limited vs a full PA detail page.
    """

    def __init__(
        self,
        layer_url: str = DEFAULT_LAYER_URL,
        timeout_s: int = 30,
    ) -> None:
        self.layer_url = (layer_url or DEFAULT_LAYER_URL).rstrip("/")
        self.timeout_s = int(timeout_s)

    def _query(self, params: Mapping[str, str]) -> Dict[str, Any]:
        url = f"{self.layer_url}/query?{urlencode(dict(params))}"
        return _request_json(url, timeout_s=self.timeout_s)

    def raw_url_for_parcel(self, parcel_id: str) -> str:
        where = f"PARCEL_ID='{_escape_sql_string(parcel_id)}'"
        params = {
            "f": "json",
            "where": where,
            "outFields": "PARCEL_ID,OWN_NAME,PHY_ADDR1,PHY_ADDR2,PHY_CITY,PHY_ZIPCD,DOR_UC,LND_SQFOOT,ACT_YR_BLT,SALE_PRC1,SALE_YR1,SALE_MO1",
            "returnGeometry": "false",
        }
        return f"{self.layer_url}/query?{urlencode(params)}"

    def query_bbox(self, bbox: BBox, limit: int = 2000) -> List[Dict[str, Any]]:
        min_lon, min_lat, max_lon, max_lat = bbox
        params = {
            "f": "json",
            "where": "1=1",
            "geometry": f"{min_lon},{min_lat},{max_lon},{max_lat}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outSR": "4326",
            "outFields": "PARCEL_ID",
            "returnGeometry": "true",
            "resultRecordCount": str(min(int(limit), 2000)),
        }
        data = self._query(params)
        return list(data.get("features") or [])

    def fetch_parcels(
        self,
        parcel_ids: Sequence[str],
        batch_size: int = 50,
        include_geometry: bool = False,
    ) -> Dict[str, FDORCentroidRow]:
        out: Dict[str, FDORCentroidRow] = {}
        cleaned = [str(pid) for pid in parcel_ids if pid]
        for batch in _chunked(cleaned, int(batch_size)):
            where = "PARCEL_ID IN (%s)" % ",".join(
                [f"'{_escape_sql_string(pid)}'" for pid in batch]
            )
            data = self._query(
                {
                    "f": "json",
                    "where": where,
                    "outFields": "PARCEL_ID,OWN_NAME,PHY_ADDR1,PHY_ADDR2,PHY_CITY,PHY_ZIPCD,DOR_UC,LND_SQFOOT,ACT_YR_BLT,SALE_PRC1,SALE_YR1,SALE_MO1",
                    "returnGeometry": "true" if include_geometry else "false",
                    "outSR": "4326",
                    "resultRecordCount": "2000",
                }
            )
            feats = data.get("features") or []
            for feat in feats:
                attrs = feat.get("attributes") or {}
                geom = feat.get("geometry") or {}
                pid = _as_str(attrs.get("PARCEL_ID"))
                if not pid:
                    continue

                lon = None
                lat = None
                try:
                    # ArcGIS point geometry: {x, y}
                    if include_geometry and ("x" in geom and "y" in geom):
                        x = geom.get("x")
                        y = geom.get("y")
                        if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                            lon = float(x)
                            lat = float(y)
                except Exception:
                    lon = None
                    lat = None

                phy1 = _as_str(attrs.get("PHY_ADDR1"))
                phy2 = _as_str(attrs.get("PHY_ADDR2"))
                city = _as_str(attrs.get("PHY_CITY"))
                zipcd = _as_str(attrs.get("PHY_ZIPCD"))
                situs = " ".join([p for p in [phy1, phy2] if p]).strip()
                if city:
                    situs = (situs + ", " + city).strip(", ") if situs else city
                if zipcd:
                    situs = (situs + " " + zipcd).strip() if situs else zipcd

                row = FDORCentroidRow(
                    parcel_id=pid,
                    lon=lon,
                    lat=lat,
                    owner_name=_as_str(attrs.get("OWN_NAME")),
                    situs_address=situs,
                    situs_city=city,
                    situs_zip=zipcd,
                    land_use_code=_as_str(attrs.get("DOR_UC")),
                    land_sqft=_as_float(attrs.get("LND_SQFOOT")),
                    year_built=_as_int(attrs.get("ACT_YR_BLT")),
                    last_sale_price=_as_float(attrs.get("SALE_PRC1")),
                    last_sale_date=_sale_date_from_parts(
                        attrs.get("SALE_YR1"), attrs.get("SALE_MO1")
                    ),
                    raw_source_url=self.raw_url_for_parcel(pid),
                )
                out[pid] = row
        return out
