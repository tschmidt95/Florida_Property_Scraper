from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# Default parcel-polygon layer (FDOR statewide cadastral).
# This is deterministic and uses the same PARCEL_ID identifiers as the FDOR centroid feed.
DEFAULT_FDOR_CADASTRAL_LAYER_URL = (
    "https://services9.arcgis.com/Gh9awoU677aKree0/arcgis/rest/services/"
    "Florida_Statewide_Cadastral/FeatureServer/0"
)


def _ua() -> str:
    return os.getenv("FPS_HTTP_USER_AGENT") or "Mozilla/5.0 (FloridaPropertyScraper)"


def _request_json(url: str, timeout_s: int = 30) -> Dict[str, Any]:
    req = Request(url, headers={"User-Agent": _ua()})
    with urlopen(req, timeout=int(timeout_s)) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _escape_sql_string(s: str) -> str:
    return (s or "").replace("'", "''")


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


def _geojson_from_esri_geometry(geom: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert ArcGIS JSON geometry to GeoJSON.

    We support the minimum needed for parcel overlays.
    """

    if not isinstance(geom, dict):
        return None

    # Polygon: {rings: [[[x,y],...], ...]}
    rings = geom.get("rings")
    if isinstance(rings, list) and rings:
        try:
            coords: List[List[List[float]]] = []
            for ring in rings:
                if not isinstance(ring, list) or len(ring) < 4:
                    continue
                out_ring: List[List[float]] = []
                for pt in ring:
                    if (
                        isinstance(pt, (list, tuple))
                        and len(pt) >= 2
                        and isinstance(pt[0], (int, float))
                        and isinstance(pt[1], (int, float))
                    ):
                        out_ring.append([float(pt[0]), float(pt[1])])
                if len(out_ring) >= 4:
                    coords.append(out_ring)
            if not coords:
                return None
            return {"type": "Polygon", "coordinates": coords}
        except Exception:
            return None

    # Point: {x, y}
    if "x" in geom and "y" in geom:
        try:
            x = geom.get("x")
            y = geom.get("y")
            if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                return {"type": "Point", "coordinates": [float(x), float(y)]}
        except Exception:
            return None

    return None


@lru_cache(maxsize=1)
def discover_polygon_layer_url(timeout_s: int = 15) -> Optional[str]:
    """Return the parcel-polygon FeatureServer layer URL.

    You can override with FPS_FDOR_PARCEL_POLYGONS_LAYER_URL.
    """

    override = os.getenv("FPS_FDOR_PARCEL_POLYGONS_LAYER_URL", "").strip()
    if override:
        return override.rstrip("/")

    layer_url = os.getenv("FPS_FDOR_CADASTRAL_LAYER_URL", "").strip() or DEFAULT_FDOR_CADASTRAL_LAYER_URL
    layer_url = layer_url.rstrip("/")

    # Validate the target to catch typos/misconfig early.
    try:
        meta = _request_json(f"{layer_url}?f=pjson", timeout_s=timeout_s)
        gt = str(meta.get("geometryType") or "").lower()
        if "polygon" not in gt:
            return None
    except Exception:
        return None

    return layer_url


@dataclass
class FDORParcelPolygonClient:
    """Best-effort parcel-boundary lookup via an ArcGIS FeatureServer polygon layer."""

    layer_url: Optional[str] = None
    timeout_s: int = 25

    def __post_init__(self) -> None:
        if not self.layer_url:
            self.layer_url = discover_polygon_layer_url() or None
        if self.layer_url:
            self.layer_url = self.layer_url.rstrip("/")

    def _query(self, params: Mapping[str, str]) -> Dict[str, Any]:
        if not self.layer_url:
            raise RuntimeError("parcel polygon layer_url unavailable")
        url = f"{self.layer_url}/query?{urlencode(dict(params))}"
        return _request_json(url, timeout_s=self.timeout_s)

    def fetch_parcel_geometries(
        self,
        parcel_ids: Sequence[str],
        batch_size: int = 25,
    ) -> Dict[str, Dict[str, Any]]:
        """Return GeoJSON geometries keyed by parcel_id."""

        if not self.layer_url:
            return {}

        cleaned = [str(pid).strip() for pid in parcel_ids if str(pid).strip()]
        if not cleaned:
            return {}

        out: Dict[str, Dict[str, Any]] = {}

        for batch in _chunked(cleaned, int(batch_size)):
            where = "PARCEL_ID IN (%s)" % ",".join(
                [f"'{_escape_sql_string(pid)}'" for pid in batch]
            )
            data = self._query(
                {
                    "f": "json",
                    "where": where,
                    "outFields": "PARCEL_ID",
                    "returnGeometry": "true",
                    "outSR": "4326",
                    "resultRecordCount": "2000",
                }
            )
            feats = data.get("features") or []
            for feat in feats:
                if not isinstance(feat, dict):
                    continue
                attrs = feat.get("attributes") or {}
                geom = feat.get("geometry") or {}
                pid = str(attrs.get("PARCEL_ID") or "").strip()
                if not pid:
                    continue
                gj = _geojson_from_esri_geometry(geom)
                if gj is None:
                    continue
                out[pid] = gj
import requests

def fetch_fdor_attrs_by_parcel_ids(parcel_ids: list[str]) -> dict[str, dict]:
    """
    Returns {parcel_id: {"owner_name": "...", "situs_address": "..."}}
    Best-effort: field names vary, so we try common attribute keys.
    """
    if not parcel_ids:
        return {}

    url = parcel_polygon_layer_url()

    # ArcGIS IN() can be picky; chunk it
    out: dict[str, dict] = {}
    CHUNK = 100
    for i in range(0, len(parcel_ids), CHUNK):
        chunk = parcel_ids[i : i + CHUNK]
        quoted = ",".join([f"'{p}'" for p in chunk])
        where = f"PARCELID IN ({quoted}) OR PARCEL_ID IN ({quoted}) OR PARCEL IN ({quoted})"

        params = {
            "f": "json",
            "where": where,
            "outFields": "*",
            "returnGeometry": "false",
        }
        r = requests.get(f"{url}/query", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        feats = data.get("features", []) or []

        for feat in feats:
            attrs = feat.get("attributes") or {}

            # parcel id field (varies)
            pid = (
                attrs.get("PARCELID")
                or attrs.get("PARCEL_ID")
                or attrs.get("PARCEL")
                or attrs.get("PARCELNO")
                or attrs.get("PARCEL_NO")
            )
            if not pid:
                continue
            pid = str(pid)

            # owner field (varies)
            owner = (
                attrs.get("OWNER")
                or attrs.get("OWNER_NAME")
                or attrs.get("OWNERNME1")
                or attrs.get("OWNER1")
                or attrs.get("OWN_NAME")
            ) or ""

            # situs field (varies)
            situs = (
                attrs.get("SITUS")
                or attrs.get("SITUS_ADDRESS")
                or attrs.get("SITE_ADDR")
                or attrs.get("ADDRESS")
                or attrs.get("PROP_ADDR")
            ) or ""

            out[pid] = {
                "owner_name": (str(owner).strip() if owner is not None else ""),
                "situs_address": (str(situs).strip() if situs is not None else ""),
            }

    return out
