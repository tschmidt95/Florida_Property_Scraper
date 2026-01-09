from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from florida_property_scraper.parcels.geometry_provider import BBox, Feature, feature_id


@dataclass
class SeminoleProvider:
    """Pilot county provider backed by a local GeoJSON file.

    Uses an STRtree spatial index on load.
    """

    geojson_path: Path
    county: str = "seminole"

    _loaded: bool = False
    _builds: int = 0
    _features: List[Feature] = None  # type: ignore[assignment]
    _geoms: List[Any] = None  # shapely geometries (optional)
    _tree: Any = None  # STRtree (optional)
    _bboxes: List[BBox] = None  # type: ignore[assignment]

    def load(self) -> None:
        if self._loaded:
            return

        self._builds += 1

        self._features = []
        self._geoms = []
        self._tree = None
        self._bboxes = []

        if not self.geojson_path.exists():
            self._loaded = True
            return

        raw = json.loads(self.geojson_path.read_text(encoding="utf-8"))
        feats = raw.get("features") if isinstance(raw, dict) else None
        if not isinstance(feats, list):
            self._loaded = True
            return

        has_shapely = True
        try:
            from shapely.geometry import shape as s_shape  # type: ignore[import-not-found]
            from shapely.strtree import STRtree  # type: ignore[import-not-found]
        except Exception:
            has_shapely = False

        for feat in feats:
            if not isinstance(feat, dict):
                continue
            geom = feat.get("geometry")
            if not isinstance(geom, dict):
                continue
            props = feat.get("properties") if isinstance(feat.get("properties"), dict) else {}
            parcel_id = props.get("parcel_id") or props.get("PARCEL_ID") or feat.get("parcel_id") or feat.get("PARCEL_ID")
            if not parcel_id:
                continue
            parcel_id = str(parcel_id)

            sgeom = None
            if has_shapely:
                try:
                    sgeom = s_shape(geom)
                except Exception:
                    sgeom = None

            f = Feature(
                feature_id=feature_id(self.county, parcel_id),
                county=self.county,
                parcel_id=parcel_id,
                geometry=geom,
            )
            self._features.append(f)

            # Always compute bbox for fallback + quick rejection.
            bb = self._bbox_from_geometry(geom)
            if bb is None:
                # If we can't compute a bbox, skip it (can't query reliably).
                self._features.pop()
                continue
            self._bboxes.append(bb)

            if sgeom is not None:
                self._geoms.append(sgeom)
            else:
                self._geoms.append(None)

        if has_shapely:
            # Build STRtree only on valid geometries.
            valid = [g for g in self._geoms if g is not None]
            if valid:
                self._tree = STRtree(valid)

        self._loaded = True

    @staticmethod
    def _bbox_from_geometry(geom: Dict[str, Any]) -> Optional[BBox]:
        coords = geom.get("coordinates")
        if coords is None:
            return None

        def _walk(obj: Any):
            if isinstance(obj, (list, tuple)) and len(obj) == 2 and all(
                isinstance(x, (int, float)) for x in obj
            ):
                yield float(obj[0]), float(obj[1])
                return
            if isinstance(obj, (list, tuple)):
                for it in obj:
                    yield from _walk(it)

        xs: List[float] = []
        ys: List[float] = []
        for x, y in _walk(coords):
            xs.append(x)
            ys.append(y)
        if not xs or not ys:
            return None
        return (min(xs), min(ys), max(xs), max(ys))

    @staticmethod
    def _bbox_intersects(a: BBox, b: BBox) -> bool:
        return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])

    def query(self, bbox: BBox) -> List[Feature]:
        if not self._loaded:
            self.load()
        if not self._features or not self._bboxes:
            return []

        # Preferred: STRtree if present.
        if self._tree is not None:
            try:
                from shapely.geometry import box as s_box  # type: ignore[import-not-found]
            except Exception:
                # If shapely vanished, fall back to bbox checks.
                self._tree = None
            else:
                query_geom = s_box(bbox[0], bbox[1], bbox[2], bbox[3])
                candidates = self._tree.query(query_geom)

                # shapely STRtree.query may return a numpy array; never rely on truthiness.
                try:
                    n = int(getattr(candidates, "size", len(candidates)))
                except Exception:
                    n = 0
                if candidates is None or n == 0:
                    return []

                # Depending on Shapely version, candidates may be:
                # - array of integer indices into the STRtree input geometries
                # - array of geometry objects
                first = candidates[0]

                out: List[Feature] = []
                # Index-returning path (common in shapely 2.x builds)
                if isinstance(first, (int,)) or first.__class__.__name__ in ("int64", "int32"):
                    valid_indices = [i for i, g in enumerate(self._geoms) if g is not None]
                    for v in candidates:
                        try:
                            tree_idx = int(v)
                        except Exception:
                            continue
                        if tree_idx < 0 or tree_idx >= len(valid_indices):
                            continue
                        feat_idx = valid_indices[tree_idx]
                        g = self._geoms[feat_idx]
                        if g is None:
                            continue
                        try:
                            if not g.intersects(query_geom):
                                continue
                        except Exception:
                            continue
                        out.append(self._features[feat_idx])
                else:
                    idx_by_id = {}
                    for i, g in enumerate(self._geoms):
                        if g is not None:
                            idx_by_id[id(g)] = i

                    for g in candidates:
                        i = idx_by_id.get(id(g))
                        if i is None:
                            continue
                        try:
                            if not g.intersects(query_geom):
                                continue
                        except Exception:
                            continue
                        out.append(self._features[i])
                out.sort(key=lambda f: f.parcel_id)
                return out

        # Fallback: bbox-only index (still avoids per-feature geometry parsing).
        out: List[Feature] = []
        for i, bb in enumerate(self._bboxes):
            if self._bbox_intersects(bbox, bb):
                out.append(self._features[i])
        out.sort(key=lambda f: f.parcel_id)
        return out
