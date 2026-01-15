import { useEffect, useMemo, useRef, useState, type ChangeEvent } from 'react';

import L from 'leaflet';
import type { LatLngLiteral } from 'leaflet';
import { CircleMarker, GeoJSON, MapContainer, Marker, TileLayer, useMap } from 'react-leaflet';

import 'leaflet-draw';

import {
  parcelsEnrich,
  parcelsGeometry,
  parcelsSearchNormalized,
  permitsByParcel,
  type ParcelAttributeFilters,
  type ParcelRecord,
  type ParcelSearchListItem,
  type PermitRecord,
} from '../lib/api';

type MapStatus = 'loading' | 'loaded' | 'failed';

type DrawnCircle = { center: LatLngLiteral; radius_m: number };

function DrawControls({
  drawnItemsRef,
  onPolygon,
  onDeleted,
  onDrawingChange,
}: {
  drawnItemsRef: React.MutableRefObject<L.FeatureGroup | null>;
  onPolygon: (geom: GeoJSON.Polygon) => void;
  onDeleted: () => void;
  onDrawingChange: (isDrawing: boolean) => void;
}) {
  const map = useMap();
  const wasDraggingEnabledRef = useRef<boolean>(false);
  const activePolygonHandlerRef = useRef<any>(null);
  const firstVertexGuardedRef = useRef<boolean>(false);
  const onPolygonRef = useRef(onPolygon);
  const onDeletedRef = useRef(onDeleted);
  const onDrawingChangeRef = useRef(onDrawingChange);

  useEffect(() => {
    onPolygonRef.current = onPolygon;
  }, [onPolygon]);

  useEffect(() => {
    onDeletedRef.current = onDeleted;
  }, [onDeleted]);

  useEffect(() => {
    onDrawingChangeRef.current = onDrawingChange;
  }, [onDrawingChange]);

  useEffect(() => {
    const drawnItems = new L.FeatureGroup();
    drawnItemsRef.current = drawnItems;
    map.addLayer(drawnItems);

    const control = new L.Control.Draw({
      draw: {
        polygon: {
          maxPoints: 0,
          allowIntersection: true,
          showArea: true,
        },
        polyline: false,
        rectangle: false,
        circle: false,
        marker: false,
        circlemarker: false,
      },
      edit: {
        featureGroup: drawnItems,
        edit: false,
        remove: true,
      },
    });

    map.addControl(control);

    const handleDrawStart = (e: any) => {
      wasDraggingEnabledRef.current = !!map.dragging?.enabled?.();
      if (wasDraggingEnabledRef.current) map.dragging.disable();

      // Reset per-draw state.
      firstVertexGuardedRef.current = false;

      // Track the active polygon handler so we can explicitly finish on dblclick.
      // (Leaflet.Draw generally supports finishing via clicking the first vertex;
      // dblclick support varies by browser/config.)
      try {
        if (e?.layerType === 'polygon') {
          activePolygonHandlerRef.current = (control as any)?._toolbars?.draw?._modes?.polygon?.handler ?? null;
        } else {
          activePolygonHandlerRef.current = null;
        }
      } catch {
        activePolygonHandlerRef.current = null;
      }

      onDrawingChangeRef.current(true);
    };

    const handleDrawStop = () => {
      if (wasDraggingEnabledRef.current) map.dragging.enable();
      wasDraggingEnabledRef.current = false;
      activePolygonHandlerRef.current = null;
      firstVertexGuardedRef.current = false;
      onDrawingChangeRef.current(false);
    };

    const handleDrawVertex = (e: any) => {
      // Leaflet.Draw finishes polygons when clicking the first vertex.
      // Product requirement: ONLY finish on dblclick or explicit Finish button.
      if (firstVertexGuardedRef.current) return;
      try {
        const layers = e?.layers;
        const markers: any[] = layers?.getLayers?.() || [];
        const first = markers[0];
        if (!first) return;

        // Remove Leaflet.Draw's finish-on-first-click handler, but keep a click handler
        // that just stops propagation.
        first.off('click');
        first.on('click', (evt: any) => {
          try {
            evt?.originalEvent?.preventDefault?.();
            evt?.originalEvent?.stopPropagation?.();
          } catch {
            // ignore
          }
        });
        firstVertexGuardedRef.current = true;
      } catch {
        // ignore
      }
    };

    const handleDblClick = () => {
      // Finish the polygon on double click.
      // Leaflet.Draw may finish on dblclick already, but this makes the behavior explicit.
      const handler = activePolygonHandlerRef.current;
      if (!handler) return;
      try {
        if (typeof handler._finishShape === 'function') {
          handler._finishShape();
          return;
        }
        if (typeof handler.completeShape === 'function') {
          handler.completeShape();
        }
      } catch {
        // ignore
      }
    };

    const handleCreated = (e: any) => {
      try {
        drawnItems.clearLayers();
        if (e?.layer) drawnItems.addLayer(e.layer);
      } catch {
        // ignore
      }

      if (e?.layerType === 'polygon') {
        try {
          // Build a stable GeoJSON Polygon from Leaflet lat/lngs.
          // This avoids subtle coordinate-order issues that can lead to
          // bbox/intersection queries returning 0 parcels.
          const latlngs = e.layer?.getLatLngs?.();

          const toRing = (ring: any[]): number[][] => {
            const coords: number[][] = [];
            for (const p of ring) {
              const lat = typeof p?.lat === 'number' ? p.lat : null;
              const lng = typeof p?.lng === 'number' ? p.lng : null;
              if (lat === null || lng === null) continue;
              coords.push([lng, lat]);
            }
            if (coords.length >= 3) {
              const first = coords[0];
              const last = coords[coords.length - 1];
              if (first[0] !== last[0] || first[1] !== last[1]) coords.push([...first]);
            }
            return coords;
          };

          let rings: any[][] = [];
          if (Array.isArray(latlngs) && latlngs.length) {
            if (Array.isArray(latlngs[0]) && latlngs[0]?.length && typeof latlngs[0][0]?.lat === 'number') {
              // LatLng[][]
              rings = latlngs as any[][];
            } else if (typeof (latlngs as any)[0]?.lat === 'number') {
              // LatLng[]
              rings = [latlngs as any[]];
            }
          }

          const coordinates = rings.map(toRing).filter((r) => r.length >= 4);
          if (coordinates.length) {
            onPolygonRef.current({ type: 'Polygon', coordinates } as GeoJSON.Polygon);
          }
        } catch {
          // ignore
        }
      }
    };

    const handleDeleted = () => {
      onDeletedRef.current();
    };

    map.on(L.Draw.Event.DRAWSTART, handleDrawStart);
    map.on(L.Draw.Event.DRAWSTOP, handleDrawStop);
    map.on(L.Draw.Event.DRAWVERTEX, handleDrawVertex);
    map.on(L.Draw.Event.CREATED, handleCreated);
    map.on(L.Draw.Event.DELETED, handleDeleted);
    map.on('dblclick', handleDblClick);

    return () => {
      map.off(L.Draw.Event.DRAWSTART, handleDrawStart);
      map.off(L.Draw.Event.DRAWSTOP, handleDrawStop);
      map.off(L.Draw.Event.DRAWVERTEX, handleDrawVertex);
      map.off(L.Draw.Event.CREATED, handleCreated);
      map.off(L.Draw.Event.DELETED, handleDeleted);
      map.off('dblclick', handleDblClick);
      try {
        map.removeControl(control);
      } catch {
        // ignore
      }
      try {
        map.removeLayer(drawnItems);
      } catch {
        // ignore
      }
      if (drawnItemsRef.current === drawnItems) drawnItemsRef.current = null;
    };
  }, [drawnItemsRef, map]);

  return null;
}

function MultiSelectFilter({
  title,
  options,
  selected,
  query,
  onQuery,
  onSelected,
}: {
  title: string;
  options: string[];
  selected: string[];
  query: string;
  onQuery: (q: string) => void;
  onSelected: (next: string[]) => void;
}) {
  const filtered = useMemo(() => {
    const q = query.trim().toUpperCase();
    if (!q) return options;
    return options.filter((o) => o.toUpperCase().includes(q));
  }, [options, query]);

  const selectedSet = useMemo(() => new Set(selected), [selected]);

  const toggle = (v: string) => {
    if (selectedSet.has(v)) onSelected(selected.filter((x) => x !== v));
    else onSelected([...selected, v]);
  };

  const remove = (v: string) => {
    if (!selectedSet.has(v)) return;
    onSelected(selected.filter((x) => x !== v));
  };

  const selectAll = () => {
    onSelected([...options]);
  };

  const clear = () => {
    onSelected([]);
  };

  return (
    <div className="rounded-xl border border-cre-border/40 bg-cre-bg p-3">
      <div className="flex items-center justify-between gap-2">
        <div>
          <div className="text-xs font-semibold text-cre-text">{title}</div>
          <div className="text-[11px] text-cre-muted">
            {selected.length} selected · {options.length} options
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            className="rounded-lg border border-cre-border/40 bg-cre-surface px-2 py-1 text-[11px] text-cre-text"
            onClick={selectAll}
            disabled={!options.length}
          >
            Select all
          </button>
          <button
            type="button"
            className="rounded-lg border border-cre-border/40 bg-cre-surface px-2 py-1 text-[11px] text-cre-text"
            onClick={clear}
            disabled={!selected.length}
          >
            Clear
          </button>
        </div>
      </div>

      {selected.length ? (
        <div className="mt-2 flex flex-wrap gap-1">
          {selected.slice(0, 40).map((v) => (
            <button
              key={`chip:${v}`}
              type="button"
              className="inline-flex items-center gap-1 rounded-full border border-cre-border/40 bg-cre-surface px-2 py-1 text-[11px] text-cre-text hover:bg-cre-bg"
              onClick={() => remove(v)}
              title="Click to remove"
            >
              <span className="max-w-[240px] truncate">{v}</span>
              <span className="text-cre-muted">×</span>
            </button>
          ))}
          {selected.length > 40 ? (
            <div className="px-1 py-1 text-[11px] text-cre-muted">+{selected.length - 40} more</div>
          ) : null}
        </div>
      ) : (
        <div className="mt-2 text-[11px] text-cre-muted">No selections yet.</div>
      )}

      <input
        className="mt-2 w-full rounded-lg border border-cre-border/40 bg-cre-surface px-2 py-1 text-sm text-cre-text"
        value={query}
        onChange={(e) => onQuery(e.target.value)}
        placeholder="Search options..."
      />

      <div className="mt-2 max-h-40 overflow-auto rounded-lg border border-cre-border/40 bg-cre-surface p-2">
        {!options.length ? (
          <div className="text-xs text-cre-muted">No options (field not available).</div>
        ) : filtered.length ? (
          <div className="space-y-1">
            {filtered.slice(0, 250).map((o) => (
              <label key={o} className="flex items-center gap-2 text-xs text-cre-text">
                <input
                  type="checkbox"
                  checked={selectedSet.has(o)}
                  onChange={() => toggle(o)}
                />
                <span className="truncate">{o}</span>
              </label>
            ))}
            {filtered.length > 250 ? (
              <div className="pt-1 text-[11px] text-cre-muted">
                Showing first 250 matches. Refine your search.
              </div>
            ) : null}
          </div>
        ) : (
          <div className="text-xs text-cre-muted">No matches.</div>
        )}
      </div>
    </div>
  );
}

export default function MapSearch({
  onMapStatus,
}: {
  onMapStatus?: (status: MapStatus) => void;
}) {
  const [county, setCounty] = useState('orange');
  const [drawnPolygon, setDrawnPolygon] = useState<GeoJSON.Polygon | null>(null);
  const [drawnCircle, setDrawnCircle] = useState<DrawnCircle | null>(null);

  const [isDrawing, setIsDrawing] = useState(false);

  const [parcels, setParcels] = useState<ParcelSearchListItem[]>([]);
  const [records, setRecords] = useState<ParcelRecord[]>([]);
  const [selectedParcelId, setSelectedParcelId] = useState<string | null>(null);

  const [selectedPermits, setSelectedPermits] = useState<PermitRecord[]>([]);
  const [selectedPermitsLoading, setSelectedPermitsLoading] = useState(false);
  const [selectedPermitsError, setSelectedPermitsError] = useState<string | null>(null);

  const [loading, setLoading] = useState(false);
  const [errorBanner, setErrorBanner] = useState<string | null>(
    'Draw a polygon or circle, then click Run.',
  );

  type FilterForm = {
    minSqft: string;
    maxSqft: string;
    lotSizeUnit: 'sqft' | 'acres';
    minLotSize: string;
    maxLotSize: string;
    minBeds: string;
    minBaths: string;
    minYearBuilt: string;
    maxYearBuilt: string;
    propertyType: string;
    zoning: string;
    minValue: string;
    maxValue: string;
    minLandValue: string;
    maxLandValue: string;
    minBuildingValue: string;
    maxBuildingValue: string;
    lastSaleStart: string;
    lastSaleEnd: string;
  };

  const [filterForm, setFilterForm] = useState<FilterForm>({
    minSqft: '',
    maxSqft: '',
    lotSizeUnit: 'sqft',
    minLotSize: '',
    maxLotSize: '',
    minBeds: '',
    minBaths: '',
    minYearBuilt: '',
    maxYearBuilt: '',
    propertyType: '',
    zoning: '',
    minValue: '',
    maxValue: '',
    minLandValue: '',
    maxLandValue: '',
    minBuildingValue: '',
    maxBuildingValue: '',
    lastSaleStart: '',
    lastSaleEnd: '',
  });
  const [autoEnrichMissing, setAutoEnrichMissing] = useState(false);

  const [zoningOptions, setZoningOptions] = useState<string[]>([]);
  const [futureLandUseOptions, setFutureLandUseOptions] = useState<string[]>([]);
  const [zoningQuery, setZoningQuery] = useState('');
  const [futureLandUseQuery, setFutureLandUseQuery] = useState('');
  const [selectedZoning, setSelectedZoning] = useState<string[]>([]);
  const [selectedFutureLandUse, setSelectedFutureLandUse] = useState<string[]>([]);

  const [sourceCounts, setSourceCounts] = useState<{ live: number; cache: number }>({
    live: 0,
    cache: 0,
  });
  const [showLive, setShowLive] = useState(true);
  const [showCache, setShowCache] = useState(true);

  const [lastRequest, setLastRequest] = useState<any | null>(null);
  const [lastResponseCount, setLastResponseCount] = useState<number>(0);
  const [lastError, setLastError] = useState<string | null>(null);

  const [ownersQuery, setOwnersQuery] = useState('');

  const [parcelLinesEnabled, setParcelLinesEnabled] = useState(false);
  const [parcelLinesLoading, setParcelLinesLoading] = useState(false);
  const [parcelLinesError, setParcelLinesError] = useState<string | null>(null);
  const [parcelLinesFC, setParcelLinesFC] = useState<GeoJSON.FeatureCollection | null>(null);

  const drawnItemsRef = useRef<L.FeatureGroup | null>(null);
  const activeReq = useRef(0);

  const recordById = useMemo(() => {
    const m = new Map<string, ParcelRecord>();
    for (const r of records) {
      if (r?.parcel_id) m.set(r.parcel_id, r);
    }
    return m;
  }, [records]);

  const rows = useMemo(() => {
    return parcels.filter((p) => {
      const src = p.source;
      if (src === 'live') return showLive;
      if (src === 'cache') return showCache;
      return true;
    });
  }, [parcels, showCache, showLive]);

  const ownersRows = useMemo(() => {
    const q = ownersQuery.trim().toLowerCase();
    const base = parcels;
    if (!q) return base;
    return base.filter((p) => {
      const owner = (p.owner_name || '').toLowerCase();
      const addr = (p.address || '').toLowerCase();
      return owner.includes(q) || addr.includes(q) || p.parcel_id.toLowerCase().includes(q);
    });
  }, [ownersQuery, parcels]);

  const geometryStatus = useMemo(() => {
    if (drawnPolygon) return 'Polygon selected';
    if (drawnCircle) return `Circle selected (${Math.round(drawnCircle.radius_m)} m)`;
    return 'No geometry selected';
  }, [drawnCircle, drawnPolygon]);

  function clearDrawings() {
    try {
      drawnItemsRef.current?.clearLayers?.();
    } catch {
      // ignore
    }
    setDrawnPolygon(null);
    setDrawnCircle(null);
    setSelectedParcelId(null);
    setParcels([]);
    setRecords([]);
    setZoningOptions([]);
    setFutureLandUseOptions([]);
    setZoningQuery('');
    setFutureLandUseQuery('');
    setSelectedZoning([]);
    setSelectedFutureLandUse([]);
  }

  useEffect(() => {
    let cancelled = false;

    async function loadSelectedDetails(parcelId: string) {
      setSelectedPermitsError(null);
      setSelectedPermitsLoading(true);
      try {
        const permits = await permitsByParcel({ county, parcel_id: parcelId, limit: 200 });
        if (cancelled) return;
        setSelectedPermits(permits);
      } catch (e) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        setSelectedPermitsError(msg);
        setSelectedPermits([]);
      } finally {
        if (!cancelled) setSelectedPermitsLoading(false);
      }

      // Best-effort: enrich the selected parcel into PA cache for richer fields.
      if (!['orange', 'seminole'].includes((county || '').toLowerCase())) return;
      try {
        const resp = await parcelsEnrich({ county, parcel_ids: [parcelId], limit: 1 });
        if (cancelled) return;
        const enriched = resp.records || [];
        if (!enriched.length) return;
        const rec = enriched[0];
        setRecords((prev) => prev.map((r) => (r.parcel_id === parcelId ? { ...r, ...rec } : r)));
      } catch {
        // ignore enrichment errors for the details panel
      }
    }

    if (!selectedParcelId) {
      setSelectedPermits([]);
      setSelectedPermitsLoading(false);
      setSelectedPermitsError(null);
      return;
    }

    void loadSelectedDetails(selectedParcelId);
    return () => {
      cancelled = true;
    };
  }, [county, selectedParcelId]);

  useEffect(() => {
    let cancelled = false;

    async function loadParcelLines() {
      if (!parcelLinesEnabled) return;
      setParcelLinesError(null);

      const ids = parcels.map((p) => p.parcel_id).filter(Boolean).slice(0, 50);
      if (!ids.length) {
        setParcelLinesFC(null);
        return;
      }

      setParcelLinesLoading(true);
      try {
        const fc = await parcelsGeometry({ county, parcel_ids: ids });
        if (cancelled) return;
        if (!fc.features?.length) {
          setParcelLinesFC(null);
          setParcelLinesEnabled(false);
          setParcelLinesError('Parcel geometry not available for this county yet.');
          return;
        }
        setParcelLinesFC(fc);
      } catch (e) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        setParcelLinesFC(null);
        setParcelLinesEnabled(false);
        setParcelLinesError(msg);
      } finally {
        if (!cancelled) setParcelLinesLoading(false);
      }
    }

    // Only fetch geometry when the user has an active search area.
    if (!drawnPolygon && !drawnCircle) {
      setParcelLinesFC(null);
      return;
    }

    void loadParcelLines();
    return () => {
      cancelled = true;
    };
  }, [county, drawnCircle, drawnPolygon, parcelLinesEnabled, parcels]);

  async function enrichVisible() {
    setLastError(null);
    setErrorBanner(null);
    if (!parcels.length) {
      setErrorBanner('No records to enrich yet.');
      return;
    }

    const ids = parcels.map((p) => p.parcel_id)
      .slice(0, 150);

    if (!ids.length) return;

    setLoading(true);
    const reqId = ++activeReq.current;
    try {
      const resp = await parcelsEnrich({ county, parcel_ids: ids, limit: ids.length });
      if (reqId !== activeReq.current) return;

      const enriched = resp.records || [];
      const map = new Map(enriched.map((r) => [r.parcel_id, r] as const));
      const merged = records.length
        ? records.map((r) => map.get(r.parcel_id) ?? r)
        : enriched;

      setRecords(merged);

      const counts = { live: 0, cache: 0 };
      for (const r of merged) {
        if (r.source === 'live') counts.live++;
        else if (r.source === 'cache') counts.cache++;
      }
      setSourceCounts(counts);
      setErrorBanner('Enrichment complete (cached into PA DB).');
    } catch (e) {
      if (reqId !== activeReq.current) return;
      const msg = e instanceof Error ? e.message : String(e);
      setLastError(msg);
      setErrorBanner(`Enrichment failed: ${msg}`);
    } finally {
      if (reqId === activeReq.current) setLoading(false);
    }
  }

  async function run() {
    setLastError(null);
    setErrorBanner(null);

    if (!drawnPolygon && !drawnCircle) {
      setErrorBanner('Draw polygon or radius first');
      setParcels([]);
      setRecords([]);
      return;
    }

    const toIntOrNull = (v: string): number | null => {
      const s = v.trim();
      if (!s) return null;
      const n = Number(s);
      if (!Number.isFinite(n)) return null;
      return Math.trunc(n);
    };
    const toFloatOrNull = (v: string): number | null => {
      const s = v.trim();
      if (!s) return null;
      const n = Number(s);
      if (!Number.isFinite(n)) return null;
      return n;
    };

    const minLotSize = toFloatOrNull(filterForm.minLotSize);
    const maxLotSize = toFloatOrNull(filterForm.maxLotSize);
    const hasLotSize = minLotSize !== null || maxLotSize !== null;
    const lotSizeUnit = filterForm.lotSizeUnit === 'acres' ? 'acres' : 'sqft';

    const toSqft = (v: number | null): number | null => {
      if (v === null) return null;
      if (lotSizeUnit === 'acres') return v * 43560.0;
      return v;
    };
    const minLotSizeSqft = hasLotSize ? toSqft(minLotSize) : null;
    const maxLotSizeSqft = hasLotSize ? toSqft(maxLotSize) : null;

    const filters: ParcelAttributeFilters = {
      min_sqft: toIntOrNull(filterForm.minSqft),
      max_sqft: toIntOrNull(filterForm.maxSqft),
      // Preferred contract: always send normalized sqft values.
      min_lot_size_sqft: minLotSizeSqft,
      max_lot_size_sqft: maxLotSizeSqft,
      // Legacy contract (kept null to avoid accidental double-filtering).
      lot_size_unit: null,
      min_lot_size: null,
      max_lot_size: null,
      min_beds: toIntOrNull(filterForm.minBeds),
      min_baths: toFloatOrNull(filterForm.minBaths),
      min_year_built: toIntOrNull(filterForm.minYearBuilt),
      max_year_built: toIntOrNull(filterForm.maxYearBuilt),
      property_type: filterForm.propertyType.trim() || null,
      zoning: filterForm.zoning.trim() || null,
      zoning_in: selectedZoning.length ? selectedZoning : null,
      future_land_use_in: selectedFutureLandUse.length ? selectedFutureLandUse : null,
      min_value: toIntOrNull(filterForm.minValue),
      max_value: toIntOrNull(filterForm.maxValue),
      min_land_value: toIntOrNull(filterForm.minLandValue),
      max_land_value: toIntOrNull(filterForm.maxLandValue),
      min_building_value: toIntOrNull(filterForm.minBuildingValue),
      max_building_value: toIntOrNull(filterForm.maxBuildingValue),
      last_sale_date_start: filterForm.lastSaleStart.trim() || null,
      last_sale_date_end: filterForm.lastSaleEnd.trim() || null,
    };
    const hasAnyFilters = Object.values(filters).some((v) => v !== null && v !== undefined && v !== '');

    const payload: any = {
      county,
      live: true,
      limit: 50,
      include_geometry: false,
      filters: hasAnyFilters ? filters : undefined,
      enrich: hasAnyFilters ? autoEnrichMissing : false,
      enrich_limit: hasAnyFilters ? 25 : undefined,
    };

    if (drawnPolygon) {
      payload.polygon_geojson = drawnPolygon;
    } else if (drawnCircle) {
      payload.center = drawnCircle.center;
      payload.radius_m = drawnCircle.radius_m;
    }

    setLastRequest(payload);
    setLoading(true);

    const reqId = ++activeReq.current;
    try {
      const resp = await parcelsSearchNormalized(payload);
      if (reqId !== activeReq.current) return;

      const uniqSorted = (arr: unknown): string[] => {
        if (!Array.isArray(arr)) return [];
        const set = new Set<string>();
        for (const v of arr) {
          if (typeof v !== 'string') continue;
          const s = v.trim();
          if (s) set.add(s);
        }
        return Array.from(set).sort((a, b) => a.localeCompare(b));
      };

      setZoningOptions(uniqSorted((resp as any).zoning_options));
      setFutureLandUseOptions(uniqSorted((resp as any).future_land_use_options));

      const recs = resp.records || [];
      const list = Array.isArray((resp as any).parcels) ? ((resp as any).parcels as ParcelSearchListItem[]) : [];
      const warnings = (resp as any).warnings as string[] | undefined;
      setLastResponseCount(list.length || recs.length);

      const rawCounts = resp.summary?.source_counts || {};
      setSourceCounts({
        live: Number(rawCounts.live || 0),
        cache: Number(rawCounts.cache || 0),
      });

      if (!list.length && !recs.length) {
        const msg = warnings?.length
          ? `No results returned. ${warnings.join(' / ')}`
          : 'No results returned from backend.';
        setLastError(msg);
        setErrorBanner(msg);
        setParcels([]);
        setRecords([]);
        return;
      }

      if (warnings?.length) {
        setErrorBanner(`Warnings: ${warnings.join(' / ')}`);
      }

      setParcels(list);
      setRecords(recs);

      // Refresh owner-list query results immediately.
      setOwnersQuery('');

      // If parcel lines are enabled, refresh them based on the new parcel_ids.
      setParcelLinesError(null);
      setParcelLinesFC(null);
    } catch (e) {
      if (reqId !== activeReq.current) return;
      const msg = e instanceof Error ? e.message : String(e);
      setLastError(msg);
      setErrorBanner(`Request failed: ${msg}`);
      setParcels([]);
      setRecords([]);
      setParcelLinesFC(null);
      setParcelLinesEnabled(false);
      setParcelLinesError(null);
    } finally {
      if (reqId === activeReq.current) setLoading(false);
    }
  }

  return (
    <div className="flex h-full min-h-[520px]">
      <aside className="w-[420px] shrink-0 border-r border-cre-border/40 bg-cre-surface p-4">
        <div className="flex items-center justify-between gap-2">
          <div>
            <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">
              Map Search
            </div>
            <div className="text-sm text-cre-text">Polygon or radius → parcels</div>
          </div>

          <select
            className="rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-sm text-cre-text"
            value={county}
            onChange={(e: ChangeEvent<HTMLSelectElement>) => {
              setCounty(e.target.value);
              setParcels([]);
              setRecords([]);
              setSourceCounts({ live: 0, cache: 0 });
              setSelectedParcelId(null);
              setZoningOptions([]);
              setFutureLandUseOptions([]);
              setZoningQuery('');
              setFutureLandUseQuery('');
              setSelectedZoning([]);
              setSelectedFutureLandUse([]);
            }}
          >
            <option value="orange">Orange</option>
            <option value="seminole">Seminole</option>
            <option value="broward">Broward</option>
            <option value="alachua">Alachua</option>
          </select>
        </div>

        {errorBanner ? (
          <div className="mt-3 rounded-xl border border-cre-accent/40 bg-cre-bg p-3 text-sm text-cre-text">
            <div className="font-semibold text-cre-accent">Notice</div>
            <div className="mt-1 text-xs text-cre-muted">{errorBanner}</div>
          </div>
        ) : null}

        <div className="mt-4 rounded-xl border border-cre-border/40 bg-cre-bg p-3">
          <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Filters</div>
          <div className="mt-2 grid grid-cols-2 gap-2 text-xs">
            <label className="space-y-1">
              <div className="text-cre-muted">Min Sqft</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.minSqft}
                onChange={(e) => setFilterForm((p) => ({ ...p, minSqft: e.target.value }))}
                placeholder="e.g. 2000"
              />
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Max Sqft</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.maxSqft}
                onChange={(e) => setFilterForm((p) => ({ ...p, maxSqft: e.target.value }))}
                placeholder=""
              />
            </label>

            <label className="space-y-1">
              <div className="text-cre-muted">Parcel Size Unit</div>
              <select
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                value={filterForm.lotSizeUnit}
                onChange={(e) =>
                  setFilterForm((p) => ({
                    ...p,
                    lotSizeUnit: (e.target.value === 'acres' ? 'acres' : 'sqft') as any,
                  }))
                }
              >
                <option value="sqft">Sqft</option>
                <option value="acres">Acres</option>
              </select>
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Min Parcel Size</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="decimal"
                value={filterForm.minLotSize}
                onChange={(e) => setFilterForm((p) => ({ ...p, minLotSize: e.target.value }))}
                placeholder={filterForm.lotSizeUnit === 'acres' ? 'e.g. 0.25' : 'e.g. 8000'}
              />
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Max Parcel Size</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="decimal"
                value={filterForm.maxLotSize}
                onChange={(e) => setFilterForm((p) => ({ ...p, maxLotSize: e.target.value }))}
                placeholder={filterForm.lotSizeUnit === 'acres' ? '' : ''}
              />
            </label>

            <label className="space-y-1">
              <div className="text-cre-muted">Min Beds</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.minBeds}
                onChange={(e) => setFilterForm((p) => ({ ...p, minBeds: e.target.value }))}
                placeholder="e.g. 3"
              />
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Min Baths</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="decimal"
                value={filterForm.minBaths}
                onChange={(e) => setFilterForm((p) => ({ ...p, minBaths: e.target.value }))}
                placeholder="e.g. 2"
              />
            </label>

            <label className="space-y-1">
              <div className="text-cre-muted">Min Year Built</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.minYearBuilt}
                onChange={(e) => setFilterForm((p) => ({ ...p, minYearBuilt: e.target.value }))}
                placeholder="e.g. 1990"
              />
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Max Year Built</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.maxYearBuilt}
                onChange={(e) => setFilterForm((p) => ({ ...p, maxYearBuilt: e.target.value }))}
                placeholder=""
              />
            </label>

            <label className="space-y-1">
              <div className="text-cre-muted">Property Type</div>
              <select
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                value={filterForm.propertyType}
                onChange={(e) => setFilterForm((p) => ({ ...p, propertyType: e.target.value }))}
              >
                <option value="">Any</option>
                <option value="residential">Residential</option>
                <option value="commercial">Commercial</option>
              </select>
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Zoning (contains)</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                value={filterForm.zoning}
                onChange={(e) => setFilterForm((p) => ({ ...p, zoning: e.target.value }))}
                placeholder="e.g. R-1"
              />
            </label>

          <div className="mt-3 space-y-3">
            <MultiSelectFilter
              title="Current Zoning (multi-select)"
              options={zoningOptions}
              selected={selectedZoning}
              query={zoningQuery}
              onQuery={setZoningQuery}
              onSelected={setSelectedZoning}
            />
            <MultiSelectFilter
              title="Future Land Use (multi-select)"
              options={futureLandUseOptions}
              selected={selectedFutureLandUse}
              query={futureLandUseQuery}
              onQuery={setFutureLandUseQuery}
              onSelected={setSelectedFutureLandUse}
            />
            <div className="text-[11px] text-cre-muted">
              Tip: “Zoning contains” and multi-select both apply (AND).
            </div>
          </div>

            <label className="space-y-1">
              <div className="text-cre-muted">Min Total Value</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.minValue}
                onChange={(e) => setFilterForm((p) => ({ ...p, minValue: e.target.value }))}
                placeholder="e.g. 350000"
              />
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Max Total Value</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.maxValue}
                onChange={(e) => setFilterForm((p) => ({ ...p, maxValue: e.target.value }))}
                placeholder=""
              />
            </label>

            <label className="space-y-1">
              <div className="text-cre-muted">Min Land Value</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.minLandValue}
                onChange={(e) => setFilterForm((p) => ({ ...p, minLandValue: e.target.value }))}
                placeholder=""
              />
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Max Land Value</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.maxLandValue}
                onChange={(e) => setFilterForm((p) => ({ ...p, maxLandValue: e.target.value }))}
                placeholder=""
              />
            </label>

            <label className="space-y-1">
              <div className="text-cre-muted">Min Building Value</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.minBuildingValue}
                onChange={(e) => setFilterForm((p) => ({ ...p, minBuildingValue: e.target.value }))}
                placeholder=""
              />
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Max Building Value</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                inputMode="numeric"
                value={filterForm.maxBuildingValue}
                onChange={(e) => setFilterForm((p) => ({ ...p, maxBuildingValue: e.target.value }))}
                placeholder=""
              />
            </label>

            <label className="space-y-1">
              <div className="text-cre-muted">Last Sale Start</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                type="date"
                value={filterForm.lastSaleStart}
                onChange={(e) => setFilterForm((p) => ({ ...p, lastSaleStart: e.target.value }))}
              />
            </label>
            <label className="space-y-1">
              <div className="text-cre-muted">Last Sale End</div>
              <input
                className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-cre-text"
                type="date"
                value={filterForm.lastSaleEnd}
                onChange={(e) => setFilterForm((p) => ({ ...p, lastSaleEnd: e.target.value }))}
              />
            </label>
          </div>

          <label className="mt-3 flex items-center gap-2 text-xs text-cre-text">
            <input
              type="checkbox"
              checked={autoEnrichMissing}
              onChange={(e) => setAutoEnrichMissing(e.target.checked)}
            />
            Auto-enrich missing (slower)
          </label>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <button
            type="button"
            className="rounded-xl bg-cre-accent px-4 py-2 text-sm font-semibold text-black hover:brightness-95 disabled:opacity-60"
            onClick={() => void run()}
            disabled={loading}
          >
            {loading ? 'Running…' : 'Run'}
          </button>
          <button
            type="button"
            className="rounded-xl border border-cre-border/40 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface"
            onClick={clearDrawings}
          >
            Clear
          </button>

          <button
            type="button"
            className="rounded-xl border border-cre-border/40 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface disabled:opacity-60"
            onClick={() => void enrichVisible()}
            disabled={loading || parcels.length === 0}
            title="Fetch live attributes + cache"
          >
            Enrich results
          </button>
        </div>

        <div className="mt-4 overflow-hidden rounded-xl border border-cre-border/40">
          <div className="border-b border-cre-border/40 bg-cre-bg px-3 py-2 text-xs font-semibold uppercase tracking-widest text-cre-muted">
            Owners (Polygon): {ownersRows.length}
          </div>

          <div className="border-b border-cre-border/40 bg-cre-bg px-3 py-2">
            <input
              className="w-full rounded-lg border border-cre-border/40 bg-cre-bg px-2 py-1 text-xs text-cre-text"
              placeholder="Filter by owner, address, or parcel id"
              value={ownersQuery}
              onChange={(e) => setOwnersQuery(e.target.value)}
            />
            {!parcels.length ? (
              <div className="mt-2 text-xs text-cre-muted">No parcels found.</div>
            ) : null}
          </div>

          <div className="max-h-[240px] overflow-auto">
            <table className="w-full text-left text-xs">
              <thead className="sticky top-0 bg-cre-surface text-[11px] uppercase tracking-widest text-cre-muted">
                <tr>
                  <th className="px-2 py-2">Owner</th>
                  <th className="px-2 py-2">Address</th>
                  <th className="px-2 py-2">Parcel ID</th>
                </tr>
              </thead>
              <tbody>
                {ownersRows.map((p) => {
                  const selected = selectedParcelId === p.parcel_id;
                  return (
                    <tr
                      key={`owners:${p.parcel_id}`}
                      className={
                        selected
                          ? 'bg-cre-accent/15'
                          : 'border-t border-cre-border/30 hover:bg-cre-bg'
                      }
                      onClick={() => setSelectedParcelId(p.parcel_id)}
                    >
                      <td className="px-2 py-2 text-cre-text">{p.owner_name || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{p.address || '—'}</td>
                      <td className="px-2 py-2 font-mono text-[11px] text-cre-text">{p.parcel_id}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        <div className="mt-4 text-xs text-cre-muted">
          Draw: polygon tool or circle tool (top-right on the map).
        </div>

        <div className="mt-2 text-xs text-cre-muted">
          Status: <span className="font-semibold text-cre-text">{geometryStatus}</span>
        </div>

        <div className="mt-4 overflow-hidden rounded-xl border border-cre-border/40">
          <div className="border-b border-cre-border/40 bg-cre-bg px-3 py-2 text-xs font-semibold uppercase tracking-widest text-cre-muted">
            Results: {rows.length} (live {sourceCounts.live} / cache {sourceCounts.cache})
          </div>

          <div className="flex flex-wrap gap-3 border-b border-cre-border/40 bg-cre-bg px-3 py-2 text-xs text-cre-text">
            <label className="flex items-center gap-2">
              <input type="checkbox" checked={showLive} onChange={(e) => setShowLive(e.target.checked)} />
              Live
            </label>
            <label className="flex items-center gap-2">
              <input type="checkbox" checked={showCache} onChange={(e) => setShowCache(e.target.checked)} />
              Cache
            </label>
          </div>

          <div className="max-h-[420px] overflow-auto">
            <table className="w-full text-left text-xs">
              <thead className="sticky top-0 bg-cre-surface text-[11px] uppercase tracking-widest text-cre-muted">
                <tr>
                  <th className="px-2 py-2">Parcel</th>
                  <th className="px-2 py-2">Src</th>
                  <th className="px-2 py-2">Address</th>
                  <th className="px-2 py-2">Owner</th>
                  <th className="px-2 py-2">Zoning</th>
                  <th className="px-2 py-2">Land use</th>
                  <th className="px-2 py-2">Sqft</th>
                  <th className="px-2 py-2">Beds</th>
                  <th className="px-2 py-2">Baths</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((p) => {
                  const rec = recordById.get(p.parcel_id);
                  const selected = selectedParcelId === p.parcel_id;

                  const addr = (p.address || rec?.situs_address || rec?.address || '').trim();
                  const owner = (p.owner_name || rec?.owner_name || '').trim();
                  const srcLabel = (p.source || (rec as any)?.source || '—').toString().toUpperCase();
                  const sqftLiving = rec?.sqft?.find((s) => s.type === 'living')?.value ?? rec?.living_area_sqft ?? null;
                  const zoning = (rec?.zoning || '').trim();
                  const landUse = ((rec as any)?.flu || rec?.land_use || '').toString().trim();
                  const beds = rec?.beds ?? null;
                  const baths = rec?.baths ?? null;
                  return (
                    <tr
                      key={p.parcel_id}
                      className={
                        selected
                          ? 'bg-cre-accent/15'
                          : 'border-t border-cre-border/30 hover:bg-cre-bg'
                      }
                      onClick={() => setSelectedParcelId(p.parcel_id)}
                    >
                      <td className="px-2 py-2 font-mono text-[11px] text-cre-text">{p.parcel_id}</td>
                      <td className="px-2 py-2 text-[11px] text-cre-text">{srcLabel}</td>
                      <td className="px-2 py-2 text-cre-text">{addr || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{owner || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{zoning || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{landUse || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{sqftLiving ?? '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{beds ?? '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{baths ?? '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        {selectedParcelId ? (
          <div className="mt-4 rounded-xl border border-cre-border/40 bg-cre-bg p-3 text-xs text-cre-text">
            {(() => {
              const rec = records.find((r) => r.parcel_id === selectedParcelId);
              const p = parcels.find((x) => x.parcel_id === selectedParcelId) || null;
              if (!rec) {
                if (!p) return <div className="text-cre-muted">Select a parcel to view details.</div>;
                return (
                  <div className="space-y-2">
                    <div className="flex items-center justify-between gap-2">
                      <div className="font-semibold">Details</div>
                      <div className="text-[11px] text-cre-muted">Source: {(p.source || '—').toUpperCase()}</div>
                    </div>
                    <div className="font-mono text-[11px] text-cre-muted">{p.parcel_id}</div>
                    <div>{p.address || '—'}</div>
                    <div>Owner: {p.owner_name || '—'}</div>
                    <div className="text-[11px] text-cre-muted">(More fields available after enrichment.)</div>
                  </div>
                );
              }
              const url = rec.raw_source_url || '';
              const sources = (rec.data_sources || []).filter((s) => s && s.url);
              const sqftLiving =
                rec.sqft?.find((s) => s.type === 'living')?.value ?? rec.living_area_sqft ?? null;
              const photoUrl = (rec.photo_url || '').trim();
              const lender = (rec.mortgage_lender || '').trim();
              const mortgageAmount = typeof rec.mortgage_amount === 'number' ? rec.mortgage_amount : null;
              const mortgageDate = (rec.mortgage_date || '').trim();
              return (
                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-2">
                    <div className="font-semibold">Details</div>
                    <div className="text-[11px] text-cre-muted">Source: {(rec.source || '—').toUpperCase()}</div>
                  </div>

                  {photoUrl ? (
                    <a href={photoUrl} target="_blank" rel="noreferrer">
                      <img
                        src={photoUrl}
                        alt="Parcel photo"
                        className="w-full rounded-lg border border-cre-border/40"
                        loading="lazy"
                      />
                    </a>
                  ) : null}

                  <div className="font-mono text-[11px] text-cre-muted">{rec.parcel_id}</div>
                  <div>{rec.situs_address || rec.address || '—'}</div>
                  <div>Owner: {rec.owner_name || '—'}</div>
                  <div>Land use: {rec.land_use || '—'}</div>
                  <div>Zoning: {rec.zoning || '—'}</div>
                  <div>Year built: {rec.year_built ?? '—'}</div>
                  <div>Living sqft: {sqftLiving ?? '—'}</div>
                  <div>Total value: {rec.total_value ?? '—'}</div>
                  <div>Last sale price: {rec.last_sale_price ?? '—'}</div>

                  <div className="pt-1">
                    <div className="font-semibold">Mortgage</div>
                    <div>Lender: {lender || '—'}</div>
                    <div>Original amount: {mortgageAmount ?? '—'}</div>
                    <div>Date: {mortgageDate || '—'}</div>
                  </div>

                  <div className="pt-1">
                    <div className="font-semibold">Permits</div>
                    {selectedPermitsLoading ? (
                      <div className="text-cre-muted">Loading permits…</div>
                    ) : selectedPermitsError ? (
                      <div className="text-cre-muted">Permits unavailable: {selectedPermitsError}</div>
                    ) : selectedPermits.length ? (
                      <div className="space-y-1">
                        <div className="text-[11px] text-cre-muted">{selectedPermits.length} record(s)</div>
                        {selectedPermits.slice(0, 25).map((p) => (
                          <div key={`${p.county}:${p.permit_number}`} className="rounded-lg border border-cre-border/40 bg-cre-surface p-2">
                            <div className="flex items-center justify-between gap-2">
                              <div className="font-mono text-[11px] text-cre-text">{p.permit_number}</div>
                              <div className="text-[11px] text-cre-muted">{p.status || '—'}</div>
                            </div>
                            <div className="text-cre-text">{p.permit_type || '—'}</div>
                            <div className="text-[11px] text-cre-muted">
                              Issued: {p.issue_date || '—'} · Final: {p.final_date || '—'}
                            </div>
                            {p.description ? <div className="pt-1 text-cre-text">{p.description}</div> : null}
                          </div>
                        ))}
                        {selectedPermits.length > 25 ? (
                          <div className="text-[11px] text-cre-muted">
                            Showing first 25 permits.
                          </div>
                        ) : null}
                      </div>
                    ) : (
                      <div className="text-cre-muted">No permits found in DB for this parcel.</div>
                    )}
                  </div>

                  {sources.length ? (
                    <div>
                      Data sources:{' '}
                      {sources.map((s, i) => (
                        <span key={`${s.name}:${s.url}`}>
                          {i ? ' / ' : ''}
                          <a className="underline" href={s.url} target="_blank" rel="noreferrer">
                            {s.name || 'source'}
                          </a>
                        </span>
                      ))}
                    </div>
                  ) : url ? (
                    <div>
                      Raw source:{' '}
                      <a className="underline" href={url} target="_blank" rel="noreferrer">
                        open
                      </a>
                    </div>
                  ) : null}
                </div>
              );
            })()}
          </div>
        ) : null}

        <details className="mt-4 rounded-xl border border-cre-border/40 bg-cre-bg p-3">
          <summary className="cursor-pointer text-sm font-semibold text-cre-text">Debug</summary>
          <div className="mt-3 space-y-2 text-xs text-cre-muted">
            <div>
              <div className="font-semibold text-cre-text">Last response count</div>
              <div>{lastResponseCount}</div>
            </div>
            <div>
              <div className="font-semibold text-cre-text">Last error</div>
              <pre className="whitespace-pre-wrap break-words">{lastError || '—'}</pre>
            </div>
            <div>
              <div className="font-semibold text-cre-text">Last request JSON</div>
              <pre className="max-h-56 overflow-auto whitespace-pre-wrap break-words">{JSON.stringify(lastRequest, null, 2) || '—'}</pre>
            </div>
          </div>
        </details>
      </aside>

      <main className="flex-1 bg-cre-bg p-4">
        <div className="relative h-full overflow-hidden rounded-2xl border border-cre-border/40 bg-cre-surface shadow-panel">
          <div className="absolute left-3 top-3 z-[500] rounded-xl border border-cre-border/40 bg-cre-bg/95 px-3 py-2 text-xs text-cre-text shadow-panel">
            <label className="flex items-center gap-2" title="May be slower; available where supported.">
              <input
                type="checkbox"
                checked={parcelLinesEnabled}
                disabled={isDrawing || parcelLinesLoading}
                onChange={(e) => {
                  const next = e.target.checked;
                  setParcelLinesError(null);
                  setParcelLinesFC(null);
                  setParcelLinesEnabled(next);
                }}
              />
              Parcel Lines (beta)
              {parcelLinesLoading ? <span className="text-cre-muted">…</span> : null}
            </label>
            {parcelLinesError ? (
              <div className="mt-1 max-w-[320px] text-[11px] text-cre-muted">{parcelLinesError}</div>
            ) : null}
          </div>

          <MapContainer
            center={[28.5383, -81.3792]}
            zoom={12}
            doubleClickZoom={false}
            style={{ height: '100%', width: '100%' }}
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              eventHandlers={{
                load: () => onMapStatus?.('loaded'),
                tileerror: () => onMapStatus?.('failed'),
              }}
            />

            <DrawControls
              drawnItemsRef={drawnItemsRef}
              onDrawingChange={(v) => setIsDrawing(v)}
              onPolygon={(geom) => {
                setSelectedParcelId(null);
                setErrorBanner(null);
                setDrawnPolygon(geom);
                setDrawnCircle(null);
              }}
              onDeleted={() => {
                setDrawnPolygon(null);
                setDrawnCircle(null);
              }}
            />

            {parcelLinesEnabled && parcelLinesFC ? (
              <GeoJSON
                data={parcelLinesFC as any}
                interactive={!isDrawing}
                style={() => ({
                  color: '#3b82f6',
                  weight: 2,
                  opacity: 0.85,
                  fillOpacity: 0.0,
                })}
                onEachFeature={(feature, layer) => {
                  const props: any = (feature as any)?.properties || {};
                  const addr = String(props.situs_address || '').trim();
                  const owner = String(props.owner_name || '').trim();
                  const pid = String(props.parcel_id || '').trim();
                  const lines = [addr || '—', owner ? `Owner: ${owner}` : 'Owner: —', pid ? `Parcel: ${pid}` : 'Parcel: —'];
                  try {
                    (layer as any).bindTooltip(lines.join('\n'), {
                      sticky: true,
                      direction: 'top',
                      opacity: 0.95,
                      className: 'text-xs',
                    });
                  } catch {
                    // ignore
                  }
                }}
              />
            ) : null}

            {rows.map((p) => {
              const lat = typeof p.lat === 'number' && Number.isFinite(p.lat) ? p.lat : null;
              const lng = typeof p.lng === 'number' && Number.isFinite(p.lng) ? p.lng : null;
              if (lat === null || lng === null) return null;

              const selected = selectedParcelId === p.parcel_id;
              const pos: [number, number] = [lat, lng];
              if (selected) {
                return (
                  <CircleMarker
                    key={p.parcel_id}
                    center={pos}
                    radius={9}
                    pathOptions={{ color: '#D08E02', weight: 2, fillOpacity: 0.35 }}
                  />
                );
              }
              return (
                <Marker
                  key={p.parcel_id}
                  position={pos}
                  interactive={!isDrawing}
                  eventHandlers={{
                    click: () => setSelectedParcelId(p.parcel_id),
                  }}
                />
              );
            })}
          </MapContainer>
        </div>
      </main>
    </div>
  );
}
