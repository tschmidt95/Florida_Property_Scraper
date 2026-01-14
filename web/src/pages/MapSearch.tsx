import { useEffect, useMemo, useRef, useState, type ChangeEvent } from 'react';

import L from 'leaflet';
import type { LatLngLiteral } from 'leaflet';
import { CircleMarker, FeatureGroup, MapContainer, Marker, TileLayer } from 'react-leaflet';
import { EditControl } from 'react-leaflet-draw';

import 'leaflet-draw';

import { parcelsEnrich, parcelsSearch, type ParcelAttributeFilters, type ParcelRecord } from '../lib/api';

type MapStatus = 'loading' | 'loaded' | 'failed';

type DrawnCircle = { center: LatLngLiteral; radius_m: number };

function patchLeafletDrawPolygonBehavior(): void {
  // Leaflet.draw default UX finishes a polygon by clicking the first vertex.
  // For acquisition search we only want explicit finish (toolbar) or double-click.
  const anyL = L as any;
  const proto = anyL?.Draw?.Polygon?.prototype;
  if (!proto || proto.__fpsPatched) return;
  proto.__fpsPatched = true;

  const originalUpdateFinishHandler = proto._updateFinishHandler;
  proto._updateFinishHandler = function patchedUpdateFinishHandler(this: any) {
    if (typeof originalUpdateFinishHandler === 'function') {
      originalUpdateFinishHandler.call(this);
    }
    try {
      // Remove click-to-finish on the first marker. Keep dblclick + toolbar finish.
      const first = Array.isArray(this._markers) ? this._markers[0] : null;
      if (first && typeof first.off === 'function' && this._finishShape) {
        first.off('click', this._finishShape, this);
      }
    } catch {
      // ignore
    }
  };
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

  const [records, setRecords] = useState<ParcelRecord[]>([]);
  const [selectedParcelId, setSelectedParcelId] = useState<string | null>(null);

  const [loading, setLoading] = useState(false);
  const [errorBanner, setErrorBanner] = useState<string | null>(
    'Draw a polygon or circle, then click Run.',
  );

  type FilterForm = {
    minSqft: string;
    maxSqft: string;
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

  const [sourceCounts, setSourceCounts] = useState<{ live: number; cache: number }>({
    live: 0,
    cache: 0,
  });
  const [showLive, setShowLive] = useState(true);
  const [showCache, setShowCache] = useState(true);

  const [lastRequest, setLastRequest] = useState<any | null>(null);
  const [lastResponseCount, setLastResponseCount] = useState<number>(records.length);
  const [lastError, setLastError] = useState<string | null>(null);

  const featureGroupRef = useRef<any>(null);
  const activeReq = useRef(0);

  const drawOptions = useMemo(() => {
    return {
      polyline: false,
      rectangle: false,
      marker: false,
      circlemarker: false,
      polygon: {
        // Explicitly ensure unlimited vertices.
        // Leaflet.draw treats 0/undefined as "no cap".
        maxPoints: 0,
        repeatMode: false,
      },
      circle: true,
    };
  }, []);
  const editOptions = useMemo(() => {
    return { edit: {}, remove: true };
  }, []);

  const rows = useMemo(() => {
    return records.filter((r) => {
      if (r.source === 'live') return showLive;
      if (r.source === 'cache') return showCache;
      return true;
    });
  }, [records, showCache, showLive]);

  const geometryStatus = useMemo(() => {
    if (drawnPolygon) return 'Polygon selected';
    if (drawnCircle) return `Circle selected (${Math.round(drawnCircle.radius_m)} m)`;
    return 'No geometry selected';
  }, [drawnCircle, drawnPolygon]);

  function clearDrawings() {
    try {
      featureGroupRef.current?.clearLayers?.();
    } catch {
      // ignore
    }
    setDrawnPolygon(null);
    setDrawnCircle(null);
    setSelectedParcelId(null);
  }

  useEffect(() => {
    patchLeafletDrawPolygonBehavior();
  }, []);

  async function enrichVisible() {
    setLastError(null);
    setErrorBanner(null);
    if (!records.length) {
      setErrorBanner('No records to enrich yet.');
      return;
    }

    const ids = records.map((r) => r.parcel_id)
      .slice(0, 150);

    if (!ids.length) return;

    setLoading(true);
    const reqId = ++activeReq.current;
    try {
      const resp = await parcelsEnrich({ county, parcel_ids: ids, limit: ids.length });
      if (reqId !== activeReq.current) return;

      const enriched = resp.records || [];
      const map = new Map(enriched.map((r) => [r.parcel_id, r] as const));
      const merged = records.map((r) => map.get(r.parcel_id) ?? r);

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

    const filters: ParcelAttributeFilters = {
      min_sqft: toIntOrNull(filterForm.minSqft),
      max_sqft: toIntOrNull(filterForm.maxSqft),
      min_beds: toIntOrNull(filterForm.minBeds),
      min_baths: toFloatOrNull(filterForm.minBaths),
      min_year_built: toIntOrNull(filterForm.minYearBuilt),
      max_year_built: toIntOrNull(filterForm.maxYearBuilt),
      property_type: filterForm.propertyType.trim() || null,
      zoning: filterForm.zoning.trim() || null,
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
      const resp = await parcelsSearch(payload);
      if (reqId !== activeReq.current) return;

      const recs = resp.records || [];
      const warnings = (resp as any).warnings as string[] | undefined;
      setLastResponseCount(recs.length);

      const rawCounts = resp.summary?.source_counts || {};
      setSourceCounts({
        live: Number(rawCounts.live || 0),
        cache: Number(rawCounts.cache || 0),
      });

      if (!recs.length) {
        const msg = warnings?.length
          ? `No results returned. ${warnings.join(' / ')}`
          : 'No results returned from backend.';
        setLastError(msg);
        setErrorBanner(msg);
        setRecords([]);
        return;
      }

      if (warnings?.length) {
        setErrorBanner(`Warnings: ${warnings.join(' / ')}`);
      }

      setRecords(recs);
    } catch (e) {
      if (reqId !== activeReq.current) return;
      const msg = e instanceof Error ? e.message : String(e);
      setLastError(msg);
      setErrorBanner(`Request failed: ${msg}`);
      setRecords([]);
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
              setRecords([]);
              setSourceCounts({ live: 0, cache: 0 });
              setSelectedParcelId(null);
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
            disabled={loading || records.length === 0}
            title="Fetch live attributes + cache"
          >
            Enrich results
          </button>
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
                {rows.map((r) => {
                  const selected = selectedParcelId === r.parcel_id;
                  const addr = (r.address || r.situs_address || '').trim();
                  const sqftLiving = r.sqft?.find((s) => s.type === 'living')?.value ?? r.living_area_sqft ?? null;
                  const srcLabel = r.source.toUpperCase();
                  return (
                    <tr
                      key={r.parcel_id}
                      className={
                        selected
                          ? 'bg-cre-accent/15'
                          : 'border-t border-cre-border/30 hover:bg-cre-bg'
                      }
                      onClick={() => setSelectedParcelId(r.parcel_id)}
                    >
                      <td className="px-2 py-2 font-mono text-[11px] text-cre-text">{r.parcel_id}</td>
                      <td className="px-2 py-2 text-[11px] text-cre-text">{srcLabel}</td>
                      <td className="px-2 py-2 text-cre-text">{addr || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.owner_name || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.zoning || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{(r.flu || r.land_use) || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{sqftLiving ?? '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.beds ?? '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.baths ?? '—'}</td>
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
              if (!rec) return <div className="text-cre-muted">Select a parcel to view details.</div>;
              const url = rec.raw_source_url || '';
              const sources = (rec.data_sources || []).filter((s) => s && s.url);
              const sqftLiving =
                rec.sqft?.find((s) => s.type === 'living')?.value ?? rec.living_area_sqft ?? null;
              return (
                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-2">
                    <div className="font-semibold">Details</div>
                    <div className="text-[11px] text-cre-muted">Source: {rec.source.toUpperCase()}</div>
                  </div>
                  <div className="font-mono text-[11px] text-cre-muted">{rec.parcel_id}</div>
                  <div>{rec.situs_address || rec.address || '—'}</div>
                  <div>Owner: {rec.owner_name || '—'}</div>
                  <div>Land use: {rec.land_use || '—'}</div>
                  <div>Zoning: {rec.zoning || '—'}</div>
                  <div>Year built: {rec.year_built ?? '—'}</div>
                  <div>Living sqft: {sqftLiving ?? '—'}</div>
                  <div>Total value: {rec.total_value ?? '—'}</div>
                  <div>Last sale price: {rec.last_sale_price ?? '—'}</div>

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
        <div className="h-full overflow-hidden rounded-2xl border border-cre-border/40 bg-cre-surface shadow-panel">
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

            <FeatureGroup ref={featureGroupRef}>
              <EditControl
                position="topright"
                onDrawStart={() => setIsDrawing(true)}
                onDrawStop={() => setIsDrawing(false)}
                onCreated={(e: any) => {
                  setSelectedParcelId(null);
                  setErrorBanner(null);
                  try {
                    if (e.layerType === 'polygon') {
                      const gj = e.layer.toGeoJSON();
                      const geom = gj?.geometry;
                      if (geom?.type === 'Polygon') {
                        setDrawnPolygon(geom as any);
                        setDrawnCircle(null);
                      }
                    }
                    if (e.layerType === 'circle') {
                      const c = e.layer;
                      const ll = c.getLatLng?.();
                      const radius = c.getRadius?.();
                      if (ll && typeof radius === 'number') {
                        setDrawnCircle({ center: { lat: ll.lat, lng: ll.lng }, radius_m: radius });
                        setDrawnPolygon(null);
                      }
                    }
                  } catch {
                    // ignore
                  }
                }}
                onEdited={(e: any) => {
                  try {
                    const layers = e.layers;
                    layers.eachLayer((layer: any) => {
                      if (layer.getRadius && layer.getLatLng) {
                        const ll = layer.getLatLng();
                        const radius = layer.getRadius();
                        if (ll && typeof radius === 'number') {
                          setDrawnCircle({ center: { lat: ll.lat, lng: ll.lng }, radius_m: radius });
                          setDrawnPolygon(null);
                        }
                      } else if (layer.toGeoJSON) {
                        const gj = layer.toGeoJSON();
                        const geom = gj?.geometry;
                        if (geom?.type === 'Polygon') {
                          setDrawnPolygon(geom as any);
                          setDrawnCircle(null);
                        }
                      }
                    });
                  } catch {
                    // ignore
                  }
                }}
                onDeleted={() => {
                  setDrawnPolygon(null);
                  setDrawnCircle(null);
                }}
                draw={drawOptions as any}
                edit={editOptions as any}
              />
            </FeatureGroup>

            {rows.map((r) => {
              const selected = selectedParcelId === r.parcel_id;
              const pos: [number, number] = [r.lat, r.lng];
              if (selected) {
                return (
                  <CircleMarker
                    key={r.parcel_id}
                    center={pos}
                    radius={9}
                    pathOptions={{ color: '#D08E02', weight: 2, fillOpacity: 0.35 }}
                  />
                );
              }
              return (
                <Marker
                  key={r.parcel_id}
                  position={pos}
                  interactive={!isDrawing}
                  eventHandlers={{
                    click: () => setSelectedParcelId(r.parcel_id),
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
