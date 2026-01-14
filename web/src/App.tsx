import {
  Component,
  lazy,
  Suspense,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';

import type { LatLngLiteral } from 'leaflet';
import { FeatureGroup, MapContainer, TileLayer, GeoJSON, Circle, Marker, useMap, useMapEvents } from 'react-leaflet';
import { EditControl } from 'react-leaflet-draw';

import { advancedSearch, debugPing, parcelsSearch, type ParcelRecord, type SearchResult } from './lib/api';

const LazyMapSearch = lazy(() => import('./pages/MapSearch'));

class MapErrorBoundary extends Component<
  { children: ReactNode },
  { hasError: boolean }
> {
  state: { hasError: boolean } = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch() {
    // show banner; error details are in console
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="m-4 rounded-xl border border-cre-accent/40 bg-cre-surface p-4">
          <div className="text-sm font-semibold text-cre-accent">
            MapSearch disabled — check console + web/BUILD_ERRORS.txt
          </div>
          <div className="mt-1 text-xs text-cre-muted">
            Falling back to the legacy UI below.
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

function FitToFeatures({ fc }: { fc: GeoJSON.FeatureCollection | null }) {
  const map = useMap();
  if (!fc || !fc.features.length) return null;
  // Best-effort fit bounds; ignore errors for invalid geometries.
  try {
    const bounds: [number, number, number, number] = [
      Infinity,
      Infinity,
      -Infinity,
      -Infinity,
    ];
    for (const f of fc.features) {
      const g = f.geometry;
      if (!g) continue;
      const coords: any = (g as any).coordinates;
      if (!coords) continue;
      const walk = (obj: any) => {
        if (Array.isArray(obj) && obj.length === 2 && typeof obj[0] === 'number' && typeof obj[1] === 'number') {
          const lng = obj[0];
          const lat = obj[1];
          bounds[0] = Math.min(bounds[0], lat);
          bounds[1] = Math.min(bounds[1], lng);
          bounds[2] = Math.max(bounds[2], lat);
          bounds[3] = Math.max(bounds[3], lng);
          return;
        }
        if (Array.isArray(obj)) {
          for (const it of obj) walk(it);
        }
      };
      walk(coords);
    }
    if (Number.isFinite(bounds[0]) && Number.isFinite(bounds[1]) && Number.isFinite(bounds[2]) && Number.isFinite(bounds[3])) {
      map.fitBounds(
        [
          [bounds[0], bounds[1]],
          [bounds[2], bounds[3]],
        ],
        { padding: [20, 20] },
      );
    }
  } catch {
    // ignore
  }
  return null;
}

function RadiusClickHandler({
  enabled,
  onPick,
}: {
  enabled: boolean;
  onPick: (ll: LatLngLiteral) => void;
}) {
  useMapEvents({
    click(e) {
      if (!enabled) return;
      onPick({ lat: e.latlng.lat, lng: e.latlng.lng });
    },
  });
  return null;
}

export default function App() {
  const [apiOk, setApiOk] = useState(false);
  const [apiGit, setApiGit] = useState<{ sha: string; branch: string } | null>(null);

  const [mode, setMode] = useState<'search' | 'map'>('map');
  const [query, setQuery] = useState('');
  const [geometrySearchEnabled, setGeometrySearchEnabled] = useState(false);
  const [selectedCounty, setSelectedCounty] = useState('Orange');
  const [liveFetchEnabled, setLiveFetchEnabled] = useState(false);
  const [searchFields, setSearchFields] = useState({
    owner: true,
    address: true,
    parcel_id: true,
    city: false,
    zip: false,
  });
  const [noPermitsYears, setNoPermitsYears] = useState(15);
  const [permitStatus, setPermitStatus] = useState<string>('');
  const [permitTypes, setPermitTypes] = useState<string>('');
  const [sort, setSort] = useState<
    'relevance' | 'score_desc' | 'last_permit_oldest' | 'last_permit_newest'
  >('relevance');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);

  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState<string | null>(null);
  const [lookupResult, setLookupResult] = useState<any | null>(null);

  const [parcelLoading, setParcelLoading] = useState(false);
  const [parcelError, setParcelError] = useState<string | null>(null);
  const [parcelRecords, setParcelRecords] = useState<ParcelRecord[]>([]);
  const [selectedParcelId, setSelectedParcelId] = useState<string | null>(null);
  const [radiusMiles, setRadiusMiles] = useState(0.25);
  const [radiusCenter, setRadiusCenter] = useState<LatLngLiteral | null>(null);
  const [radiusMode, setRadiusMode] = useState(false);
  const [zoningText, setZoningText] = useState('');
  const [zoningSelected, setZoningSelected] = useState<string[]>([]);
  const featureGroupRef = useRef<any>(null);

  const activeRequestId = useRef(0);

  const primaryLoading = mode === 'map' ? lookupLoading : loading;
  const primaryActionLabel = mode === 'map' ? 'Lookup' : 'Run';

  const buildBanner = useMemo(() => {
    const anyEnv: any = (import.meta as any).env || {};
    const branch = anyEnv.VITE_GIT_BRANCH || 'unknown';
    const shaRaw = anyEnv.VITE_GIT_SHA || 'dev';
    const sha = typeof shaRaw === 'string' && shaRaw.length > 7 ? shaRaw.slice(0, 7) : shaRaw;
    const ts = anyEnv.VITE_BUILD_TIME || new Date().toISOString();
    return `Progress / Build: ${branch} • ${sha} • ${ts}`;
  }, []);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const ping = await debugPing();
        if (cancelled) return;
        setApiOk(!!ping.ok);
        setApiGit(ping.git);
      } catch {
        if (cancelled) return;
        setApiOk(false);
        setApiGit(null);
      }
    };
    void tick();
    const id = window.setInterval(tick, 15000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  function runPrimaryAction() {
    if (mode === 'map') {
      void runLookup();
    } else {
      void runSearch();
    }
  }

  async function runLookup() {
    setLookupLoading(true);
    setLookupError(null);
    setLookupResult(null);
    try {
      const resp = await fetch('/api/lookup/address', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ county: selectedCounty.toLowerCase(), address: query, include_contacts: false, live: liveFetchEnabled }),
      });
      if (!resp.ok) {
        const text = await resp.text().catch(() => '');
        throw new Error(text || `HTTP ${resp.status}`);
      }
      const json = await resp.json();
      setLookupResult(json);
    } catch (e) {
      setLookupError(e instanceof Error ? e.message : String(e));
    } finally {
      setLookupLoading(false);
    }
  }

  const counties = useMemo(
    () => ['Alachua', 'Broward', 'Duval', 'Hillsborough', 'Orange', 'Palm Beach'],
    [],
  );

  const zoningOptions = useMemo(() => {
    const set = new Set<string>();
    for (const r of parcelRecords) {
      const z = (r.zoning || '').trim();
      if (z) set.add(z);
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [parcelRecords]);

  const filteredParcelRecords = useMemo(() => {
    const txt = zoningText.trim().toLowerCase();
    const selected = new Set(zoningSelected);
    return parcelRecords.filter((r) => {
      const z = (r.zoning || '').trim();
      if (selected.size && !selected.has(z)) return false;
      if (txt && !z.toLowerCase().includes(txt)) return false;
      return true;
    });
  }, [parcelRecords, zoningSelected, zoningText]);

  const featureCollection: GeoJSON.FeatureCollection | null = useMemo(() => {
    const feats: GeoJSON.Feature[] = [];
    for (const r of filteredParcelRecords) {
      if (!r.geometry) continue;
      feats.push({
        type: 'Feature',
        id: `${r.county}:${r.parcel_id}`,
        properties: {
          parcel_id: r.parcel_id,
          county: r.county,
          zoning: r.zoning,
          owner_name: r.owner_name,
          situs_address: r.situs_address,
        },
        geometry: r.geometry as any,
      });
    }
    return { type: 'FeatureCollection', features: feats };
  }, [filteredParcelRecords]);

  async function runParcelSearch(payload: any) {
    setParcelLoading(true);
    setParcelError(null);
    setSelectedParcelId(null);
    try {
      const resp = await parcelsSearch(payload);
      setParcelRecords(resp.records || []);
    } catch (e) {
      setParcelRecords([]);
      setParcelError(e instanceof Error ? e.message : String(e));
    } finally {
      setParcelLoading(false);
    }
  }

  const selectedFields = useMemo(() => {
    const out: Array<'owner' | 'address' | 'parcel_id' | 'city' | 'zip'> = [];
    if (searchFields.owner) out.push('owner');
    if (searchFields.address) out.push('address');
    if (searchFields.parcel_id) out.push('parcel_id');
    if (searchFields.city) out.push('city');
    if (searchFields.zip) out.push('zip');
    return out;
  }, [searchFields]);

  async function runSearch() {
    const requestId = activeRequestId.current + 1;
    activeRequestId.current = requestId;

    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      if (selectedFields.length === 0) {
        setError('Select at least one field to search in.');
        setResults([]);
        return;
      }

      const permitStatusList = permitStatus.trim()
        ? [permitStatus.trim()]
        : null;
      const permitTypesList = permitTypes
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s.length > 0);

      const next = await advancedSearch({
        county: selectedCounty,
        text: query.trim().length > 0 ? query.trim() : null,
        fields: selectedFields,
        filters: {
          no_permits_in_years: Number.isFinite(noPermitsYears) ? noPermitsYears : 15,
          permit_status: permitStatusList,
          permit_types: permitTypesList.length ? permitTypesList : null,
          city: null,
          zip: null,
          min_score: null,
        },
        sort,
        limit: 50,
      });
      if (activeRequestId.current !== requestId) {
        return;
      }
      setResults(next);
    } catch (e) {
      if (activeRequestId.current !== requestId) {
        return;
      }
      const message = e instanceof Error ? e.message : 'Unknown error';
      setError(message);
      setResults([]);
    } finally {
      if (activeRequestId.current === requestId) {
        setLoading(false);
      }
    }
  }

  function clearSearch() {
    activeRequestId.current += 1;
    setQuery('');
    setResults([]);
    setError(null);
    setHasSearched(false);
    setLoading(false);
  }

  return (
    <div className="min-h-screen bg-cre-bg text-cre-text">
      <header className="flex items-center gap-3 border-b border-cre-border/30 bg-cre-surface px-4 py-3">
        <div className="font-serif text-lg font-semibold tracking-tight text-cre-primary">
          Florida Property Scraper
        </div>

        <div className="ml-2 text-xs">
          <span className={apiOk ? 'text-emerald-300' : 'text-red-300'}>
            {apiOk ? 'API OK' : 'API DOWN'}
          </span>
          {apiGit ? (
            <span className="ml-2 text-cre-muted">
              ({apiGit.branch} • {apiGit.sha})
            </span>
          ) : null}
        </div>

        <div className="ml-6 flex items-center gap-2">
          <button
            type="button"
            className={
              mode === 'map'
                ? 'rounded-xl bg-cre-primary px-3 py-1.5 text-sm font-semibold text-white'
                : 'rounded-xl border border-cre-border/30 bg-cre-surface px-3 py-1.5 text-sm text-cre-text hover:bg-cre-bg'
            }
            onClick={() => setMode('map')}
          >
            Map
          </button>
          <button
            type="button"
            className={
              mode === 'search'
                ? 'rounded-xl bg-cre-primary px-3 py-1.5 text-sm font-semibold text-white'
                : 'rounded-xl border border-cre-border/30 bg-cre-surface px-3 py-1.5 text-sm text-cre-text hover:bg-cre-bg'
            }
            onClick={() => setMode('search')}
          >
            Search
          </button>
        </div>

        <div className="ml-auto flex w-full max-w-2xl items-center gap-2">
          <div className="hidden shrink-0 text-xs text-slate-500 sm:block">
            Selected:{' '}
            <span className="font-medium text-slate-700">{selectedCounty}</span>
          </div>
          <input
            className="w-full rounded-xl border border-cre-border/30 bg-cre-surface px-3 py-2 text-sm shadow-sm outline-none focus:border-cre-accent/70"
            placeholder="Search owners, addresses, parcels…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                runPrimaryAction();
              }
              if (e.key === 'Escape') {
                e.preventDefault();
                clearSearch();
              }
            }}
          />
          <button
            type="button"
            className="rounded-xl bg-cre-primary px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-black"
            onClick={runPrimaryAction}
            disabled={primaryLoading}
          >
            {primaryLoading ? 'Working…' : primaryActionLabel}
          </button>
        </div>
      </header>

      <div className="border-b border-cre-border/20 bg-cre-surface px-4 py-2 text-xs text-cre-muted">
        {buildBanner}
      </div>

      <MapErrorBoundary>
        <Suspense fallback={<div className="p-6 text-sm text-cre-text">Loading…</div>}>
          <LazyMapSearch />
        </Suspense>
      </MapErrorBoundary>

      <div className="flex">
        <aside className="w-80 border-r border-cre-border/30 bg-cre-surface p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Counties
          </div>
          <ul className="mt-2 space-y-1">
            {counties.map((name) => (
              <li key={name}>
                <button
                  type="button"
                  className={
                    name === selectedCounty
                      ? 'w-full rounded-md bg-slate-900 px-2 py-2 text-left text-sm font-medium text-white'
                      : 'w-full rounded-md px-2 py-2 text-left text-sm hover:bg-slate-100'
                  }
                  onClick={() => setSelectedCounty(name)}
                  aria-pressed={name === selectedCounty}
                >
                  {name}
                </button>
              </li>
            ))}
          </ul>

          <div className="mt-6 rounded-lg border border-slate-200 bg-slate-50 p-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-sm font-medium">Geometry search</div>
                <div className="text-xs text-slate-500">beta</div>
              </div>

              <label className="relative inline-flex cursor-pointer items-center">
                <input
                  type="checkbox"
                  className="peer sr-only"
                  checked={geometrySearchEnabled}
                  onChange={(e) => setGeometrySearchEnabled(e.target.checked)}
                />
                <div className="h-6 w-11 rounded-full bg-slate-300 peer-checked:bg-slate-900 after:absolute after:left-1 after:top-1 after:h-4 after:w-4 after:rounded-full after:bg-white after:transition peer-checked:after:translate-x-5" />
              </label>
            </div>
          </div>

          <div className="mt-4 rounded-xl border border-cre-border/30 bg-cre-bg p-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-sm font-medium">Live fetch</div>
                <div className="text-xs text-cre-muted">Scrape/fetch when missing</div>
              </div>

              <label className="relative inline-flex cursor-pointer items-center">
                <input
                  type="checkbox"
                  className="peer sr-only"
                  checked={liveFetchEnabled}
                  onChange={(e) => setLiveFetchEnabled(e.target.checked)}
                />
                <div className="h-6 w-11 rounded-full bg-slate-300 peer-checked:bg-cre-accent after:absolute after:left-1 after:top-1 after:h-4 after:w-4 after:rounded-full after:bg-white after:transition peer-checked:after:translate-x-5" />
              </label>
            </div>
          </div>

          <div className="mt-6">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Search In
            </div>
            <div className="mt-2 space-y-2 text-sm">
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={searchFields.owner}
                  onChange={(e) =>
                    setSearchFields((s) => ({ ...s, owner: e.target.checked }))
                  }
                />
                Owner
              </label>
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={searchFields.address}
                  onChange={(e) =>
                    setSearchFields((s) => ({ ...s, address: e.target.checked }))
                  }
                />
                Address
              </label>
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={searchFields.parcel_id}
                  onChange={(e) =>
                    setSearchFields((s) => ({ ...s, parcel_id: e.target.checked }))
                  }
                />
                Parcel ID
              </label>
              <label className="flex items-center gap-2 opacity-60">
                <input type="checkbox" checked={false} disabled />
                City (requires DB column)
              </label>
              <label className="flex items-center gap-2 opacity-60">
                <input type="checkbox" checked={false} disabled />
                ZIP (requires DB column)
              </label>
            </div>
          </div>

          <div className="mt-6">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Filters
            </div>
            <div className="mt-2 space-y-3 text-sm">
              <label className="block">
                <div className="text-xs text-slate-500">No permits in last N years</div>
                <input
                  className="mt-1 w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm shadow-sm outline-none focus:border-slate-400"
                  type="number"
                  min={0}
                  max={120}
                  value={noPermitsYears}
                  onChange={(e) => setNoPermitsYears(Number(e.target.value))}
                />
              </label>

              <label className="block">
                <div className="text-xs text-slate-500">Permit status</div>
                <select
                  className="mt-1 w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm shadow-sm outline-none focus:border-slate-400"
                  value={permitStatus}
                  onChange={(e) => setPermitStatus(e.target.value)}
                >
                  <option value="">Any</option>
                  <option value="Open">Open</option>
                  <option value="Closed">Closed</option>
                </select>
              </label>

              <label className="block">
                <div className="text-xs text-slate-500">Permit types (comma-separated)</div>
                <input
                  className="mt-1 w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm shadow-sm outline-none focus:border-slate-400"
                  placeholder="Building, Electrical"
                  value={permitTypes}
                  onChange={(e) => setPermitTypes(e.target.value)}
                />
              </label>
            </div>
          </div>

          <div className="mt-6">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Sort
            </div>
            <select
              className="mt-2 w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm shadow-sm outline-none focus:border-slate-400"
              value={sort}
              onChange={(e) =>
                setSort(
                  e.target.value as
                    | 'relevance'
                    | 'score_desc'
                    | 'last_permit_oldest'
                    | 'last_permit_newest',
                )
              }
            >
              <option value="relevance">Relevance</option>
              <option value="score_desc">Score (desc)</option>
              <option value="last_permit_newest">Last permit newest</option>
              <option value="last_permit_oldest">Last permit oldest</option>
            </select>
          </div>

          <div className="mt-6">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Address lookup
            </div>
            <button
              type="button"
              className="mt-2 w-full rounded-xl bg-cre-accent px-3 py-2 text-sm font-semibold text-black shadow-sm hover:brightness-95"
              onClick={() => void runLookup()}
              disabled={lookupLoading || !query.trim()}
            >
              {lookupLoading ? 'Looking up…' : 'Lookup address'}
            </button>
            {lookupError ? (
              <div className="mt-2 text-xs text-red-700">{lookupError}</div>
            ) : null}
            {lookupResult ? (
              <pre className="mt-2 max-h-40 overflow-auto rounded-lg border border-cre-border/30 bg-cre-surface p-2 text-xs">
                {JSON.stringify(lookupResult, null, 2)}
              </pre>
            ) : null}
          </div>
        </aside>

        <main className="flex-1 p-6">
          {mode === 'map' ? (
            <div className="space-y-4">
              <div className="rounded-xl border border-cre-border/30 bg-cre-surface p-4 shadow-panel">
                <div className="flex flex-wrap items-center gap-3">
                  <div className="text-sm font-semibold text-cre-primary">Map Search</div>
                  <div className="ml-auto flex items-center gap-2">
                    <button
                      type="button"
                      className={
                        radiusMode
                          ? 'rounded-xl bg-cre-primary px-3 py-2 text-sm font-semibold text-white'
                          : 'rounded-xl border border-cre-border/30 bg-cre-surface px-3 py-2 text-sm hover:bg-cre-bg'
                      }
                      onClick={() => {
                        setRadiusMode((v) => !v);
                        setRadiusCenter(null);
                      }}
                    >
                      Radius
                    </button>
                    <div className="flex items-center gap-2">
                      <label className="text-xs text-cre-muted">Miles</label>
                      <input
                        className="w-24 rounded-lg border border-cre-border/30 bg-cre-surface px-2 py-1 text-sm"
                        type="number"
                        step="0.05"
                        min="0.05"
                        value={radiusMiles}
                        onChange={(e) => setRadiusMiles(Number(e.target.value) || 0.25)}
                      />
                    </div>
                    <button
                      type="button"
                      className="rounded-xl border border-cre-border/30 bg-cre-surface px-3 py-2 text-sm hover:bg-cre-bg"
                      onClick={() => {
                        featureGroupRef.current?.clearLayers?.();
                        setRadiusCenter(null);
                        setParcelRecords([]);
                        setParcelError(null);
                        setSelectedParcelId(null);
                      }}
                    >
                      Clear
                    </button>
                  </div>
                </div>

                <div className="mt-3 h-[420px] overflow-hidden rounded-xl border border-cre-border/30">
                  <MapContainer
                    center={[28.5383, -81.3792]}
                    zoom={12}
                    style={{ height: '100%', width: '100%' }}
                  >
                    <TileLayer
                      attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />

                    <RadiusClickHandler
                      enabled={radiusMode}
                      onPick={(next) => {
                        setRadiusCenter(next);
                        void runParcelSearch({
                          county: selectedCounty.toLowerCase(),
                          center: { lat: next.lat, lng: next.lng },
                          radius_m: radiusMiles * 1609.344,
                          live: liveFetchEnabled,
                          limit: 200,
                          include_geometry: true,
                        });
                      }}
                    />

                    <FeatureGroup ref={featureGroupRef}>
                      <EditControl
                        position="topright"
                        onCreated={(e: any) => {
                          try {
                            const layer = e.layer;
                            const geo = layer.toGeoJSON();
                            const geom = geo?.geometry;
                            if (!geom) return;
                            setRadiusMode(false);
                            setRadiusCenter(null);
                            void runParcelSearch({
                              county: selectedCounty.toLowerCase(),
                              geometry: geom,
                              live: liveFetchEnabled,
                              limit: 200,
                              include_geometry: true,
                            });
                          } catch {
                            // ignore
                          }
                        }}
                        draw={{
                          polyline: false,
                          rectangle: false,
                          circle: false,
                          circlemarker: false,
                          marker: false,
                          polygon: geometrySearchEnabled,
                        }}
                        edit={{ edit: false, remove: true }}
                      />
                    </FeatureGroup>

                    {radiusCenter ? (
                      <>
                        <Marker position={radiusCenter} />
                        <Circle
                          center={radiusCenter}
                          radius={radiusMiles * 1609.344}
                          pathOptions={{ color: '#D08E02', weight: 2, fillOpacity: 0.12 }}
                        />
                      </>
                    ) : null}

                    {featureCollection && featureCollection.features.length ? (
                      <>
                        <GeoJSON
                          data={featureCollection as any}
                          style={(feature: any) => {
                            const pid = feature?.properties?.parcel_id;
                            const selected = selectedParcelId && pid === selectedParcelId;
                            return {
                              color: selected ? '#D08E02' : '#050706',
                              weight: selected ? 3 : 1,
                              fillOpacity: selected ? 0.25 : 0.12,
                            };
                          }}
                          onEachFeature={(feature: any, layer: any) => {
                            layer.on('click', () => {
                              const pid = feature?.properties?.parcel_id;
                              if (typeof pid === 'string') setSelectedParcelId(pid);
                            });
                          }}
                        />
                        <FitToFeatures fc={featureCollection} />
                      </>
                    ) : null}
                  </MapContainer>
                </div>

                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <div className="text-xs text-cre-muted">
                    {parcelLoading ? 'Searching…' : `${filteredParcelRecords.length} shown / ${parcelRecords.length} returned`}
                  </div>
                  {parcelError ? <div className="text-xs text-red-700">{parcelError}</div> : null}

                  <div className="ml-auto flex flex-wrap items-center gap-2">
                    <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Zoning</div>
                    <select
                      className="rounded-lg border border-cre-border/30 bg-cre-surface px-2 py-1 text-sm"
                      value={zoningSelected[0] || ''}
                      onChange={(e) => setZoningSelected(e.target.value ? [e.target.value] : [])}
                    >
                      <option value="">All</option>
                      {zoningOptions.map((z) => (
                        <option key={z} value={z}>
                          {z}
                        </option>
                      ))}
                    </select>
                    <input
                      className="w-48 rounded-lg border border-cre-border/30 bg-cre-surface px-2 py-1 text-sm"
                      placeholder="Filter zoning text…"
                      value={zoningText}
                      onChange={(e) => setZoningText(e.target.value)}
                    />
                  </div>
                </div>
              </div>

              <div className="overflow-hidden rounded-xl border border-cre-border/30 bg-cre-surface shadow-panel">
                <div className="border-b border-cre-border/30 px-4 py-3">
                  <div className="text-sm font-semibold text-cre-primary">Results</div>
                  <div className="text-xs text-cre-muted">Click a row to highlight on map.</div>
                </div>
                <div className="max-h-[420px] overflow-auto">
                  <table className="w-full text-left text-sm">
                    <thead className="sticky top-0 bg-cre-surface">
                      <tr className="text-xs uppercase tracking-wide text-cre-muted">
                        <th className="px-3 py-2">Parcel</th>
                        <th className="px-3 py-2">Situs</th>
                        <th className="px-3 py-2">Owner</th>
                        <th className="px-3 py-2">Class</th>
                        <th className="px-3 py-2">Land use</th>
                        <th className="px-3 py-2">Zoning</th>
                        <th className="px-3 py-2">Sqft</th>
                        <th className="px-3 py-2">Lot sqft</th>
                        <th className="px-3 py-2">Beds</th>
                        <th className="px-3 py-2">Baths</th>
                        <th className="px-3 py-2">Year</th>
                        <th className="px-3 py-2">Sale</th>
                        <th className="px-3 py-2">Source</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredParcelRecords.map((r) => {
                        const selected = selectedParcelId === r.parcel_id;
                        return (
                          <tr
                            key={r.parcel_id}
                            className={
                              selected
                                ? 'bg-cre-accent/10'
                                : 'border-t border-cre-border/20 hover:bg-cre-bg'
                            }
                            onClick={() => setSelectedParcelId(r.parcel_id)}
                          >
                            <td className="whitespace-nowrap px-3 py-2 font-mono text-xs">{r.parcel_id}</td>
                            <td className="px-3 py-2">{r.situs_address || '—'}</td>
                            <td className="px-3 py-2">{r.owner_name || '—'}</td>
                            <td className="px-3 py-2">{r.property_class || '—'}</td>
                            <td className="px-3 py-2">{r.land_use || '—'}</td>
                            <td className="px-3 py-2">{r.zoning || '—'}</td>
                            <td className="px-3 py-2">{r.living_area_sqft ?? '—'}</td>
                            <td className="px-3 py-2">{r.lot_size_sqft ?? '—'}</td>
                            <td className="px-3 py-2">{r.beds ?? '—'}</td>
                            <td className="px-3 py-2">{r.baths ?? '—'}</td>
                            <td className="px-3 py-2">{r.year_built ?? '—'}</td>
                            <td className="px-3 py-2">
                              {r.last_sale_date ? `${r.last_sale_date}` : '—'}
                              {r.last_sale_price ? ` ($${Math.round(r.last_sale_price).toLocaleString()})` : ''}
                            </td>
                            <td className="px-3 py-2">{r.source}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          ) : (
            <div className="overflow-hidden rounded-xl border border-cre-border/30 bg-cre-surface shadow-panel">
              <div className="border-b border-cre-border/30 px-4 py-3">
                <div className="text-sm font-semibold text-cre-primary">Search Results</div>
                <div className="text-xs text-cre-muted">Advanced search across permits/records.</div>
              </div>
              <div className="max-h-[720px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-cre-surface">
                    <tr className="text-xs uppercase tracking-wide text-cre-muted">
                      <th className="px-3 py-2">Owner</th>
                      <th className="px-3 py-2">Address</th>
                      <th className="px-3 py-2">County</th>
                      <th className="px-3 py-2">Parcel</th>
                      <th className="px-3 py-2">Last Permit</th>
                      <th className="px-3 py-2">Permits (15y)</th>
                      <th className="px-3 py-2">Source</th>
                      <th className="px-3 py-2">Score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {loading ? (
                      <tr className="border-t border-cre-border/20">
                        <td className="px-3 py-3 text-cre-muted" colSpan={8}>
                          Loading…
                        </td>
                      </tr>
                    ) : error ? (
                      <tr className="border-t border-cre-border/20">
                        <td className="px-3 py-3 text-red-700" colSpan={8}>
                          {error}
                        </td>
                      </tr>
                    ) : results.length === 0 ? (
                      <tr className="border-t border-cre-border/20">
                        <td className="px-3 py-3 text-cre-muted" colSpan={8}>
                          {hasSearched ? 'No matches found.' : 'No results yet.'}
                        </td>
                      </tr>
                    ) : (
                      results.map((r, idx) => (
                        <tr
                          key={`${r.owner}-${r.address}-${idx}`}
                          className="border-t border-cre-border/20 hover:bg-cre-bg"
                        >
                          <td className="px-3 py-2">{r.owner}</td>
                          <td className="px-3 py-2">{r.address}</td>
                          <td className="px-3 py-2">{r.county}</td>
                          <td className="px-3 py-2 font-mono text-xs">{r.parcel_id ?? ''}</td>
                          <td className="px-3 py-2">{r.last_permit_date ?? ''}</td>
                          <td className="px-3 py-2">{r.permits_last_15y_count ?? 0}</td>
                          <td className="px-3 py-2">{r.source ?? ''}</td>
                          <td className="px-3 py-2">{r.score}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
              <div className="flex items-center justify-between gap-3 border-t border-cre-border/20 px-4 py-2 text-xs text-cre-muted">
                <div>
                  Press <span className="font-medium text-cre-text">Enter</span> to run,
                  <span className="font-medium text-cre-text"> Escape</span> to clear.
                </div>
                {hasSearched && !loading && !error ? (
                  <div>{results.length} result{results.length === 1 ? '' : 's'}</div>
                ) : null}
              </div>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}
