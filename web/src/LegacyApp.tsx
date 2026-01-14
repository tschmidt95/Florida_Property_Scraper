import { useMemo, useRef, useState } from 'react';

import type { LatLngLiteral } from 'leaflet';
import {
  Circle,
  FeatureGroup,
  GeoJSON,
  MapContainer,
  Marker,
  TileLayer,
  useMap,
  useMapEvents,
} from 'react-leaflet';
import { EditControl } from 'react-leaflet-draw';

import {
  advancedSearch,
  parcelsSearch,
  type ParcelRecord,
  type SearchResult,
} from './lib/api';

function FitToFeatures({ fc }: { fc: GeoJSON.FeatureCollection | null }) {
  const map = useMap();
  if (!fc || !fc.features.length) return null;
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const coords: any = (g as any).coordinates;
      if (!coords) continue;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const walk = (obj: any) => {
        if (
          Array.isArray(obj) &&
          obj.length === 2 &&
          typeof obj[0] === 'number' &&
          typeof obj[1] === 'number'
        ) {
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
    if (
      Number.isFinite(bounds[0]) &&
      Number.isFinite(bounds[1]) &&
      Number.isFinite(bounds[2]) &&
      Number.isFinite(bounds[3])
    ) {
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

export default function LegacyApp() {
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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const featureGroupRef = useRef<any>(null);
  const activeRequestId = useRef(0);

  const primaryLoading = mode === 'map' ? lookupLoading : loading;
  const primaryActionLabel = mode === 'map' ? 'Lookup' : 'Run';

  const legacyBuildBanner = useMemo(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const anyEnv: any = (import.meta as any).env || {};
    const branch = anyEnv.VITE_GIT_BRANCH || 'unknown';
    const shaRaw = anyEnv.VITE_GIT_SHA || 'dev';
    const sha =
      typeof shaRaw === 'string' && shaRaw.length > 7 ? shaRaw.slice(0, 7) : shaRaw;
    const ts = anyEnv.VITE_BUILD_TIME || new Date().toISOString();
    return `Progress / Build: ${branch} • ${sha} • ${ts}`;
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
        body: JSON.stringify({
          county: selectedCounty.toLowerCase(),
          address: query,
          include_contacts: false,
          live: liveFetchEnabled,
        }),
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
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        geometry: r.geometry as any,
      });
    }
    return { type: 'FeatureCollection', features: feats };
  }, [filteredParcelRecords]);

  async function runSearch() {
    const requestId = ++activeRequestId.current;
    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      const allowed: Array<'owner' | 'address' | 'parcel_id' | 'city' | 'zip'> = [
        'owner',
        'address',
        'parcel_id',
        'city',
        'zip',
      ];
      const selectedFields = allowed.filter((k) => searchFields[k]);

      const permitStatusList = permitStatus.trim() ? [permitStatus.trim()] : null;
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
          Florida Property Scraper (Legacy)
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
            Selected: <span className="font-medium text-slate-700">{selectedCounty}</span>
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
        {legacyBuildBanner}
      </div>

      <div className="p-4 text-sm text-cre-text">
        Legacy UI is available as a fallback. If MapSearch is disabled, use this.
      </div>
    </div>
  );
}
