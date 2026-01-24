import { useCallback, useEffect, useMemo, useRef, useState, type ChangeEvent } from 'react';

import L from 'leaflet';
import type { LatLngLiteral } from 'leaflet';
import { CircleMarker, GeoJSON, MapContainer, Marker, TileLayer, useMap } from 'react-leaflet';

import {
  createSavedSearch,
  listAlerts,
  listSavedSearches,
  markAlertRead,
  parcelsEnrich,
  parcelsGeometry,
  parcelsSearchNormalized,
  permitsByParcel,
  runSavedSearch,
  triggersByParcel,
  triggersRollupByParcel,
  triggersRollupsSearch,
  type AlertsInboxRecord,
  type ParcelAttributeFilters,
  type ParcelRecord,
  type ParcelSearchListItem,
  type PermitRecord,
  type SavedSearchRecord,
  type TriggerAlertRecord,
  type TriggerEventRecord,
  type TriggerRollupRecord,
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
  const onPolygonRef = useRef(onPolygon);
  const onDeletedRef = useRef(onDeleted);
  const onDrawingChangeRef = useRef(onDrawingChange);
  const drawControlRef = useRef<L.Control.Draw | null>(null);
  const drawLayerRef = useRef<L.FeatureGroup | null>(null);

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
    // Guard against duplicate init (HMR / edge-case remounts).
    // React StrictMode is disabled, but this keeps Leaflet.draw controls stable.
    const anyMap = map as any;
    if (anyMap && anyMap.__fpsDrawControlAttached) {
      return;
    }

    const drawnItems = new L.FeatureGroup();
    drawnItemsRef.current = drawnItems;
    drawLayerRef.current = drawnItems;
    map.addLayer(drawnItems);

    const isDrawingRef = { current: false };
    const prevInteractionRef = {
      current: {
        draggingEnabled: true,
        doubleClickZoomEnabled: true,
      },
    };


    const drawControlOptions = {
      draw: {
        polygon: {
          allowIntersection: false,
          showArea: false,
          repeatMode: false,
        },
        polyline: false,
        rectangle: false,
        circle: false,
        marker: false,
        circlemarker: false,
      },
      // Keep the UI deterministic: one toolbar (polygon draw) only.
      // Users can still clear shapes via the sidebar "Clear" button.
      edit: false,
    } as const;

    const control = new L.Control.Draw(drawControlOptions as any);
    drawControlRef.current = control;
    try {
      (map as any).__fpsDrawControlAttached = true;
    } catch {
      // ignore
    }
    map.addControl(control);

    const handleDrawStart = () => {
      isDrawingRef.current = true;
      onDrawingChangeRef.current(true);

      try {
        prevInteractionRef.current.draggingEnabled = map.dragging?.enabled?.() ?? true;
        prevInteractionRef.current.doubleClickZoomEnabled = map.doubleClickZoom?.enabled?.() ?? true;
      } catch {
        // ignore
      }
      try {
        map.doubleClickZoom?.disable?.();
      } catch {
        // ignore
      }
      try {
        map.dragging?.disable?.();
      } catch {
        // ignore
      }
    };

    const handleDrawStop = () => {
      isDrawingRef.current = false;
      onDrawingChangeRef.current(false);

      try {
        if (prevInteractionRef.current.doubleClickZoomEnabled) map.doubleClickZoom?.enable?.();
      } catch {
        // ignore
      }
      try {
        if (prevInteractionRef.current.draggingEnabled) map.dragging?.enable?.();
      } catch {
        // ignore
      }
    };

    const handleCreated = (e: any) => {
      // Drawing is effectively finished; restore map interactions.
      try {
        if (prevInteractionRef.current.doubleClickZoomEnabled) map.doubleClickZoom?.enable?.();
      } catch {
        // ignore
      }
      try {
        if (prevInteractionRef.current.draggingEnabled) map.dragging?.enable?.();
      } catch {
        // ignore
      }

      try {
        drawnItems.clearLayers();
        if (e?.layer) drawnItems.addLayer(e.layer);
      } catch {
        // ignore
      }

      if (e?.layerType === 'polygon') {
        try {
          // Use Leaflet's GeoJSON export so coordinate order is correct ([lng, lat])
          // and only ensure the first ring is closed.
          const gj = e.layer?.toGeoJSON?.();
          const geom = gj?.geometry;
          const coordsAny = geom?.type === 'Polygon' ? (geom as any).coordinates : null;
          const ringAny = Array.isArray(coordsAny?.[0]) ? (coordsAny[0] as any[]) : null;

          if (ringAny && ringAny.length >= 3) {
            const first = ringAny[0];
            const last = ringAny[ringAny.length - 1];
            const closed =
              Array.isArray(first) &&
              Array.isArray(last) &&
              first.length >= 2 &&
              last.length >= 2 &&
              first[0] === last[0] &&
              first[1] === last[1];

            const closedRing = closed ? ringAny : [...ringAny, first];
            const coordsClosed = [closedRing, ...(Array.isArray(coordsAny) ? coordsAny.slice(1) : [])];

            if (closedRing.length < 4) {
              return;
            }
            onPolygonRef.current({ type: 'Polygon', coordinates: coordsClosed } as GeoJSON.Polygon);
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
    map.on(L.Draw.Event.CREATED, handleCreated);
    map.on(L.Draw.Event.DELETED, handleDeleted);

    return () => {
      map.off(L.Draw.Event.DRAWSTART, handleDrawStart);
      map.off(L.Draw.Event.DRAWSTOP, handleDrawStop);
      map.off(L.Draw.Event.CREATED, handleCreated);
      map.off(L.Draw.Event.DELETED, handleDeleted);
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
      try {
        delete (map as any).__fpsDrawControlAttached;
      } catch {
        // ignore
      }
      if (drawnItemsRef.current === drawnItems) drawnItemsRef.current = null;
      if (drawLayerRef.current === drawnItems) drawLayerRef.current = null;
      if (drawControlRef.current === control) drawControlRef.current = null;
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
  renderOption,
}: {
  title: string;
  options: string[];
  selected: string[];
  query: string;
  onQuery: (q: string) => void;
  onSelected: (next: string[]) => void;
  renderOption?: (v: string) => string;
}) {
  const render = useMemo(() => {
    return typeof renderOption === 'function' ? renderOption : (v: string) => v;
  }, [renderOption]);

  const filtered = useMemo(() => {
    const q = query.trim().toUpperCase();
    if (!q) return options;
    return options.filter((o) => {
      try {
        return render(o).toUpperCase().includes(q) || o.toUpperCase().includes(q);
      } catch {
        return o.toUpperCase().includes(q);
      }
    });
  }, [options, query, render]);

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

  const [isOpen, setIsOpen] = useState(true);

  return (
    <div className="w-full rounded-xl border border-cre-border/40 bg-cre-bg p-3">
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
            onClick={() => setIsOpen((v) => !v)}
          >
            {isOpen ? 'Hide' : 'Show'}
          </button>
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
              <span className="max-w-[240px] truncate">{render(v)}</span>
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

      {isOpen ? (
        <>
          <input
            className="mt-2 w-full rounded-lg border border-cre-border/40 bg-cre-surface px-2 py-2 text-xs text-cre-text"
            value={query}
            onChange={(e) => onQuery(e.target.value)}
            placeholder="Search options..."
          />

          <div className="mt-2 max-h-64 overflow-auto rounded-lg border border-cre-border/40 bg-cre-surface p-2">
            {!options.length ? (
              <div className="text-xs text-cre-muted">No options (field not available).</div>
            ) : filtered.length ? (
              <div className="space-y-1">
                {filtered.slice(0, 250).map((o) => (
                  <label
                    key={o}
                    className="flex cursor-pointer select-none items-center gap-2 rounded-md px-1 py-1 text-xs text-cre-text hover:bg-cre-bg"
                    title={render(o)}
                  >
                    <input
                      type="checkbox"
                      checked={selectedSet.has(o)}
                      onChange={() => toggle(o)}
                    />
                    <span className="truncate">{render(o)}</span>
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
        </>
      ) : null}
    </div>
  );
}

type SignalCatalogItem = {
  key: string;
  label: string;
  group: string;
  tier: 'critical' | 'strong' | 'support' | 'info';
  comingSoon?: boolean;
};

// NOTE: This is a frontend mirror of the backend trigger taxonomy
// (see src/florida_property_scraper/triggers/taxonomy.py). This catalog is
// intentionally stable and always renders, even when counts are zero.
const SIGNALS_CATALOG: SignalCatalogItem[] = [
  // Permits
  { key: 'permit_demolition', label: 'Permit: demolition', group: 'Permits', tier: 'critical' },
  { key: 'permit_structural', label: 'Permit: structural', group: 'Permits', tier: 'critical' },
  { key: 'permit_roof', label: 'Permit: roof', group: 'Permits', tier: 'strong' },
  { key: 'permit_hvac', label: 'Permit: HVAC', group: 'Permits', tier: 'strong' },
  { key: 'permit_electrical', label: 'Permit: electrical', group: 'Permits', tier: 'strong' },
  { key: 'permit_plumbing', label: 'Permit: plumbing', group: 'Permits', tier: 'strong' },
  { key: 'permit_pool', label: 'Permit: pool', group: 'Permits', tier: 'strong' },
  { key: 'permit_fire', label: 'Permit: fire', group: 'Permits', tier: 'strong' },
  { key: 'permit_sitework', label: 'Permit: sitework', group: 'Permits', tier: 'strong' },
  { key: 'permit_tenant_improvement', label: 'Permit: tenant improvement', group: 'Permits', tier: 'strong' },
  { key: 'permit_remodel', label: 'Permit: remodel', group: 'Permits', tier: 'strong' },
  { key: 'permit_generator', label: 'Permit: generator', group: 'Permits', tier: 'support' },
  { key: 'permit_windows', label: 'Permit: windows', group: 'Permits', tier: 'support' },
  { key: 'permit_doors', label: 'Permit: doors', group: 'Permits', tier: 'support' },
  { key: 'permit_solar', label: 'Permit: solar', group: 'Permits', tier: 'support' },
  { key: 'permit_fence', label: 'Permit: fence', group: 'Permits', tier: 'support' },
  { key: 'permit_sign', label: 'Permit: sign', group: 'Permits', tier: 'support' },

  // Official records (distress + transactions)
  { key: 'lis_pendens', label: 'Lis pendens', group: 'Official Records', tier: 'critical' },
  { key: 'foreclosure_filing', label: 'Foreclosure: filing', group: 'Official Records', tier: 'critical' },
  { key: 'foreclosure_judgment', label: 'Foreclosure: judgment', group: 'Official Records', tier: 'critical' },
  { key: 'foreclosure', label: 'Foreclosure (generic)', group: 'Official Records', tier: 'critical' },
  { key: 'deed_recorded', label: 'Deed recorded', group: 'Official Records', tier: 'strong' },
  { key: 'deed_warranty', label: 'Deed: warranty', group: 'Official Records', tier: 'strong' },
  { key: 'deed_quitclaim', label: 'Deed: quitclaim', group: 'Official Records', tier: 'strong' },
  { key: 'mortgage_recorded', label: 'Mortgage recorded', group: 'Official Records', tier: 'support' },
  { key: 'mortgage_satisfaction', label: 'Mortgage satisfaction', group: 'Official Records', tier: 'strong' },
  { key: 'mortgage_assignment', label: 'Mortgage assignment', group: 'Official Records', tier: 'support' },

  // Liens
  { key: 'mechanics_lien', label: "Mechanic's lien", group: 'Liens', tier: 'strong' },
  { key: 'hoa_lien', label: 'HOA lien', group: 'Liens', tier: 'strong' },
  { key: 'irs_tax_lien', label: 'IRS tax lien', group: 'Liens', tier: 'strong' },
  { key: 'state_tax_lien', label: 'State tax lien', group: 'Liens', tier: 'strong' },
  { key: 'code_enforcement_lien', label: 'Code enforcement lien', group: 'Liens', tier: 'critical' },
  { key: 'judgment_lien', label: 'Judgment lien', group: 'Liens', tier: 'strong' },
  { key: 'utility_lien', label: 'Utility lien', group: 'Liens', tier: 'strong' },
  { key: 'lien_recorded', label: 'Lien recorded (generic)', group: 'Liens', tier: 'critical' },

  // Tax collector
  { key: 'delinquent_tax', label: 'Delinquent tax', group: 'Tax Collector', tier: 'critical' },
  { key: 'tax_certificate_issued', label: 'Tax certificate issued', group: 'Tax Collector', tier: 'strong' },
  { key: 'tax_certificate_redeemed', label: 'Tax certificate redeemed', group: 'Tax Collector', tier: 'strong' },
  { key: 'payment_plan_started', label: 'Payment plan started', group: 'Tax Collector', tier: 'strong' },
  { key: 'payment_plan_defaulted', label: 'Payment plan defaulted', group: 'Tax Collector', tier: 'strong' },
  { key: 'tax_deed_application', label: 'Tax deed application', group: 'Tax Collector', tier: 'critical', comingSoon: true },

  // Code enforcement
  { key: 'code_case_opened', label: 'Code case opened', group: 'Code Enforcement', tier: 'strong' },
  { key: 'unsafe_structure', label: 'Unsafe structure', group: 'Code Enforcement', tier: 'critical' },
  { key: 'condemnation', label: 'Condemnation', group: 'Code Enforcement', tier: 'critical' },
  { key: 'demolition_order', label: 'Demolition order', group: 'Code Enforcement', tier: 'critical' },
  { key: 'abatement_order', label: 'Abatement order', group: 'Code Enforcement', tier: 'critical' },
  { key: 'board_hearing_set', label: 'Board hearing set', group: 'Code Enforcement', tier: 'strong' },
  { key: 'fines_imposed', label: 'Fines imposed', group: 'Code Enforcement', tier: 'strong' },
  { key: 'reinspection_failed', label: 'Reinspection failed', group: 'Code Enforcement', tier: 'strong' },
  { key: 'repeat_violation', label: 'Repeat violation', group: 'Code Enforcement', tier: 'strong' },

  // Courts (placeholders)
  { key: 'probate_opened', label: 'Probate opened', group: 'Courts', tier: 'critical', comingSoon: true },
  { key: 'divorce_filed', label: 'Divorce filed', group: 'Courts', tier: 'critical', comingSoon: true },
  { key: 'eviction_filing', label: 'Eviction filing', group: 'Courts', tier: 'critical', comingSoon: true },
];

export default function MapSearch({
  onMapStatus,
}: {
  onMapStatus?: (status: MapStatus) => void;
}) {
  const [county, setCounty] = useState('');
  const [drawnPolygon, setDrawnPolygon] = useState<GeoJSON.Polygon | null>(null);
  const [drawnCircle, setDrawnCircle] = useState<DrawnCircle | null>(null);

  const drawnPolygonRef = useRef<GeoJSON.Polygon | null>(null);
  const drawnCircleRef = useRef<DrawnCircle | null>(null);

  const [isDrawing, setIsDrawing] = useState(false);

  const [runDebugLoading, setRunDebugLoading] = useState(false);
  const [runDebugOut, setRunDebugOut] = useState<
    | {
        payload: any;
        polygonRingLen?: number;
        polygonFirst?: [number, number] | null;
        polygonBbox?: { minLng: number; minLat: number; maxLng: number; maxLat: number } | null;
        polygonClosed?: boolean;
        recordsLen: number;
        sample: Array<{ parcel_id: string; owner: string; address: string }>;
        zoningOptionsLen: number;
        futureLandUseOptionsLen: number;
      }
    | { payload: any; error: string }
    | null
  >(null);

  const [parcels, setParcels] = useState<ParcelSearchListItem[]>([]);
  const [records, setRecords] = useState<ParcelRecord[]>([]);
  const [selectedParcelId, setSelectedParcelId] = useState<string | null>(null);

  const [selectedPermits, setSelectedPermits] = useState<PermitRecord[]>([]);
  const [selectedPermitsLoading, setSelectedPermitsLoading] = useState(false);
  const [selectedPermitsError, setSelectedPermitsError] = useState<string | null>(null);

  const [selectedTriggerEvents, setSelectedTriggerEvents] = useState<TriggerEventRecord[]>([]);
  const [selectedAlerts, setSelectedAlerts] = useState<TriggerAlertRecord[]>([]);
  const [selectedTriggersLoading, setSelectedTriggersLoading] = useState(false);
  const [selectedTriggersError, setSelectedTriggersError] = useState<string | null>(null);

  const [selectedRollup, setSelectedRollup] = useState<TriggerRollupRecord | null>(null);
  const [selectedRollupLoading, setSelectedRollupLoading] = useState(false);
  const [selectedRollupError, setSelectedRollupError] = useState<string | null>(null);

  const [triggerLookupParcelId, setTriggerLookupParcelId] = useState('');

  const [signalsDrawerOpen, setSignalsDrawerOpen] = useState(false);

  const [rollupsEnabled, setRollupsEnabled] = useState(false);
  const [rollupsMinScore, setRollupsMinScore] = useState('');
  const [rollupsGroupOfficialRecords, setRollupsGroupOfficialRecords] = useState(false);
  const [rollupsGroupPermits, setRollupsGroupPermits] = useState(false);
  const [rollupsTriggerGroups, setRollupsTriggerGroups] = useState<string[]>([]);
  const [rollupsTriggerGroupsQuery, setRollupsTriggerGroupsQuery] = useState('');
  const [rollupsTriggerKeys, setRollupsTriggerKeys] = useState<string[]>([]);
  const [rollupsTriggerKeysQuery, setRollupsTriggerKeysQuery] = useState('');
  const [rollupsTierCritical, setRollupsTierCritical] = useState(false);
  const [rollupsTierStrong, setRollupsTierStrong] = useState(false);
  const [rollupsTierSupport, setRollupsTierSupport] = useState(false);
  const [rollupsLastSummary, setRollupsLastSummary] = useState<
    | {
        candidate_count: number;
        returned_count: number;
        parcel_ids_count: number;
      }
    | null
  >(null);
  const [rollupsError, setRollupsError] = useState<string | null>(null);

  const [rollupsMap, setRollupsMap] = useState<Record<string, TriggerRollupRecord>>({});

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

  const emptyFilterForm: FilterForm = {
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
  };

  const [filterForm, setFilterForm] = useState<FilterForm>(emptyFilterForm);
  const [autoEnrichMissing, setAutoEnrichMissing] = useState(false);

  const [sortKey, setSortKey] = useState<
    'relevance' | 'last_sale_date_desc' | 'year_built_desc' | 'sqft_desc'
  >('relevance');

  const [lastCounts, setLastCounts] = useState<{
    candidateCount: number | null;
    filteredCount: number | null;
  } | null>(null);

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

  const debugUiEnabled = useMemo(() => {
    try {
      if (typeof window === 'undefined') return false;
      const v = new URLSearchParams(window.location.search).get('debug');
      return v === '1' || v === 'true';
    } catch {
      return false;
    }
  }, []);

  const [lastRequest, setLastRequest] = useState<any | null>(null);
  const [lastResponseSummary, setLastResponseSummary] = useState<any | null>(null);
  const [debugEvidence, setDebugEvidence] = useState<
    | {
        requestJson: string;
        responseMeta: {
          search_id?: string;
          request_origin?: string;
          response_url?: string;
          candidate_count?: number | null;
          filtered_count?: number | null;
          warnings?: string[];
        };
        normalized_filters?: any;
        debug_timing_ms?: any;
        debug_counts?: any;
        debug_flags?: any;
        prefilter?: any;
      }
    | null
  >(null);
  const [lastResponseCount, setLastResponseCount] = useState<number>(0);
  const [lastError, setLastError] = useState<string | null>(null);
  const [softWarnings, setSoftWarnings] = useState<string[]>([]);

  const [resultsQuery, setResultsQuery] = useState('');

  const [toast, setToast] = useState<string | null>(null);

  const signalCatalogKeysSet = useMemo(() => new Set(SIGNALS_CATALOG.map((x) => x.key)), []);
  const signalCatalogByGroup = useMemo(() => {
    const m = new Map<string, SignalCatalogItem[]>();
    for (const it of SIGNALS_CATALOG) {
      const arr = m.get(it.group) || [];
      arr.push(it);
      m.set(it.group, arr);
    }
    for (const [g, items] of m.entries()) {
      items.sort((a, b) => a.label.localeCompare(b.label));
      m.set(g, items);
    }
    return Array.from(m.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  }, []);

  const unknownSelectedTriggerKeys = useMemo(() => {
    const unknown: string[] = [];
    for (const k of rollupsTriggerKeys) {
      if (!signalCatalogKeysSet.has(k)) unknown.push(k);
    }
    return unknown;
  }, [rollupsTriggerKeys, signalCatalogKeysSet]);

  const comingSoonSelectedTriggerKeys = useMemo(() => {
    const cs = new Set(SIGNALS_CATALOG.filter((x) => x.comingSoon).map((x) => x.key));
    return rollupsTriggerKeys.filter((k) => cs.has(k));
  }, [rollupsTriggerKeys]);

  const [savedSearches, setSavedSearches] = useState<SavedSearchRecord[]>([]);
  const [savedSearchesLoading, setSavedSearchesLoading] = useState(false);
  const [savedSearchesError, setSavedSearchesError] = useState<string | null>(null);
  const [selectedSavedSearchId, setSelectedSavedSearchId] = useState<string>('');

  const [alertsStatus, setAlertsStatus] = useState<string>('new');
  const [alertsInbox, setAlertsInbox] = useState<AlertsInboxRecord[]>([]);
  const [alertsLoading, setAlertsLoading] = useState(false);
  const [alertsError, setAlertsError] = useState<string | null>(null);

  const [parcelLinesEnabled, setParcelLinesEnabled] = useState(false);
  const [parcelLinesLoading, setParcelLinesLoading] = useState(false);
  const [parcelLinesStatus, setParcelLinesStatus] = useState<
    'idle' | 'loading' | 'ok' | 'empty' | 'error'
  >('idle');
  const [parcelLinesLastIdsCount, setParcelLinesLastIdsCount] = useState<number>(0);
  const [parcelLinesError, setParcelLinesError] = useState<string | null>(null);
  const [parcelLinesFC, setParcelLinesFC] = useState<GeoJSON.FeatureCollection | null>(null);
  const [parcelLinesFeatureCount, setParcelLinesFeatureCount] = useState<number>(0);

  const drawnItemsRef = useRef<L.FeatureGroup | null>(null);
  const activeReq = useRef(0);
  const toastTimerRef = useRef<number | null>(null);

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

  const visibleRows = useMemo(() => {
    const q = resultsQuery.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((p) => {
      const owner = (p.owner_name || '').toLowerCase();
      const addr = (p.address || '').toLowerCase();
      return owner.includes(q) || addr.includes(q) || p.parcel_id.toLowerCase().includes(q);
    });
  }, [resultsQuery, rows]);

  const geometryStatus = useMemo(() => {
    if (drawnPolygon) return 'Polygon selected';
    if (drawnCircle) return `Circle selected (${Math.round(drawnCircle.radius_m)} m)`;
    return 'No geometry selected';
  }, [drawnCircle, drawnPolygon]);

  const filtersActive = useMemo(() => {
    const hasText = (s: string) => s.trim().length > 0;
    if (selectedZoning.length) return true;
    if (selectedFutureLandUse.length) return true;
    return (
      hasText(filterForm.minSqft) ||
      hasText(filterForm.maxSqft) ||
      hasText(filterForm.minLotSize) ||
      hasText(filterForm.maxLotSize) ||
      hasText(filterForm.minBeds) ||
      hasText(filterForm.minBaths) ||
      hasText(filterForm.minYearBuilt) ||
      hasText(filterForm.maxYearBuilt) ||
      hasText(filterForm.propertyType) ||
      hasText(filterForm.zoning) ||
      hasText(filterForm.minValue) ||
      hasText(filterForm.maxValue) ||
      hasText(filterForm.minLandValue) ||
      hasText(filterForm.maxLandValue) ||
      hasText(filterForm.minBuildingValue) ||
      hasText(filterForm.maxBuildingValue) ||
      hasText(filterForm.lastSaleStart) ||
      hasText(filterForm.lastSaleEnd)
    );
  }, [filterForm, selectedFutureLandUse, selectedZoning]);

  const formatCodeLabel = useMemo(() => {
    return (raw: string) => {
      const s = String(raw || '').trim().replace(/\s+/g, ' ');
      const idx = s.indexOf(' - ');
      if (idx > 0) {
        const code = s.slice(0, idx).trim();
        const label = s.slice(idx + 3).trim();
        if (code && label) return `${code} — ${label}`;
      }
      const idx2 = s.indexOf(' — ');
      if (idx2 > 0) {
        const code = s.slice(0, idx2).trim();
        const label = s.slice(idx2 + 3).trim();
        if (code && label) return `${code} — ${label}`;
      }
      return s;
    };
  }, []);

  function clearFilters() {
    setFilterForm({ ...emptyFilterForm });
    setSelectedZoning([]);
    setSelectedFutureLandUse([]);
    setZoningQuery('');
    setFutureLandUseQuery('');
    setRollupsEnabled(false);
    setRollupsMinScore('');
    setRollupsGroupOfficialRecords(false);
    setRollupsGroupPermits(false);
    setRollupsTriggerGroups([]);
    setRollupsTriggerGroupsQuery('');
    setRollupsTriggerKeys([]);
    setRollupsTriggerKeysQuery('');
    setRollupsTierCritical(false);
    setRollupsTierStrong(false);
    setRollupsTierSupport(false);
    setRollupsLastSummary(null);
    setRollupsError(null);
    setErrorBanner('Filters cleared. Click Run to refresh results.');
  }

  function clearDrawings() {
    try {
      drawnItemsRef.current?.clearLayers?.();
    } catch {
      // ignore
    }
    drawnPolygonRef.current = null;
    drawnCircleRef.current = null;
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

  function showToast(msg: string) {
    setToast(msg);
    try {
      if (toastTimerRef.current) window.clearTimeout(toastTimerRef.current);
      toastTimerRef.current = window.setTimeout(() => setToast(null), 2600);
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    return () => {
      try {
        if (toastTimerRef.current) window.clearTimeout(toastTimerRef.current);
      } catch {
        // ignore
      }
    };
  }, []);

  const refreshSavedSearches = useCallback(
    async (nextCounty?: string) => {
      const c = (nextCounty ?? county).trim();
      setSavedSearchesError(null);
      setSavedSearchesLoading(true);
      try {
        const items = await listSavedSearches({ county: c });
        setSavedSearches(items);

        const ids = items.map((s) => String(s.id || '')).filter(Boolean);
        setSelectedSavedSearchId((prev) => {
          const p = (prev || '').trim();
          if (p && ids.includes(p)) return p;
          return ids.length ? ids[0] : '';
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        setSavedSearchesError(msg);
        setSavedSearches([]);
        setSelectedSavedSearchId('');
      } finally {
        setSavedSearchesLoading(false);
      }
    },
    [county]
  );

  const refreshAlerts = useCallback(
    async (nextSavedSearchId?: string, nextStatus?: string) => {
      const sid = (nextSavedSearchId ?? selectedSavedSearchId).trim();
      if (!sid) {
        setAlertsInbox([]);
        setAlertsError(null);
        return;
      }

      setAlertsError(null);
      setAlertsLoading(true);
      try {
        const st = (nextStatus ?? alertsStatus).trim();
        const items = await listAlerts({
          saved_search_id: sid,
          county,
          status: st ? st : undefined,
          limit: 200,
          offset: 0,
        });
        setAlertsInbox(items);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        setAlertsError(msg);
        setAlertsInbox([]);
      } finally {
        setAlertsLoading(false);
      }
    },
    [alertsStatus, county, selectedSavedSearchId]
  );

  useEffect(() => {
    void refreshSavedSearches(county);
  }, [county, refreshSavedSearches]);

  useEffect(() => {
    void refreshAlerts(selectedSavedSearchId, alertsStatus);
  }, [alertsStatus, county, refreshAlerts, selectedSavedSearchId]);

  async function runSelectedSavedSearch() {
    const sid = selectedSavedSearchId.trim();
    if (!sid) return;
    try {
      const resp = await runSavedSearch({ saved_search_id: sid, limit: 2000 });
      const ok = Boolean((resp as any).ok);
      if (ok) {
        const added = Number((resp as any).added || 0);
        const removed = Number((resp as any).removed || 0);
        showToast(`Saved search ran: +${added} / -${removed}`);
      } else {
        showToast('Saved search run returned non-ok');
      }
      await refreshAlerts(sid, alertsStatus);
      await refreshSavedSearches(county);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      showToast(`Run failed: ${msg}`);
    }
  }

  async function saveCurrentSearch() {
    const poly = drawnPolygonRef.current ?? drawnPolygon;
    if (!poly) {
      showToast('Draw a polygon to save a search.');
      return;
    }

    const built = buildSearchPayloadForMap();
    if ('error' in built) {
      showToast(built.error);
      return;
    }

    const defaultName = `${county.toUpperCase()} Saved Search`;
    const name = (typeof window !== 'undefined' ? window.prompt('Saved Search name', defaultName) : defaultName) || defaultName;

    try {
      const payload = built.payload as any;
      const filters = (payload?.filters && typeof payload.filters === 'object') ? payload.filters : {};
      const sort = typeof payload?.sort === 'string' ? payload.sort : null;
      const ss = await createSavedSearch({
        name,
        county,
        geometry: poly as any,
        filters,
        enrich: false,
        sort,
      });
      showToast('Saved search created.');
      await refreshSavedSearches(county);
      setSelectedSavedSearchId(String(ss.id || '').trim());
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      showToast(`Save failed: ${msg}`);
    }
  }

  async function markInboxAlertRead(a: AlertsInboxRecord) {
    try {
      await markAlertRead(Number(a.id));
      await refreshAlerts(selectedSavedSearchId, alertsStatus);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      showToast(`Mark read failed: ${msg}`);
    }
  }

  useEffect(() => {
    let cancelled = false;

    async function loadSelectedDetails(parcelId: string) {
      setTriggerLookupParcelId(parcelId);
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

      setSelectedTriggersError(null);
      setSelectedTriggersLoading(true);
      try {
        const resp = await triggersByParcel({
          county,
          parcel_id: parcelId,
          limit_events: 100,
          limit_alerts: 50,
          status: 'open',
        });
        if (cancelled) return;
        setSelectedTriggerEvents(resp.trigger_events || []);
        setSelectedAlerts(resp.alerts || []);
      } catch (e) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        setSelectedTriggersError(msg);
        setSelectedTriggerEvents([]);
        setSelectedAlerts([]);
      } finally {
        if (!cancelled) setSelectedTriggersLoading(false);
      }

      setSelectedRollupError(null);
      setSelectedRollupLoading(true);
      try {
        const rollup = await triggersRollupByParcel({ county, parcel_id: parcelId });
        if (cancelled) return;
        setSelectedRollup(rollup);
      } catch (e) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        // Rollup can be absent if rollups haven't been rebuilt yet.
        setSelectedRollup(null);
        setSelectedRollupError(msg);
      } finally {
        if (!cancelled) setSelectedRollupLoading(false);
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
      setSelectedTriggerEvents([]);
      setSelectedAlerts([]);
      setSelectedTriggersLoading(false);
      setSelectedTriggersError(null);
      setSelectedRollup(null);
      setSelectedRollupLoading(false);
      setSelectedRollupError(null);
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

      const ids = parcels.map((p) => p.parcel_id).filter(Boolean).slice(0, 25);
      setParcelLinesLastIdsCount(ids.length);
      if (!ids.length) {
        setParcelLinesFC(null);
        setParcelLinesFeatureCount(0);
        setParcelLinesStatus('empty');
        return;
      }

      setParcelLinesLoading(true);
      setParcelLinesStatus('loading');
      try {
        const fc = await parcelsGeometry({ county, parcel_ids: ids });
        if (cancelled) return;
        if (!fc.features?.length) {
          setParcelLinesFC(null);
          setParcelLinesFeatureCount(0);
          setParcelLinesStatus('empty');
          setParcelLinesError('0 features returned (parcel geometry may not be available for this county).');
          return;
        }
        setParcelLinesFC(fc);
        setParcelLinesFeatureCount(fc.features.length);
        setParcelLinesStatus('ok');
      } catch (e) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        setParcelLinesFC(null);
        setParcelLinesFeatureCount(0);
        setParcelLinesStatus('error');
        setParcelLinesError(msg);
      } finally {
        if (!cancelled) setParcelLinesLoading(false);
      }
    }

    // Only fetch geometry when the user has an active search area.
    if (!drawnPolygon && !drawnCircle) {
      setParcelLinesFC(null);
      setParcelLinesFeatureCount(0);
      setParcelLinesStatus(parcelLinesEnabled ? 'idle' : 'idle');
      setParcelLinesLastIdsCount(0);
      return;
    }

    void loadParcelLines();
    return () => {
      cancelled = true;
    };
  }, [county, drawnCircle, drawnPolygon, parcelLinesEnabled, parcels]);

  function buildSearchPayloadForMap(): { payload: any } | { error: string } {
    let poly = drawnPolygonRef.current ?? drawnPolygon;
    const circle = drawnCircleRef.current ?? drawnCircle;

    if (!poly && !circle) {
      return { error: 'Draw polygon or radius first' };
    }

    const toIntOrNull = (v: string): number | null => {
      const s = v.trim().replace(/,/g, '');
      if (!s) return null;
      const n = Number(s);
      if (!Number.isFinite(n)) return null;
      return Math.trunc(n);
    };
    const toFloatOrNull = (v: string): number | null => {
      const s = v.trim().replace(/,/g, '');
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
    const minAcres = hasLotSize && lotSizeUnit === 'acres' ? minLotSize : null;
    const maxAcres = hasLotSize && lotSizeUnit === 'acres' ? maxLotSize : null;

    const filters0: ParcelAttributeFilters = {
      min_sqft: toFloatOrNull(filterForm.minSqft),
      max_sqft: toFloatOrNull(filterForm.maxSqft),
      min_acres: minAcres,
      max_acres: maxAcres,
      min_lot_size_sqft: minLotSizeSqft,
      max_lot_size_sqft: maxLotSizeSqft,
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

    // IMPORTANT: omit blank filter keys entirely so "blank" never restricts results.
    const filters: any = {};
    for (const [k, v] of Object.entries(filters0 as any)) {
      if (v === null || v === undefined) continue;
      if (typeof v === 'string' && !v.trim()) continue;
      if (Array.isArray(v) && v.length === 0) continue;
      filters[k] = v;
    }
    const hasAnyFilters = Object.keys(filters).length > 0;

    // If the user wants auto-enrichment, honor it even for baseline runs.
    // This is especially important for counties where zoning/FLU options come
    // from live enrichment.
    const enrich = autoEnrichMissing;

    const payload: any = {
  live: true,
  limit: 25,
  include_geometry: false,
  filters: hasAnyFilters ? filters : undefined,
  enrich,
  enrich_limit: enrich ? 10 : undefined,
  sort: sortKey,
};

if (county && county.trim()) {
  payload.county = county;
}

    // Proof + forward-compat: include selected trigger keys in the request payload.
    // Filtering by signals is currently applied via the rollups prefilter (when enabled).
    // Always include trigger_keys as an array (never null)
    try {
      const keys = (rollupsTriggerKeys || []).map((k) => String(k || '').trim()).filter((k) => k);
      payload.trigger_keys = keys;
    } catch {
      payload.trigger_keys = [];
    }

    if (debugUiEnabled) {
      payload.debug = true;
    }

    if (poly) {
      // Ensure coordinates are [lng, lat] (GeoJSON order) WITHOUT mutating state
let polyOut = poly;

if (
  poly &&
  Array.isArray(poly.coordinates) &&
  Array.isArray(poly.coordinates[0]) &&
  poly.coordinates[0].length > 0
) {
  const ring = poly.coordinates[0];
  const first = ring[0];

  // Heuristic: if abs(first[0]) < 31 and abs(first[1]) > 60, it's [lat, lng] (Florida)
  if (
    Array.isArray(first) &&
    first.length === 2 &&
    Math.abs(first[0]) < 31 &&
    Math.abs(first[1]) > 60
  ) {
    const swapped = ring.map((p: any) => {
      const [lat, lng] = p as [number, number];
      return [lng, lat];
    });
    polyOut = { ...poly, coordinates: [swapped] };
  }
}

payload.polygon_geojson = polyOut;
    } else if (circle) {
      payload.center = circle.center;
      payload.radius_m = circle.radius_m;
    }

    return { payload };
  }

  async function runDebug() {
    setRunDebugOut(null);
    setRunDebugLoading(true);
    try {
      const built = buildSearchPayloadForMap();
      if ('error' in built) {
        setRunDebugOut({ payload: null, error: built.error });
        return;
      }
      const payload = built.payload;
      setLastRequest(payload);
      // (no console logs here; keep logs centralized in Run)

      let polygonRingLen: number | undefined;
      let polygonFirst: [number, number] | null | undefined;
      let polygonClosed: boolean | undefined;
      let polygonBbox:
        | { minLng: number; minLat: number; maxLng: number; maxLat: number }
        | null
        | undefined;
      let coordOrderSample: any = null;

      try {
        const poly = payload?.polygon_geojson;
        const ring = Array.isArray(poly?.coordinates?.[0]) ? (poly.coordinates[0] as any[]) : null;
        if (ring && ring.length) {
          polygonRingLen = ring.length;
          const first = ring[0];
          const last = ring[ring.length - 1];
          polygonClosed =
            Array.isArray(first) &&
            Array.isArray(last) &&
            first.length >= 2 &&
            last.length >= 2 &&
            first[0] === last[0] &&
            first[1] === last[1];

          if (Array.isArray(first) && first.length >= 2) {
            polygonFirst = [Number(first[0]), Number(first[1])];
          } else {
            polygonFirst = null;
          }

          // Sample first 5 coordinate pairs for debug
          coordOrderSample = ring.slice(0, 5);

          let minLng = Infinity;
          let maxLng = -Infinity;
          let minLat = Infinity;
          let maxLat = -Infinity;
          for (const pt of ring) {
            if (!Array.isArray(pt) || pt.length < 2) continue;
            const lng = Number(pt[0]);
            const lat = Number(pt[1]);
            if (!Number.isFinite(lng) || !Number.isFinite(lat)) continue;
            minLng = Math.min(minLng, lng);
            maxLng = Math.max(maxLng, lng);
            minLat = Math.min(minLat, lat);
            maxLat = Math.max(maxLat, lat);
          }
          if (
            Number.isFinite(minLng) &&
            Number.isFinite(minLat) &&
            Number.isFinite(maxLng) &&
            Number.isFinite(maxLat)
          ) {
            polygonBbox = { minLng, minLat, maxLng, maxLat };
          } else {
            polygonBbox = null;
          }
        }
      } catch {
        // ignore
      }

      const resp = await parcelsSearchNormalized(payload);
      const recs = resp.records || [];
      const zoningOpts = Array.isArray((resp as any).zoning_options) ? ((resp as any).zoning_options as any[]) : [];
      const fluOpts = Array.isArray((resp as any).future_land_use_options) ? ((resp as any).future_land_use_options as any[]) : [];

      const sample = recs.slice(0, 3).map((r) => ({
        parcel_id: (r.parcel_id || '').trim(),
        owner: (r.owner_name || '').trim(),
        address: (r.situs_address || r.address || '').trim(),
      }));

      setRunDebugOut({
        payload,
        polygonRingLen,
        polygonFirst: polygonFirst ?? null,
        polygonBbox: polygonBbox ?? null,
        polygonClosed,
        recordsLen: recs.length,
        sample,
        zoningOptionsLen: zoningOpts.filter((x) => typeof x === 'string' && x.trim()).length,
        futureLandUseOptionsLen: fluOpts.filter((x) => typeof x === 'string' && x.trim()).length,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setRunDebugOut({ payload: lastRequest, error: msg });
    } finally {
      setRunDebugLoading(false);
    }
  }

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
    setSoftWarnings([]);
    setErrorBanner(null);
    setRollupsError(null);
    setRollupsLastSummary(null);

    const built = buildSearchPayloadForMap();
    if ('error' in built) {
      setErrorBanner(built.error);
      setParcels([]);
      setRecords([]);
      return;
    }

    const payload = built.payload;
    const hadFilters = !!(payload as any)?.filters;

    const parsePositiveIntOrNull = (raw: string): number | null => {
      const s = String(raw || '').trim().replace(/,/g, '');
      if (!s) return null;
      const n = Number(s);
      if (!Number.isFinite(n)) return null;
      const i = Math.trunc(n);
      if (i <= 0) return null;
      return i;
    };

    const rollupsMinScoreN = parsePositiveIntOrNull(rollupsMinScore);
    const rollupsAnyGroups: string[] = [];
    if (rollupsGroupOfficialRecords) rollupsAnyGroups.push('official_records');
    if (rollupsGroupPermits) rollupsAnyGroups.push('permits');
    for (const g of rollupsTriggerGroups) {
      const gg = String(g || '').trim();
      if (gg) rollupsAnyGroups.push(gg);
    }

    const rollupsKeys: string[] = [];
    for (const k of rollupsTriggerKeys) {
      const kk = String(k || '').trim();
      if (kk) rollupsKeys.push(kk);
    }
    const rollupsTiers: string[] = [];
    if (rollupsTierCritical) rollupsTiers.push('critical');
    if (rollupsTierStrong) rollupsTiers.push('strong');
    if (rollupsTierSupport) rollupsTiers.push('support');
    const rollupsActive =
      rollupsEnabled &&
      (rollupsMinScoreN !== null || rollupsAnyGroups.length > 0 || rollupsTiers.length > 0 || rollupsKeys.length > 0);

    if (rollupsEnabled && !rollupsActive) {
      setRollupsError('Select at least one trigger filter (group/tier/min score).');
    }

    if (rollupsActive) {
      try {
        const rollupsReq: any = {
          county,
          min_score: rollupsMinScoreN,
          any_groups: rollupsAnyGroups.length ? rollupsAnyGroups : null,
          trigger_groups: rollupsAnyGroups.length ? rollupsAnyGroups : null,
          trigger_keys: rollupsKeys.length ? rollupsKeys : null,
          tiers: rollupsTiers.length ? rollupsTiers : null,
          limit: 2000,
          offset: 0,
        };
        if ((payload as any)?.polygon_geojson) rollupsReq.polygon_geojson = (payload as any).polygon_geojson;
        if ((payload as any)?.center && (payload as any)?.radius_m) {
          rollupsReq.center = (payload as any).center;
          rollupsReq.radius_m = (payload as any).radius_m;
        }

        const rollupsResp = await triggersRollupsSearch(rollupsReq);
        const ids = Array.isArray(rollupsResp.parcel_ids) ? rollupsResp.parcel_ids : [];
        try {
          const nextMap: Record<string, TriggerRollupRecord> = {};
          const rr = Array.isArray((rollupsResp as any).rollups) ? ((rollupsResp as any).rollups as TriggerRollupRecord[]) : [];
          for (const r of rr) {
            const pid = (r?.parcel_id || '').trim();
            if (pid) nextMap[pid] = r;
          }
          setRollupsMap(nextMap);
        } catch {
          setRollupsMap({});
        }
        setRollupsLastSummary({
          candidate_count: Number(rollupsResp.candidate_count || 0),
          returned_count: Number(rollupsResp.returned_count || 0),
          parcel_ids_count: ids.length,
        });

        if (!ids.length) {
          setErrorBanner('0 parcels matched trigger rollups filters.');
          setParcels([]);
          setRecords([]);
          return;
        }

        (payload as any).parcel_id_in = ids;
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        setRollupsError(msg);
        setErrorBanner(`Trigger rollups search failed: ${msg}`);
        setParcels([]);
        setRecords([]);
        return;
      }
    }
    if (!rollupsActive) {
      setRollupsMap({});
    }
    const requestOrigin = (() => {
      try {
        return typeof window !== 'undefined' ? String(window.location.origin || '') : '';
      } catch {
        return '';
      }
    })();
    const requestVia = requestOrigin.includes(':5173') ? 'vite-proxy' : 'direct';
    setLastRequest(payload);
    setLastResponseSummary(null);
    if (debugUiEnabled) {
      try {
        setDebugEvidence({
          requestJson: JSON.stringify(payload, null, 2),
          responseMeta: { request_origin: requestOrigin },
          prefilter: rollupsActive
            ? {
                rollups_enabled: true,
                min_score: rollupsMinScoreN,
                any_groups: rollupsAnyGroups,
                trigger_groups: rollupsAnyGroups,
                trigger_keys: rollupsKeys,
                tiers: rollupsTiers,
              }
            : { rollups_enabled: false },
        });
      } catch {
        setDebugEvidence({ requestJson: String(payload), responseMeta: { request_origin: requestOrigin } });
      }
    }
    console.log('[Run] payload', payload);

    try {
      const poly = (payload as any)?.polygon_geojson;
      const coords = poly?.coordinates?.[0];
      const ringLen = Array.isArray(coords) ? coords.length : 0;

      let bbox: any = null;
      if (Array.isArray(coords) && coords.length) {
        let minLng = Infinity;
        let minLat = Infinity;
        let maxLng = -Infinity;
        let maxLat = -Infinity;
        for (const p of coords) {
          if (!Array.isArray(p) || p.length < 2) continue;
          const lng = Number(p[0]);
          const lat = Number(p[1]);
          if (!Number.isFinite(lng) || !Number.isFinite(lat)) continue;
          minLng = Math.min(minLng, lng);
          minLat = Math.min(minLat, lat);
          maxLng = Math.max(maxLng, lng);
          maxLat = Math.max(maxLat, lat);
        }
        if (
          Number.isFinite(minLng) &&
          Number.isFinite(minLat) &&
          Number.isFinite(maxLng) &&
          Number.isFinite(maxLat)
        ) {
          bbox = {
            minLng: Number(minLng.toFixed(6)),
            minLat: Number(minLat.toFixed(6)),
            maxLng: Number(maxLng.toFixed(6)),
            maxLat: Number(maxLat.toFixed(6)),
          };
        }
      }

      const f = (payload as any)?.filters || null;
      const filtersSummary = f
        ? {
            min_sqft: f.min_sqft ?? null,
            max_sqft: f.max_sqft ?? null,
            min_acres: f.min_acres ?? null,
            max_acres: f.max_acres ?? null,
            lot_size_unit: f.lot_size_unit ?? null,
            min_lot_size: f.min_lot_size ?? null,
            max_lot_size: f.max_lot_size ?? null,
            zoning_in_len: Array.isArray(f.zoning_in) ? f.zoning_in.length : 0,
            flu_in_len: Array.isArray(f.future_land_use_in) ? f.future_land_use_in.length : 0,
          }
        : null;

      // Keep polygon coords out of the console; only log summary.
      console.log('[FILTERDBG] request', {
        county: (payload as any)?.county,
        polygonRingLen: ringLen,
        bbox,
        filters: filtersSummary,
        enrich: (payload as any)?.enrich,
        enrich_limit: (payload as any)?.enrich_limit,
      });
    } catch {
      // ignore
    }
    setLoading(true);

    const reqId = ++activeReq.current;
    try {
      const resp = await parcelsSearchNormalized(payload);
      if (reqId !== activeReq.current) return;

      if (debugUiEnabled) {
        try {
          const summary = (resp as any).summary || {};
          const warnings = Array.isArray((resp as any).warnings) ? ((resp as any).warnings as string[]) : [];
          const meta = {
            search_id: (resp as any).search_id,
            request_origin: requestOrigin,
            response_url: (resp as any)?.debug_flags?.response_url || undefined,
            candidate_count:
              typeof summary.candidate_count === 'number'
                ? summary.candidate_count
                : Number.isFinite(Number(summary.candidate_count))
                  ? Number(summary.candidate_count)
                  : null,
            filtered_count:
              typeof summary.filtered_count === 'number'
                ? summary.filtered_count
                : Number.isFinite(Number(summary.filtered_count))
                  ? Number(summary.filtered_count)
                  : null,
            warnings,
          };
          setDebugEvidence((prev) => ({
            requestJson: prev?.requestJson ?? JSON.stringify(payload, null, 2),
            responseMeta: meta,
            normalized_filters: (resp as any).normalized_filters,
            debug_timing_ms: (resp as any).debug_timing_ms,
            debug_counts: (resp as any).debug_counts,
            debug_flags: (resp as any).debug_flags,
            prefilter: prev?.prefilter,
          }));
        } catch {
          // ignore
        }
      }

      const summary = (resp as any).summary || {};
      const candidateCountRaw = typeof summary.candidate_count === 'number' ? summary.candidate_count : Number(summary.candidate_count);
      const filteredCountRaw = typeof summary.filtered_count === 'number' ? summary.filtered_count : Number(summary.filtered_count);
      const candidateCount = Number.isFinite(candidateCountRaw) ? candidateCountRaw : null;
      const filteredCount = Number.isFinite(filteredCountRaw)
        ? filteredCountRaw
        : Array.isArray((resp as any).records)
          ? (resp as any).records.length
          : null;

      setLastCounts({ candidateCount, filteredCount });

      const uniqSorted = (arr: unknown): string[] => {
        if (!Array.isArray(arr)) return [];
        const set = new Set<string>();
        for (const v of arr) {
          if (typeof v !== 'string') continue;
          const s = v.trim().replace(/\s+/g, ' ');
          if (s) set.add(s);
        }
        return Array.from(set).sort((a, b) => a.localeCompare(b));
      };

      const isJunkZoningOption = (s: string): boolean => {
        // Guard against known placeholders that show up in Orange zoning options.
        // Examples seen: "01/01/1993", "01/001".
        if (/^\d{2}\/\d{2}\/\d{4}$/.test(s)) return true;
        if (/^\d{2}\/\d{3}$/.test(s)) return true;
        return false;
      };

      const rawZoningOptions = uniqSorted((resp as any).zoning_options).filter((s) => !isJunkZoningOption(s));
      setZoningOptions(rawZoningOptions);
      setFutureLandUseOptions(uniqSorted((resp as any).future_land_use_options));

      const recs = resp.records || [];
      const list = Array.isArray((resp as any).parcels) ? ((resp as any).parcels as ParcelSearchListItem[]) : [];

      console.log('[Run] response', { recordsLen: recs.length });

      try {
        const searchId = (resp as any).search_id;
        const summary = (resp as any).summary || {};
        console.log('[FILTERDBG] response', {
          search_id: searchId,
          candidate_count: summary.candidate_count,
          filtered_count: summary.filtered_count,
          zoning_options_len: Array.isArray((resp as any).zoning_options)
            ? (resp as any).zoning_options.length
            : 0,
          future_land_use_options_len: Array.isArray((resp as any).future_land_use_options)
            ? (resp as any).future_land_use_options.length
            : 0,
          warnings_len: Array.isArray((resp as any).warnings) ? (resp as any).warnings.length : 0,
          field_stats: (resp as any).field_stats || null,
        });
      } catch {
        // ignore
      }
      const warningsAll = (resp as any).warnings as string[] | undefined;

      const isSoftWarning = (w: string): boolean => {
        const s = String(w || '').toLowerCase();
        return s.includes('swapped date range');
      };

      const soft = Array.isArray(warningsAll) ? warningsAll.filter(isSoftWarning) : [];
      const otherWarnings = Array.isArray(warningsAll) ? warningsAll.filter((w) => !isSoftWarning(w)) : [];
      setSoftWarnings(soft);
      setLastResponseCount(list.length || recs.length);

      const rawCounts = resp.summary?.source_counts || {};
      setSourceCounts({
        live: Number(rawCounts.live || 0),
        cache: Number(rawCounts.cache || 0),
      });

      try {
        const warnings = Array.isArray((resp as any).warnings) ? ((resp as any).warnings as string[]) : [];
        setLastResponseSummary({
          request_via: requestVia,
          search_id: (resp as any).search_id,
          returned_count: list.length || recs.length,
          candidate_count: candidateCount,
          filtered_count: filteredCount,
          records_truncated: Boolean((resp as any).records_truncated),
          source_counts: {
            live: Number(rawCounts.live || 0),
            cache: Number(rawCounts.cache || 0),
          },
          warnings,
          error_reason: (resp as any).error_reason ?? null,
          request: {
            has_polygon: Boolean((payload as any)?.polygon_geojson),
            min_sqft: (payload as any)?.filters?.min_sqft ?? null,
            trigger_keys: (payload as any)?.trigger_keys ?? null,
          },
          unsupported_trigger_keys: unknownSelectedTriggerKeys.length ? unknownSelectedTriggerKeys : null,
          coming_soon_selected_keys: comingSoonSelectedTriggerKeys.length ? comingSoonSelectedTriggerKeys : null,
        });
      } catch {
        // ignore
      }

      if (!list.length && !recs.length) {
        const hint =
          hadFilters && candidateCount && candidateCount > 0 && filteredCount === 0
            ? '0 matches your filters. Try widening ranges or clearing filters.'
            : null;

        const msg = hint
          ? hint
          : warningsAll?.length
            ? `No results returned. ${warningsAll.join(' / ')}`
            : 'No results returned from backend.';
        setLastError(msg);
        setErrorBanner(msg);
        setParcels([]);
        setRecords([]);
        return;
      }

      if (hadFilters && candidateCount && candidateCount > 0 && filteredCount === 0) {
        setErrorBanner('0 matches your filters. Try widening ranges or clearing filters.');
      } else if (otherWarnings.length) {
        setErrorBanner(`Warnings: ${otherWarnings.join(' / ')}`);
      }

      setParcels(list);
      setRecords(recs);

      // Refresh list filter immediately.
      setResultsQuery('');

      // If parcel lines are enabled, refresh them based on the new parcel_ids.
      setParcelLinesError(null);
      setParcelLinesFC(null);
      setParcelLinesFeatureCount(0);
    } catch (e) {
      if (reqId !== activeReq.current) return;
      const msg = e instanceof Error ? e.message : String(e);
      setLastError(msg);
      setErrorBanner(`Request failed (${requestVia}): ${msg}`);
      try {
        setLastResponseSummary({
          request_via: requestVia,
          error: msg,
          request: lastRequest,
        });
      } catch {
        // ignore
      }
      setParcels([]);
      setRecords([]);
      setParcelLinesFC(null);
      setParcelLinesEnabled(false);
      setParcelLinesError(null);
      setParcelLinesFeatureCount(0);
    } finally {
      if (reqId === activeReq.current) setLoading(false);
    }
  }

  const signalGroupOptions = useMemo(
    () => [
      { key: 'permits', label: 'Permits' },
      { key: 'official_records', label: 'Official Records' },
      { key: 'courts', label: 'Courts' },
      { key: 'tax', label: 'Tax' },
      { key: 'code_enforcement', label: 'Code Enforcement' },
      { key: 'gis_planning', label: 'Appraiser / Planning' },
    ],
    []
  );

  const distressPresets = useMemo(
    () => [
      { id: 'lis', label: 'Lis pendens', keys: ['lis_pendens'], groups: ['official_records'] },
      {
        id: 'foreclosure',
        label: 'Foreclosure',
        keys: ['foreclosure_filing', 'foreclosure_judgment', 'foreclosure'],
        groups: ['official_records'],
      },
      { id: 'tax', label: 'Delinquent tax', keys: ['delinquent_tax', 'tax_certificate_issued'], groups: ['tax'] },
      {
        id: 'code',
        label: 'Code case',
        keys: ['code_case_opened', 'unsafe_structure', 'condemnation', 'demolition_order', 'abatement_order', 'lien_recorded'],
        groups: ['code_enforcement'],
      },
      {
        id: 'liens',
        label: 'Liens',
        keys: ['mechanics_lien', 'hoa_lien', 'irs_tax_lien', 'state_tax_lien', 'code_enforcement_lien', 'judgment_lien', 'utility_lien'],
        groups: ['official_records'],
      },
      { id: 'probate', label: 'Probate', keys: ['probate_opened', 'probate'], groups: ['courts'] },
      { id: 'divorce', label: 'Divorce', keys: ['divorce_filed', 'divorce'], groups: ['courts'] },
    ],
    []
  );

  const toggleValueInList = useCallback((prev: string[], raw: string): string[] => {
    const v = String(raw || '').trim();
    if (!v) return prev;
    const exists = prev.includes(v);
    return exists ? prev.filter((x) => x !== v) : [...prev, v];
  }, []);

  return (
    <div
      className="flex h-full min-h-[520px]"
      style={{
        ['--cre-bg' as any]: '248 250 252',
        ['--cre-surface' as any]: '255 255 255',
        ['--cre-text' as any]: '15 23 42',
        ['--cre-muted' as any]: '71 85 105',
        ['--cre-border' as any]: '203 213 225',
        ['--cre-accent' as any]: '59 130 246',
      }}
    >
      {toast ? (
        <div className="pointer-events-none fixed left-1/2 top-4 z-[1000] w-[420px] -translate-x-1/2 rounded-xl border border-cre-border/60 bg-cre-surface px-4 py-2 text-sm text-cre-text shadow-lg">
          {toast}
        </div>
      ) : null}

      <aside className="h-full w-[420px] shrink-0 overflow-y-auto border-r border-cre-border/60 bg-cre-bg p-4">
        <div className="flex items-center justify-between gap-2">
          <div>
            <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Map Search</div>
            <div className="text-sm text-cre-text">Search → Area → Signals → Results</div>
          </div>

          <select
            className="rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-sm text-cre-text"
            value={county}
            onChange={(e: ChangeEvent<HTMLSelectElement>) => {
              const next = e.target.value;
              setCounty(next);
              setParcels([]);
              setRecords([]);
              setSourceCounts({ live: 0, cache: 0 });
              setSelectedParcelId(null);
              setSignalsDrawerOpen(false);
              setZoningOptions([]);
              setFutureLandUseOptions([]);
              setZoningQuery('');
              setFutureLandUseQuery('');
              setSelectedZoning([]);
              setSelectedFutureLandUse([]);
              setRollupsEnabled(false);
              setRollupsLastSummary(null);
              setRollupsError(null);
              setRollupsMap({});
              setSelectedSavedSearchId('');
              setAlertsInbox([]);
              setAlertsError(null);
            }}
          >
            <option value="orange">Orange</option>
            <option value="seminole">Seminole</option>
            <option value="broward">Broward</option>
            <option value="alachua">Alachua</option>
          </select>
        </div>

        {errorBanner ? (
          <div className="mt-3 rounded-xl border border-cre-border/60 bg-cre-surface p-3 text-sm text-cre-text">
            <div className="font-semibold">Notice</div>
            <div className="mt-1 text-xs text-cre-muted">{errorBanner}</div>
          </div>
        ) : null}

        <div className="mt-4 rounded-xl border border-cre-border/60 bg-cre-surface p-3">
          <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">1) Search</div>

          <div className="mt-2 grid gap-2">
            <input
              className="w-full rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-2 text-sm text-cre-text"
              placeholder="Filter results by owner, address, or parcel id"
              value={resultsQuery}
              onChange={(e) => setResultsQuery(e.target.value)}
            />

            <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
              <div className="flex flex-wrap gap-3 text-cre-text">
                <label className="flex items-center gap-2">
                  <input type="checkbox" checked={showLive} onChange={(e) => setShowLive(e.target.checked)} />
                  Live
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" checked={showCache} onChange={(e) => setShowCache(e.target.checked)} />
                  Cache
                </label>
              </div>
              {filtersActive ? (
                <button
                  type="button"
                  className="rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-1 text-xs text-cre-text hover:bg-cre-surface"
                  onClick={clearFilters}
                >
                  Clear filters
                </button>
              ) : null}
            </div>

            <details className="rounded-xl border border-cre-border/60 bg-cre-bg p-3">
              <summary className="cursor-pointer select-none text-xs font-semibold text-cre-text">
                Property filters
                {filtersActive ? <span className="ml-2 text-[11px] text-cre-muted">(active)</span> : null}
              </summary>

              <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
                <label className="space-y-1">
                  <div className="text-cre-muted">Min Sqft</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minSqft}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minSqft: e.target.value }))}
                    placeholder="e.g. 2000"
                  />
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Sqft</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxSqft}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxSqft: e.target.value }))}
                    placeholder=""
                  />
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Sort</div>
                  <select
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    value={sortKey}
                    onChange={(e) => setSortKey(((e.target.value as any) || 'relevance') as any)}
                  >
                    <option value="relevance">Relevance (default)</option>
                    <option value="last_sale_date_desc">Last sale date (newest)</option>
                    <option value="year_built_desc">Year built (newest)</option>
                    <option value="sqft_desc">Living sqft (largest)</option>
                  </select>
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Parcel Size Unit</div>
                  <select
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
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
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="decimal"
                    value={filterForm.minLotSize}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minLotSize: e.target.value }))}
                    placeholder={filterForm.lotSizeUnit === 'acres' ? 'e.g. 0.25' : 'e.g. 8000'}
                  />
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Parcel Size</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="decimal"
                    value={filterForm.maxLotSize}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxLotSize: e.target.value }))}
                  />
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Beds</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minBeds}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minBeds: e.target.value }))}
                    placeholder="e.g. 3"
                  />
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Min Baths</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="decimal"
                    value={filterForm.minBaths}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minBaths: e.target.value }))}
                    placeholder="e.g. 2"
                  />
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Year Built</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minYearBuilt}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minYearBuilt: e.target.value }))}
                    placeholder="e.g. 1990"
                  />
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Year Built</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxYearBuilt}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxYearBuilt: e.target.value }))}
                  />
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Property Type</div>
                  <select
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
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
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    value={filterForm.zoning}
                    onChange={(e) => setFilterForm((p) => ({ ...p, zoning: e.target.value }))}
                    placeholder="e.g. R-1"
                  />
                </label>

                <div className="col-span-2 mt-3 space-y-3">
                  <MultiSelectFilter
                    title="Current Zoning (multi-select)"
                    options={zoningOptions}
                    selected={selectedZoning}
                    query={zoningQuery}
                    onQuery={setZoningQuery}
                    onSelected={setSelectedZoning}
                    renderOption={formatCodeLabel}
                  />
                  <MultiSelectFilter
                    title="Future Land Use (multi-select)"
                    options={futureLandUseOptions}
                    selected={selectedFutureLandUse}
                    query={futureLandUseQuery}
                    onQuery={setFutureLandUseQuery}
                    onSelected={setSelectedFutureLandUse}
                    renderOption={formatCodeLabel}
                  />
                  <div className="text-[11px] text-cre-muted">Tip: “Zoning contains” and multi-select both apply (AND).</div>
                </div>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Total Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minValue}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minValue: e.target.value }))}
                    placeholder="e.g. 350000"
                  />
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Total Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxValue}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxValue: e.target.value }))}
                  />
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Land Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minLandValue}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minLandValue: e.target.value }))}
                  />
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Land Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxLandValue}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxLandValue: e.target.value }))}
                  />
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Building Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minBuildingValue}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minBuildingValue: e.target.value }))}
                  />
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Building Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxBuildingValue}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxBuildingValue: e.target.value }))}
                  />
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Last Sale Start</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    type="text"
                    inputMode="numeric"
                    value={filterForm.lastSaleStart}
                    onChange={(e) => setFilterForm((p) => ({ ...p, lastSaleStart: e.target.value }))}
                    placeholder="YYYY-MM-DD or MM/DD/YYYY"
                  />
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Last Sale End</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    type="text"
                    inputMode="numeric"
                    value={filterForm.lastSaleEnd}
                    onChange={(e) => setFilterForm((p) => ({ ...p, lastSaleEnd: e.target.value }))}
                    placeholder="YYYY-MM-DD or MM/DD/YYYY"
                  />
                </label>

                {softWarnings.length ? (
                  <div className="col-span-2 -mt-1 rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-1 text-[11px] text-cre-muted">
                    {softWarnings.join(' / ')}
                  </div>
                ) : null}
              </div>

              <label className="mt-3 flex items-center gap-2 text-xs text-cre-text">
                <input type="checkbox" checked={autoEnrichMissing} onChange={(e) => setAutoEnrichMissing(e.target.checked)} />
                Auto-enrich missing (slower)
              </label>
            </details>
          </div>
        </div>

        <div className="mt-4 rounded-xl border border-cre-border/60 bg-cre-surface p-3">
          <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">2) Draw / Area</div>
          <div className="mt-2 text-xs text-cre-muted">Use polygon or circle tools (top-right of map).</div>
          <div className="mt-2 text-xs text-cre-muted">
            Status: <span className="font-semibold text-cre-text">{geometryStatus}</span>
          </div>

          <div className="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              className="rounded-xl border border-cre-border/60 bg-cre-bg px-3 py-2 text-sm text-cre-text hover:bg-cre-surface"
              onClick={clearDrawings}
            >
              Clear area
            </button>
            <label className="flex items-center gap-2 rounded-xl border border-cre-border/60 bg-cre-bg px-3 py-2 text-xs text-cre-text">
              <input
                type="checkbox"
                checked={parcelLinesEnabled}
                disabled={isDrawing || parcelLinesLoading}
                onChange={(e) => {
                  const next = e.target.checked;
                  setParcelLinesError(null);
                  setParcelLinesFC(null);
                  setParcelLinesFeatureCount(0);
                  setParcelLinesLastIdsCount(0);
                  setParcelLinesStatus('idle');
                  setParcelLinesEnabled(next);
                }}
              />
              Parcel lines
              {parcelLinesLoading ? <span className="text-cre-muted">…</span> : null}
            </label>
          </div>
          {parcelLinesEnabled ? (
            <div className="mt-2 text-[11px] text-cre-muted">
              status: {parcelLinesStatus} · ids: {parcelLinesLastIdsCount} · features: {parcelLinesFeatureCount}
            </div>
          ) : null}
          {parcelLinesError ? <div className="mt-1 text-[11px] text-cre-muted">{parcelLinesError}</div> : null}
        </div>

        <div className="mt-4 rounded-xl border border-cre-border/60 bg-cre-surface p-3">
          <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">3) Signals</div>
          <div className="mt-3 space-y-3 text-xs">
              <label className="space-y-1">
                <div className="text-cre-muted">Seller intent</div>
                <select
                  className="w-full rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-2 text-sm text-cre-text"
                  value={(rollupsMinScore || '').trim()}
                  onChange={(e) => setRollupsMinScore(e.target.value)}
                >
                  <option value="">Any</option>
                  <option value="30">Some intent (≥ 30)</option>
                  <option value="50">Likely seller (≥ 50)</option>
                  <option value="70">High intent (≥ 70)</option>
                </select>
              </label>

              <div className="space-y-1">
                <div className="text-cre-muted">Signal groups</div>
                <div className="flex flex-wrap gap-3 pt-1 text-cre-text">
                  {signalGroupOptions.map((g) => (
                    <label key={g.key} className="flex items-center gap-2">
                      <input
                        type="checkbox"
                        checked={rollupsTriggerGroups.includes(g.key)}
                        onChange={() => setRollupsTriggerGroups((prev) => toggleValueInList(prev, g.key))}
                      />
                      {g.label}
                    </label>
                  ))}
                </div>
              </div>

              <div className="space-y-1">
                <div className="text-cre-muted">Distress (quick picks)</div>
                <div className="flex flex-wrap gap-2">
                  {distressPresets.map((p) => {
                    const active = p.keys.every((k) => rollupsTriggerKeys.includes(k));
                    return (
                      <button
                        key={p.id}
                        type="button"
                        className={
                          active
                            ? 'rounded-full bg-cre-accent px-3 py-1 text-[12px] font-semibold text-white'
                            : 'rounded-full border border-cre-border/60 bg-cre-bg px-3 py-1 text-[12px] text-cre-text hover:bg-cre-surface'
                        }
                        onClick={() => {
                          setRollupsEnabled(true);
                          setRollupsTriggerGroups((prev) => {
                            let next = prev;
                            for (const g of p.groups) next = next.includes(g) ? next : [...next, g];
                            return next;
                          });
                          setRollupsTriggerKeys((prev) => {
                            const hasAll = p.keys.every((k) => prev.includes(k));
                            if (hasAll) return prev.filter((k) => !p.keys.includes(k));
                            const next = [...prev];
                            for (const k of p.keys) if (!next.includes(k)) next.push(k);
                            return next;
                          });
                        }}
                      >
                        {p.label}
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="space-y-1">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-cre-muted">Trigger catalog (taxonomy)</div>
                  <button
                    type="button"
                    className="rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-1 text-[11px] text-cre-text hover:bg-cre-surface disabled:opacity-60"
                    onClick={() => {
                      setRollupsTriggerKeys([]);
                      setRollupsLastSummary(null);
                      setRollupsError(null);
                    }}
                    disabled={!rollupsTriggerKeys.length}
                  >
                    Clear selected
                  </button>
                </div>

                <div className="rounded-lg border border-cre-border/60 bg-cre-bg">
                  <div className="border-b border-cre-border/60 px-2 py-2 text-[11px] text-cre-muted">
                    Selected: <span className="font-semibold text-cre-text">{rollupsTriggerKeys.length}</span>
                    {comingSoonSelectedTriggerKeys.length ? (
                      <span className="ml-2 text-amber-700">
                        Coming soon selected: {comingSoonSelectedTriggerKeys.length}
                      </span>
                    ) : null}
                  </div>
                  <div className="max-h-64 overflow-auto p-2">
                    <div className="space-y-3">
                      {signalCatalogByGroup.map(([group, items]) => (
                        <div key={`cat:${group}`}>
                          <div className="text-[11px] font-semibold uppercase tracking-wide text-cre-muted">{group}</div>
                          <div className="mt-1 space-y-1">
                            {items.map((it) => {
                              const checked = rollupsTriggerKeys.includes(it.key);
                              return (
                                <label
                                  key={`sig:${it.key}`}
                                  className="flex cursor-pointer select-none items-center gap-2 rounded-md px-1 py-1 text-xs text-cre-text hover:bg-cre-surface"
                                  title={it.key}
                                >
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    onChange={() => {
                                      setRollupsTriggerKeys((prev) => toggleValueInList(prev, it.key));
                                    }}
                                  />
                                  <span className="truncate">{it.label}</span>
                                  {it.comingSoon ? (
                                    <span className="ml-auto rounded-full border border-amber-400/60 bg-amber-50 px-2 py-0.5 text-[10px] font-semibold text-amber-800">
                                      Coming soon
                                    </span>
                                  ) : (
                                    <span className="ml-auto font-mono text-[10px] text-cre-muted">{it.key}</span>
                                  )}
                                </label>
                              );
                            })}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
                {unknownSelectedTriggerKeys.length ? (
                  <div className="text-[11px] text-amber-700">
                    Unknown selected keys (will be sent but may not be supported):{' '}
                    {unknownSelectedTriggerKeys.join(', ')}
                  </div>
                ) : null}
              </div>

              <div className="space-y-1">
                <div className="text-cre-muted">Tier</div>
                <div className="flex flex-wrap gap-3 pt-1 text-cre-text">
                  <label className="flex items-center gap-2">
                    <input type="checkbox" checked={rollupsTierCritical} onChange={(e) => setRollupsTierCritical(e.target.checked)} />
                    Critical
                  </label>
                  <label className="flex items-center gap-2">
                    <input type="checkbox" checked={rollupsTierStrong} onChange={(e) => setRollupsTierStrong(e.target.checked)} />
                    Strong
                  </label>
                  <label className="flex items-center gap-2">
                    <input type="checkbox" checked={rollupsTierSupport} onChange={(e) => setRollupsTierSupport(e.target.checked)} />
                    Support
                  </label>
                </div>
              </div>

              <div className="rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-2 text-[11px] text-cre-muted">
                {rollupsError
                  ? `Rollups: ${rollupsError}`
                  : rollupsLastSummary
                    ? `Rollups last run: ${rollupsLastSummary.returned_count} matched (of ${rollupsLastSummary.candidate_count})`
                    : 'Active filters summary will show after you Run.'}
              </div>
            </div>
        </div>

        <div className="mt-4 rounded-xl border border-cre-border/60 bg-cre-surface p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">4) Results</div>
            <div className="text-xs text-cre-muted">
              {lastCounts && lastCounts.candidateCount !== null && lastCounts.filteredCount !== null
                ? `Showing ${lastCounts.filteredCount} of ${lastCounts.candidateCount}`
                : `Showing ${visibleRows.length} (live ${sourceCounts.live} / cache ${sourceCounts.cache})`}
            </div>
          </div>

          <div className="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              className="rounded-xl bg-cre-accent px-4 py-2 text-sm font-semibold text-white hover:brightness-95 disabled:opacity-60"
              onClick={() => void run()}
              disabled={loading}
            >
              {loading ? 'Running…' : 'Run'}
            </button>
            <button
              type="button"
              className="rounded-xl border border-cre-border/60 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface disabled:opacity-60"
              onClick={() => void enrichVisible()}
              disabled={loading || parcels.length === 0}
            >
              Enrich
            </button>
            <button
              type="button"
              className="rounded-xl border border-cre-border/60 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface"
              onClick={() => void saveCurrentSearch()}
              title="Saves the current polygon + filters as a saved search"
            >
              Save search
            </button>
          </div>

          <div className="mt-3 rounded-xl border border-cre-border/60 bg-cre-bg p-3">
            <div className="flex items-center justify-between">
              <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Saved searches + alerts</div>
              <button
                type="button"
                className="rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-[11px] text-cre-text hover:bg-cre-bg disabled:opacity-60"
                disabled={savedSearchesLoading}
                onClick={() => void refreshSavedSearches(county)}
              >
                Refresh
              </button>
            </div>

            {savedSearchesError ? <div className="mt-2 text-[11px] text-cre-muted">{savedSearchesError}</div> : null}

            <div className="mt-2 grid gap-2">
              <select
                className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-2 text-sm text-cre-text"
                value={selectedSavedSearchId}
                onChange={(e) => setSelectedSavedSearchId(e.target.value)}
              >
                <option value="">(Select saved search)</option>
                {savedSearches.map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.name} · {s.id}
                  </option>
                ))}
              </select>

              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  className="rounded-lg bg-cre-accent px-3 py-2 text-xs font-semibold text-white disabled:opacity-60"
                  disabled={!selectedSavedSearchId}
                  onClick={() => void runSelectedSavedSearch()}
                >
                  Run saved search
                </button>
                <select
                  className="rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-2 text-xs text-cre-text"
                  value={alertsStatus}
                  onChange={(e) => setAlertsStatus(e.target.value)}
                >
                  <option value="new">New</option>
                  <option value="read">Read</option>
                  <option value="">All</option>
                </select>
                <button
                  type="button"
                  className="rounded-lg border border-cre-border/60 bg-cre-surface px-3 py-2 text-xs text-cre-text hover:bg-cre-bg disabled:opacity-60"
                  disabled={!selectedSavedSearchId || alertsLoading}
                  onClick={() => void refreshAlerts(selectedSavedSearchId, alertsStatus)}
                >
                  Refresh alerts
                </button>
              </div>

              {alertsError ? <div className="text-[11px] text-cre-muted">{alertsError}</div> : null}
              {selectedSavedSearchId ? (
                alertsLoading ? (
                  <div className="text-[11px] text-cre-muted">Loading alerts…</div>
                ) : alertsInbox.length ? (
                  <div className="max-h-[220px] space-y-2 overflow-auto">
                    {alertsInbox.slice(0, 25).map((a) => (
                      <div key={`alert:${a.id}`} className="rounded-lg border border-cre-border/60 bg-cre-surface p-2">
                        <div className="flex items-start justify-between gap-2">
                          <div>
                            <div className="font-mono text-[11px] text-cre-text">{a.alert_key}</div>
                            <div className="text-[11px] text-cre-muted">
                              {a.county}:{a.parcel_id} · sev {a.severity} · {a.status}
                            </div>
                          </div>
                          {String(a.status || '').toLowerCase() !== 'read' ? (
                            <button
                              type="button"
                              className="rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-1 text-[11px] text-cre-text hover:bg-cre-surface"
                              onClick={() => void markInboxAlertRead(a)}
                            >
                              Mark read
                            </button>
                          ) : null}
                        </div>
                        <div className="mt-1 text-[11px] text-cre-muted">Last: {a.last_seen_at}</div>
                      </div>
                    ))}
                    {alertsInbox.length > 25 ? <div className="text-[11px] text-cre-muted">Showing first 25 alerts.</div> : null}
                  </div>
                ) : (
                  <div className="text-[11px] text-cre-muted">No alerts.</div>
                )
              ) : (
                <div className="text-[11px] text-cre-muted">Select a saved search to view alerts.</div>
              )}
            </div>
          </div>

          <div className="mt-3 max-h-[520px] space-y-2 overflow-auto">
            {visibleRows.length ? (
              visibleRows.slice(0, 250).map((p) => {
                const rec = recordById.get(p.parcel_id);
                const rollup = rollupsMap[p.parcel_id] || null;
                const addr = (p.address || rec?.situs_address || rec?.address || '').trim() || '—';
                const owner = (p.owner_name || rec?.owner_name || '').trim() || '—';
                const srcLabel = (p.source || (rec as any)?.source || '—').toString().toUpperCase();

                const groupsBadges: Array<{ k: string; label: string }> = [];
                if (rollup && Number(rollup.has_official_records || 0) > 0) groupsBadges.push({ k: 'or', label: 'Records' });
                if (rollup && Number(rollup.has_permits || 0) > 0) groupsBadges.push({ k: 'p', label: 'Permits' });
                if (rollup && Number(rollup.has_tax || 0) > 0) groupsBadges.push({ k: 't', label: 'Tax' });
                if (rollup && Number(rollup.has_code_enforcement || 0) > 0) groupsBadges.push({ k: 'ce', label: 'Code' });
                if (rollup && Number(rollup.has_courts || 0) > 0) groupsBadges.push({ k: 'ct', label: 'Courts' });
                if (rollup && Number(rollup.has_gis_planning || 0) > 0) groupsBadges.push({ k: 'gp', label: 'Appraiser' });

                return (
                  <button
                    key={`result:${p.parcel_id}`}
                    type="button"
                    className={
                      selectedParcelId === p.parcel_id
                        ? 'w-full rounded-xl border border-cre-accent bg-cre-bg p-3 text-left shadow-sm'
                        : 'w-full rounded-xl border border-cre-border/60 bg-cre-bg p-3 text-left hover:bg-cre-surface'
                    }
                    onClick={() => {
                      setSelectedParcelId(p.parcel_id);
                      setSignalsDrawerOpen(true);
                    }}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div>
                        <div className="text-sm font-semibold text-cre-text">{addr}</div>
                        <div className="mt-1 text-xs text-cre-muted">{owner}</div>
                        <div className="mt-1 font-mono text-[11px] text-cre-muted">{p.parcel_id}</div>
                      </div>
                      <div className="text-[11px] text-cre-muted">{srcLabel}</div>
                    </div>

                    <div className="mt-2 flex flex-wrap items-center gap-2">
                      {rollup ? (
                        <span className="rounded-full border border-cre-border/60 bg-cre-surface px-2 py-1 text-[11px] text-cre-text">
                          Score {rollup.seller_score} · c{rollup.count_critical} / s{rollup.count_strong} / p{rollup.count_support}
                        </span>
                      ) : null}
                      {groupsBadges.map((g) => (
                        <span key={`${p.parcel_id}:${g.k}`} className="rounded-full border border-cre-border/60 bg-cre-surface px-2 py-1 text-[11px] text-cre-text">
                          {g.label}
                        </span>
                      ))}
                      <span className="rounded-full border border-cre-border/60 bg-cre-surface px-2 py-1 text-[11px] text-cre-text">
                        View signals →
                      </span>
                    </div>
                  </button>
                );
              })
            ) : (
              <div className="rounded-xl border border-cre-border/60 bg-cre-bg p-3 text-sm text-cre-muted">No results yet. Draw an area and Run.</div>
            )}
            {visibleRows.length > 250 ? (
              <div className="text-[11px] text-cre-muted">Showing first 250 results.</div>
            ) : null}
          </div>
        </div>

        <details className="mt-4 rounded-xl border border-cre-border/60 bg-cre-surface p-3">
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
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                className="rounded-lg border border-cre-border/60 bg-cre-bg px-3 py-2 text-xs text-cre-text hover:bg-cre-surface disabled:opacity-60"
                onClick={() => void runDebug()}
                disabled={runDebugLoading}
              >
                {runDebugLoading ? 'Debug…' : 'Run (debug)'}
              </button>
            </div>
            {runDebugOut ? (
              <pre className="max-h-56 overflow-auto whitespace-pre-wrap break-words">{JSON.stringify(runDebugOut, null, 2)}</pre>
            ) : null}
          </div>
        </details>

        <details className="mt-4 rounded-xl border border-cre-border/60 bg-cre-surface p-3" open>
          <summary className="cursor-pointer text-sm font-semibold text-cre-text">Last Request / Response Proof</summary>
          <div className="mt-3 space-y-3 text-xs text-cre-muted">
            <div>
              <div className="font-semibold text-cre-text">Last Request JSON</div>
              <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-words rounded-lg border border-cre-border/60 bg-cre-bg p-2">
                {lastRequest ? JSON.stringify(lastRequest, null, 2) : '—'}
              </pre>
            </div>
            <div>
              <div className="font-semibold text-cre-text">Last Response Summary</div>
              <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-words rounded-lg border border-cre-border/60 bg-cre-bg p-2">
                {lastResponseSummary ? JSON.stringify(lastResponseSummary, null, 2) : '—'}
              </pre>
            </div>
          </div>
        </details>
      </aside>

      <main className="flex-1 bg-cre-bg p-4">
        <div className="relative h-full overflow-hidden rounded-2xl border border-cre-border/60 bg-cre-surface shadow-panel">
          <MapContainer center={[28.5383, -81.3792]} zoom={12} doubleClickZoom={false} style={{ height: '100%', width: '100%' }}>
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
                setSignalsDrawerOpen(false);
                setErrorBanner(null);
                drawnPolygonRef.current = geom;
                drawnCircleRef.current = null;
                setDrawnPolygon(geom);
                setDrawnCircle(null);
              }}
              onDeleted={() => {
                drawnPolygonRef.current = null;
                drawnCircleRef.current = null;
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
                  const pid = String(props.parcel_id || props.parcelId || '').trim();
                  const rec = pid ? recordById.get(pid) : undefined;
                  const fallbackFromList = pid ? parcels.find((pp) => pp.parcel_id === pid) : undefined;
                  const addr = String(
                    props.situs_address || props.address || rec?.situs_address || rec?.address || fallbackFromList?.address || ''
                  ).trim();
                  const owner = String(props.owner_name || props.owner || rec?.owner_name || fallbackFromList?.owner_name || '').trim();
                  const lines = [
                    owner ? `Owner: ${owner}` : 'Owner: —',
                    addr ? `Address: ${addr}` : 'Address: —',
                    pid ? `Parcel: ${pid}` : 'Parcel: —',
                  ];
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

            {visibleRows.map((p) => {
              const lat = typeof p.lat === 'number' && Number.isFinite(p.lat) ? p.lat : null;
              const lng = typeof p.lng === 'number' && Number.isFinite(p.lng) ? p.lng : null;
              if (lat === null || lng === null) return null;

              const selected = selectedParcelId === p.parcel_id;
              const pos: [number, number] = [lat, lng];
              if (selected) {
                return <CircleMarker key={p.parcel_id} center={pos} radius={9} pathOptions={{ color: '#2563eb', weight: 2, fillOpacity: 0.35 }} />;
              }
              return (
                <Marker
                  key={p.parcel_id}
                  position={pos}
                  interactive={!isDrawing}
                  eventHandlers={{
                    click: () => {
                      setSelectedParcelId(p.parcel_id);
                      setSignalsDrawerOpen(true);
                    },
                  }}
                />
              );
            })}
          </MapContainer>

          <div
            className={
              signalsDrawerOpen
                ? 'pointer-events-auto absolute inset-0 bg-black/10'
                : 'pointer-events-none absolute inset-0 bg-transparent'
            }
            onClick={() => setSignalsDrawerOpen(false)}
          />

          <div
            className={
              signalsDrawerOpen
                ? 'absolute right-0 top-0 h-full w-[420px] translate-x-0 border-l border-cre-border/60 bg-cre-surface shadow-xl transition-transform'
                : 'absolute right-0 top-0 h-full w-[420px] translate-x-full border-l border-cre-border/60 bg-cre-surface shadow-xl transition-transform'
            }
          >
            <div className="flex h-full flex-col">
              <div className="flex items-center justify-between gap-2 border-b border-cre-border/60 px-4 py-3">
                <div>
                  <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Signals</div>
                  <div className="font-mono text-[12px] text-cre-text">{selectedParcelId || '—'}</div>
                </div>
                <button
                  type="button"
                  className="rounded-lg border border-cre-border/60 bg-cre-bg px-3 py-1 text-sm text-cre-text hover:bg-cre-surface"
                  onClick={() => setSignalsDrawerOpen(false)}
                >
                  Close
                </button>
              </div>

              <div className="flex-1 overflow-auto px-4 py-3">
                {!selectedParcelId ? (
                  <div className="text-sm text-cre-muted">Select a parcel to view signals.</div>
                ) : (
                  <div className="space-y-4">
                    {(() => {
                      const rec = selectedParcelId ? records.find((r) => r.parcel_id === selectedParcelId) : null;
                      const p = selectedParcelId ? parcels.find((x) => x.parcel_id === selectedParcelId) : null;
                      const addr = (rec?.situs_address || rec?.address || p?.address || '').trim();
                      const owner = (rec?.owner_name || p?.owner_name || '').trim();
                      return (
                        <div className="rounded-xl border border-cre-border/60 bg-cre-bg p-3">
                          <div className="text-sm font-semibold text-cre-text">{addr || '—'}</div>
                          <div className="mt-1 text-xs text-cre-muted">{owner || '—'}</div>
                          {(rec || p) ? (
                            <div className="mt-2 text-[11px] text-cre-muted">
                              Year {(rec?.year_built ?? p?.year_built) ?? '—'} · Beds {(rec?.beds ?? p?.beds) ?? '—'} · Baths {(rec?.baths ?? p?.baths) ?? '—'} · Just {typeof (rec?.just_value ?? p?.just_value) === 'number' && (rec?.just_value ?? p?.just_value) ? `$${Math.round((rec?.just_value ?? p?.just_value)).toLocaleString()}` : '—'} · Assessed {typeof (rec?.assessed_value ?? p?.assessed_value) === 'number' && (rec?.assessed_value ?? p?.assessed_value) ? `$${Math.round((rec?.assessed_value ?? p?.assessed_value)).toLocaleString()}` : '—'} · Taxable {typeof (rec?.taxable_value ?? p?.taxable_value) === 'number' && (rec?.taxable_value ?? p?.taxable_value) ? `$${Math.round((rec?.taxable_value ?? p?.taxable_value)).toLocaleString()}` : '—'}
                            </div>
                          ) : null}
                        </div>
                      );
                    })()}

                    <div className="rounded-xl border border-cre-border/60 bg-cre-bg p-3">
                      <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Rollup</div>
                      {selectedRollupLoading ? (
                        <div className="mt-2 text-sm text-cre-muted">Loading rollup…</div>
                      ) : selectedRollup ? (
                        <div className="mt-2 space-y-1 text-sm text-cre-text">
                          <div>Seller score: {selectedRollup.seller_score}</div>
                          <div className="text-xs text-cre-muted">Tier counts: c{selectedRollup.count_critical} / s{selectedRollup.count_strong} / p{selectedRollup.count_support}</div>
                        </div>
                      ) : (
                        <div className="mt-2 text-sm text-cre-muted">Rollup unavailable: {selectedRollupError || '—'}</div>
                      )}
                    </div>

                    <div className="rounded-xl border border-cre-border/60 bg-cre-bg p-3">
                      <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Alerts</div>
                      {selectedTriggersLoading ? (
                        <div className="mt-2 text-sm text-cre-muted">Loading alerts…</div>
                      ) : selectedTriggersError ? (
                        <div className="mt-2 text-sm text-cre-muted">{selectedTriggersError}</div>
                      ) : selectedAlerts.length ? (
                        <div className="mt-2 space-y-2">
                          {selectedAlerts.slice(0, 25).map((a) => (
                            <div key={`ta:${a.id}`} className="rounded-lg border border-cre-border/60 bg-cre-surface p-2">
                              <div className="flex items-center justify-between gap-2">
                                <div className="font-mono text-[11px] text-cre-text">{a.alert_key}</div>
                                <div className="text-[11px] text-cre-muted">sev {a.severity}</div>
                              </div>
                              <div className="text-[11px] text-cre-muted">Last: {a.last_seen_at}</div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="mt-2 text-sm text-cre-muted">No open alerts for this parcel.</div>
                      )}
                    </div>

                    <div className="rounded-xl border border-cre-border/60 bg-cre-bg p-3">
                      <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Triggers timeline</div>
                      {selectedTriggersLoading ? (
                        <div className="mt-2 text-sm text-cre-muted">Loading triggers…</div>
                      ) : selectedTriggersError ? (
                        <div className="mt-2 text-sm text-cre-muted">{selectedTriggersError}</div>
                      ) : selectedTriggerEvents.length ? (
                        <div className="mt-2 space-y-2">
                          {selectedTriggerEvents
                            .slice()
                            .sort((a, b) => String(b.trigger_at || '').localeCompare(String(a.trigger_at || '')))
                            .slice(0, 50)
                            .map((t) => (
                              <div key={`te:${t.id}`} className="rounded-lg border border-cre-border/60 bg-cre-surface p-2">
                                <div className="flex items-center justify-between gap-2">
                                  <div className="font-mono text-[11px] text-cre-text">{t.trigger_key}</div>
                                  <div className="text-[11px] text-cre-muted">sev {t.severity}</div>
                                </div>
                                <div className="text-[11px] text-cre-muted">{t.trigger_at}</div>
                                <div className="text-[11px] text-cre-muted">{t.source_connector_key}:{t.source_event_type}</div>
                              </div>
                            ))}
                        </div>
                      ) : (
                        <div className="mt-2 text-sm text-cre-muted">No triggers found.</div>
                      )}
                    </div>

                    <div className="rounded-xl border border-cre-border/60 bg-cre-bg p-3">
                      <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Permits</div>
                      {selectedPermitsLoading ? (
                        <div className="mt-2 text-sm text-cre-muted">Loading permits…</div>
                      ) : selectedPermitsError ? (
                        <div className="mt-2 text-sm text-cre-muted">{selectedPermitsError}</div>
                      ) : selectedPermits.length ? (
                        <div className="mt-2 space-y-2">
                          <div className="text-[11px] text-cre-muted">{selectedPermits.length} record(s)</div>
                          {selectedPermits.slice(0, 10).map((pp) => (
                            <div key={`permit:${pp.county}:${pp.permit_number}`} className="rounded-lg border border-cre-border/60 bg-cre-surface p-2">
                              <div className="flex items-center justify-between gap-2">
                                <div className="font-mono text-[11px] text-cre-text">{pp.permit_number}</div>
                                <div className="text-[11px] text-cre-muted">{pp.status || '—'}</div>
                              </div>
                              <div className="text-xs text-cre-text">{pp.permit_type || '—'}</div>
                              <div className="text-[11px] text-cre-muted">Issued: {pp.issue_date || '—'} · Final: {pp.final_date || '—'}</div>
                            </div>
                          ))}
                          {selectedPermits.length > 10 ? <div className="text-[11px] text-cre-muted">Showing first 10 permits.</div> : null}
                        </div>
                      ) : (
                        <div className="mt-2 text-sm text-cre-muted">No permits found.</div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
