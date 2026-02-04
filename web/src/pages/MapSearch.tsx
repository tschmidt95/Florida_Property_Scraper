import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import L from 'leaflet';
import type { LatLngLiteral } from 'leaflet';
import { CircleMarker, GeoJSON, MapContainer, Marker, TileLayer, useMap } from 'react-leaflet';

import {
  apiFetch,
  evaluateTriggers,
  fetchProviderStatus,
  createSavedSearch,
  listAlerts,
  listSavedSearches,
  markAlertRead,
  parcelsEnrich,
  runEnrichment,
  parcelsGeometry,
  parcelsSearchNormalized,
  fetchParcelDetail,
  fetchSourceCoverage,
  permitsByParcel,
  runSavedSearch,
  resolveCountyAuto,
  type SourceCoverage,
  triggersByParcel,
  triggersRollupByParcel,
  triggersRollupsSearch,
  type AlertsInboxRecord,
  type EnrichmentResponse,
  type ProviderStatusResponse,
  type TriggerEvaluateResponse,
  type ParcelAttributeFilters,
  type ParcelDetail,
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

type OwnerEnrichmentResponse = {
  county: string;
  parcel_id: string;
  pa_parcel_id?: string | null;
  owner_name?: string | null;
  owner_mailing_address?: string | null;
  provider?: string | null;
  configured: boolean;
  status: string;
  phones: string[];
  emails: string[];
  cache_hit?: boolean;
};

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

        // IMPORTANT: disable draw mode after creating geometry so marker clicks work
        try {
          const tb = (drawControlRef.current as any)?._toolbars?.draw;
          const poly = tb?._modes?.polygon?.handler;
          const rect = tb?._modes?.rectangle?.handler;
          const circ = tb?._modes?.circle?.handler;
          poly?.disable?.();
          rect?.disable?.();
          circ?.disable?.();
        } catch {}
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
            {options.length === 0 ? (
              <div className="text-xs text-cre-muted">No options (field not available).</div>
            ) : filtered.length ? (
              <div className="space-y-1">
                {filtered.slice(0, 250).map((o) => (
                  <label
                    key={o}
                    className="flex cursor-pointer select-none items-center gap-2 rounded-md px-1 py-1 text-xs text-cre-text hover:bg-cre-bg"
                    title={render(o)}
                  >
                    <input type="checkbox" checked={selectedSet.has(o)} onChange={() => toggle(o)} />
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
  // Ownership (local PA-derived signals)
  { key: 'absentee_owner', label: 'Absentee owner', group: 'Ownership', tier: 'strong' },
  { key: 'homestead', label: 'Homestead', group: 'Ownership', tier: 'support' },

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

const ENABLED_SIGNAL_KEYS_DEFAULT = new Set(SIGNALS_CATALOG.filter((x) => !x.comingSoon).map((x) => x.key));

export default function MapSearch({
  onMapStatus,
  backendOk,
  backendError,
}: {
  onMapStatus?: (status: MapStatus) => void;
  backendOk?: boolean;
  backendError?: string | null;
}) {
  const [county, setCounty] = useState('');
  const [drawnPolygon, setDrawnPolygon] = useState<GeoJSON.Polygon | null>(null);
  const [drawnCircle, setDrawnCircle] = useState<DrawnCircle | null>(null);
  const [polygonMatchMode, setPolygonMatchMode] = useState<'intersects' | 'centroid_inside' | 'contains'>('intersects');

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

  const [selectedParcelDetail, setSelectedParcelDetail] = useState<ParcelDetail | null>(null);
  const [selectedParcelDetailLoading, setSelectedParcelDetailLoading] = useState(false);
  const [selectedParcelDetailError, setSelectedParcelDetailError] = useState<string | null>(null);

  const [ownerEnrichment, setOwnerEnrichment] = useState<OwnerEnrichmentResponse | null>(null);
  const [ownerEnrichmentLoading, setOwnerEnrichmentLoading] = useState(false);
  const [ownerEnrichmentError, setOwnerEnrichmentError] = useState<string | null>(null);

  const [triggerLookupParcelId, setTriggerLookupParcelId] = useState('');

  const [signalsDrawerOpen, setSignalsDrawerOpen] = useState(false);

  const [backendStatus, setBackendStatus] = useState<'checking' | 'ok' | 'down'>('checking');
  const [backendStatusDetail, setBackendStatusDetail] = useState<string>('');

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
    propertyTypeMode: 'contains' | 'equals';
    zoning: string;
    zoningMatch: 'contains' | 'equals';
    futureLandUse: string;
    futureLandUseMatch: 'contains' | 'equals';
    minValue: string;
    maxValue: string;
    minLandValue: string;
    maxLandValue: string;
    minBuildingValue: string;
    maxBuildingValue: string;
    lastSaleStart: string;
    lastSaleEnd: string;
    minOwnershipYears: string;
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
    propertyTypeMode: 'contains',
    zoning: '',
    zoningMatch: 'contains',
    futureLandUse: '',
    futureLandUseMatch: 'contains',
    minValue: '',
    maxValue: '',
    minLandValue: '',
    maxLandValue: '',
    minBuildingValue: '',
    maxBuildingValue: '',
    lastSaleStart: '',
    lastSaleEnd: '',
    minOwnershipYears: '',
  };

  const [filterForm, setFilterForm] = useState<FilterForm>(emptyFilterForm);

  const [sortKey, setSortKey] = useState<
    'relevance' | 'last_sale_date_desc' | 'year_built_desc' | 'sqft_desc'
  >('relevance');

  const [lastCounts, setLastCounts] = useState<{
    candidateCount: number | null;
    filteredCount: number | null;
  } | null>(null);
  const [hoverFieldsMode, setHoverFieldsMode] = useState<string>('');

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
  const [lastResponseRaw, setLastResponseRaw] = useState<any | null>(null);
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
  const [lastExplainError, setLastExplainError] = useState<any | null>(null);
  const [softWarnings, setSoftWarnings] = useState<string[]>([]);
  const [activeFiltersSummary, setActiveFiltersSummary] = useState<string>('None');
  const [activeSignalsSummary, setActiveSignalsSummary] = useState<string>('None');
  const [lastEnrichment, setLastEnrichment] = useState<EnrichmentResponse | null>(null);
  const [lastTriggerEval, setLastTriggerEval] = useState<TriggerEvaluateResponse | null>(null);

  const [pagingMeta, setPagingMeta] = useState<{
    total: number | null;
    loaded: number;
    isPaging: boolean;
    hasMore: boolean;
  }>({ total: null, loaded: 0, isPaging: false, hasMore: false });

  const [resultsQuery, setResultsQuery] = useState('');

  const [toast, setToast] = useState<string | null>(null);

  const backendUnavailable = backendOk === false;
  const [sourceCoverage, setSourceCoverage] = useState<SourceCoverage | null>(null);
  const [sourceCoverageLoading, setSourceCoverageLoading] = useState(false);
  const [sourceCoverageError, setSourceCoverageError] = useState<string | null>(null);
  const [providerStatus, setProviderStatus] = useState<ProviderStatusResponse | null>(null);
  const [providerStatusLoading, setProviderStatusLoading] = useState(false);
  const [providerStatusError, setProviderStatusError] = useState<string | null>(null);

  const resolveCountyFromGeometry = useCallback(async (): Promise<string> => {
    const existing = county.trim();
    if (existing) return existing;
    const poly = drawnPolygonRef.current ?? drawnPolygon;
    const circle = drawnCircleRef.current ?? drawnCircle;
    if (!poly && !circle) return '';
    try {
      const payload = poly
        ? { polygon_geojson: poly }
        : { center: circle?.center, radius_m: circle?.radius_m };
      const resp = await resolveCountyAuto(payload as any);
      const counties = Array.isArray(resp?.counties) ? resp.counties : [];
      if (counties.length > 1) {
        return '';
      }
      const resolved = String(resp?.county || '').trim().toLowerCase();
      if (resolved) {
        setCounty(resolved);
        return resolved;
      }
    } catch {
      // ignore
    }
    return '';
  }, [county, drawnPolygon, drawnCircle]);

  const [signalsCatalogOverride, setSignalsCatalogOverride] = useState<SignalCatalogItem[] | null>(null);
  const liveSignalKeys = useMemo(
    () => new Set((sourceCoverage?.available_signal_keys || []) as string[]),
    [sourceCoverage]
  );
  const supportedSignalsCatalog = useMemo(() => {
    const base = signalsCatalogOverride || SIGNALS_CATALOG;
    const enforceLive = liveSignalKeys.size > 0;
    return base.map((it) => ({
      ...it,
      comingSoon: Boolean(it.comingSoon) || (enforceLive && !liveSignalKeys.has(it.key)),
    }));
  }, [signalsCatalogOverride, liveSignalKeys]);
  const signalCatalogKeysSet = useMemo(
    () => new Set(supportedSignalsCatalog.map((x) => x.key)),
    [supportedSignalsCatalog]
  );
  const enabledSignalKeys = useMemo(() => {
    if (liveSignalKeys.size > 0) return liveSignalKeys;
    if (signalsCatalogOverride) {
      return new Set(
        signalsCatalogOverride
          .filter((x) => !x.comingSoon)
          .map((x) => x.key)
      );
    }
    return ENABLED_SIGNAL_KEYS_DEFAULT;
  }, [liveSignalKeys, signalsCatalogOverride]);

  const signalCatalogByGroup = useMemo(() => {
    const m = new Map<string, SignalCatalogItem[]>();
    for (const it of supportedSignalsCatalog) {
      const arr = m.get(it.group) || [];
      arr.push(it);
      m.set(it.group, arr);
    }
    for (const [g, items] of m.entries()) {
      items.sort((a, b) => a.label.localeCompare(b.label));
      m.set(g, items);
    }
    return Array.from(m.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  }, [supportedSignalsCatalog]);

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
  const [fieldStats, setFieldStats] = useState<any | null>(null);
  const [resultSetCount, setResultSetCount] = useState(0);

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

  const computeResultSetFieldStats = useCallback((rows: Array<Partial<ParcelRecord>>): any => {
    const total = Array.isArray(rows) ? rows.length : 0;
    const present: Record<string, number> = {};
    const missing: Record<string, number> = {};
    const coverage: Record<string, number> = {};

    const keys = [
      'living_area_sqft',
      'lot_size_sqft',
      'lot_size_acres',
      'beds',
      'baths',
      'year_built',
      'ownership_years',
      'property_type',
      'zoning',
      'future_land_use',
      'total_value',
      'land_value',
      'building_value',
      'assessed_value',
      'last_sale_date',
    ];
    for (const k of keys) {
      present[k] = 0;
      missing[k] = 0;
      coverage[k] = 0;
    }
    if (!total) return { total, present, missing, coverage };

    const hasValue = (val: unknown): boolean => {
      if (val === null || val === undefined) return false;
      if (typeof val === 'number') return Number.isFinite(val) && val > 0;
      if (typeof val === 'string') return val.trim().length > 0;
      if (Array.isArray(val)) return val.length > 0;
      return Boolean(val);
    };

    for (const r of rows) {
      const livingSqft =
        (r as any)?.living_area_sqft ??
        (Array.isArray((r as any)?.sqft)
          ? (r as any).sqft.find((s: any) => s?.type === 'living')?.value
          : null);
      const lotSqft =
        (r as any)?.lot_size_sqft ??
        (Array.isArray((r as any)?.sqft)
          ? (r as any).sqft.find((s: any) => s?.type === 'lot')?.value
          : null);
      const lotAcres = (r as any)?.lot_size_acres ?? null;
      const propertyType =
        (r as any)?.property_type ?? (r as any)?.property_type_raw ?? (r as any)?.land_use ?? null;

      const values: Record<string, unknown> = {
        living_area_sqft: livingSqft,
        lot_size_sqft: lotSqft,
        lot_size_acres: lotAcres,
        beds: (r as any)?.beds,
        baths: (r as any)?.baths,
        year_built: (r as any)?.year_built,
        ownership_years: (r as any)?.ownership_years,
        property_type: propertyType,
        zoning: (r as any)?.zoning,
        future_land_use: (r as any)?.future_land_use,
        total_value: (r as any)?.total_value ?? (r as any)?.just_value,
        land_value: (r as any)?.land_value,
        building_value: (r as any)?.building_value,
        assessed_value: (r as any)?.assessed_value,
        last_sale_date: (r as any)?.last_sale_date,
      };

      for (const k of keys) {
        if (hasValue(values[k])) present[k] += 1;
        else missing[k] += 1;
      }
    }

    for (const k of keys) {
      coverage[k] = total ? present[k] / total : 0;
    }

    return { total, present, missing, coverage };
  }, []);

  const fieldAvailability = useMemo(() => {
    const present = (fieldStats?.present || {}) as Record<string, number>;
    const has = (key: string) => {
      if (resultSetCount <= 0) return true;
      return Number(present[key] || 0) > 0;
    };
    return {
      living_area_sqft: has('living_area_sqft'),
      lot_size_sqft: has('lot_size_sqft'),
      lot_size_acres: has('lot_size_acres'),
      beds: has('beds'),
      baths: has('baths'),
      year_built: has('year_built'),
      total_value: has('total_value'),
      land_value: has('land_value'),
      building_value: has('building_value'),
      assessed_value: has('assessed_value'),
      last_sale_date: has('last_sale_date'),
      property_type: has('property_type'),
      zoning: has('zoning'),
      future_land_use: has('future_land_use'),
    };
  }, [fieldStats, resultSetCount]);

  const lotSizeAvailable = fieldAvailability.lot_size_sqft || fieldAvailability.lot_size_acres;

  const fieldCoverageNote = useCallback(
    (fieldKey: string, hint: string) => {
      if (resultSetCount <= 0) {
        return (
          <div className="text-[10px] text-cre-muted" title={`${hint} (No records yet)`}>
            —
          </div>
        );
      }
      const cov = (fieldStats?.coverage || {}) as Record<string, number>;
      const present = (fieldStats?.present || {}) as Record<string, number>;
      const missing = (fieldStats?.missing || {}) as Record<string, number>;
      const candidateFields = (fieldStats?.coverage_candidates as any)?.fields || null;
      const candidateRow = candidateFields && typeof candidateFields === 'object' ? candidateFields[fieldKey] : null;
      const raw = cov[fieldKey];
      const pct = typeof raw === 'number' ? Math.round(raw * 100) : 0;
      const count =
        typeof present[fieldKey] === 'number' && typeof missing[fieldKey] === 'number'
          ? ` (${present[fieldKey]}/${present[fieldKey] + missing[fieldKey]})`
          : '';
      let label = `Coverage: ${pct}%${count}`;
      if (filtersActive && candidateRow && typeof candidateRow.pct === 'number') {
        const candPct = Math.round(candidateRow.pct * 100);
        const candCount =
          typeof candidateRow.present === 'number' && typeof candidateRow.total === 'number'
            ? ` (${candidateRow.present}/${candidateRow.total})`
            : '';
        label = `Coverage (returned): ${pct}%${count} · Candidates: ${candPct}%${candCount}`;
      }
      return (
        <div className="text-[10px] text-cre-muted" title={hint}>
          {label}
        </div>
      );
    },
    [fieldStats, filtersActive, resultSetCount]
  );

  const downloadCsv = useCallback(() => {
    const data = resultsQuery.trim() ? visibleRows || [] : rows || [];
    if (!data.length) {
      setToast('No results to download yet.');
      return;
    }
    const header = [
      'parcel_id',
      'county',
      'address',
      'owner_name',
      'owner_mailing_address',
      'beds',
      'baths',
      'year_built',
      'living_area_sqft',
      'lot_size_sqft',
      'lot_size_acres',
      'zoning',
      'future_land_use',
      'land_value',
      'building_value',
      'total_value',
      'assessed_value',
      'taxable_value',
      'last_sale_date',
      'last_sale_price',
      'source',
      'signal_keys',
      'seller_score',
      'trigger_keys',
    ];
    const escapeCsv = (val: unknown) => {
      const s = String(val ?? '').replace(/\r?\n/g, ' ').trim();
      if (s.includes(',') || s.includes('"') || s.includes('\n')) {
        return `"${s.replace(/"/g, '""')}"`;
      }
      return s;
    };
    const lines = [header.join(',')];
    for (const p of data) {
      const rec = recordById.get(p.parcel_id);
      const rollup = (rec as any)?.rollup || null;
      const signalKeys = Array.isArray((rec as any)?.signal_keys) ? (rec as any)?.signal_keys : [];
      const rollupKeys = Array.isArray(rollup?.trigger_keys) ? rollup.trigger_keys : [];
      lines.push([
        escapeCsv(p.parcel_id),
        escapeCsv(p.county || rec?.county || county),
        escapeCsv(p.address || rec?.situs_address || rec?.address || ''),
        escapeCsv(p.owner_name || rec?.owner_name || ''),
        escapeCsv((rec as any)?.owner_mailing_address || ''),
        escapeCsv(rec?.beds ?? ''),
        escapeCsv(rec?.baths ?? ''),
        escapeCsv(rec?.year_built ?? ''),
        escapeCsv(rec?.living_area_sqft ?? ''),
        escapeCsv(rec?.lot_size_sqft ?? ''),
        escapeCsv(rec?.lot_size_acres ?? ''),
        escapeCsv(rec?.zoning ?? ''),
        escapeCsv(rec?.future_land_use ?? ''),
        escapeCsv(rec?.land_value ?? ''),
        escapeCsv(rec?.building_value ?? ''),
        escapeCsv(rec?.total_value ?? ''),
        escapeCsv(rec?.assessed_value ?? ''),
        escapeCsv(rec?.taxable_value ?? ''),
        escapeCsv(rec?.last_sale_date ?? ''),
        escapeCsv(rec?.last_sale_price ?? ''),
        escapeCsv(rec?.source ?? p.source ?? ''),
        escapeCsv(signalKeys.join('|')),
        escapeCsv(typeof rollup?.seller_score === 'number' ? rollup.seller_score : ''),
        escapeCsv(rollupKeys.join('|')),
      ].join(','));
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const label = county.trim() || 'all';
    a.download = `parcels_${label}_${Date.now()}.csv`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 500);
  }, [visibleRows, rows, resultsQuery, county, recordById]);

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

  useEffect(() => {
    let cancelled = false;
    async function loadSignalsCatalog() {
      try {
        const resp = await apiFetch('/api/signals/catalog', { headers: { Accept: 'application/json' } });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        const items = Array.isArray((data as any)?.signals) ? (data as any).signals : null;
        if (!items || !Array.isArray(items)) return;
        const mapped: SignalCatalogItem[] = items
          .map((it: any) => ({
            key: String(it?.key || ''),
            label: String(it?.label || it?.key || ''),
            group: String(it?.group || 'Signals'),
            tier: (String(it?.tier || 'info') as any) || 'info',
            comingSoon: Boolean(it?.coming_soon) || Boolean(it?.comingSoon) || Boolean(it?.implemented === false),
          }))
          .filter((it: SignalCatalogItem) => it.key && it.label);
        if (!cancelled && mapped.length) setSignalsCatalogOverride(mapped);
      } catch {
        // ignore
      }
    }
    void loadSignalsCatalog();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!drawnPolygon && !drawnCircle) return;
    void resolveCountyFromGeometry();
  }, [drawnCircle, drawnPolygon, resolveCountyFromGeometry]);

  useEffect(() => {
    let cancelled = false;
    async function loadOwnerEnrichment() {
      if (!selectedParcelId) {
        setOwnerEnrichment(null);
        setOwnerEnrichmentError(null);
        return;
      }
      const rec = records.find((r) => r.parcel_id === selectedParcelId);
      const list = parcels.find((p) => p.parcel_id === selectedParcelId);
      const countyKey = (rec?.county || list?.county || county || '').trim().toLowerCase();
      if (!countyKey) return;
      setOwnerEnrichmentLoading(true);
      setOwnerEnrichmentError(null);
      try {
        const resp = await apiFetch(
          `/api/owners/enrich?county=${encodeURIComponent(countyKey)}&parcel_id=${encodeURIComponent(
            selectedParcelId,
          )}`,
          { headers: { Accept: 'application/json' } },
        );
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = (await resp.json()) as OwnerEnrichmentResponse;
        if (!cancelled) setOwnerEnrichment(data);
      } catch (e) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        setOwnerEnrichmentError(msg);
      } finally {
        if (!cancelled) setOwnerEnrichmentLoading(false);
      }
    }

    void loadOwnerEnrichment();
    return () => {
      cancelled = true;
    };
  }, [selectedParcelId, records, parcels, county]);

  useEffect(() => {
    let cancelled = false;
    async function loadCoverage() {
      let c = county.trim();
      if (!c) {
        c = await resolveCountyFromGeometry();
      }
      if (!c) {
        if (!cancelled) {
          setSourceCoverage(null);
          setSourceCoverageError('County auto-detection required to load coverage.');
        }
        return;
      }
      setSourceCoverageLoading(true);
      try {
        const data = await fetchSourceCoverage(c);
        if (!cancelled) {
          setSourceCoverage(data);
          setSourceCoverageError(null);
        }
      } catch (e) {
        if (!cancelled) {
          setSourceCoverage(null);
          setSourceCoverageError('Failed to load coverage.');
        }
      } finally {
        if (!cancelled) setSourceCoverageLoading(false);
      }
    }
    void loadCoverage();
    return () => {
      cancelled = true;
    };
  }, [county, resolveCountyFromGeometry]);

  useEffect(() => {
    let cancelled = false;
    async function loadProviderStatus() {
      let c = county.trim();
      if (!c) {
        c = await resolveCountyFromGeometry();
      }
      if (!c) {
        if (!cancelled) {
          setProviderStatus(null);
          setProviderStatusError('County auto-detection required to load providers.');
        }
        return;
      }
      setProviderStatusLoading(true);
      try {
        const data = await fetchProviderStatus(c);
        if (!cancelled) {
          setProviderStatus(data);
          setProviderStatusError(null);
        }
      } catch (e) {
        if (!cancelled) {
          setProviderStatus(null);
          setProviderStatusError('Failed to load provider status.');
        }
      } finally {
        if (!cancelled) setProviderStatusLoading(false);
      }
    }
    void loadProviderStatus();
    return () => {
      cancelled = true;
    };
  }, [county, resolveCountyFromGeometry]);

  const refreshSavedSearches = useCallback(
    async (nextCounty?: string) => {
      let c = (nextCounty ?? county).trim();
      if (!c) {
        c = await resolveCountyFromGeometry();
      }
      if (!c) {
        setSavedSearches([]);
        setSelectedSavedSearchId('');
        setSavedSearchesError('Select a county to use saved searches.');
        setSavedSearchesLoading(false);
        return;
      }
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
    [county, resolveCountyFromGeometry]
  );

  const refreshAlerts = useCallback(
    async (nextSavedSearchId?: string, nextStatus?: string) => {
      let c = county.trim();
      if (!c) {
        c = await resolveCountyFromGeometry();
      }
      if (!c) {
        setAlertsInbox([]);
        setAlertsError('Select a county to view alerts.');
        return;
      }
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
          county: c,
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
    [alertsStatus, county, selectedSavedSearchId, resolveCountyFromGeometry]
  );

  useEffect(() => {
    void refreshSavedSearches(county);
  }, [county, refreshSavedSearches]);

  useEffect(() => {
    void refreshAlerts(selectedSavedSearchId, alertsStatus);
  }, [alertsStatus, county, refreshAlerts, selectedSavedSearchId]);

  async function runSelectedSavedSearch() {
    let c = county.trim();
    if (!c) {
      c = await resolveCountyFromGeometry();
    }
    if (!c) {
      showToast('Select a county to run saved searches.');
      return;
    }
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
      await refreshSavedSearches(c);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      showToast(`Run failed: ${msg}`);
    }
  }

  async function saveCurrentSearch() {
    let c = county.trim();
    if (!c) {
      c = await resolveCountyFromGeometry();
    }
    if (!c) {
      showToast('Select a county to save searches.');
      return;
    }
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

    const defaultName = `${c.toUpperCase()} Saved Search`;
    const name = (typeof window !== 'undefined' ? window.prompt('Saved Search name', defaultName) : defaultName) || defaultName;

    try {
      const payload = built.payload as any;
      const filters = (payload?.filters && typeof payload.filters === 'object') ? payload.filters : {};
      const sort = typeof payload?.sort === 'string' ? payload.sort : null;
      const ss = await createSavedSearch({
        name,
        county: c,
        geometry: poly as any,
        filters,
        enrich: false,
        sort,
      });
      showToast('Saved search created.');
      await refreshSavedSearches(c);
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
    const controller = new AbortController();

    async function loadSelectedDetails(parcelId: string) {
      let selectedCounty =
        (parcels.find((p) => p.parcel_id === parcelId)?.county ||
          records.find((r) => r.parcel_id === parcelId)?.county ||
          county ||
          '')
          .toString()
          .trim();
      if (!selectedCounty) {
        selectedCounty = await resolveCountyFromGeometry();
      }
      if (!selectedCounty) {
        setSelectedPermitsError('Select a county to load parcel signals.');
        setSelectedTriggersError('Select a county to load parcel signals.');
        setSelectedRollupError('Select a county to load parcel signals.');
        return;
      }
      setTriggerLookupParcelId(parcelId);
      setSelectedPermitsError(null);
      setSelectedPermitsLoading(true);
      try {
        const permits = await permitsByParcel({ county: selectedCounty, parcel_id: parcelId, limit: 200 });
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
          county: selectedCounty,
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
        const rollup = await triggersRollupByParcel({ county: selectedCounty, parcel_id: parcelId });
        if (cancelled) return;
        if (rollup) {
          setSelectedRollup(rollup);
          setSelectedRollupError(null);
        } else {
          setSelectedRollup(null);
          setSelectedRollupError(null);
        }
      } catch (e) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        // Rollup can be absent if rollups haven't been rebuilt yet.
        setSelectedRollup(null);
        setSelectedRollupError(msg);
      } finally {
        if (!cancelled) setSelectedRollupLoading(false);
      }

    }

    async function loadSelectedParcelDetail(parcelId: string) {
      const selectedCounty =
        (parcels.find((p) => p.parcel_id === parcelId)?.county ||
          records.find((r) => r.parcel_id === parcelId)?.county ||
          county ||
          '')
          .toString()
          .trim();
      let resolvedCounty = selectedCounty;
      if (!resolvedCounty) {
        resolvedCounty = await resolveCountyFromGeometry();
      }
      if (!resolvedCounty) {
        setSelectedParcelDetailError('Select a county to load parcel details.');
        setSelectedParcelDetail(null);
        return;
      }
      setSelectedParcelDetail(null);
      setSelectedParcelDetailError(null);
      setSelectedParcelDetailLoading(true);
      try {
        const detail = await fetchParcelDetail({
          parcel_id: parcelId,
          county: resolvedCounty,
          include_geometry: true,
          signal: controller.signal,
        });
        if (cancelled) return;
        setSelectedParcelDetail(detail);
      } catch (e: any) {
        if (cancelled) return;
        if (e?.name === 'AbortError') return;
        const msg = e instanceof Error ? e.message : String(e);
        setSelectedParcelDetailError(msg || 'parcel_detail_failed');
        setSelectedParcelDetail(null);
      } finally {
        if (!cancelled) setSelectedParcelDetailLoading(false);
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
      setSelectedParcelDetail(null);
      setSelectedParcelDetailLoading(false);
      setSelectedParcelDetailError(null);
      return () => {
        cancelled = true;
        controller.abort();
      };
    }

    void loadSelectedParcelDetail(selectedParcelId);
    void loadSelectedDetails(selectedParcelId);
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [county, resolveCountyFromGeometry, selectedParcelId]);

  useEffect(() => {
    let cancelled = false;

    async function loadParcelLines() {
      if (!parcelLinesEnabled) return;
      setParcelLinesError(null);

      const uniqueCounties = Array.from(
        new Set(parcels.map((p) => (p?.county || '').trim()).filter(Boolean))
      );
      let countyForLines = county.trim() || (uniqueCounties.length === 1 ? uniqueCounties[0] : '');
      if (!countyForLines) {
        countyForLines = await resolveCountyFromGeometry();
      }
      if (!countyForLines) {
        setParcelLinesFC(null);
        setParcelLinesFeatureCount(0);
        setParcelLinesStatus('empty');
        setParcelLinesError('Select a county to load parcel geometry.');
        return;
      }

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
        const fc = await parcelsGeometry({ county: countyForLines, parcel_ids: ids });
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
  }, [county, drawnCircle, drawnPolygon, parcelLinesEnabled, parcels, resolveCountyFromGeometry]);

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

    const filters0: ParcelAttributeFilters & { min_ownership_years?: number | null } = {
      min_sqft: toFloatOrNull(filterForm.minSqft),
      max_sqft: toFloatOrNull(filterForm.maxSqft),
      missing_policy: null,
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
      min_ownership_years: toIntOrNull(filterForm.minOwnershipYears),
    };

    if (filterForm.propertyTypeMode === 'equals' && filterForm.propertyType.trim()) {
      (filters0 as any).property_type = [filterForm.propertyType.trim()];
    }

    if (filterForm.zoningMatch === 'equals') {
      const z = filterForm.zoning.trim();
      if (z) {
        (filters0 as any).zoning = null;
        (filters0 as any).zoning_in = [z];
      }
    }

    if (filterForm.futureLandUseMatch === 'equals') {
      const flu = filterForm.futureLandUse.trim();
      if (flu) {
        (filters0 as any).future_land_use_in = [flu];
      }
    } else if (filterForm.futureLandUseMatch === 'contains') {
      const flu = filterForm.futureLandUse.trim();
      if (flu) {
        (filters0 as any).future_land_use = flu;
      }
    }

    // IMPORTANT: omit blank filter keys entirely so "blank" never restricts results.
    const filters: any = {};
    for (const [k, v] of Object.entries(filters0 as any)) {
      if (v === null || v === undefined) continue;
      if (typeof v === 'string' && !v.trim()) continue;
      if (Array.isArray(v) && v.length === 0) continue;
      filters[k] = v;
    }
    const hasAnyFilters = Object.keys(filters).length > 0;
    if (hasAnyFilters) {
      filters.missing_policy = 'lenient';
    }

    const resolvedCounty = county.trim();
    const countyLocked = false;
    const payload: any = {
      limit: 500,
      include_geometry: false,
      filters: hasAnyFilters ? filters : undefined,
      sort: sortKey,
      explain: true,
    };
    payload.polygon_match_mode = polygonMatchMode;
    if (resolvedCounty && countyLocked) payload.county = resolvedCounty;

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

  const summarizeFilters = useCallback((filters: any): string => {
    if (!filters || typeof filters !== 'object') return 'None';
    const parts: string[] = [];
    const push = (label: string, v: unknown) => {
      if (v === null || v === undefined) return;
      if (typeof v === 'string' && !v.trim()) return;
      if (Array.isArray(v) && !v.length) return;
      parts.push(`${label}: ${Array.isArray(v) ? v.join(', ') : v}`);
    };
    push('min_sqft', filters.min_sqft);
    push('max_sqft', filters.max_sqft);
    push('min_acres', filters.min_acres);
    push('max_acres', filters.max_acres);
    push('min_lot_size_sqft', filters.min_lot_size_sqft);
    push('max_lot_size_sqft', filters.max_lot_size_sqft);
    push('min_beds', filters.min_beds);
    push('min_baths', filters.min_baths);
    push('min_year_built', filters.min_year_built);
    push('max_year_built', filters.max_year_built);
    push('property_type', filters.property_type);
    push('zoning', filters.zoning);
    push('zoning_in', filters.zoning_in);
    push('future_land_use_in', filters.future_land_use_in);
    push('min_value', filters.min_value);
    push('max_value', filters.max_value);
    push('min_land_value', filters.min_land_value);
    push('max_land_value', filters.max_land_value);
    push('min_building_value', filters.min_building_value);
    push('max_building_value', filters.max_building_value);
    push('last_sale_date_start', filters.last_sale_date_start);
    push('last_sale_date_end', filters.last_sale_date_end);
    return parts.length ? parts.join(' · ') : 'None';
  }, []);

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

    const uniqueCounties = Array.from(new Set(parcels.map((p) => (p?.county || '').trim()).filter(Boolean)));
    let countyForEnrich = county.trim() || (uniqueCounties.length === 1 ? uniqueCounties[0] : '');
    if (!countyForEnrich) {
      countyForEnrich = await resolveCountyFromGeometry();
    }
    if (!countyForEnrich) {
      setErrorBanner('Select a county to enrich results.');
      return;
    }

    const ids = parcels.map((p) => p.parcel_id)
      .slice(0, 150);

    if (!ids.length) return;

    if (backendUnavailable) {
      setErrorBanner(backendError || 'Backend unavailable.');
      return;
    }
    setLoading(true);
    const reqId = ++activeReq.current;
    try {
      const resp = await parcelsEnrich({ county: countyForEnrich, parcel_ids: ids, limit: ids.length, max_per_minute: 30 });
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
      const fetchedOk = Number((resp as any).fetched_ok || 0);
      const cached = Number((resp as any).cached || 0);
      const failed = Number((resp as any).fetched_failed || 0);
      setErrorBanner(
        `Enrichment complete (cached=${cached}, fetched_ok=${fetchedOk}, failed=${failed}).`,
      );
    } catch (e) {
      if (reqId !== activeReq.current) return;
      const msg = e instanceof Error ? e.message : String(e);
      setLastError(msg);
      if (/HTTP\s+404|HTTP\s+501|Not Found/i.test(msg)) {
        setErrorBanner('Enrichment not implemented on backend yet.');
      } else {
        setErrorBanner(`Enrichment failed: ${msg}`);
      }
    } finally {
      if (reqId === activeReq.current) setLoading(false);
    }
  }

  async function enrichSelectedProviders() {
    setLastError(null);
    setErrorBanner(null);
    const ids = selectedParcelId ? [selectedParcelId] : visibleRows.map((p) => p.parcel_id).slice(0, 50);
    if (!ids.length) {
      setErrorBanner('Select a parcel or load results before enrichment.');
      return;
    }

    let countyForEnrich = county.trim() || (ids.length === 1 ? (records.find((r) => r.parcel_id === ids[0])?.county || '') : '');
    if (!countyForEnrich) {
      countyForEnrich = await resolveCountyFromGeometry();
    }
    if (!countyForEnrich) {
      setErrorBanner('Select a county to enrich results.');
      return;
    }

    setLoading(true);
    try {
      const resp = await runEnrichment({
        county: countyForEnrich,
        parcel_ids: ids,
        providers: [
          'permits',
          'tax',
          'courts',
          'official_records',
          'deeds',
          'code_enforcement',
          'liens',
          'utilities',
        ],
        limit: ids.length,
      });
      setLastEnrichment(resp);
      setErrorBanner(`Enrichment complete (${resp.results.length} provider results).`);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (/HTTP\s+404|HTTP\s+501|Not Found/i.test(msg)) {
        setErrorBanner('Provider enrichment not implemented on backend yet.');
      } else {
        setErrorBanner(`Enrichment failed: ${msg}`);
      }
    } finally {
      setLoading(false);
    }
  }

  async function runTriggersForSelected() {
    setLastError(null);
    setErrorBanner(null);
    const ids = selectedParcelId ? [selectedParcelId] : visibleRows.map((p) => p.parcel_id).slice(0, 50);
    if (!ids.length) {
      setErrorBanner('Select a parcel or load results before running triggers.');
      return;
    }

    let countyForEval = county.trim() || (ids.length === 1 ? (records.find((r) => r.parcel_id === ids[0])?.county || '') : '');
    if (!countyForEval) {
      countyForEval = await resolveCountyFromGeometry();
    }
    if (!countyForEval) {
      setErrorBanner('Select a county to evaluate triggers.');
      return;
    }

    setLoading(true);
    try {
      const resp = await evaluateTriggers({ county: countyForEval, parcel_ids: ids, trigger_keys: null });
      setLastTriggerEval(resp);
      setErrorBanner(`Triggers evaluated (${resp.results.length} parcels).`);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (/HTTP\s+404|HTTP\s+501|Not Found/i.test(msg)) {
        setErrorBanner('Trigger evaluation not implemented on backend yet.');
      } else {
        setErrorBanner(`Trigger evaluation failed: ${msg}`);
      }
    } finally {
      setLoading(false);
    }
  }

  async function run() {
    setLastError(null);
    setLastExplainError(null);
    setSoftWarnings([]);
    setErrorBanner(null);
    setRollupsError(null);
    setRollupsLastSummary(null);

    const hasGeometry = Boolean(
      drawnPolygonRef.current || drawnCircleRef.current || drawnPolygon || drawnCircle,
    );
    if (!hasGeometry && resultsQuery.trim()) {
      setErrorBanner('Text filter applied to currently loaded results.');
      return;
    }

    const built = buildSearchPayloadForMap();
    if ('error' in built) {
      setErrorBanner(built.error);
      setParcels([]);
      setRecords([]);
      return;
    }

    const payload = built.payload;
    const hadFilters = !!(payload as any)?.filters;

    if (!county.trim()) {
      try {
        const resolved = await resolveCountyFromGeometry();
        if (resolved) {
          (payload as any).county = resolved;
        }
      } catch {
        // ignore
      }
    }

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
      (rollupsKeys.length > 0 || rollupsAnyGroups.length > 0 || rollupsTiers.length > 0 || rollupsMinScoreN !== null);

    (payload as any).trigger_groups = rollupsAnyGroups;
    (payload as any).trigger_tiers = rollupsTiers;
    if (rollupsMinScoreN !== null) {
      (payload as any).trigger_min_score = rollupsMinScoreN;
    }

    let countyForRollups = county.trim();
    if (rollupsEnabled && !countyForRollups) {
      try {
        const resolved = await resolveCountyFromGeometry();
        if (resolved) {
          countyForRollups = resolved;
        }
      } catch {
        // ignore
      }
    }
    if (rollupsEnabled && !countyForRollups) {
      setRollupsError('Unable to resolve county for signals/rollups filters.');
      setErrorBanner('Unable to resolve county for signals/rollups filters.');
      setParcels([]);
      setRecords([]);
      return;
    }
    if (rollupsEnabled) {
      const parts: string[] = [];
      if (rollupsMinScoreN !== null) parts.push(`min score: ${rollupsMinScoreN}`);
      if (rollupsAnyGroups.length) parts.push(`groups: ${rollupsAnyGroups.join(', ')}`);
      if (rollupsKeys.length) parts.push(`keys: ${rollupsKeys.join(', ')}`);
      if (rollupsTiers.length) parts.push(`tiers: ${rollupsTiers.join(', ')}`);
      setActiveSignalsSummary(parts.length ? parts.join(' · ') : 'None');
    } else {
      setActiveSignalsSummary('None');
    }

    if (rollupsEnabled && !rollupsActive) {
      setRollupsError('Select at least one trigger filter (group/tier/min score).');
    }

    if (rollupsActive) {
      setRollupsMap({});
      try {
        setRollupsLastSummary(null);
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
    const filterSummary = summarizeFilters((payload as any)?.filters);
    setActiveFiltersSummary(`${filterSummary}${filterSummary !== 'None' ? ' · ' : ''}polygon_match_mode: ${polygonMatchMode}`);

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
    setPagingMeta({ total: null, loaded: 0, isPaging: true, hasMore: false });

    const reqId = ++activeReq.current;
    try {
      const pageLimit = typeof (payload as any)?.limit === 'number' ? (payload as any).limit : 500;
      let cursor: string | null = null;
      let hasMore = true;
      let totalCount: number | null = null;
      let lastResp: any = null;
      const allRecords: ParcelRecord[] = [];
      const allParcels: ParcelSearchListItem[] = [];
      const seenRecords = new Set<string>();
      const seenParcels = new Set<string>();

      while (hasMore) {
        if (reqId !== activeReq.current) return;
        const pagePayload = { ...payload, limit: pageLimit, cursor } as any;
        const resp = await parcelsSearchNormalized(pagePayload);
        if (reqId !== activeReq.current) return;

        lastResp = resp;
        const recs = resp.records || [];
        const list = Array.isArray((resp as any).parcels) ? ((resp as any).parcels as ParcelSearchListItem[]) : [];

        for (const r of recs) {
          const pid = (r?.parcel_id || '').trim();
          const ckey = String((r as any)?.county || '').trim().toLowerCase();
          const key = ckey ? `${ckey}:${pid}` : pid;
          if (!pid || seenRecords.has(key)) continue;
          seenRecords.add(key);
          allRecords.push(r);
        }
        for (const p of list) {
          const pid = (p?.parcel_id || '').trim();
          const ckey = String((p as any)?.county || '').trim().toLowerCase();
          const key = ckey ? `${ckey}:${pid}` : pid;
          if (!pid || seenParcels.has(key)) continue;
          seenParcels.add(key);
          allParcels.push(p);
        }

        const respTotal = (resp as any).total_count ?? (resp as any)?.summary?.total_count ?? (resp as any)?.explain?.final_count;
        totalCount = Number.isFinite(Number(respTotal)) ? Number(respTotal) : totalCount;
        hasMore = Boolean((resp as any).has_more);
        cursor = (resp as any).next_cursor ?? null;
        if (hasMore && !cursor) {
          hasMore = false;
        }

        setPagingMeta({
          total: totalCount ?? allParcels.length,
          loaded: allParcels.length,
          isPaging: hasMore,
          hasMore,
        });

        if (!hasMore) break;
      }

      const resp = lastResp;
      if (!resp) {
        throw new Error('No response received from backend.');
      }

      setLastResponseRaw(resp);
      try {
        setHoverFieldsMode(String((resp as any).hover_fields_mode || '').trim());
      } catch {
        setHoverFieldsMode('');
      }

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

      const recs = allRecords;
      const list = allParcels;

      if (!county.trim()) {
        const unique = Array.from(new Set(list.map((p) => (p?.county || '').trim()).filter(Boolean)));
        if (unique.length === 1) {
          setCounty(unique[0]);
        }
      }

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
      const coverageRows: Array<Partial<ParcelRecord>> = recs.length
        ? recs
        : (list as Array<Partial<ParcelRecord>>);
      const backendFieldStats = (resp as any).field_stats;
      const fallbackFieldStats = computeResultSetFieldStats(coverageRows);
      const nextFieldStats = backendFieldStats && typeof backendFieldStats === 'object'
        ? backendFieldStats
        : fallbackFieldStats;
      setFieldStats(nextFieldStats);
      const nextResultCount = (list.length || recs.length) ?? 0;
      setResultSetCount(nextResultCount);

      const isSoftWarning = (w: string): boolean => {
        const s = String(w || '').toLowerCase();
        return s.includes('swapped date range');
      };

      const soft = Array.isArray(warningsAll) ? warningsAll.filter(isSoftWarning) : [];
      const otherWarnings = Array.isArray(warningsAll) ? warningsAll.filter((w) => !isSoftWarning(w)) : [];
      const sqftMin = (payload as any)?.filters?.min_sqft;
      const sqftMax = (payload as any)?.filters?.max_sqft;
      const needsSqft = sqftMin !== null && sqftMin !== undefined || sqftMax !== null && sqftMax !== undefined;
      const cov = (nextFieldStats as any)?.coverage || {};
      const sqftCoverage = typeof cov.living_area_sqft === 'number' ? cov.living_area_sqft : null;
      const lowSqftCoverage = Boolean(needsSqft && nextResultCount > 0 && sqftCoverage !== null && sqftCoverage < 0.2);
      const coverageWarning = lowSqftCoverage
        ? 'Most records missing sqft; consider lenient missing policy.'
        : null;
      setSoftWarnings(coverageWarning ? [...soft, coverageWarning] : soft);
      setLastResponseCount(list.length || recs.length);

      const rawCounts = resp.summary?.source_counts || {};
      setSourceCounts({
        live: Number(rawCounts.live || 0),
        cache: Number(rawCounts.cache || 0),
      });

      const markerEligible = list.filter((p) =>
        typeof p.lat === 'number' && Number.isFinite(p.lat) && typeof p.lng === 'number' && Number.isFinite(p.lng)
      );
      const markersRendered = markerEligible.length;
      const missingLatLngCount = Math.max(0, list.length - markersRendered);

      const explain = (resp as any).explain || null;
      const stageCounts = explain?.stage_counts || null;
      const droppedReasons = explain?.dropped_reasons || null;
      const missingFieldCounts = explain?.missing_field_counts || null;
      const markersPossibleCount =
        typeof explain?.markers_possible_count === 'number' ? explain.markers_possible_count : null;
      const filteredOutCount =
        stageCounts && typeof stageCounts.candidates === 'number' && typeof stageCounts.returned === 'number'
          ? Math.max(0, Number(stageCounts.candidates) - Number(stageCounts.returned))
          : candidateCount !== null && filteredCount !== null
            ? Math.max(0, Number(candidateCount) - Number(filteredCount))
            : null;

      try {
        const warnings = Array.isArray((resp as any).warnings) ? ((resp as any).warnings as string[]) : [];
        setLastResponseSummary({
          request_via: requestVia,
          search_id: (resp as any).search_id,
          records_count: recs.length,
          markers_rendered: markersRendered,
          markers_possible_count: markersPossibleCount ?? markersRendered,
          missing_latlng_count: missingLatLngCount,
          stage_counts: stageCounts,
          dropped_reasons: droppedReasons,
          missing_field_counts: missingFieldCounts,
          filtered_out_count: filteredOutCount,
          returned_count: list.length || recs.length,
          total_count: typeof (resp as any).total_count === 'number' ? (resp as any).total_count : totalCount,
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
        setFieldStats(null);
        setResultSetCount(0);
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
      setPagingMeta({ total: totalCount ?? list.length, loaded: list.length, isPaging: false, hasMore: false });
    } catch (e) {
      if (reqId !== activeReq.current) return;
      const payload = (e as any)?.payload ?? null;
      const status = (payload as any)?.http_status ?? (payload as any)?.status ?? (e as any)?.status;
      const statusText = (payload as any)?.status_text ?? (payload as any)?.statusText ?? (e as any)?.statusText;
      const responseText =
        (payload as any)?.response_text_snippet ??
        (payload as any)?.responseTextSnippet ??
        (payload as any)?.responseText ??
        (e as any)?.responseText;
      const baseMsg = e instanceof Error ? e.message : String(e);
      const statusLine = status ? `HTTP ${status}${statusText ? ` ${statusText}` : ''}` : '';
      const detail = responseText ? String(responseText).slice(0, 300) : '';
      const msg = [statusLine, baseMsg, detail ? `(${detail})` : ''].filter(Boolean).join(' ');
      if (payload && typeof payload === 'object') {
        setLastExplainError(payload);
      } else {
        setLastExplainError(null);
      }
      setLastError(msg);
      setErrorBanner(`Request failed (${requestVia}): ${msg}`);
      setPagingMeta({ total: null, loaded: 0, isPaging: false, hasMore: false });
      try {
        setLastResponseSummary({
          request_via: requestVia,
          error: msg,
          request: lastRequest,
          error_payload: payload,
        });
      } catch {
        // ignore
      }
      setParcels([]);
      setRecords([]);
      setFieldStats(null);
      setResultSetCount(0);
      setParcelLinesFC(null);
      setParcelLinesEnabled(false);
      setParcelLinesError(null);
      setParcelLinesFeatureCount(0);
    } finally {
      if (reqId === activeReq.current) setLoading(false);
      setPagingMeta((prev) => ({ ...prev, isPaging: false, hasMore: false }));
    }
  }

  const signalGroupOptions = useMemo(() => {
    const liveGroups = new Set((sourceCoverage?.available_signal_groups || []) as string[]);
    const enforce = liveGroups.size > 0;
    const base = [
      { key: 'ownership', label: 'Ownership' },
      { key: 'permits', label: 'Permits' },
      { key: 'official_records', label: 'Official Records' },
      { key: 'courts', label: 'Courts' },
      { key: 'tax', label: 'Tax Collector' },
      { key: 'code_enforcement', label: 'Code Enforcement' },
      { key: 'gis_planning', label: 'Appraiser-Planning' },
    ];
    return base.map((g) => ({ ...g, enabled: !enforce || liveGroups.has(g.key) }));
  }, [sourceCoverage]);

  const distressPresets = useMemo(
    () => [
      { id: 'absentee', label: 'Absentee owner', keys: ['absentee_owner'], groups: ['ownership'] },
      { id: 'homestead', label: 'Homestead', keys: ['homestead'], groups: ['ownership'] },
    ],
    []
  );

  const toggleValueInList = useCallback((prev: string[], raw: string): string[] => {
    const v = String(raw || '').trim();
    if (!v) return prev;
    const exists = prev.includes(v);
    return exists ? prev.filter((x) => x !== v) : [...prev, v];
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function checkBackend() {
      try {
        const health = await apiFetch('/api/health');
        if (!health.ok) throw new Error(`health ${health.status}`);
        const ping = await apiFetch('/api/debug/ping');
        if (!ping.ok) throw new Error(`ping ${ping.status}`);
        if (!cancelled) {
          setBackendStatus('ok');
          setBackendStatusDetail('Backend OK');
        }
      } catch (e) {
        if (!cancelled) {
          const msg = e instanceof Error ? e.message : String(e);
          setBackendStatus('down');
          setBackendStatusDetail(`Backend down: ${msg}`);
        }
      }
    }
    checkBackend();
    return () => {
      cancelled = true;
    };
  }, []);

  const lastExplainRequestUrl =
    typeof (lastExplainError as any)?.url === 'string'
      ? String((lastExplainError as any).url)
      : typeof (lastExplainError as any)?.request_url === 'string'
        ? String((lastExplainError as any).request_url)
        : '';
  const lastExplainAbsolute = /^https?:\/\//i.test(lastExplainRequestUrl);
  const lastExplainHttpStatus =
    typeof (lastExplainError as any)?.http_status === 'number'
      ? Number((lastExplainError as any).http_status)
      : typeof (lastExplainError as any)?.status === 'number'
        ? Number((lastExplainError as any).status)
        : null;
  const lastExplainContentType =
    typeof (lastExplainError as any)?.content_type === 'string'
      ? String((lastExplainError as any).content_type)
      : typeof (lastExplainError as any)?.contentType === 'string'
        ? String((lastExplainError as any).contentType)
        : '';
  const lastExplainBodySnippet =
    typeof (lastExplainError as any)?.response_text_snippet === 'string'
      ? String((lastExplainError as any).response_text_snippet)
      : typeof (lastExplainError as any)?.responseTextSnippet === 'string'
        ? String((lastExplainError as any).responseTextSnippet)
        : typeof (lastExplainError as any)?.responseText === 'string'
          ? String((lastExplainError as any).responseText)
          : '';
  const lastExplainDetail = typeof (lastExplainError as any)?.detail === 'string'
    ? String((lastExplainError as any).detail)
    : '';
  const lastExplainHint = typeof (lastExplainError as any)?.hint === 'string'
    ? String((lastExplainError as any).hint)
    : '';

  return (
    <div
      className="flex h-screen min-h-[520px] overflow-hidden"
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

      <aside className="flex h-screen w-[420px] shrink-0 flex-col border-r border-cre-border/60 bg-cre-bg p-4 min-h-0 overflow-hidden">
        <div className="flex items-center justify-between gap-2">
          <div>
            <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Map Search</div>
            <div className="text-sm text-cre-text">Search → Area → Signals → Results</div>
          </div>

          <div className="rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-xs text-cre-text">
            County: {county ? county.toUpperCase() : 'AUTO'}
          </div>
        </div>

        {errorBanner ? (
          <div className="mt-3 rounded-xl border border-cre-border/60 bg-cre-surface p-3 text-sm text-cre-text">
            <div className="font-semibold">Notice</div>
            <div className="mt-1 text-xs text-cre-muted">{errorBanner}</div>
            {lastError ? <div className="mt-2 text-[11px] text-cre-muted">Last error: {lastError}</div> : null}
            {lastExplainError ? (
              <div className="mt-2 space-y-2 text-[11px] text-cre-muted">
                <div>Backend error: {String(lastExplainError.error || lastExplainError.message || 'Unknown')}</div>
                {lastExplainError.where ? <div>Where: {String(lastExplainError.where)}</div> : null}
                {lastExplainDetail ? <div>Detail: {lastExplainDetail}</div> : null}
                {lastExplainHint ? <div>Hint: {lastExplainHint}</div> : null}
                {lastExplainHttpStatus ? <div>http_status: {lastExplainHttpStatus}</div> : null}
                {lastExplainContentType ? <div>content_type: {lastExplainContentType}</div> : null}
                {lastExplainRequestUrl ? <div>request_url: {lastExplainRequestUrl}</div> : null}
                {lastExplainBodySnippet ? (
                  <div className="rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-1 text-[11px] text-cre-muted">
                    response_snippet: {lastExplainBodySnippet}
                  </div>
                ) : null}
                {lastExplainAbsolute ? (
                  <div className="rounded-lg border border-red-200 bg-red-50 px-2 py-1 text-[11px] font-semibold text-red-700">
                    ABSOLUTE API URL DETECTED – THIS IS WRONG IN DEV
                  </div>
                ) : null}
                <button
                  type="button"
                  className="rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-1 text-[11px] text-cre-text hover:bg-cre-surface"
                  onClick={() => {
                    try {
                      const payload = {
                        request: lastRequest,
                        error: lastExplainError,
                        request_url: lastExplainRequestUrl || null,
                        http_status: lastExplainHttpStatus,
                        content_type: lastExplainContentType || null,
                        response_snippet: lastExplainBodySnippet || null,
                      };
                      void navigator.clipboard?.writeText?.(JSON.stringify(payload, null, 2));
                      showToast('Bug payload copied.');
                    } catch {
                      // ignore
                    }
                  }}
                >
                  Copy bug payload
                </button>
              </div>
            ) : null}
            {lastRequest ? (
              <details className="mt-2 rounded-lg border border-cre-border/60 bg-cre-bg p-2 text-[11px] text-cre-muted">
                <summary className="cursor-pointer select-none text-cre-text">Last request JSON</summary>
                <pre className="mt-2 max-h-48 overflow-auto whitespace-pre-wrap break-words">{JSON.stringify(lastRequest, null, 2)}</pre>
              </details>
            ) : null}
          </div>
        ) : null}

        <div className="mt-4 flex min-h-0 flex-1 flex-col">
          <div className="min-h-0 flex-1 overflow-y-auto pr-1">
            <div className="rounded-xl border border-cre-border/60 bg-cre-surface p-3">
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

            <details className="rounded-xl border border-cre-border/60 bg-cre-bg p-3" open>
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
                    disabled={!fieldAvailability.living_area_sqft}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minSqft: e.target.value }))}
                    placeholder="e.g. 2000"
                  />
                  {!fieldAvailability.living_area_sqft
                    ? fieldCoverageNote('living_area_sqft', 'Source: leads.sqlite.parcel_table1.LIVING_AREA or leads.sqlite.pa_properties.living_sf')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Sqft</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxSqft}
                    disabled={!fieldAvailability.living_area_sqft}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxSqft: e.target.value }))}
                    placeholder=""
                  />
                  {!fieldAvailability.living_area_sqft
                    ? fieldCoverageNote('living_area_sqft', 'Source: leads.sqlite.parcel_table1.LIVING_AREA or leads.sqlite.pa_properties.living_sf')
                    : null}
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
                    disabled={!lotSizeAvailable}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minLotSize: e.target.value }))}
                    placeholder={filterForm.lotSizeUnit === 'acres' ? 'e.g. 0.25' : 'e.g. 8000'}
                  />
                  {!lotSizeAvailable
                    ? fieldCoverageNote('lot_size_sqft', 'Source: leads.sqlite.pa_properties.land_sf/land_acres')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Parcel Size</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="decimal"
                    value={filterForm.maxLotSize}
                    disabled={!lotSizeAvailable}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxLotSize: e.target.value }))}
                  />
                  {!lotSizeAvailable
                    ? fieldCoverageNote('lot_size_sqft', 'Source: leads.sqlite.pa_properties.land_sf/land_acres')
                    : null}
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Beds</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minBeds}
                    disabled={!fieldAvailability.beds}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minBeds: e.target.value }))}
                    placeholder="e.g. 3"
                  />
                  {!fieldAvailability.beds
                    ? fieldCoverageNote('beds', 'Source: leads.sqlite.pa_properties.bedrooms')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Min Baths</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="decimal"
                    value={filterForm.minBaths}
                    disabled={!fieldAvailability.baths}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minBaths: e.target.value }))}
                    placeholder="e.g. 2"
                  />
                  {!fieldAvailability.baths
                    ? fieldCoverageNote('baths', 'Source: leads.sqlite.pa_properties.bathrooms')
                    : null}
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Year Built</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minYearBuilt}
                    disabled={!fieldAvailability.year_built}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minYearBuilt: e.target.value }))}
                    placeholder="e.g. 1990"
                  />
                  {!fieldAvailability.year_built
                    ? fieldCoverageNote('year_built', 'Source: leads.sqlite.parcel_table1.BASE_YR_BLT or leads.sqlite.pa_properties.year_built')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Year Built</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxYearBuilt}
                    disabled={!fieldAvailability.year_built}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxYearBuilt: e.target.value }))}
                  />
                  {!fieldAvailability.year_built
                    ? fieldCoverageNote('year_built', 'Source: leads.sqlite.parcel_table1.BASE_YR_BLT or leads.sqlite.pa_properties.year_built')
                    : null}
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Ownership Years</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minOwnershipYears}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minOwnershipYears: e.target.value }))}
                    placeholder="e.g. 10"
                  />
                  {fieldCoverageNote('ownership_years', 'Source: leads.sqlite.pa_properties.last_sale_date')}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Property Type</div>
                  <select
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    value={filterForm.propertyType}
                    disabled={!fieldAvailability.property_type}
                    onChange={(e) => setFilterForm((p) => ({ ...p, propertyType: e.target.value }))}
                  >
                    <option value="">Any</option>
                    <option value="sfr">Single Family</option>
                    <option value="condo">Condo</option>
                    <option value="townhome">Townhome</option>
                    <option value="mfh">Multi-Family</option>
                    <option value="mobile_home">Mobile / Manufactured</option>
                    <option value="mixed_use">Mixed Use</option>
                    <option value="retail">Retail</option>
                    <option value="office">Office</option>
                    <option value="industrial">Industrial</option>
                    <option value="agricultural">Agricultural</option>
                    <option value="vacant_land">Vacant Land</option>
                    <option value="residential">Residential (generic)</option>
                    <option value="commercial">Commercial (generic)</option>
                  </select>
                  <select
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    value={filterForm.propertyTypeMode}
                    disabled={!fieldAvailability.property_type}
                    onChange={(e) =>
                      setFilterForm((p) => ({
                        ...p,
                        propertyTypeMode: e.target.value === 'equals' ? 'equals' : 'contains',
                      }))
                    }
                  >
                    <option value="contains">Contains</option>
                    <option value="equals">Equals</option>
                  </select>
                  {!fieldAvailability.property_type
                    ? fieldCoverageNote('property_type', 'Source: leads.sqlite.pa_properties.use_type / land_use_code')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Zoning</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    value={filterForm.zoning}
                    disabled={!fieldAvailability.zoning}
                    onChange={(e) => setFilterForm((p) => ({ ...p, zoning: e.target.value }))}
                    placeholder="e.g. R-1"
                  />
                  <select
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    value={filterForm.zoningMatch}
                    disabled={!fieldAvailability.zoning}
                    onChange={(e) =>
                      setFilterForm((p) => ({
                        ...p,
                        zoningMatch: e.target.value === 'equals' ? 'equals' : 'contains',
                      }))
                    }
                  >
                    <option value="contains">Contains</option>
                    <option value="equals">Equals</option>
                  </select>
                  {!fieldAvailability.zoning
                    ? fieldCoverageNote('zoning', 'Source: leads.sqlite.pa_properties.zoning')
                    : null}
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
                  {!fieldAvailability.zoning
                    ? fieldCoverageNote('zoning', 'Source: leads.sqlite.pa_properties.zoning')
                    : null}
                  <label className="space-y-1">
                    <div className="text-cre-muted">Future Land Use</div>
                    <input
                      className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                      value={filterForm.futureLandUse}
                      disabled={!fieldAvailability.future_land_use}
                      onChange={(e) => setFilterForm((p) => ({ ...p, futureLandUse: e.target.value }))}
                      placeholder="e.g. RES"
                    />
                    <select
                      className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                      value={filterForm.futureLandUseMatch}
                      disabled={!fieldAvailability.future_land_use}
                      onChange={(e) =>
                        setFilterForm((p) => ({
                          ...p,
                          futureLandUseMatch: e.target.value === 'equals' ? 'equals' : 'contains',
                        }))
                      }
                    >
                      <option value="contains">Contains</option>
                      <option value="equals">Equals</option>
                    </select>
                  </label>
                  <MultiSelectFilter
                    title="Future Land Use (multi-select)"
                    options={futureLandUseOptions}
                    selected={selectedFutureLandUse}
                    query={futureLandUseQuery}
                    onQuery={setFutureLandUseQuery}
                    onSelected={setSelectedFutureLandUse}
                    renderOption={formatCodeLabel}
                  />
                  {!fieldAvailability.future_land_use
                    ? fieldCoverageNote('future_land_use', 'Source: leads.sqlite.pa_properties.future_land_use')
                    : null}
                  <div className="text-[11px] text-cre-muted">Tip: “Zoning contains” and multi-select both apply (AND).</div>
                </div>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Total Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minValue}
                    disabled={!fieldAvailability.total_value}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minValue: e.target.value }))}
                    placeholder="e.g. 350000"
                  />
                  {!fieldAvailability.total_value
                    ? fieldCoverageNote('total_value', 'Source: leads.sqlite.parcel_table1.TOTAL_JUST_VALUE or leads.sqlite.pa_properties.just_value')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Total Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxValue}
                    disabled={!fieldAvailability.total_value}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxValue: e.target.value }))}
                  />
                  {!fieldAvailability.total_value
                    ? fieldCoverageNote('total_value', 'Source: leads.sqlite.parcel_table1.TOTAL_JUST_VALUE or leads.sqlite.pa_properties.just_value')
                    : null}
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Land Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minLandValue}
                    disabled={!fieldAvailability.land_value}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minLandValue: e.target.value }))}
                  />
                  {!fieldAvailability.land_value
                    ? fieldCoverageNote('land_value', 'Source: leads.sqlite.parcel_table1.APPR_LAND or leads.sqlite.pa_properties.land_value')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Land Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxLandValue}
                    disabled={!fieldAvailability.land_value}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxLandValue: e.target.value }))}
                  />
                  {!fieldAvailability.land_value
                    ? fieldCoverageNote('land_value', 'Source: leads.sqlite.parcel_table1.APPR_LAND or leads.sqlite.pa_properties.land_value')
                    : null}
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Min Building Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.minBuildingValue}
                    disabled={!fieldAvailability.building_value}
                    onChange={(e) => setFilterForm((p) => ({ ...p, minBuildingValue: e.target.value }))}
                  />
                  {!fieldAvailability.building_value
                    ? fieldCoverageNote('building_value', 'Source: leads.sqlite.parcel_table1.APPR_BLDG or leads.sqlite.pa_properties.improvement_value')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Max Building Value</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    inputMode="numeric"
                    value={filterForm.maxBuildingValue}
                    disabled={!fieldAvailability.building_value}
                    onChange={(e) => setFilterForm((p) => ({ ...p, maxBuildingValue: e.target.value }))}
                  />
                  {!fieldAvailability.building_value
                    ? fieldCoverageNote('building_value', 'Source: leads.sqlite.parcel_table1.APPR_BLDG or leads.sqlite.pa_properties.improvement_value')
                    : null}
                </label>

                <label className="space-y-1">
                  <div className="text-cre-muted">Last Sale Start</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    type="text"
                    inputMode="numeric"
                    value={filterForm.lastSaleStart}
                    disabled={!fieldAvailability.last_sale_date}
                    onChange={(e) => setFilterForm((p) => ({ ...p, lastSaleStart: e.target.value }))}
                    placeholder="YYYY-MM-DD or MM/DD/YYYY"
                  />
                  {!fieldAvailability.last_sale_date
                    ? fieldCoverageNote('last_sale_date', 'Source: leads.sqlite.pa_properties.last_sale_date')
                    : null}
                </label>
                <label className="space-y-1">
                  <div className="text-cre-muted">Last Sale End</div>
                  <input
                    className="w-full rounded-lg border border-cre-border/60 bg-cre-surface px-2 py-1 text-cre-text"
                    type="text"
                    inputMode="numeric"
                    value={filterForm.lastSaleEnd}
                    disabled={!fieldAvailability.last_sale_date}
                    onChange={(e) => setFilterForm((p) => ({ ...p, lastSaleEnd: e.target.value }))}
                    placeholder="YYYY-MM-DD or MM/DD/YYYY"
                  />
                  {!fieldAvailability.last_sale_date
                    ? fieldCoverageNote('last_sale_date', 'Source: leads.sqlite.pa_properties.last_sale_date')
                    : null}
                </label>

                {softWarnings.length ? (
                  <div className="col-span-2 -mt-1 rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-1 text-[11px] text-cre-muted">
                    {softWarnings.join(' / ')}
                  </div>
                ) : null}
              </div>

            </details>
          </div>
        </div>
        <div className="rounded-xl border border-cre-border/60 bg-cre-surface p-3">
          <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">2) Draw / Area</div>
          <div className="mt-2 text-xs text-cre-muted">Use polygon or circle tools (top-right of map).</div>
          <div className="mt-2 text-xs text-cre-muted">
            Status: <span className="font-semibold text-cre-text">{geometryStatus}</span>
          </div>

          <div className="mt-2">
            <label className="text-xs text-cre-muted">
              Polygon match mode
              <select
                className="mt-1 w-full rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-2 text-sm text-cre-text"
                value={polygonMatchMode}
                onChange={(e) => setPolygonMatchMode(e.target.value as typeof polygonMatchMode)}
              >
                <option value="intersects">Intersects (default)</option>
                <option value="centroid_inside">Centroid inside</option>
                <option value="contains">Contains</option>
              </select>
            </label>
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

        <details className="rounded-xl border border-cre-border/60 bg-cre-surface p-3" open>
          <summary className="cursor-pointer select-none text-xs font-semibold uppercase tracking-widest text-cre-muted">
            3) Signals
          </summary>
          <div className="mt-3 space-y-3 text-xs">
              <label className="space-y-1">
                <div className="text-cre-muted">Seller intent</div>
                <select
                  className="w-full rounded-lg border border-cre-border/60 bg-cre-bg px-2 py-2 text-sm text-cre-text"
                  value={(rollupsMinScore || '').trim()}
                  onChange={(e) => {
                    setRollupsEnabled(true);
                    setRollupsMinScore(e.target.value);
                  }}
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
                        disabled={!g.enabled}
                        onChange={() => {
                          if (!g.enabled) return;
                          setRollupsEnabled(true);
                          setRollupsTriggerGroups((prev) => toggleValueInList(prev, g.key));
                        }}
                      />
                      {g.label}
                      {!g.enabled ? (
                        <span className="rounded-full border border-amber-400/60 bg-amber-50 px-2 py-0.5 text-[10px] font-semibold text-amber-800">
                          Coming soon
                        </span>
                      ) : null}
                    </label>
                  ))}
                </div>
              </div>

              <div className="space-y-1">
                <div className="text-cre-muted">Distress (quick picks)</div>
                <div className="flex flex-wrap gap-2">
                  {distressPresets.map((p) => {
                    const active = p.keys.every((k) => rollupsTriggerKeys.includes(k));
                    const enabled = p.keys.every((k) => enabledSignalKeys.has(k));
                    return (
                      <button
                        key={p.id}
                        type="button"
                        className={
                          active
                            ? 'rounded-full bg-cre-accent px-3 py-1 text-[12px] font-semibold text-white'
                            : enabled
                              ? 'rounded-full border border-cre-border/60 bg-cre-bg px-3 py-1 text-[12px] text-cre-text hover:bg-cre-surface'
                              : 'rounded-full border border-amber-400/60 bg-amber-50 px-3 py-1 text-[12px] text-amber-800 opacity-70'
                        }
                        disabled={!enabled}
                        onClick={() => {
                          if (!enabled) return;
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
                        {!enabled ? <span className="ml-2 text-[10px] font-semibold">Coming soon</span> : null}
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
                              const enabled = enabledSignalKeys.has(it.key);
                              return (
                                <label
                                  key={`sig:${it.key}`}
                                  className="flex cursor-pointer select-none items-center gap-2 rounded-md px-1 py-1 text-xs text-cre-text hover:bg-cre-surface"
                                  title={it.key}
                                >
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    disabled={!enabled}
                                    onChange={() => {
                                      if (!enabled) return;
                                      setRollupsEnabled(true);
                                      setRollupsTriggerKeys((prev) => toggleValueInList(prev, it.key));
                                    }}
                                  />
                                  <span className="truncate">{it.label}</span>
                                  {!enabled ? (
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
                    <input
                      type="checkbox"
                      checked={rollupsTierCritical}
                      onChange={(e) => {
                        setRollupsEnabled(true);
                        setRollupsTierCritical(e.target.checked);
                      }}
                    />
                    Critical
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={rollupsTierStrong}
                      onChange={(e) => {
                        setRollupsEnabled(true);
                        setRollupsTierStrong(e.target.checked);
                      }}
                    />
                    Strong
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={rollupsTierSupport}
                      onChange={(e) => {
                        setRollupsEnabled(true);
                        setRollupsTierSupport(e.target.checked);
                      }}
                    />
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
        </details>

        <div className="rounded-xl border border-cre-border/60 bg-cre-surface p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">4) Results</div>
            <div className="text-xs text-cre-muted">
              {lastCounts && lastCounts.candidateCount !== null && lastCounts.filteredCount !== null
                ? `Showing ${lastCounts.filteredCount} of ${lastCounts.candidateCount}`
                : `Loaded ${parcels.length} · Displaying ${visibleRows.length} (live ${sourceCounts.live} / cache ${sourceCounts.cache})`}
            </div>
          </div>
          {hoverFieldsMode === 'evidence_only' ? (
            <div className="mt-1 text-[11px] text-cre-muted">
              Hover fields: not enriched yet (evidence-only).
            </div>
          ) : null}
          {lastCounts && lastCounts.candidateCount !== null && lastCounts.filteredCount !== null ? (
            <div className="mt-1 text-[11px] text-cre-muted">
              Candidates: {lastCounts.candidateCount} • Returned: {lastResponseCount} • Filtered:{' '}
              {Math.max(0, Number(lastCounts.candidateCount) - Number(lastCounts.filteredCount))}
            </div>
          ) : null}

          <div className="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              className="rounded-xl border border-cre-border/60 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface disabled:opacity-60"
              onClick={() => void enrichVisible()}
              disabled={loading || parcels.length === 0}
              title={parcels.length === 0 ? 'No parcels to enrich yet.' : 'Enrich visible parcels'}
            >
              Enrich
            </button>
            <button
              type="button"
              className="rounded-xl border border-cre-border/60 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface disabled:opacity-60"
              onClick={() => void enrichSelectedProviders()}
              disabled={loading || parcels.length === 0}
              title={parcels.length === 0 ? 'No parcels loaded yet.' : 'Run provider enrichment for selected parcels'}
            >
              Enrich selected
            </button>
            <button
              type="button"
              className="rounded-xl border border-cre-border/60 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface disabled:opacity-60"
              onClick={() => void runTriggersForSelected()}
              disabled={loading || parcels.length === 0}
              title={parcels.length === 0 ? 'No parcels loaded yet.' : 'Evaluate triggers for selected parcels'}
            >
              Run triggers
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

          {lastEnrichment || lastTriggerEval ? (
            <div className="mt-2 rounded-lg border border-cre-border/60 bg-cre-bg px-3 py-2 text-[11px] text-cre-muted">
              {lastEnrichment
                ? `Enrichment: ${lastEnrichment.results.length} provider results`
                : 'Enrichment: —'}
              {' • '}
              {lastTriggerEval
                ? `Triggers: ${lastTriggerEval.results.length} parcels evaluated`
                : 'Triggers: —'}
            </div>
          ) : null}

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
                  <div className="space-y-2">
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

          <div className="mt-3 rounded-xl border border-cre-border/60 bg-cre-bg p-3">
            <div className="flex items-center justify-between gap-2">
              <div className="text-sm font-semibold text-cre-text">Results</div>
              <button
                type="button"
                className="rounded-lg border border-cre-border/60 bg-cre-surface px-3 py-1 text-xs text-cre-text hover:bg-cre-bg"
                onClick={downloadCsv}
              >
                Download CSV
              </button>
            </div>
              <div className="mt-1 text-xs text-cre-muted">
                Loaded {pagingMeta.loaded || parcels.length} of{' '}
                {pagingMeta.total ?? (lastResponseSummary as any)?.total_count ?? lastResponseCount} · Displaying{' '}
                {visibleRows.length}
                {resultsQuery.trim() ? ' (text filter)' : ''}
              </div>
            {pagingMeta.isPaging ? (
              <div className="mt-1 text-xs text-cre-muted">Loading pages…</div>
            ) : null}
              {debugUiEnabled && visibleRows.length < parcels.length && !resultsQuery.trim() ? (
                <div className="mt-1 text-[11px] text-amber-700">
                  Displaying {visibleRows.length} of {parcels.length}. Check Live/Cache toggles or filters.
                </div>
              ) : null}
              <div className="mt-1 text-[11px] text-cre-muted">Active filters: {activeFiltersSummary}</div>
              <div className="mt-1 text-[11px] text-cre-muted">Active signals: {activeSignalsSummary}</div>
            {debugUiEnabled ? (
              <div className="mt-1 text-[11px] text-cre-muted">
                Debug: total_count={
                  pagingMeta.total ?? (lastResponseSummary as any)?.total_count ?? lastResponseCount
                }{' '}
                returned_count={lastResponseCount} first_pid={parcels[0]?.parcel_id || '—'} last_pid={
                  parcels.length ? parcels[parcels.length - 1]?.parcel_id : '—'
                }
              </div>
            ) : null}
            {(() => {
              const dropped = (lastResponseSummary as any)?.dropped_reasons || null;
              if (!dropped || typeof dropped !== 'object') return null;
              const entries = Object.entries(dropped as Record<string, number>)
                .filter(([, v]) => Number(v) > 0)
                .sort((a, b) => Number(b[1]) - Number(a[1]))
                .slice(0, 4);
              if (!entries.length) return null;
              return (
                <div className="mt-1 text-[11px] text-cre-muted">
                  Dropped: {entries.map(([k, v]) => `${k} (${v})`).join(', ')}
                </div>
              );
            })()}
          </div>

          <div className="mt-3 max-h-[45vh] space-y-2 overflow-y-auto pr-1">
            {visibleRows.length ? (
              visibleRows.map((p) => {
                const rec = recordById.get(p.parcel_id);
                const rollup = (rec as any)?.rollup || rollupsMap[p.parcel_id] || null;
                const addr = (p.address || rec?.situs_address || rec?.address || '').trim() || '—';
                const owner = (p.owner_name || rec?.owner_name || '').trim() || '—';
                const countyLabel = (p.county || rec?.county || county || '').toUpperCase() || '—';
                const beds = rec?.beds ?? (p as any)?.beds ?? null;
                const baths = rec?.baths ?? (p as any)?.baths ?? null;
                const sqft = rec?.living_area_sqft ?? (p as any)?.living_sf ?? null;
                const yearBuilt = rec?.year_built ?? (p as any)?.year_built ?? null;
                const zoning = (rec?.zoning ?? (p as any)?.zoning ?? '').trim();
                const propertyType = (rec?.property_type ?? (rec as any)?.property_type_raw ?? (rec as any)?.land_use ?? '').toString().trim();
                const fmtNum = (val: number | null | undefined) =>
                  typeof val === 'number' && Number.isFinite(val) ? val.toLocaleString() : '—';
                const srcLabel = (p.source || (rec as any)?.source || '—').toString().toUpperCase();

                const groupsBadges: Array<{ k: string; label: string }> = [];
                if (rollup && Number(rollup.has_official_records || 0) > 0) groupsBadges.push({ k: 'or', label: 'Records' });
                if (rollup && Number(rollup.has_permits || 0) > 0) groupsBadges.push({ k: 'p', label: 'Permits' });
                if (rollup && Number(rollup.has_tax || 0) > 0) groupsBadges.push({ k: 't', label: 'Tax' });
                if (rollup && Number(rollup.has_code_enforcement || 0) > 0) groupsBadges.push({ k: 'ce', label: 'Code' });
                if (rollup && Number(rollup.has_courts || 0) > 0) groupsBadges.push({ k: 'ct', label: 'Courts' });
                if (rollup && Number(rollup.has_gis_planning || 0) > 0) groupsBadges.push({ k: 'gp', label: 'Appraiser' });
                const signals = (rec as any)?.signals || {};
                if (!rollup) {
                  if (signals.has_official_records) groupsBadges.push({ k: 'or', label: 'Records' });
                  if (signals.has_permits) groupsBadges.push({ k: 'p', label: 'Permits' });
                  if (signals.has_tax_events) groupsBadges.push({ k: 't', label: 'Tax' });
                  if (signals.has_code_enforcement) groupsBadges.push({ k: 'ce', label: 'Code' });
                  if (signals.has_courts) groupsBadges.push({ k: 'ct', label: 'Courts' });
                  if (signals.has_gis_planning) groupsBadges.push({ k: 'gp', label: 'Appraiser' });
                }

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
                        <div className="mt-1 text-[11px] text-cre-muted">
                          {countyLabel} · Beds {beds ?? '—'} · Baths {baths ?? '—'} · Living {fmtNum(sqft)} sqft · Year {yearBuilt ?? '—'}
                        </div>
                        <div className="mt-1 text-[11px] text-cre-muted">
                          Zoning {zoning || '—'} · Type {propertyType || '—'}
                        </div>
                        <div className="mt-1 font-mono text-[11px] text-cre-muted">{p.parcel_id}</div>
                      </div>
                      <div className="text-[11px] text-cre-muted">{srcLabel}</div>
                    </div>

                    <div className="mt-2 flex flex-wrap items-center gap-2">
                      {signals.absentee_owner ? (
                        <span className="rounded-full border border-amber-300/60 bg-amber-50 px-2 py-1 text-[11px] text-amber-900">
                          Absentee owner
                        </span>
                      ) : null}
                      {signals.homestead ? (
                        <span className="rounded-full border border-emerald-300/60 bg-emerald-50 px-2 py-1 text-[11px] text-emerald-900">
                          Homestead
                        </span>
                      ) : null}
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
          </div>

          <div className="sticky bottom-0 -mx-4 mt-3 shrink-0 border-t border-cre-border/60 bg-cre-bg/95 px-4 py-3 backdrop-blur">
            <div className="flex items-center gap-2">
              <button
                type="button"
                className="flex-1 rounded-xl bg-cre-accent px-4 py-2 text-sm font-semibold text-white hover:brightness-95 disabled:opacity-60"
                onClick={() => void run()}
                disabled={loading || (!drawnPolygon && !drawnCircle && !resultsQuery.trim())}
              >
                {loading ? 'Running…' : 'Run'}
              </button>
              <button
                type="button"
                className="rounded-xl border border-cre-border/60 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface"
                onClick={() => {
                  clearFilters();
                  clearDrawings();
                }}
              >
                Clear
              </button>
              <button
                type="button"
                className="rounded-xl border border-cre-border/60 bg-cre-bg px-4 py-2 text-sm text-cre-text hover:bg-cre-surface disabled:opacity-60"
                onClick={downloadCsv}
                disabled={!rows.length}
              >
                Export CSV
              </button>
            </div>
            <div className="mt-2 text-[11px] text-cre-muted">
              {drawnPolygon || drawnCircle ? 'Geometry selected' : resultsQuery.trim() ? 'Text filter active' : 'Draw an area or type a text filter to enable Run.'}
            </div>
          </div>
        </div>
      </aside>

      <main className="flex-1 min-h-0 bg-cre-bg p-4">
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
                ? 'pointer-events-none absolute inset-0 bg-black/10'
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
                {selectedParcelDetailLoading ? (
                  <div className="text-xs text-cre-muted">Parcel detail loading…</div>
                ) : null}
                {selectedParcelDetailError ? (
                  <div className="text-xs text-red-600">{selectedParcelDetailError}</div>
                ) : null}
                {!selectedParcelId ? (
                  <div className="text-sm text-cre-muted">Select a parcel to view signals.</div>
                ) : (
                  <div className="space-y-4">
                    {(() => {
                      const rec = selectedParcelId ? records.find((r) => r.parcel_id === selectedParcelId) : null;
                      const p = selectedParcelId ? parcels.find((x) => x.parcel_id === selectedParcelId) : null;
                      const detail = selectedParcelDetail;
                      const fmtNum = (val: number | null | undefined) =>
                        typeof val === 'number' && Number.isFinite(val) ? val.toLocaleString() : '—';
                      const fmtMoney = (val: number | null | undefined) =>
                        typeof val === 'number' && Number.isFinite(val) && val > 0
                          ? `$${Math.round(val).toLocaleString()}`
                          : '—';
                      const addr = (rec?.situs_address || rec?.address || p?.address || '').trim();
                      const owner = (rec?.owner_name || p?.owner_name || '').trim();
                      const mailing = (
                        detail?.owner_mailing_address ||
                        detail?.mailing_address ||
                        (rec as any)?.owner_mailing_address ||
                        p?.owner_mailing_address ||
                        ''
                      ).trim();
                      const yearBuilt = detail?.year_built ?? rec?.year_built ?? p?.year_built ?? null;
                      const beds = detail?.beds ?? rec?.beds ?? p?.beds ?? null;
                      const baths = detail?.baths ?? rec?.baths ?? p?.baths ?? null;
                      const livingArea = detail?.living_area_sqft ?? rec?.living_area_sqft ?? p?.living_sf ?? null;
                      const lotSqft = detail?.lot_size_sqft ?? rec?.lot_size_sqft ?? p?.land_sf ?? null;
                      const lotAcres = detail?.lot_size_acres ?? rec?.lot_size_acres ?? p?.land_acres ?? null;
                      const zoning = (detail?.zoning ?? rec?.zoning ?? p?.zoning ?? '').trim();
                      const futureLandUse = (detail?.future_land_use ?? rec?.future_land_use ?? p?.future_land_use ?? '').trim();
                      const justValue = detail?.just_value ?? rec?.just_value ?? p?.just_value ?? null;
                      const assessedValue = detail?.assessed_value ?? rec?.assessed_value ?? p?.assessed_value ?? null;
                      const taxableValue = detail?.taxable_value ?? rec?.taxable_value ?? p?.taxable_value ?? null;
                      const landValue = detail?.land_value ?? rec?.land_value ?? p?.land_value ?? null;
                      const buildingValue = detail?.building_value ?? rec?.building_value ?? p?.improvement_value ?? null;
                      const totalValue = detail?.total_value ?? rec?.total_value ?? null;
                      return (
                        <div className="rounded-xl border border-cre-border/60 bg-cre-bg p-3">
                          <div className="text-sm font-semibold text-cre-text">{addr || '—'}</div>
                          <div className="mt-1 text-xs text-cre-muted">{owner || '—'}</div>
                          {mailing ? (
                            <div className="mt-1 text-[11px] text-cre-muted">Mailing: {mailing}</div>
                          ) : null}
                          {(rec || p || detail) ? (
                            <div className="mt-2 text-[11px] text-cre-muted">
                              Year {yearBuilt ?? '—'} · Beds {beds ?? '—'} · Baths {baths ?? '—'} · Living {fmtNum(livingArea)} sqft · Lot {fmtNum(lotSqft)} sqft ({fmtNum(lotAcres)} ac)
                            </div>
                          ) : null}
                          <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-[11px] text-cre-muted">
                            <div>Zoning: {zoning || '—'}</div>
                            <div>FLU: {futureLandUse || '—'}</div>
                            <div>Just: {fmtMoney(justValue)}</div>
                            <div>Assessed: {fmtMoney(assessedValue)}</div>
                            <div>Taxable: {fmtMoney(taxableValue)}</div>
                            <div>Land: {fmtMoney(landValue)}</div>
                            <div>Building: {fmtMoney(buildingValue)}</div>
                            <div>Total: {fmtMoney(totalValue)}</div>
                          </div>
                        </div>
                      );
                    })()}

                    <div className="rounded-xl border border-cre-border/60 bg-cre-bg p-3">
                      <div className="text-xs font-semibold uppercase tracking-widest text-cre-muted">Owner contact</div>
                      {ownerEnrichmentLoading ? (
                        <div className="mt-2 text-sm text-cre-muted">Loading contact enrichment…</div>
                      ) : ownerEnrichmentError ? (
                        <div className="mt-2 text-sm text-cre-muted">{ownerEnrichmentError}</div>
                      ) : ownerEnrichment ? (
                        <div className="mt-2 space-y-1 text-[11px] text-cre-muted">
                          <div>
                            Provider: {ownerEnrichment.provider || '—'} · Status: {ownerEnrichment.status}
                          </div>
                          {!ownerEnrichment.configured ? (
                            <div className="text-amber-700">Not configured (set OWNER_ENRICH_PROVIDER + API key).</div>
                          ) : null}
                          <div>Mailing: {ownerEnrichment.owner_mailing_address || '—'}</div>
                          <div>Phones: {ownerEnrichment.phones?.length ? ownerEnrichment.phones.join(', ') : '—'}</div>
                          <div>Emails: {ownerEnrichment.emails?.length ? ownerEnrichment.emails.join(', ') : '—'}</div>
                        </div>
                      ) : (
                        <div className="mt-2 text-sm text-cre-muted">No enrichment available.</div>
                      )}
                    </div>

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
