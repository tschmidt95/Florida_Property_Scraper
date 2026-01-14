import { useMemo, useRef, useState, type ChangeEvent } from 'react';

import L from 'leaflet';
import type { LatLngLiteral } from 'leaflet';
import { CircleMarker, FeatureGroup, MapContainer, Marker, TileLayer } from 'react-leaflet';
import { EditControl } from 'react-leaflet-draw';

import 'leaflet-draw';

import { parcelsSearch, type ParcelRecord } from '../lib/api';

type DrawnCircle = { center: LatLngLiteral; radius_m: number };

function demoRecords(county: string): ParcelRecord[] {
  // Deterministic pseudo-random points around downtown Orlando.
  const out: ParcelRecord[] = [];
  let seed = 1337;
  const rand = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };

  const base = { lat: 28.5383, lng: -81.3792 };
  const zoning = ['R-1', 'C-2', 'PUD', 'R-2', 'MU', 'I-1'];
  for (let i = 0; i < 25; i++) {
    const dLat = (rand() - 0.5) * 0.03;
    const dLng = (rand() - 0.5) * 0.03;
    out.push({
      parcel_id: `DEMO-${String(i + 1).padStart(3, '0')}`,
      county,
      address: `Demo ${100 + i} W ${i % 2 ? 'Pine' : 'Orange'} St, Orlando, FL`,
      situs_address: `Demo ${100 + i} W ${i % 2 ? 'Pine' : 'Orange'} St, Orlando, FL`,
      owner_name: `Demo Owner ${i + 1}`,
      property_class: 'DEMO',
      land_use: i % 3 === 0 ? 'Residential' : i % 3 === 1 ? 'Commercial' : 'Mixed',
      flu: i % 3 === 0 ? 'RES' : i % 3 === 1 ? 'COM' : 'MIX',
      zoning: zoning[i % zoning.length],
      living_area_sqft: 1200 + (i % 7) * 150,
      lot_size_sqft: 4000 + (i % 9) * 250,
      beds: i % 3 === 0 ? 3 : i % 3 === 1 ? 2 : 4,
      baths: i % 3 === 0 ? 2 : i % 3 === 1 ? 1.5 : 3,
      year_built: 1980 + (i % 30),
      last_sale_date: null,
      last_sale_price: null,
      source: 'missing',
      lat: base.lat + dLat,
      lng: base.lng + dLng,
    });
  }
  return out;
}

export default function MapSearch() {
  const [county, setCounty] = useState('orange');
  const [drawnPolygon, setDrawnPolygon] = useState<GeoJSON.Polygon | null>(null);
  const [drawnCircle, setDrawnCircle] = useState<DrawnCircle | null>(null);

  const [records, setRecords] = useState<ParcelRecord[]>(demoRecords('orange'));
  const [selectedParcelId, setSelectedParcelId] = useState<string | null>(null);

  const [loading, setLoading] = useState(false);
  const [errorBanner, setErrorBanner] = useState<string | null>(
    'DEMO MODE: Draw a polygon or circle, then click Run.',
  );

  const [lastRequest, setLastRequest] = useState<any | null>(null);
  const [lastResponseCount, setLastResponseCount] = useState<number>(records.length);
  const [lastError, setLastError] = useState<string | null>(null);

  const featureGroupRef = useRef<any>(null);
  const activeReq = useRef(0);

  const rows = useMemo(() => records, [records]);

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

  async function run() {
    setLastError(null);
    setErrorBanner(null);

    if (!drawnPolygon && !drawnCircle) {
      setErrorBanner('Draw polygon or radius first');
      setRecords(demoRecords(county));
      return;
    }

    const payload: any = {
      county,
      live: true,
      limit: 200,
      include_geometry: true,
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

      if (!recs.length) {
        const msg = warnings?.length
          ? `No results returned. ${warnings.join(' / ')}`
          : 'No results returned from backend.';
        setLastError(msg);
        setErrorBanner(`${msg} Showing DEMO dataset so the map workflow stays visible.`);
        setRecords(demoRecords(county));
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
      setErrorBanner(`Request failed: ${msg}. Showing DEMO dataset so the map workflow stays visible.`);
      setRecords(demoRecords(county));
    } finally {
      if (reqId === activeReq.current) setLoading(false);
    }
  }

  return (
    <div className="flex h-[calc(100vh-96px)] min-h-[720px]">
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
              setRecords(demoRecords(e.target.value));
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
        </div>

        <div className="mt-4 text-xs text-cre-muted">
          Draw: polygon tool or circle tool (top-right on the map).
        </div>

        <div className="mt-2 text-xs text-cre-muted">
          Status: <span className="font-semibold text-cre-text">{geometryStatus}</span>
        </div>

        <div className="mt-4 overflow-hidden rounded-xl border border-cre-border/40">
          <div className="border-b border-cre-border/40 bg-cre-bg px-3 py-2 text-xs font-semibold uppercase tracking-widest text-cre-muted">
            Results ({rows.length})
          </div>
          <div className="max-h-[420px] overflow-auto">
            <table className="w-full text-left text-xs">
              <thead className="sticky top-0 bg-cre-surface text-[11px] uppercase tracking-widest text-cre-muted">
                <tr>
                  <th className="px-2 py-2">Parcel</th>
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
                      <td className="px-2 py-2 text-cre-text">{addr || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.owner_name || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.zoning || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{(r.flu || r.land_use) || '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.living_area_sqft ?? '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.beds ?? '—'}</td>
                      <td className="px-2 py-2 text-cre-text">{r.baths ?? '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

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
            style={{ height: '100%', width: '100%' }}
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />

            <FeatureGroup ref={featureGroupRef}>
              <EditControl
                position="topright"
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
                draw={{
                  polyline: false,
                  rectangle: false,
                  marker: false,
                  circlemarker: false,
                  polygon: true,
                  circle: true,
                }}
                edit={{ edit: {}, remove: true }}
              />
            </FeatureGroup>

            {records.map((r) => {
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
