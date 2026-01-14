export type SearchResult = {
  owner: string;
  address: string;
  county: string;
  score: number;
  parcel_id?: string;
  source?: string;
  last_permit_date?: string | null;
  permits_last_15y_count?: number;
  matched_fields?: string[];
};

export type ParcelSearchRequest =
  | {
      county: string;
      geometry: GeoJSON.Geometry;
      radius?: never;
      center?: never;
      radius_m?: never;
      live?: boolean;
      enrich?: boolean;
      enrich_limit?: number;
      limit?: number;
      include_geometry?: boolean;
    }
  | {
      county: string;
      geometry?: never;
      radius: { center: [number, number]; miles: number };
      center?: never;
      radius_m?: never;
      live?: boolean;
      enrich?: boolean;
      enrich_limit?: number;
      limit?: number;
      include_geometry?: boolean;
    };

export type ParcelSearchRequestV2 = {
  county: string;
  geometry?: never;
  radius?: never;
  center: { lat: number; lng: number };
  radius_m: number;
  live?: boolean;
  enrich?: boolean;
  enrich_limit?: number;
  limit?: number;
  include_geometry?: boolean;
};

export type ParcelAttributeFilters = {
  min_sqft?: number | null;
  max_sqft?: number | null;
  min_year_built?: number | null;
  max_year_built?: number | null;
  min_beds?: number | null;
  min_baths?: number | null;
  property_type?: string | null;
  zoning?: string | string[] | null;
  zoning_in?: string[] | null;
  future_land_use_in?: string[] | null;
  min_value?: number | null;
  max_value?: number | null;
  min_land_value?: number | null;
  max_land_value?: number | null;
  min_building_value?: number | null;
  max_building_value?: number | null;
  last_sale_date_start?: string | null; // YYYY-MM-DD
  last_sale_date_end?: string | null; // YYYY-MM-DD
};

export type ParcelMapSearchRequest =
  | {
      county: string;
      polygon_geojson: GeoJSON.Polygon;
      center?: never;
      radius_m?: never;
      live?: boolean;
      enrich?: boolean;
      enrich_limit?: number;
      limit?: number;
      include_geometry?: boolean;
      filters?: ParcelAttributeFilters;
    }
  | {
      county: string;
      polygon_geojson?: never;
      center: { lat: number; lng: number };
      radius_m: number;
      live?: boolean;
      enrich?: boolean;
      enrich_limit?: number;
      limit?: number;
      include_geometry?: boolean;
      filters?: ParcelAttributeFilters;
    };

export type ParcelRecord = {
  parcel_id: string;
  county: string;
  situs_address: string;
  owner_name: string;
  land_use: string;
  future_land_use?: string | null;
  zoning: string | null;
  zoning_reason?: string | null;
  sqft?: Array<{ type: 'living' | 'lot'; value: number }>;
  beds: number | null;
  baths: number | null;
  year_built: number | null;
  last_sale_date: string | null;
  last_sale_price: number | null;
  land_value?: number | null;
  building_value?: number | null;
  total_value?: number | null;
  source: 'live' | 'cache';
  raw_source_url?: string;
  data_sources?: Array<{ name: string; url: string }>;
  provenance?: Record<string, { source: string; url: string }>;
  field_provenance?: Record<string, { source_url: string; raw_label: string }>;

  // Back-compat fields still emitted by the API.
  address?: string;
  property_class?: string;
  flu?: string;
  living_area_sqft?: number | null;
  lot_size_sqft?: number | null;
  lat: number;
  lng: number;
  geometry?: GeoJSON.Geometry;
};

export type ParcelSearchResponse = {
  county: string;
  zoning_options?: string[];
  future_land_use_options?: string[];
  summary: {
    count: number;
    source_counts: Record<string, number>;
    source_counts_legacy?: Record<string, number>;
  };
  records: ParcelRecord[];
  warnings?: string[];
  // Back-compat keys from the existing API
  count?: number;
  results?: unknown[];
};

export type ParcelsEnrichRequest = {
  county: string;
  parcel_ids: string[];
  limit?: number;
};

export type ParcelsEnrichResponse = {
  county: string;
  count: number;
  records: ParcelRecord[];
  errors?: Record<string, unknown>;
};

export type DebugPingResponse = {
  ok: boolean;
  server_time: string;
  git: { sha: string; branch: string };
};

export async function debugPing(): Promise<DebugPingResponse> {
  const resp = await fetch('/api/debug/ping', { headers: { Accept: 'application/json' } });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    const detail = text ? `: ${text}` : '';
    throw new Error(`HTTP ${resp.status} ${resp.statusText}${detail}`);
  }
  return (await resp.json()) as DebugPingResponse;
}

export type AdvancedSearchFilters = {
  no_permits_in_years?: number | null;
  permit_status?: string[] | null;
  permit_types?: string[] | null;
  city?: string | null;
  zip?: string | null;
  min_score?: number | null;
};

export type AdvancedSearchRequest = {
  county: string | null;
  text: string | null;
  fields: Array<'owner' | 'address' | 'parcel_id' | 'city' | 'zip'>;
  filters: AdvancedSearchFilters;
  sort:
    | 'relevance'
    | 'score_desc'
    | 'last_permit_oldest'
    | 'last_permit_newest';
  limit: number;
};

function buildSearchUrl(q: string, county: string): string {
  const params = new URLSearchParams({
    q,
    county,
  });
  return `/api/search?${params.toString()}`;
}

export async function search(q: string, county: string): Promise<SearchResult[]> {
  const resp = await fetch(buildSearchUrl(q, county), {
    method: 'GET',
    headers: {
      Accept: 'application/json',
    },
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    const detail = text ? `: ${text}` : '';
    throw new Error(`HTTP ${resp.status} ${resp.statusText}${detail}`);
  }

  const data: unknown = await resp.json();

  if (!Array.isArray(data)) {
    throw new Error('Unexpected response: expected an array');
  }

  // Runtime validation (lightweight, to stay strict-TS friendly)
  const results: SearchResult[] = [];
  for (const item of data) {
    if (!item || typeof item !== 'object') {
      continue;
    }

    const record = item as Record<string, unknown>;
    const owner = record.owner;
    const address = record.address;
    const countyField = record.county;
    const score = record.score;
    const parcelId = record.parcel_id;
    const source = record.source;

    if (
      typeof owner === 'string' &&
      typeof address === 'string' &&
      typeof countyField === 'string' &&
      typeof score === 'number'
    ) {
      const out: SearchResult = { owner, address, county: countyField, score };

      if (typeof parcelId === 'string' && parcelId.trim().length > 0) {
        out.parcel_id = parcelId;
      }
      if (typeof source === 'string' && source.trim().length > 0) {
        out.source = source;
      }

      results.push(out);
    }
  }

  return results;
}

export async function advancedSearch(payload: AdvancedSearchRequest): Promise<SearchResult[]> {
  const resp = await fetch('/api/search/advanced', {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    const detail = text ? `: ${text}` : '';
    throw new Error(`HTTP ${resp.status} ${resp.statusText}${detail}`);
  }

  const data: unknown = await resp.json();
  if (!Array.isArray(data)) {
    throw new Error('Unexpected response: expected an array');
  }

  const results: SearchResult[] = [];
  for (const item of data) {
    if (!item || typeof item !== 'object') {
      continue;
    }
    const record = item as Record<string, unknown>;

    const owner = record.owner;
    const address = record.address;
    const countyField = record.county;
    const score = record.score;

    if (
      typeof owner === 'string' &&
      typeof address === 'string' &&
      typeof countyField === 'string' &&
      typeof score === 'number'
    ) {
      const out: SearchResult = { owner, address, county: countyField, score };

      const parcelId = record.parcel_id;
      const source = record.source;
      const lastPermitDate = record.last_permit_date;
      const permitsLast15yCount = record.permits_last_15y_count;
      const matchedFields = record.matched_fields;

      if (typeof parcelId === 'string' && parcelId.trim().length > 0) {
        out.parcel_id = parcelId;
      }
      if (typeof source === 'string' && source.trim().length > 0) {
        out.source = source;
      }
      if (typeof lastPermitDate === 'string' || lastPermitDate === null) {
        out.last_permit_date = lastPermitDate;
      }
      if (typeof permitsLast15yCount === 'number') {
        out.permits_last_15y_count = permitsLast15yCount;
      }
      if (Array.isArray(matchedFields)) {
        out.matched_fields = matchedFields.filter((v) => typeof v === 'string') as string[];
      }

      results.push(out);
    }
  }

  return results;
}

export async function parcelsSearch(
  payload: ParcelSearchRequest | ParcelSearchRequestV2 | ParcelMapSearchRequest,
): Promise<ParcelSearchResponse> {
  const resp = await fetch('/api/parcels/search', {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ ...payload, include_geometry: payload.include_geometry ?? true }),
  })

  if (!resp.ok) {
    const text = await resp.text().catch(() => '')
    const detail = text ? `: ${text}` : ''
    throw new Error(`HTTP ${resp.status} ${resp.statusText}${detail}`)
  }

  const data = (await resp.json()) as ParcelSearchResponse
  if (!data || typeof data !== 'object' || !Array.isArray((data as any).records)) {
    throw new Error('Unexpected response: expected {records: [...]}' )
  }
  return data
}

export async function parcelsEnrich(
  payload: ParcelsEnrichRequest,
): Promise<ParcelsEnrichResponse> {
  const resp = await fetch('/api/parcels/enrich', {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })

  if (!resp.ok) {
    const text = await resp.text().catch(() => '')
    const detail = text ? `: ${text}` : ''
    throw new Error(`HTTP ${resp.status} ${resp.statusText}${detail}`)
  }

  const data = (await resp.json()) as ParcelsEnrichResponse
  if (!data || typeof data !== 'object' || !Array.isArray((data as any).records)) {
    throw new Error('Unexpected response: expected {records: [...]}' )
  }
  return data
}
