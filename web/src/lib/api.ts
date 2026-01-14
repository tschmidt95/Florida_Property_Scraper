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
  limit?: number;
  include_geometry?: boolean;
};

export type ParcelRecord = {
  parcel_id: string;
  county: string;
  address?: string;
  situs_address: string;
  owner_name: string;
  property_class: string;
  land_use: string;
  flu?: string;
  zoning: string;
  living_area_sqft: number | null;
  lot_size_sqft: number | null;
  beds: number | null;
  baths: number | null;
  year_built: number | null;
  last_sale_date: string | null;
  last_sale_price: number | null;
  source: 'local' | 'live' | 'geojson' | 'missing';
  lat: number;
  lng: number;
  geometry?: GeoJSON.Geometry;
};

export type ParcelSearchResponse = {
  county: string;
  summary: { count: number; source_counts: Record<string, number> };
  records: ParcelRecord[];
  warnings?: string[];
  // Back-compat keys from the existing API
  count?: number;
  results?: unknown[];
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
  payload: ParcelSearchRequest | ParcelSearchRequestV2,
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
