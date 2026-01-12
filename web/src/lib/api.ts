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

export type AdvancedSearchFilters = {
  no_permits_in_years?: number | null;
  permit_status?: string[] | null;
  permit_types?: string[] | null;
  city?: string | null;
  zip?: string | null;
  min_score?: number | null;
};

export type AdvancedSearchRequest = {
  county?: string | null;
  text?: string | null;
  fields: string[];
  filters: AdvancedSearchFilters;
  sort: 'relevance' | 'score_desc' | 'last_permit_oldest' | 'last_permit_newest';
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
    const lastPermitDate = record.last_permit_date;
    const permitsLast15yCount = record.permits_last_15y_count;
    const matchedFields = record.matched_fields;

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
      if (typeof lastPermitDate === 'string' || lastPermitDate === null) {
        out.last_permit_date = lastPermitDate;
      }
      if (typeof permitsLast15yCount === 'number') {
        out.permits_last_15y_count = permitsLast15yCount;
      }
      if (Array.isArray(matchedFields)) {
        out.matched_fields = matchedFields.filter((f) => typeof f === 'string');
      }

      results.push(out);
    }
  }

  return results;
}

export async function advancedSearch(request: AdvancedSearchRequest): Promise<SearchResult[]> {
  const resp = await fetch('/api/search/advanced', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(request),
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
    const lastPermitDate = record.last_permit_date;
    const permitsLast15yCount = record.permits_last_15y_count;
    const matchedFields = record.matched_fields;

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
      if (typeof lastPermitDate === 'string' || lastPermitDate === null) {
        out.last_permit_date = lastPermitDate;
      }
      if (typeof permitsLast15yCount === 'number') {
        out.permits_last_15y_count = permitsLast15yCount;
      }
      if (Array.isArray(matchedFields)) {
        out.matched_fields = matchedFields.filter((f) => typeof f === 'string');
      }

      results.push(out);
    }
  }

  return results;
}
