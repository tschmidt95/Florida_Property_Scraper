export type SearchResult = {
  owner: string;
  address: string;
  county: string;
  score: number;
  parcel_id?: string;
  source?: string;
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
      results.push({
        owner,
        address,
        county: countyField,
        score,
        parcel_id: typeof parcelId === 'string' ? parcelId : undefined,
        source: typeof source === 'string' ? source : undefined,
      });
    }
  }

  return results;
}

export async function scrape(
  county: string,
  query: string,
  limit: number = 50,
): Promise<SearchResult[]> {
  const resp = await fetch('/api/scrape', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify({ county, query, limit }),
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
    const parcelId = record.parcel_id;
    const source = record.source;

    if (
      typeof owner === 'string' &&
      typeof address === 'string' &&
      typeof countyField === 'string' &&
      typeof score === 'number'
    ) {
      results.push({
        owner,
        address,
        county: countyField,
        score,
        parcel_id: typeof parcelId === 'string' ? parcelId : undefined,
        source: typeof source === 'string' ? source : undefined,
      });
    }
  }

  return results;
}
