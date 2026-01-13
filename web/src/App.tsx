import { useMemo, useRef, useState } from 'react';

import { advancedSearch, type SearchResult } from './lib/api';

export default function App() {
  const [query, setQuery] = useState('');
  const [geometrySearchEnabled, setGeometrySearchEnabled] = useState(false);
  const [selectedCounty, setSelectedCounty] = useState('Orange');
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

  const activeRequestId = useRef(0);

  async function runLookup() {
    setLookupLoading(true);
    setLookupError(null);
    setLookupResult(null);
    try {
      const resp = await fetch('/api/lookup/address', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ county: selectedCounty.toLowerCase(), address: query, include_contacts: false }),
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
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="flex items-center gap-3 border-b bg-white px-4 py-3">
        <div className="font-semibold tracking-tight">Florida Property Scraper</div>

        <div className="ml-auto flex w-full max-w-2xl items-center gap-2">
          <div className="hidden shrink-0 text-xs text-slate-500 sm:block">
            Selected:{' '}
            <span className="font-medium text-slate-700">{selectedCounty}</span>
          </div>
          <input
            className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm shadow-sm outline-none focus:border-slate-400"
            placeholder="Search owners, addresses, parcels…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                void runSearch();
              }
              if (e.key === 'Escape') {
                e.preventDefault();
                clearSearch();
              }
            }}
          />
          <button
            type="button"
            className="rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-slate-800"
            onClick={() => void runSearch()}
            disabled={loading}
          >
            {loading ? 'Running…' : 'Run'}
          </button>
        </div>
      </header>

      <div className="flex">
        <aside className="w-72 border-r bg-white p-4">
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
        </aside>

        <main className="flex-1 p-4">
          <div className="grid gap-4">
            <section className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm">
              <div className="text-sm font-medium">Map</div>
              <div className="mt-3 flex h-64 items-center justify-center rounded-md border border-dashed border-slate-200 bg-slate-50 text-sm text-slate-500">
                Map placeholder
              </div>
              {geometrySearchEnabled ? (
                <div className="mt-2 text-xs text-slate-500">
                  Geometry search is enabled (UI-only).
                </div>
              ) : null}
            </section>

            <section className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-sm font-medium">Property Lookup</div>
                  <div className="mt-1 text-xs text-slate-500 sm:hidden">
                    County: <span className="font-medium text-slate-700">{selectedCounty}</span>
                  </div>
                </div>
                <div className="flex w-full max-w-2xl items-center gap-2">
                  <input
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm shadow-sm outline-none focus:border-slate-400"
                    placeholder="Address (e.g., 105 Pineapple Lane)"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        void runSearch();
                      }
                      if (e.key === 'Escape') {
                        e.preventDefault();
                        clearSearch();
                      }
                    }}
                  />
                  <button
                    type="button"
                    className="rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-slate-800"
                    onClick={() => void runLookup()}
                    disabled={loading}
                  >
                    {loading ? 'Searching…' : 'Lookup'}
                  </button>
                </div>
              </div>

              <div className="mt-4">
                {lookupLoading ? (
                  <div className="text-sm text-slate-500">Looking up…</div>
                ) : lookupError ? (
                  <div className="text-sm text-red-700">{lookupError}</div>
                ) : lookupResult ? (
                  <div className="rounded-md border border-slate-200 bg-white p-3">
                    <div className="text-sm font-medium">Property Card</div>
                    <div className="mt-2 text-sm">
                      <div><strong>Address:</strong> {lookupResult.address}</div>
                      <div><strong>County:</strong> {lookupResult.county}</div>
                      <div><strong>Owner:</strong> {lookupResult.owner_name ?? '—'}</div>
                      <div><strong>Mailing:</strong> {lookupResult.owner_mailing_address ?? '—'}</div>
                      <div className="mt-2 text-xs text-slate-500">Beds: {lookupResult.property_fields?.beds ?? '—'} &middot; Baths: {lookupResult.property_fields?.baths ?? '—'} &middot; SF: {lookupResult.property_fields?.sf ?? '—'}</div>
                      <div className="mt-2 text-xs text-slate-500">Last sale: {lookupResult.last_sale?.date ?? '—'} {lookupResult.last_sale?.price ? `($${lookupResult.last_sale.price})` : ''}</div>
                      <div className="mt-2 text-xs text-slate-500">Contacts: {lookupResult.contacts?.phones?.length ? lookupResult.contacts.phones.join(', ') : 'Contacts unavailable'}</div>
                    </div>
                  </div>
                ) : (
                  <div className="text-sm text-slate-500">No lookup yet. Type an address and press Lookup.</div>
                )}
              </div>

              <div className="mt-3 overflow-hidden rounded-md border border-slate-200">
                <table className="w-full border-collapse text-left text-sm">
                  <thead className="bg-slate-50 text-xs text-slate-600">
                    <tr>
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
                      <tr className="border-t">
                        <td className="px-3 py-3 text-slate-500" colSpan={8}>
                          Loading…
                        </td>
                      </tr>
                    ) : error ? (
                      <tr className="border-t">
                        <td className="px-3 py-3 text-red-700" colSpan={8}>
                          {error}
                        </td>
                      </tr>
                    ) : results.length === 0 ? (
                      <tr className="border-t">
                        <td className="px-3 py-3 text-slate-500" colSpan={8}>
                          {hasSearched ? 'No matches found.' : 'No results yet.'}
                        </td>
                      </tr>
                    ) : (
                      results.map((r, idx) => (
                        <tr
                          key={`${r.owner}-${r.address}-${idx}`}
                          className="border-t hover:bg-slate-50"
                        >
                          <td className="px-3 py-2">{r.owner}</td>
                          <td className="px-3 py-2">{r.address}</td>
                          <td className="px-3 py-2">{r.county}</td>
                          <td className="px-3 py-2">{r.parcel_id ?? ''}</td>
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
              <div className="mt-2 flex items-center justify-between gap-3 text-xs text-slate-500">
                <div>
                  Press <span className="font-medium text-slate-700">Enter</span> to run,
                  <span className="font-medium text-slate-700"> Escape</span> to clear.
                </div>
                {hasSearched && !loading && !error ? (
                  <div>{results.length} result{results.length === 1 ? '' : 's'}</div>
                ) : null}
              </div>
            </section>
          </div>
        </main>
      </div>
    </div>
  );
}
