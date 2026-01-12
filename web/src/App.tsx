import { useMemo, useRef, useState } from 'react';

import { advancedSearch, type SearchResult } from './lib/api';

export default function App() {
  const [query, setQuery] = useState('');
  const [geometrySearchEnabled, setGeometrySearchEnabled] = useState(false);
  const [selectedCounty, setSelectedCounty] = useState('Orange');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);

  // Advanced search state
  const [searchFields, setSearchFields] = useState({
    owner_name: true,
    situs_address: true,
    parcel_id: true,
    city: false,
    zip: false,
  });
  const [noPermitsYears, setNoPermitsYears] = useState(15);
  const [sortBy, setSortBy] = useState<'relevance' | 'score_desc' | 'last_permit_oldest' | 'last_permit_newest'>('relevance');

  const activeRequestId = useRef(0);

  const counties = useMemo(
    () => ['Alachua', 'Broward', 'Duval', 'Hillsborough', 'Orange', 'Palm Beach'],
    [],
  );

  async function runSearch() {
    const requestId = activeRequestId.current + 1;
    activeRequestId.current = requestId;

    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      // Build fields list
      const fields = Object.entries(searchFields)
        .filter(([_, enabled]) => enabled)
        .map(([field, _]) => field);

      const next = await advancedSearch({
        county: selectedCounty,
        text: query || null,
        fields,
        filters: {
          no_permits_in_years: noPermitsYears > 0 ? noPermitsYears : null,
        },
        sort: sortBy,
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
              Search in
            </div>
            <div className="mt-2 space-y-2">
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  className="rounded border-slate-300"
                  checked={searchFields.owner_name}
                  onChange={(e) =>
                    setSearchFields({ ...searchFields, owner_name: e.target.checked })
                  }
                />
                <span>Owner Name</span>
              </label>
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  className="rounded border-slate-300"
                  checked={searchFields.situs_address}
                  onChange={(e) =>
                    setSearchFields({ ...searchFields, situs_address: e.target.checked })
                  }
                />
                <span>Address</span>
              </label>
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  className="rounded border-slate-300"
                  checked={searchFields.parcel_id}
                  onChange={(e) =>
                    setSearchFields({ ...searchFields, parcel_id: e.target.checked })
                  }
                />
                <span>Parcel ID</span>
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-400">
                <input
                  type="checkbox"
                  className="rounded border-slate-300"
                  checked={searchFields.city}
                  onChange={(e) =>
                    setSearchFields({ ...searchFields, city: e.target.checked })
                  }
                  disabled
                />
                <span>City (not available)</span>
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-400">
                <input
                  type="checkbox"
                  className="rounded border-slate-300"
                  checked={searchFields.zip}
                  onChange={(e) =>
                    setSearchFields({ ...searchFields, zip: e.target.checked })
                  }
                  disabled
                />
                <span>ZIP (not available)</span>
              </label>
            </div>
          </div>

          <div className="mt-6">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Filters
            </div>
            <div className="mt-2">
              <label className="text-sm font-medium">No permits in last</label>
              <div className="mt-1 flex items-center gap-2">
                <input
                  type="number"
                  className="w-20 rounded-md border border-slate-200 px-2 py-1 text-sm"
                  value={noPermitsYears}
                  onChange={(e) => setNoPermitsYears(parseInt(e.target.value) || 0)}
                  min="0"
                />
                <span className="text-sm text-slate-600">years</span>
              </div>
            </div>
          </div>

          <div className="mt-6">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Sort by
            </div>
            <div className="mt-2">
              <select
                className="w-full rounded-md border border-slate-200 px-2 py-1 text-sm"
                value={sortBy}
                onChange={(e) =>
                  setSortBy(
                    e.target.value as
                      | 'relevance'
                      | 'score_desc'
                      | 'last_permit_oldest'
                      | 'last_permit_newest',
                  )
                }
              >
                <option value="relevance">Relevance</option>
                <option value="score_desc">Score (High to Low)</option>
                <option value="last_permit_oldest">Last Permit (Oldest)</option>
                <option value="last_permit_newest">Last Permit (Newest)</option>
              </select>
            </div>
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
                  <div className="text-sm font-medium">Results</div>
                  <div className="mt-1 text-xs text-slate-500 sm:hidden">
                    Selected:{' '}
                    <span className="font-medium text-slate-700">{selectedCounty}</span>
                  </div>
                </div>

                {query.trim().length === 0 ? (
                  <div className="text-right text-xs text-slate-500">
                    Tip: empty query may return broad results.
                  </div>
                ) : null}
              </div>
              <div className="mt-3 overflow-hidden rounded-md border border-slate-200">
                <table className="w-full border-collapse text-left text-sm">
                  <thead className="bg-slate-50 text-xs text-slate-600">
                    <tr>
                      <th className="px-3 py-2">Owner</th>
                      <th className="px-3 py-2">Address</th>
                      <th className="px-3 py-2">County</th>
                      <th className="px-3 py-2">Parcel</th>
                      <th className="px-3 py-2">Source</th>
                      <th className="px-3 py-2">Score</th>
                      <th className="px-3 py-2">Last Permit</th>
                      <th className="px-3 py-2">Permits(15y)</th>
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
                          <td className="px-3 py-2">{r.source ?? ''}</td>
                          <td className="px-3 py-2">{r.score}</td>
                          <td className="px-3 py-2">
                            {r.last_permit_date ?? '-'}
                          </td>
                          <td className="px-3 py-2">
                            {r.permits_last_15y_count ?? 0}
                          </td>
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
