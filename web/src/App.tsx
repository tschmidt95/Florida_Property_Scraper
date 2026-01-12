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

  // Advanced search fields
  const [searchFields, setSearchFields] = useState({
    owner: true,
    address: true,
    parcel_id: true,
    city: false,
    zip: false,
  });
  const [noPermitsYears, setNoPermitsYears] = useState(15);
  const [sortBy, setSortBy] = useState('relevance');

  const activeRequestId = useRef(0);

  const counties = useMemo(
    () => ['Alachua', 'Broward', 'Duval', 'Hillsborough', 'Orange', 'Palm Beach', 'Seminole'],
    [],
  );

  async function runSearch() {
    const requestId = activeRequestId.current + 1;
    activeRequestId.current = requestId;

    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      // Build fields array from checkboxes
      const fields: string[] = [];
      if (searchFields.owner) fields.push('owner');
      if (searchFields.address) fields.push('address');
      if (searchFields.parcel_id) fields.push('parcel_id');
      if (searchFields.city) fields.push('city');
      if (searchFields.zip) fields.push('zip');

      const next = await advancedSearch({
        county: selectedCounty,
        text: query,
        fields,
        filters: {
          no_permits_in_years: noPermitsYears > 0 ? noPermitsYears : undefined,
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

          <div className="mt-6 space-y-4">
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Search In
              </div>
              <div className="mt-2 space-y-1">
                <label className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={searchFields.owner}
                    onChange={(e) =>
                      setSearchFields({ ...searchFields, owner: e.target.checked })
                    }
                    className="h-4 w-4 rounded border-slate-300"
                  />
                  Owner
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={searchFields.address}
                    onChange={(e) =>
                      setSearchFields({ ...searchFields, address: e.target.checked })
                    }
                    className="h-4 w-4 rounded border-slate-300"
                  />
                  Address
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={searchFields.parcel_id}
                    onChange={(e) =>
                      setSearchFields({ ...searchFields, parcel_id: e.target.checked })
                    }
                    className="h-4 w-4 rounded border-slate-300"
                  />
                  Parcel ID
                </label>
                <label className="flex items-center gap-2 text-sm text-slate-400" title="Not available in database">
                  <input
                    type="checkbox"
                    checked={searchFields.city}
                    onChange={(e) =>
                      setSearchFields({ ...searchFields, city: e.target.checked })
                    }
                    disabled
                    className="h-4 w-4 rounded border-slate-300"
                  />
                  City <span className="text-xs">(N/A)</span>
                </label>
                <label className="flex items-center gap-2 text-sm text-slate-400" title="Not available in database">
                  <input
                    type="checkbox"
                    checked={searchFields.zip}
                    onChange={(e) =>
                      setSearchFields({ ...searchFields, zip: e.target.checked })
                    }
                    disabled
                    className="h-4 w-4 rounded border-slate-300"
                  />
                  ZIP <span className="text-xs">(N/A)</span>
                </label>
              </div>
            </div>

            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Filters
              </div>
              <div className="mt-2 space-y-2">
                <div>
                  <label className="text-xs text-slate-600">
                    No permits in last N years
                  </label>
                  <input
                    type="number"
                    min="0"
                    value={noPermitsYears}
                    onChange={(e) => setNoPermitsYears(parseInt(e.target.value, 10) || 0)}
                    className="mt-1 w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-sm outline-none focus:border-slate-400"
                  />
                </div>
              </div>
            </div>

            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Sort
              </div>
              <div className="mt-2">
                <select
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                  className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-sm outline-none focus:border-slate-400"
                >
                  <option value="relevance">Relevance</option>
                  <option value="score_desc">Score (High to Low)</option>
                  <option value="last_permit_oldest">Last Permit (Oldest)</option>
                  <option value="last_permit_newest">Last Permit (Newest)</option>
                </select>
              </div>
            </div>
          </div>

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
                      <th className="px-3 py-2">Last Permit</th>
                      <th className="px-3 py-2">Permits (15y)</th>
                      <th className="px-3 py-2">Score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {loading ? (
                      <tr className="border-t">
                        <td className="px-3 py-3 text-slate-500" colSpan={7}>
                          Loading…
                        </td>
                      </tr>
                    ) : error ? (
                      <tr className="border-t">
                        <td className="px-3 py-3 text-red-700" colSpan={7}>
                          {error}
                        </td>
                      </tr>
                    ) : results.length === 0 ? (
                      <tr className="border-t">
                        <td className="px-3 py-3 text-slate-500" colSpan={7}>
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
                          <td className="px-3 py-2">{r.last_permit_date ?? '—'}</td>
                          <td className="px-3 py-2">{r.permits_last_15y_count ?? 0}</td>
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
