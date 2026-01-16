import fs from 'node:fs';
import { spawn } from 'node:child_process';

import { chromium } from 'playwright';

const BASE_URL = (process.env.BASE_URL || 'http://127.0.0.1:8000').replace(/\/$/, '');
const VITE_URL = (process.env.VITE_URL || 'http://127.0.0.1:5173').replace(/\/$/, '');
const DEBUG_LOG = process.env.FPS_SEARCH_DEBUG_LOG || '.fps_parcels_search_debug.jsonl';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithTimeout(url, { method = 'GET', headers, body, timeoutMs = 3000 } = {}) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { method, headers, body, signal: controller.signal });
  } finally {
    clearTimeout(t);
  }
}

async function ensureViteUp() {
  // If Vite is already up, do nothing.
  try {
    const r = await fetchWithTimeout(`${VITE_URL}/`, { timeoutMs: 1500 });
    if (r && r.status) return { ok: true, started: false, pid: null };
  } catch {
    // ignore
  }

  // Start Vite in the background with output suppressed (no files).
  let child = null;
  try {
    child = spawn('bash', ['-lc', 'cd web && npm run dev -- --host 127.0.0.1 --port 5173'], {
      stdio: 'ignore',
      detached: true,
    });
    child.unref();
  } catch {
    // ignore
  }

  const start = Date.now();
  while (Date.now() - start < 25_000) {
    try {
      const r = await fetchWithTimeout(`${VITE_URL}/`, { timeoutMs: 1500 });
      if (r && r.status) return { ok: true, started: true, pid: child && child.pid ? child.pid : null };
    } catch {
      // ignore
    }
    await sleep(250);
  }
  return { ok: false, started: true, pid: child && child.pid ? child.pid : null };
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function safeNum(v) {
  if (typeof v === 'number') return v;
  if (typeof v === 'string') {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function hasText(v) {
  return typeof v === 'string' && v.trim().length > 0;
}

function hasPositiveNumber(v) {
  const n = safeNum(v);
  return typeof n === 'number' && Number.isFinite(n) && n > 0;
}

function readJsonlBySearchId(filePath, searchId) {
  try {
    const txt = fs.readFileSync(filePath, 'utf-8');
    const lines = txt.split(/\r?\n/).filter(Boolean);
    const out = [];
    for (const line of lines) {
      try {
        const obj = JSON.parse(line);
        if (obj && obj.search_id === searchId) out.push(obj);
      } catch {
        // ignore
      }
    }
    return out;
  } catch {
    return [];
  }
}

function todayIsoDate() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function isoToUsDate(iso) {
  if (!hasText(iso)) return '';
  const m = String(iso).trim().match(/^([0-9]{4})-([0-9]{2})-([0-9]{2})$/);
  if (!m) return '';
  const yyyy = Number(m[1]);
  const mm = Number(m[2]);
  const dd = Number(m[3]);
  if (!yyyy || !mm || !dd) return '';
  return `${mm}/${dd}/${yyyy}`;
}

function parseDateAny(s) {
  if (!hasText(s)) return null;
  const str = String(s).trim();
  // ISO date or ISO timestamp.
  const t = Date.parse(str);
  if (Number.isFinite(t)) return t;
  // M/D/YYYY
  const m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    const mm = Number(m[1]);
    const dd = Number(m[2]);
    const yyyy = Number(m[3]);
    const d = Date.parse(`${yyyy}-${String(mm).padStart(2, '0')}-${String(dd).padStart(2, '0')}`);
    return Number.isFinite(d) ? d : null;
  }
  return null;
}

async function main() {
  // Keep the repo clean even if the backend writes debug artifacts.
  try {
    fs.unlinkSync(DEBUG_LOG);
  } catch {
    // ignore
  }

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  async function waitRunReady() {
    const runBtn = page.getByRole('button', { name: /^Run$/ });
    await runBtn.waitFor({ state: 'visible', timeout: 30_000 });
    const start = Date.now();
    while (Date.now() - start < 30_000) {
      try {
        if (await runBtn.isEnabled()) return runBtn;
      } catch {
        // ignore
      }
      await page.waitForTimeout(100);
    }
    throw new Error('FAIL: Run button never became enabled');
  }

  page.on('console', (msg) => {
    const t = msg.text() || '';
    if (msg.type() === 'error' || msg.type() === 'warning') {
      console.log(`[browser:${msg.type()}]`, t);
      return;
    }
    if (t.includes('[FILTERDBG]') || t.includes('[DRAWDBG]') || t.includes('[RUNDBG]') || t.includes('__BUILD_MARKER__')) {
      console.log(`[browser:${msg.type()}]`, t);
    }
  });

  page.on('pageerror', (err) => {
    console.log('[pageerror]', String(err && err.stack ? err.stack : err));
  });

  console.log(`== open ${BASE_URL}/ ==`);
  await page.goto(`${BASE_URL}/`, { waitUntil: 'domcontentloaded' });

  const maps = page.locator('.leaflet-container');
  await maps.first().waitFor({ state: 'visible', timeout: 30_000 });

  const mapCount = await maps.count();
  assert(mapCount === 1, `FAIL: expected exactly 1 .leaflet-container, saw ${mapCount}`);

  const toolbarCount = await page.locator('.leaflet-draw-toolbar').count();
  const polygonBtnCount = await page.locator('a.leaflet-draw-draw-polygon').count();
  console.log('draw_ui:', { toolbarCount, polygonBtnCount });
  assert(toolbarCount === 1, `FAIL: expected exactly 1 .leaflet-draw-toolbar, saw ${toolbarCount}`);
  assert(polygonBtnCount === 1, `FAIL: expected exactly 1 polygon draw button, saw ${polygonBtnCount}`);

  const map = maps.first();
  const mapBox0 = await map.boundingBox();
  assert(mapBox0, 'selected map bounding box missing');

  const polygonBtn = map.locator('.leaflet-draw-toolbar a.leaflet-draw-draw-polygon');
  await polygonBtn.waitFor({ state: 'visible', timeout: 30_000 });

  console.log('== select polygon tool ==');
  await polygonBtn.click();

  const box = mapBox0;
  const pts = [
    [0.30, 0.30],
    [0.70, 0.30],
    [0.75, 0.60],
    [0.50, 0.75],
    [0.25, 0.60],
  ];

  console.log('== draw polygon (5 clicks) ==');
  for (const [fx, fy] of pts) {
    const x = Math.round(box.x + box.width * fx);
    const y = Math.round(box.y + box.height * fy);
    await page.mouse.click(x, y);
    await page.waitForTimeout(120);
  }

  console.log('== finish polygon ==');
  const finishAction = page.locator('.leaflet-draw-actions a').filter({ hasText: /finish/i }).first();
  if (await finishAction.count()) {
    await finishAction.click();
  } else {
    await page.keyboard.press('Enter');
  }

  const runBtn = await waitRunReady();

  console.log('== click Run (baseline) ==');
  const [response0] = await Promise.all([
    page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
      timeout: 60_000,
    }),
    runBtn.click(),
  ]);

  const post0 = response0.request().postData() || '';
  let payload0 = null;
  try {
    payload0 = JSON.parse(post0);
  } catch {
    payload0 = null;
  }
  assert(payload0 && typeof payload0 === 'object', 'FAIL: could not parse baseline request payload JSON');
  assert(payload0.polygon_geojson, 'FAIL: baseline request missing polygon_geojson');

  // Baseline UI should NOT default last-sale dates.
  const today = todayIsoDate();
  const f0 = payload0.filters || null;
  if (f0) {
    assert(!hasText(f0.last_sale_date_start), 'FAIL: baseline UI sent last_sale_date_start unexpectedly');
    assert(!hasText(f0.last_sale_date_end), 'FAIL: baseline UI sent last_sale_date_end unexpectedly');
  }
  assert(
    !hasText(f0?.last_sale_date_start) && !hasText(f0?.last_sale_date_end),
    'FAIL: baseline UI should not set last sale date range'
  );
  assert(
    !(String(f0?.last_sale_date_start || '') === today || String(f0?.last_sale_date_end || '') === today),
    'FAIL: baseline UI appears to default last sale date to today'
  );

  const json0 = await response0.json().catch(() => ({}));
  const summary0 = json0.summary || {};
  const zoningOpts0 = Array.isArray(json0.zoning_options) ? json0.zoning_options : [];
  const fluOpts0 = Array.isArray(json0.future_land_use_options) ? json0.future_land_use_options : [];

  const baselineFiltered = safeNum(summary0.filtered_count) ?? (Array.isArray(json0.records) ? json0.records.length : 0);
  const baselineCandidate = safeNum(summary0.candidate_count) ?? null;
  console.log('baseline_counts:', { candidate_count: baselineCandidate, filtered_count: baselineFiltered });
  assert(baselineFiltered > 0, 'FAIL: expected baseline filtered_count > 0');

  if (baselineCandidate !== null) {
    const expected = `Showing ${baselineFiltered} of ${baselineCandidate} in polygon`;
    await page.getByText(expected).waitFor({ state: 'visible', timeout: 30_000 });
  }

  // Ensure the button is clickable again before setting filters and re-running.
  await waitRunReady();

  // Set acceptance filters via the UI.
  console.log('== set filters: min_sqft=2000 max_sqft=2700 min_acres=0.5 ==');

  const minSqftInput = page.locator('label', { hasText: 'Min Sqft' }).locator('input');
  const maxSqftInput = page.locator('label', { hasText: 'Max Sqft' }).locator('input');
  await minSqftInput.fill('2000');
  await maxSqftInput.fill('2700');

  const unitSelect = page.locator('label', { hasText: 'Parcel Size Unit' }).locator('select');
  await unitSelect.selectOption('acres');

  const minParcelInput = page.locator('label', { hasText: 'Min Parcel Size' }).locator('input');
  const maxParcelInput = page.locator('label', { hasText: 'Max Parcel Size' }).locator('input');
  await minParcelInput.fill('0.5');
  await maxParcelInput.fill('');

  const enrichToggle = page.locator('label', { hasText: /Auto-enrich missing/i }).locator('input[type="checkbox"]');
  if (await enrichToggle.count()) {
    // Keep proofs deterministic/fast: avoid potentially slow enrich calls.
    if (await enrichToggle.isChecked()) await enrichToggle.uncheck();
    console.log('auto_enrich_checked:', await enrichToggle.isChecked());
  }

  await page.getByText('Filters active').waitFor({ state: 'visible', timeout: 10_000 });

  console.log('== click Run (filtered) ==');
  await waitRunReady();
  const [response1] = await Promise.all([
    page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
      timeout: 180_000,
    }),
    runBtn.click(),
  ]);

  const post1 = response1.request().postData() || '';
  let payload1 = null;
  try {
    payload1 = JSON.parse(post1);
  } catch {
    payload1 = null;
  }

  assert(payload1 && typeof payload1 === 'object', 'FAIL: could not parse filtered request payload JSON');
  assert(payload1.filters, 'FAIL: filtered run did not include payload.filters');
  assert(payload1.filters.min_sqft === 2000, 'FAIL: expected payload.filters.min_sqft == 2000');
  assert(payload1.filters.max_sqft === 2700, 'FAIL: expected payload.filters.max_sqft == 2700');
  assert(payload1.filters.min_acres === 0.5, 'FAIL: expected payload.filters.min_acres == 0.5');

  // --- API-level deterministic filter + sorting proof (end-to-end without extra UI flake) ---
  const baseReq = {
    county: payload0.county || 'orange',
    polygon_geojson: payload0.polygon_geojson,
    live: true,
    include_geometry: false,
    enrich: false,
    limit: 250,
  };

  async function apiSearchVia(baseUrl, { filters, sort, debug, timeoutMs }) {
    const req = {
      ...baseReq,
      filters: filters && Object.keys(filters).length ? filters : undefined,
      sort: sort || 'relevance',
      debug: debug === true,
    };
    const resp = await fetchWithTimeout(`${baseUrl}/api/parcels/search`, {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
      body: JSON.stringify(req),
      timeoutMs: typeof timeoutMs === 'number' ? timeoutMs : 130_000,
    });
    const txt = await resp.text().catch(() => '');
    if (!resp.ok) {
      console.log('apiSearch_fail:', { baseUrl, status: resp.status, statusText: resp.statusText, req });
      console.log('apiSearch_body:', String(txt || '').slice(0, 800));
      assert(false, `FAIL: apiSearch HTTP ${resp.status} ${resp.statusText}: ${String(txt || '').slice(0, 200)}`);
    }
    let j = {};
    try {
      j = JSON.parse(txt);
    } catch {
      j = {};
    }
    const s = j.summary || {};
    const recs = Array.isArray(j.records) ? j.records : [];
    const warnings = Array.isArray(j.warnings) ? j.warnings : [];
    const filteredCount = safeNum(s.filtered_count) ?? recs.length;
    const candidateCount = safeNum(s.candidate_count) ?? null;
    return { json: j, records: recs, warnings, filteredCount, candidateCount, req };
  }

  async function apiSearch({ filters, sort, debug, timeoutMs }) {
    return apiSearchVia(BASE_URL, { filters, sort, debug, timeoutMs });
  }

  const baselineApi = await apiSearch({ filters: {}, sort: 'relevance' });
  const baselineApiFiltered = baselineApi.filteredCount;
  const baselineApiCandidate = baselineApi.candidateCount;
  console.log('baseline_api_counts:', {
    candidate_count: baselineApiCandidate,
    filtered_count: baselineApiFiltered,
  });
  assert(baselineApiFiltered > 0, 'FAIL: expected API baseline filtered_count > 0');

  // --- Vite proxy path regression check (exercises :5173 -> /api proxy -> :8000) ---
  const vite = await ensureViteUp();
  console.log('vite_up:', { ok: vite.ok, started: vite.started, pid: vite.pid });
  assert(vite.ok, 'FAIL: Vite dev server not reachable on :5173');

  const viteT0 = Date.now();
  const viteBaseline = await apiSearchVia(VITE_URL, { filters: {}, sort: 'relevance', debug: true, timeoutMs: 130_000 });
  const viteElapsedMs = Date.now() - viteT0;
  const hasTiming = !!(viteBaseline.json && typeof viteBaseline.json === 'object' && viteBaseline.json.debug_timing_ms);
  console.log('vite_proxy_baseline:', {
    elapsed_ms: viteElapsedMs,
    candidate_count: viteBaseline.candidateCount,
    filtered_count: viteBaseline.filteredCount,
    has_debug_timing_ms: hasTiming,
  });

  assert(viteBaseline.filteredCount > 0, 'FAIL: expected Vite proxy baseline filtered_count > 0');
  assert(hasTiming, 'FAIL: expected debug_timing_ms in Vite proxy response (debug=true)');
  if (viteBaseline.json && viteBaseline.json.debug_timing_ms) {
    // Require at least one known stage key.
    const keys = Object.keys(viteBaseline.json.debug_timing_ms || {});
    assert(
      keys.includes('candidate_query') || keys.includes('parse_payload'),
      'FAIL: debug_timing_ms missing expected stage keys'
    );
  }

  // 0) Empty filters must be a no-op (all keys present but blank/null/empty).
  const emptyFilters = {
    min_sqft: null,
    max_sqft: '',
    min_acres: '',
    max_acres: null,
    min_lot_size_sqft: '',
    max_lot_size_sqft: null,
    lot_size_unit: '',
    min_lot_size: '',
    max_lot_size: '',
    min_beds: '',
    min_baths: '',
    min_year_built: '',
    max_year_built: '',
    property_type: '',
    zoning: '',
    zoning_in: [],
    future_land_use_in: [],
    min_value: '',
    max_value: '',
    min_land_value: '',
    max_land_value: '',
    min_building_value: '',
    max_building_value: '',
    last_sale_date_start: '',
    last_sale_date_end: '',
  };
  const emptyNoop = await apiSearch({ filters: emptyFilters, sort: 'relevance' });
  assert(
    emptyNoop.filteredCount === baselineApiFiltered,
    `FAIL: empty filters changed filtered_count (${emptyNoop.filteredCount}) vs baseline (${baselineApiFiltered})`
  );
  if (baselineApiCandidate !== null && emptyNoop.candidateCount !== null) {
    assert(
      emptyNoop.candidateCount === baselineApiCandidate,
      `FAIL: empty filters changed candidate_count (${emptyNoop.candidateCount}) vs baseline (${baselineApiCandidate})`
    );
  }

  function pickThreshold(label, thresholds, runFn) {
    return (async () => {
      let best = null;
      for (const t of thresholds) {
        const r = await runFn(t);
        if (r.filteredCount > 0) {
          best = { threshold: t, ...r };
          if (r.filteredCount < baselineApiFiltered) break; // prefer an actual reduction
        }
      }
      assert(best, `FAIL: could not find any ${label} threshold with filtered_count > 0`);
      return best;
    })();
  }

  // 1) min_sqft adaptive
  const sqftThresholds = [3000, 2500, 2000, 1500, 1200, 1000, 800];
  const sqftPick = await pickThreshold('min_sqft', sqftThresholds, async (t) =>
    apiSearch({ filters: { min_sqft: t }, sort: 'relevance' }),
  );
  console.log('min_sqft_pick:', { t: sqftPick.threshold, filtered_count: sqftPick.filteredCount });
  assert(sqftPick.filteredCount <= baselineApiFiltered, 'FAIL: min_sqft increased filtered_count (unexpected)');

  // 2) min_baths adaptive
  const bathThresholds = [4, 3.5, 3, 2.5, 2];
  const bathsPick = await pickThreshold('min_baths', bathThresholds, async (t) =>
    apiSearch({ filters: { min_baths: t }, sort: 'relevance' }),
  );
  console.log('min_baths_pick:', { t: bathsPick.threshold, filtered_count: bathsPick.filteredCount });
  assert(bathsPick.filteredCount <= baselineApiFiltered, 'FAIL: min_baths increased filtered_count (unexpected)');

  // 3) lot size range: try both sqft and acres forms.
  const lotSqftThresholds = [20000, 12000, 9000, 8000, 6000];
  const lotSqftPick = await pickThreshold('min_lot_size_sqft', lotSqftThresholds, async (t) =>
    apiSearch({ filters: { min_lot_size_sqft: t }, sort: 'relevance' }),
  );
  console.log('min_lot_size_sqft_pick:', { t: lotSqftPick.threshold, filtered_count: lotSqftPick.filteredCount });
  assert(lotSqftPick.filteredCount <= baselineApiFiltered, 'FAIL: lot sqft filter increased filtered_count (unexpected)');

  const lotAcreThresholds = [2, 1, 0.5, 0.25, 0.1];
  const lotAcrePick = await pickThreshold('min_acres', lotAcreThresholds, async (t) =>
    apiSearch({ filters: { min_acres: t }, sort: 'relevance' }),
  );
  console.log('min_acres_pick:', { t: lotAcrePick.threshold, filtered_count: lotAcrePick.filteredCount });
  assert(lotAcrePick.filteredCount <= baselineApiFiltered, 'FAIL: acres filter increased filtered_count (unexpected)');

  // 4) last sale date range (wide then tighter)
  const saleWide = await apiSearch({
    filters: { last_sale_date_start: '1990-01-01', last_sale_date_end: today },
    sort: 'relevance',
  });
  console.log('sale_wide:', { filtered_count: saleWide.filteredCount });
  assert(saleWide.filteredCount <= baselineApiFiltered, 'FAIL: sale-wide increased filtered_count (unexpected)');

  const saleTight = await apiSearch({
    filters: { last_sale_date_start: '2020-01-01', last_sale_date_end: today },
    sort: 'relevance',
  });
  console.log('sale_tight:', { filtered_count: saleTight.filteredCount });
  assert(saleTight.filteredCount <= saleWide.filteredCount, 'FAIL: sale-tight should be non-increasing vs sale-wide');

  // 4a) blank last-sale dates must be a no-op.
  const saleBlankNoop = await apiSearch({
    filters: { last_sale_date_start: null, last_sale_date_end: null },
    sort: 'relevance',
  });
  assert(
    saleBlankNoop.filteredCount === baselineApiFiltered,
    `FAIL: blank last_sale_date range changed filtered_count (${saleBlankNoop.filteredCount}) vs baseline (${baselineApiFiltered})`
  );

  // 4b) ISO vs US date formats should behave the same.
  const todayUs = isoToUsDate(today);
  assert(hasText(todayUs), 'FAIL: could not compute todayUs');

  const saleTightIso = await apiSearch({
    filters: { last_sale_date_start: '2020-01-01', last_sale_date_end: today },
    sort: 'relevance',
  });
  const saleTightUs = await apiSearch({
    filters: { last_sale_date_start: '01/01/2020', last_sale_date_end: todayUs },
    sort: 'relevance',
  });
  const saleDateFormatIsoOk = saleTightIso.filteredCount === saleTight.filteredCount;
  const saleDateFormatUsOk = saleTightUs.filteredCount === saleTightIso.filteredCount;
  assert(saleDateFormatIsoOk, 'FAIL: ISO date format produced unexpected filtered_count');
  assert(saleDateFormatUsOk, 'FAIL: US date format did not match ISO filtered_count');

  // 4c) start>end normalization: backend should swap and warn (ISO).
  const saleSwapped = await apiSearch({
    filters: { last_sale_date_start: today, last_sale_date_end: '1990-01-01' },
    sort: 'relevance',
  });
  const swapWarn = saleSwapped.warnings.some((w) => String(w || '').toLowerCase().includes('swapped date range'));
  assert(swapWarn, 'FAIL: expected swapped date range warning in response.warnings');
  assert(
    saleSwapped.filteredCount === saleWide.filteredCount,
    `FAIL: swapped date range filtered_count (${saleSwapped.filteredCount}) != wide filtered_count (${saleWide.filteredCount})`
  );

  // 4d) start>end normalization: backend should swap and warn (US).
  const saleSwappedUs = await apiSearch({
    filters: { last_sale_date_start: todayUs, last_sale_date_end: '01/01/1990' },
    sort: 'relevance',
  });
  const swapWarnUs = saleSwappedUs.warnings.some((w) => String(w || '').toLowerCase().includes('swapped date range'));
  assert(swapWarnUs, 'FAIL: expected swapped date range warning for US date inputs');
  assert(
    saleSwappedUs.filteredCount === saleWide.filteredCount,
    `FAIL: swapped (US) filtered_count (${saleSwappedUs.filteredCount}) != wide filtered_count (${saleWide.filteredCount})`
  );

  // 5) sorting assertion: last_sale_date_desc
  const sorted = await apiSearch({ filters: {}, sort: 'last_sale_date_desc' });
  let sortOk = null;
  if (sorted.records.length < 2) {
    sortOk = 'skipped(<2 records)';
  } else {
    const dates = [];
    for (const r of sorted.records) {
      const t = parseDateAny(r?.last_sale_date);
      if (t !== null) dates.push(t);
      if (dates.length >= 5) break;
    }
    if (dates.length < 2) {
      sortOk = 'skipped(<2 parseable dates)';
    } else {
      for (let i = 1; i < dates.length; i++) {
        assert(dates[i - 1] >= dates[i], 'FAIL: last_sale_date_desc sort order violated');
      }
      sortOk = 'ok';
    }
  }

  const proofSummary =
    `PROOF_SUMMARY ui_baseline_filtered=${baselineFiltered}` +
    (baselineCandidate !== null ? ` ui_baseline_candidate=${baselineCandidate}` : '') +
    ` api_baseline_filtered=${baselineApiFiltered}` +
    (baselineApiCandidate !== null ? ` api_baseline_candidate=${baselineApiCandidate}` : '') +
    ` empty_filters_noop=${emptyNoop.filteredCount === baselineApiFiltered ? 'ok' : 'FAIL'}` +
    ` min_sqft=${sqftPick.threshold} min_sqft_filtered=${sqftPick.filteredCount}` +
    ` min_baths=${bathsPick.threshold} min_baths_filtered=${bathsPick.filteredCount}` +
    ` min_lot_size_sqft=${lotSqftPick.threshold} lot_sqft_filtered=${lotSqftPick.filteredCount}` +
    ` min_acres=${lotAcrePick.threshold} acres_filtered=${lotAcrePick.filteredCount}` +
    ` sale_wide=1990-01-01..${today} sale_wide_filtered=${saleWide.filteredCount}` +
    ` sale_tight=2020-01-01..${today} sale_tight_filtered=${saleTight.filteredCount}` +
    ` sale_date_blank_noop=${saleBlankNoop.filteredCount === baselineApiFiltered ? 'ok' : 'FAIL'}` +
    ` sale_date_format_iso=${saleDateFormatIsoOk ? 'ok' : 'FAIL'}` +
    ` sale_date_format_us=${saleDateFormatUsOk ? 'ok' : 'FAIL'}` +
    ` sale_swapped_filtered=${saleSwapped.filteredCount} sale_swapped_warn=${swapWarn ? 'yes' : 'no'}` +
    ` sale_swapped_us_warn=${swapWarnUs ? 'yes' : 'no'}` +
    ` sort_last_sale_date_desc=${sortOk}`;

  // Print PROOF_SUMMARY once at the end (after UI checks).

  let bbox = null;
  try {
    const poly = payload1.polygon_geojson;
    const ring = Array.isArray(poly?.coordinates?.[0]) ? poly.coordinates[0] : null;
    if (Array.isArray(ring) && ring.length) {
      let minLng = Infinity;
      let minLat = Infinity;
      let maxLng = -Infinity;
      let maxLat = -Infinity;
      for (const pt of ring) {
        const lng = Array.isArray(pt) ? Number(pt[0]) : NaN;
        const lat = Array.isArray(pt) ? Number(pt[1]) : NaN;
        if (!Number.isFinite(lng) || !Number.isFinite(lat)) continue;
        minLng = Math.min(minLng, lng);
        minLat = Math.min(minLat, lat);
        maxLng = Math.max(maxLng, lng);
        maxLat = Math.max(maxLat, lat);
      }
      if (Number.isFinite(minLng) && Number.isFinite(minLat) && Number.isFinite(maxLng) && Number.isFinite(maxLat)) {
        bbox = [Number(minLng.toFixed(6)), Number(minLat.toFixed(6)), Number(maxLng.toFixed(6)), Number(maxLat.toFixed(6))];
      }
    }
  } catch {
    bbox = null;
  }

  console.log('filtered_payload_keys:', {
    has_filters: !!payload1.filters,
    has_zoning_in: !!payload1.filters.zoning_in,
    has_future_land_use_in: !!payload1.filters.future_land_use_in,
    enrich: payload1.enrich,
    bbox,
  });

  const json1 = await response1.json().catch(() => ({}));
  const summary1 = json1.summary || {};

  const zoningOpts1 = Array.isArray(json1.zoning_options) ? json1.zoning_options : [];
  const fluOpts1 = Array.isArray(json1.future_land_use_options) ? json1.future_land_use_options : [];

  const filteredCount1 = safeNum(summary1.filtered_count) ?? (Array.isArray(json1.records) ? json1.records.length : 0);
  const candidateCount1 = safeNum(summary1.candidate_count) ?? null;

  console.log('filtered_counts:', { candidate_count: candidateCount1, filtered_count: filteredCount1 });

  assert(filteredCount1 > 0, 'FAIL: expected filtered_count > 0 for acceptance filter set');

  // UI should not send year_built unless the user sets it.
  assert(
    payload1.filters.min_year_built == null,
    'FAIL: expected payload.filters.min_year_built to be null/undefined before user input'
  );

  // Apply one NEW real filter end-to-end: year_built.
  // Adaptive strategy: start strict, relax until we reduce results but still keep >0.
  const minYearBuiltInput = page.locator('label', { hasText: 'Min Year Built' }).locator('input');
  const tryYears = [2005, 2000, 1990, 1980, 1970, 1950, 1900];
  let chosenMinYearBuilt = null;
  let filteredCountYear = null;

  for (const y of tryYears) {
    console.log(`== set filter: min_year_built=${y} ==`);
    await minYearBuiltInput.fill(String(y));

    console.log('== click Run (year_built filtered) ==');
    await waitRunReady();
    const [responseYear] = await Promise.all([
      page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
        timeout: 180_000,
      }),
      runBtn.click(),
    ]);

    const postYear = responseYear.request().postData() || '';
    let payloadYear = null;
    try {
      payloadYear = JSON.parse(postYear);
    } catch {
      payloadYear = null;
    }
    assert(payloadYear && payloadYear.filters, 'FAIL: year_built run did not include payload.filters');
    assert(
      payloadYear.filters.min_year_built === y,
      `FAIL: expected payload.filters.min_year_built == ${y}`
    );

    const jsonYear = await responseYear.json().catch(() => ({}));
    const summaryYear = jsonYear.summary || {};
    const fc = safeNum(summaryYear.filtered_count) ?? (Array.isArray(jsonYear.records) ? jsonYear.records.length : 0);
    assert(typeof fc === 'number' && Number.isFinite(fc), 'FAIL: year_built run did not return a numeric filtered_count');

    // Monotonicity: adding a min_year_built constraint should never increase results.
    assert(
      fc <= filteredCount1,
      `FAIL: expected year_built filtered_count (${fc}) <= prior filtered_count (${filteredCount1})`
    );

    // Keep searching until we find a strict reduction that stays > 0.
    if (fc > 0 && fc < filteredCount1) {
      chosenMinYearBuilt = y;
      filteredCountYear = fc;
      break;
    }
  }

  assert(
    typeof chosenMinYearBuilt === 'number' && Number.isFinite(chosenMinYearBuilt),
    `FAIL: could not find a min_year_built that strictly reduces results while staying > 0 (tried: ${tryYears.join(', ')})`
  );
  assert(
    typeof filteredCountYear === 'number' && Number.isFinite(filteredCountYear),
    'FAIL: chosen year_built run did not return a numeric filtered_count'
  );
  assert(zoningOpts1.length > 0, 'FAIL: expected zoning_options to be non-empty');
  assert(fluOpts1.length > 0, 'FAIL: expected future_land_use_options to be non-empty');

  if (candidateCount1 !== null) {
    const expected = `Showing ${filteredCountYear} of ${candidateCount1} in polygon`;
    await page.getByText(expected).waitFor({ state: 'visible', timeout: 30_000 });
  }

  // UI should always include the Showing...counts text once we have a polygon.
  await page.getByText(/Showing\s+\d+\s+of\s+\d+\s+in polygon/).waitFor({
    state: 'visible',
    timeout: 30_000,
  });

  // Select the first zoning option and ensure it appears in the request payload.
  console.log('== select first zoning option ==');
  const zoningBox = page
    .locator('div.w-full.rounded-xl')
    .filter({ hasText: 'Current Zoning (multi-select)' })
    .first();
  if ((await zoningBox.count()) === 0) throw new Error('FAIL: zoning multi-select box not found');

  const zoningLabels = zoningBox.locator('div.mt-2.max-h-64 label');
  const zoningLabelCount = await zoningLabels.count();
  assert(zoningLabelCount > 0, 'FAIL: no zoning options found to select');
  let zoningSelectedText = null;
  for (let i = 0; i < Math.min(zoningLabelCount, 25); i++) {
    const lbl = zoningLabels.nth(i);
    const t = (await lbl.innerText().catch(() => '')).trim();
    if (hasText(t) && !/^\d{2}\/\d{2}\/\d{4}$/.test(t) && !/^\d{2}\/\d{3}$/.test(t)) {
      zoningSelectedText = t;
      await lbl.click();
      break;
    }
  }
  assert(zoningSelectedText, 'FAIL: could not find a non-empty zoning option label to select');
  console.log('zoning_selected_label:', zoningSelectedText);

  console.log('== click Run (zoning selected) ==');
  await waitRunReady();
  const [response2] = await Promise.all([
    page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
      timeout: 180_000,
    }),
    runBtn.click(),
  ]);

  const post2 = response2.request().postData() || '';
  let payload2 = null;
  try {
    payload2 = JSON.parse(post2);
  } catch {
    payload2 = null;
  }
  assert(payload2 && payload2.filters, 'FAIL: zoning run did not include payload.filters');
  assert(
    Array.isArray(payload2.filters.zoning_in) && payload2.filters.zoning_in.length > 0,
    'FAIL: expected payload.filters.zoning_in to be a non-empty array after selecting zoning'
  );

  const json2 = await response2.json().catch(() => ({}));
  const summary2 = json2.summary || {};
  const filteredCount2 = safeNum(summary2.filtered_count) ?? (Array.isArray(json2.records) ? json2.records.length : 0);
  assert(
    typeof filteredCount2 === 'number' && Number.isFinite(filteredCount2),
    'FAIL: zoning run did not return a numeric filtered_count'
  );
  assert(
    filteredCount2 <= filteredCount1,
    `FAIL: expected zoning filtered_count (${filteredCount2}) <= prior filtered_count (${filteredCount1})`
  );

  // Select the first future land use option and ensure it appears in the request payload.
  console.log('== select first future land use option ==');
  const fluBox = page
    .locator('div.w-full.rounded-xl')
    .filter({ hasText: 'Future Land Use (multi-select)' })
    .first();
  if ((await fluBox.count()) === 0) throw new Error('FAIL: FLU multi-select box not found');

  const fluLabels = fluBox.locator('div.mt-2.max-h-64 label');
  const fluLabelCount = await fluLabels.count();
  assert(fluLabelCount > 0, 'FAIL: no FLU options found to select');
  let fluSelectedText = null;
  for (let i = 0; i < Math.min(fluLabelCount, 25); i++) {
    const lbl = fluLabels.nth(i);
    const t = (await lbl.innerText().catch(() => '')).trim();
    if (hasText(t)) {
      fluSelectedText = t;
      await lbl.click();
      break;
    }
  }
  assert(fluSelectedText, 'FAIL: could not find a non-empty FLU option label to select');
  console.log('flu_selected_label:', fluSelectedText);

  console.log('== click Run (FLU selected) ==');
  await waitRunReady();
  const [response3] = await Promise.all([
    page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
      timeout: 180_000,
    }),
    runBtn.click(),
  ]);

  const post3 = response3.request().postData() || '';
  let payload3 = null;
  try {
    payload3 = JSON.parse(post3);
  } catch {
    payload3 = null;
  }
  assert(payload3 && payload3.filters, 'FAIL: FLU run did not include payload.filters');
  assert(
    Array.isArray(payload3.filters.future_land_use_in) && payload3.filters.future_land_use_in.length > 0,
    'FAIL: expected payload.filters.future_land_use_in to be a non-empty array after selecting FLU'
  );

  const json3 = await response3.json().catch(() => ({}));
  const summary3 = json3.summary || {};
  const candidateCount3 = safeNum(summary3.candidate_count) ?? null;
  const filteredCount3 = safeNum(summary3.filtered_count) ?? (Array.isArray(json3.records) ? json3.records.length : 0);
  assert(
    typeof filteredCount3 === 'number' && Number.isFinite(filteredCount3),
    'FAIL: FLU run did not return a numeric filtered_count'
  );
  assert(
    filteredCount3 <= filteredCount2,
    `FAIL: expected FLU filtered_count (${filteredCount3}) <= zoning filtered_count (${filteredCount2})`
  );
  if (candidateCount3 !== null) {
    const expected = `Showing ${filteredCount3} of ${candidateCount3} in polygon`;
    await page.getByText(expected).waitFor({ state: 'visible', timeout: 30_000 });
  }

  // --- UI-level date swap warning (non-blocking) ---
  // Clear all other filters so this check is stable.
  console.log('== clear filters for date swap check ==');
  const clearBtn = page.getByRole('button', { name: /Clear filters/i });
  if (await clearBtn.count()) {
    await clearBtn.click();
    await page.getByText('Filters active').waitFor({ state: 'detached', timeout: 10_000 }).catch(() => null);
  }

  const uiToday = todayIsoDate();
  console.log('== set reversed last sale date range ==');
  const lastSaleStartInput = page.locator('label', { hasText: 'Last Sale Start' }).locator('input');
  const lastSaleEndInput = page.locator('label', { hasText: 'Last Sale End' }).locator('input');
  await lastSaleStartInput.fill(uiToday);
  await lastSaleEndInput.fill('1990-01-01');

  console.log('== click Run (reversed sale date range) ==');
  await waitRunReady();
  const [responseSwapUi] = await Promise.all([
    page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
      timeout: 180_000,
    }),
    runBtn.click(),
  ]);

  const jsonSwapUi = await responseSwapUi.json().catch(() => ({}));
  const warningsSwapUi = Array.isArray(jsonSwapUi.warnings) ? jsonSwapUi.warnings : [];
  const hasSwapWarning = warningsSwapUi.some((w) => String(w || '').toLowerCase().includes('swapped date range'));
  assert(hasSwapWarning, 'FAIL: expected swapped date range warning in UI run response.warnings');

  // The UI should surface this as a small non-blocking warning.
  await page.getByText(/Swapped date range/i).waitFor({ state: 'visible', timeout: 10_000 });

  console.log(proofSummary);
  console.log('PASS');
  await browser.close();

  // Best-effort cleanup (keep working tree clean).
  try {
    fs.unlinkSync(DEBUG_LOG);
  } catch {
    // ignore
  }
  // Try to avoid leaving a stray Vite process around if we started one.
  if (vite && vite.started && vite.pid) {
    try {
      process.kill(-vite.pid, 'SIGTERM');
    } catch {
      // ignore
    }
  }
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
