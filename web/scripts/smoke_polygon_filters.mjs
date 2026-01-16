import fs from 'node:fs';

import { chromium } from 'playwright';

const BASE_URL = (process.env.BASE_URL || 'http://127.0.0.1:8000').replace(/\/$/, '');
const DEBUG_LOG = process.env.FPS_SEARCH_DEBUG_LOG || '.fps_parcels_search_debug.jsonl';

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

async function main() {
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

  // Leaflet.draw usually renders multiple toolbars (draw + edit/remove), so do not
  // assert toolbarCount. Instead, ensure we only have one polygon tool.
  const toolbarCount = await page.locator('.leaflet-draw-toolbar').count();
  const polygonBtnCount = await page.locator('a.leaflet-draw-draw-polygon').count();
  console.log('draw_ui:', { toolbarCount, polygonBtnCount });
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
  assert(zoningOpts1.length > 0, 'FAIL: expected zoning_options to be non-empty');
  assert(fluOpts1.length > 0, 'FAIL: expected future_land_use_options to be non-empty');

  if (candidateCount1 !== null) {
    const expected = `Showing ${filteredCount1} of ${candidateCount1} in polygon`;
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
    if (hasText(t)) {
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
  if (candidateCount3 !== null) {
    const expected = `Showing ${filteredCount3} of ${candidateCount3} in polygon`;
    await page.getByText(expected).waitFor({ state: 'visible', timeout: 30_000 });
  }

  console.log('PROOF_SUMMARY:', {
    candidate_count: candidateCount1,
    filtered_count: filteredCount1,
    zoning_options_len: zoningOpts1.length,
    future_land_use_options_len: fluOpts1.length,
  });
  console.log('PASS');
  await browser.close();
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
