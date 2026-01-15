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

  page.on('request', (req) => {
    try {
      if (!req.url().includes('/api/parcels/search')) return;
      if (req.method() !== 'POST') return;
      console.log('observed_request:', { url: req.url(), method: req.method() });
    } catch {
      // ignore
    }
  });

  page.on('response', (resp) => {
    try {
      const req = resp.request();
      if (!resp.url().includes('/api/parcels/search')) return;
      if (req.method() !== 'POST') return;
      console.log('observed_response:', { url: resp.url(), status: resp.status() });
    } catch {
      // ignore
    }
  });

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
  console.log('leaflet_container_count:', mapCount);
  assert(mapCount === 1, `FAIL: expected exactly 1 .leaflet-container, saw ${mapCount}`);

  // Leaflet.draw usually renders multiple toolbars (draw + edit/remove), so do not
  // assert toolbarCount. Instead, ensure we only have one polygon tool.
  const toolbarCount = await page.locator('.leaflet-draw-toolbar').count();
  const polygonBtnCount = await page.locator('a.leaflet-draw-draw-polygon').count();
  console.log('leaflet_draw_toolbar_count:', toolbarCount);
  console.log('leaflet_draw_polygon_button_count:', polygonBtnCount);
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
  console.log('baseline:', {
    search_id: json0.search_id,
    candidate_count: summary0.candidate_count,
    filtered_count: summary0.filtered_count,
    zoning_options_len: zoningOpts0.length,
    future_land_use_options_len: fluOpts0.length,
  });
  assert(baselineFiltered > 0, 'FAIL: expected baseline filtered_count > 0');

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
    if (!(await enrichToggle.isChecked())) await enrichToggle.check();
    console.log('auto_enrich_checked:', await enrichToggle.isChecked());
  }

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
  assert(
    payload1.filters.min_acres === 0.5 || payload1.filters.min_lot_size_sqft != null,
    'FAIL: expected payload.filters.min_acres == 0.5 (or sqft equivalent)'
  );

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

  console.log('payload_summary:', {
    county: payload1.county,
    bbox,
    min_sqft: payload1.filters.min_sqft,
    max_sqft: payload1.filters.max_sqft,
    min_acres: payload1.filters.min_acres,
    zoning_in: payload1.filters.zoning_in || null,
    future_land_use_in: payload1.filters.future_land_use_in || null,
    enrich: payload1.enrich,
    enrich_limit: payload1.enrich_limit,
  });

  const json1 = await response1.json().catch(() => ({}));
  const summary1 = json1.summary || {};

  const zoningOpts1 = Array.isArray(json1.zoning_options) ? json1.zoning_options : [];
  const fluOpts1 = Array.isArray(json1.future_land_use_options) ? json1.future_land_use_options : [];

  const filteredCount1 = safeNum(summary1.filtered_count) ?? (Array.isArray(json1.records) ? json1.records.length : 0);
  const candidateCount1 = safeNum(summary1.candidate_count) ?? null;

  console.log('filtered:', {
    search_id: json1.search_id,
    candidate_count: candidateCount1,
    filtered_count: filteredCount1,
    zoning_options_len: zoningOpts1.length,
    future_land_use_options_len: fluOpts1.length,
    warnings_len: Array.isArray(json1.warnings) ? json1.warnings.length : 0,
  });

  assert(filteredCount1 > 0, 'FAIL: expected filtered_count > 0 for acceptance filter set');
  assert(zoningOpts1.length > 0, 'FAIL: expected zoning_options to be non-empty');
  assert(fluOpts1.length > 0, 'FAIL: expected future_land_use_options to be non-empty');

  // Strict-mode sanity: returned records should have the numeric fields needed for filtering.
  const recs1 = Array.isArray(json1.records) ? json1.records : [];
  const totalRecs1 = recs1.length;
  assert(totalRecs1 > 0, 'FAIL: response.records is empty');

  let livingPresent = 0;
  let lotAcresPresent = 0;
  let zoningPresent = 0;
  let fluPresent = 0;
  for (const r of recs1) {
    if (r && hasPositiveNumber(r.living_area_sqft)) livingPresent += 1;
    if (r && hasPositiveNumber(r.lot_size_acres)) lotAcresPresent += 1;
    if (r && hasText(r.zoning)) zoningPresent += 1;
    if (r && hasText(r.future_land_use)) fluPresent += 1;
  }

  const livingFrac = totalRecs1 ? livingPresent / totalRecs1 : 0;
  const lotAcresFrac = totalRecs1 ? lotAcresPresent / totalRecs1 : 0;
  console.log('returned_record_coverage:', {
    total: totalRecs1,
    living_area_sqft: { present: livingPresent, coverage: Number(livingFrac.toFixed(3)) },
    lot_size_acres: { present: lotAcresPresent, coverage: Number(lotAcresFrac.toFixed(3)) },
    zoning: { present: zoningPresent },
    future_land_use: { present: fluPresent },
  });

  assert(livingFrac >= 0.7, `FAIL: expected living_area_sqft coverage >= 0.7 on returned records, got ${livingFrac}`);
  assert(lotAcresFrac >= 0.7, `FAIL: expected lot_size_acres coverage >= 0.7 on returned records, got ${lotAcresFrac}`);

  const sample = recs1[0] || null;
  if (sample) {
    console.log('sample_record_fields:', {
      parcel_id: sample.parcel_id,
      living_area_sqft: sample.living_area_sqft,
      lot_size_acres: sample.lot_size_acres,
      zoning: sample.zoning,
      future_land_use: sample.future_land_use,
    });
  }

  const searchId = json1.search_id;
  if (typeof searchId === 'string' && searchId.trim()) {
    const dbg = readJsonlBySearchId(DEBUG_LOG, searchId);
    const reqEvt = dbg.find((x) => x && x.event === 'request') || null;
    const resEvt = dbg.find((x) => x && x.event === 'result') || null;

    console.log('debug_log_path:', DEBUG_LOG);
    console.log('debug_events_found:', dbg.length);
    if (reqEvt) {
      const p = reqEvt.payload || {};
      const f = p.filters || null;
      console.log('debug_request_summary:', {
        bbox: reqEvt.bbox,
        candidates_count: reqEvt.candidates_count,
        intersecting_count: reqEvt.intersecting_count,
        has_filters: !!f,
        filter_keys: f && typeof f === 'object' ? Object.keys(f) : [],
      });
    }
    if (resEvt) {
      console.log('debug_result_summary:', {
        candidate_count: resEvt.candidate_count,
        filtered_count: resEvt.filtered_count,
        warnings: resEvt.warnings,
      });
      const fs0 = resEvt.field_stats || null;
      if (fs0 && typeof fs0 === 'object') {
        console.log('debug_field_stats_present:', fs0.present || null);
        if (fs0.coverage_candidates) {
          console.log('debug_coverage_candidates:', fs0.coverage_candidates);
          const cov = fs0.coverage_candidates || {};
          const z0 = cov.zoning || null;
          const f0 = cov.future_land_use || null;
          if (z0 && typeof z0.present === 'number') {
            assert(z0.present > 0, 'FAIL: expected candidate zoning coverage present > 0');
          }
          if (f0 && typeof f0.present === 'number') {
            assert(f0.present > 0, 'FAIL: expected candidate future_land_use coverage present > 0');
          }
        }
        if (fs0.sample_candidate) {
          console.log('debug_sample_candidate:', fs0.sample_candidate);
        }
      }
    }
  } else {
    console.log('debug: no search_id in response; cannot correlate JSONL');
  }

  console.log('PASS');
  await browser.close();
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
