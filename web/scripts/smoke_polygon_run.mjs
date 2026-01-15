import { chromium } from 'playwright';

const BASE_URL = (process.env.BASE_URL || 'http://127.0.0.1:8000').replace(/\/$/, '');

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  page.on('console', (msg) => {
    const t = msg.text() || '';
    if (msg.type() === 'error' || msg.type() === 'warning') {
      console.log(`[browser:${msg.type()}]`, t);
      return;
    }
    if (t.includes('__BUILD_MARKER__') || t.includes('[DRAWDBG]') || t.includes('[RUNDBG]') || t.includes('[Run]')) {
      console.log(`[browser:${msg.type()}]`, t);
    }
  });

  page.on('pageerror', (err) => {
    console.log('[pageerror]', String(err && err.stack ? err.stack : err));
  });

  let sawSearchPost = false;
  let sawPolygonGeojson = false;
  const observedPosts = [];

  page.on('request', (req) => {
    try {
      if (!req.url().includes('/api/parcels/search')) return;
      if (req.method() !== 'POST') return;
      sawSearchPost = true;
      const post = req.postData() || '';
      if (post.includes('polygon_geojson')) sawPolygonGeojson = true;
      try {
        const parsed = JSON.parse(post);
        observedPosts.push({ url: req.url(), method: req.method(), body: parsed });
      } catch {
        // ignore
      }
    } catch {
      // ignore
    }
  });

  console.log(`== open ${BASE_URL}/ ==`);
  await page.goto(`${BASE_URL}/`, { waitUntil: 'domcontentloaded' });

  const maps = page.locator('.leaflet-container');
  await maps.first().waitFor({ state: 'visible', timeout: 30_000 });

  const mapCount = await maps.count();
  assert(mapCount >= 1, 'no .leaflet-container found');

  const perMap = [];
  let bestIdx = 0;
  let bestArea = 0;
  let preferredIdx = null;
  for (let i = 0; i < mapCount; i++) {
    const m = maps.nth(i);
    const box = await m.boundingBox();
    const w = box ? Math.round(box.width) : 0;
    const h = box ? Math.round(box.height) : 0;
    const area = box ? box.width * box.height : 0;
    const toolbarCount = await m.locator('.leaflet-draw-toolbar').count();
    const polygonBtnCount = await m.locator('a.leaflet-draw-draw-polygon').count();
    perMap.push({ idx: i, w, h, toolbarCount, polygonBtnCount });
    if (area > bestArea) {
      bestArea = area;
      bestIdx = i;
    }
    if (polygonBtnCount > 0 && preferredIdx === null) {
      preferredIdx = i;
    }
  }
  console.log('maps:', perMap);

  const chosenIdx = preferredIdx !== null ? preferredIdx : bestIdx;
  const map = maps.nth(chosenIdx);
  const mapBox0 = await map.boundingBox();
  assert(mapBox0, 'selected map bounding box missing');
  console.log('selected_map:', { idx: chosenIdx, w: Math.round(mapBox0.width), h: Math.round(mapBox0.height) });

  const leafletInfo = await page.evaluate(() => {
    const w = globalThis;
    const L = w.L;
    return {
      hasGlobalL: !!L,
      hasControl: !!(L && L.Control),
      hasDrawControl: !!(L && L.Control && L.Control.Draw),
      hasDrawEvent: !!(L && L.Draw && L.Draw.Event),
    };
  });
  console.log('leaflet:', leafletInfo);

  const drawDomInfo = {
    toolbarCount: await page.locator('.leaflet-draw-toolbar').count(),
    polygonBtnCount: await page.locator('a.leaflet-draw-draw-polygon').count(),
    anyLeafletControls: await page.locator('.leaflet-control-container a').count(),
  };
  console.log('draw_dom:', drawDomInfo);

  const controlAnchors = await page.evaluate(() => {
    const anchors = Array.from(document.querySelectorAll('.leaflet-control-container a'));
    return anchors.slice(0, 30).map((a) => ({
      className: a.className,
      title: a.getAttribute('title') || '',
      text: (a.textContent || '').trim(),
    }));
  });
  console.log('control_anchors:', controlAnchors);

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
    // Fallback: press Enter (Leaflet.draw supports keyboard finish in many configs).
    await page.keyboard.press('Enter');
  }

  console.log('== click Run (baseline) and assert POST /api/parcels/search with polygon_geojson ==');
  const runBtn = page.getByRole('button', { name: /^Run$/ });
  await runBtn.waitFor({ state: 'visible', timeout: 30_000 });

  const [response0] = await Promise.all([
    page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
      timeout: 60_000,
    }),
    runBtn.click(),
  ]);

  assert(sawSearchPost, 'FAIL: did not observe POST /api/parcels/search');
  assert(sawPolygonGeojson, 'FAIL: request payload missing polygon_geojson');

  const post0 = response0.request().postData() || '';
  let payload0 = null;
  try {
    payload0 = JSON.parse(post0);
  } catch {
    payload0 = null;
  }
  console.log('run0_payload_has_filters:', !!(payload0 && payload0.filters));

  const json0 = await response0.json().catch(() => ({}));
  const recordsLen0 = Array.isArray(json0.records) ? json0.records.length : 0;
  const parcelsLen0 = Array.isArray(json0.parcels) ? json0.parcels.length : 0;

  const zoningOpts0 = Array.isArray(json0.zoning_options) ? json0.zoning_options : [];
  const fluOpts0 = Array.isArray(json0.future_land_use_options) ? json0.future_land_use_options : [];

  console.log('response0 counts:', { recordsLen0, parcelsLen0 });
  console.log('response0 option_lens:', { zoning: zoningOpts0.length, futureLandUse: fluOpts0.length });

  assert(recordsLen0 > 0 || parcelsLen0 > 0, 'FAIL: expected records/parcels in baseline response');

  // Pick a record with a real lot_size_sqft so we can set a deterministic filter.
  const recs0 = Array.isArray(json0.records) ? json0.records : [];
  let sampleLotSqft = null;
  for (const r of recs0) {
    if (!r || typeof r !== 'object') continue;
    const v = r.lot_size_sqft;
    const n = typeof v === 'number' ? v : (typeof v === 'string' ? Number(v) : NaN);
    if (Number.isFinite(n) && n > 0) {
      sampleLotSqft = n;
      break;
    }
  }
  assert(sampleLotSqft !== null, 'FAIL: could not find any record with lot_size_sqft in baseline response');

  const minSqft = Math.max(1, Math.floor(sampleLotSqft * 0.5));
  const maxSqft = Math.ceil(sampleLotSqft * 1.5);

  // Set filters via the UI.
  console.log('== set parcel size filter (sqft) ==');
  const unitSelect = page.locator('label', { hasText: 'Parcel Size Unit' }).locator('select');
  await unitSelect.waitFor({ state: 'visible', timeout: 30_000 });
  await unitSelect.selectOption('sqft');

  const minLotInput = page.locator('label', { hasText: 'Min Parcel Size' }).locator('input');
  const maxLotInput = page.locator('label', { hasText: 'Max Parcel Size' }).locator('input');
  await minLotInput.fill(String(minSqft));
  await maxLotInput.fill(String(maxSqft));

  // Keep the UI proof fast/deterministic: explicitly disable inline enrichment.
  // (Lot-size filtering works off FDOR/PA cached fields.)
  const enrichToggle = page.locator('label', { hasText: 'Auto-enrich missing' }).locator('input[type="checkbox"]');
  if (await enrichToggle.count()) {
    const checked = await enrichToggle.isChecked();
    if (checked) {
      await enrichToggle.uncheck();
    }
    console.log('auto_enrich_checked:', await enrichToggle.isChecked());
  }

  console.log('== click Run (filtered) and assert payload.filters contains min/max lot_size_sqft ==');
  const [response1] = await Promise.all([
    page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
      timeout: 60_000,
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
  assert(
    payload1.filters.min_lot_size_sqft != null && payload1.filters.max_lot_size_sqft != null,
    'FAIL: expected min_lot_size_sqft/max_lot_size_sqft in payload.filters'
  );

  const json1 = await response1.json().catch(() => ({}));
  const recordsLen1 = Array.isArray(json1.records) ? json1.records.length : 0;
  const parcelsLen1 = Array.isArray(json1.parcels) ? json1.parcels.length : 0;

  console.log('response1 counts:', { recordsLen1, parcelsLen1 });
  assert(recordsLen1 > 0 || parcelsLen1 > 0, 'FAIL: expected records/parcels in filtered response');

  // Basic sanity: returned lots should be within range when the field is present.
  const recs1 = Array.isArray(json1.records) ? json1.records : [];
  let checkedSome = 0;
  for (const r of recs1) {
    if (!r || typeof r !== 'object') continue;
    const v = r.lot_size_sqft;
    const n = typeof v === 'number' ? v : (typeof v === 'string' ? Number(v) : NaN);
    if (!Number.isFinite(n) || n <= 0) continue;
    checkedSome++;
    assert(n >= minSqft - 1e-6 && n <= maxSqft + 1e-6, 'FAIL: filtered response contains lot_size_sqft out of range');
  }
  console.log('checked_lot_size_records:', checkedSome);

  // Print the last observed payload for debugging (keep concise).
  if (observedPosts.length) {
    const last = observedPosts[observedPosts.length - 1];
    console.log('last_observed_post_body_keys:', Object.keys(last.body || {}));
    console.log('last_observed_filters_keys:', Object.keys((last.body && last.body.filters) || {}));
  }

  console.log('PASS');

  await browser.close();
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
