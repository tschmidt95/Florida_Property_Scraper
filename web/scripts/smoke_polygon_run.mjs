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

  page.on('request', (req) => {
    try {
      if (!req.url().includes('/api/parcels/search')) return;
      if (req.method() !== 'POST') return;
      sawSearchPost = true;
      const post = req.postData() || '';
      if (post.includes('polygon_geojson')) sawPolygonGeojson = true;
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

  console.log('== click Run and assert POST /api/parcels/search with polygon_geojson ==');
  const runBtn = page.getByRole('button', { name: /^Run$/ });
  await runBtn.waitFor({ state: 'visible', timeout: 30_000 });

  const [response] = await Promise.all([
    page.waitForResponse((r) => r.url().includes('/api/parcels/search') && r.request().method() === 'POST', {
      timeout: 60_000,
    }),
    runBtn.click(),
  ]);

  assert(sawSearchPost, 'FAIL: did not observe POST /api/parcels/search');
  assert(sawPolygonGeojson, 'FAIL: request payload missing polygon_geojson');

  const json = await response.json().catch(() => ({}));
  const recordsLen = Array.isArray(json.records) ? json.records.length : 0;
  const parcelsLen = Array.isArray(json.parcels) ? json.parcels.length : 0;

  console.log('response counts:', { recordsLen, parcelsLen });
  assert(recordsLen > 0 || parcelsLen > 0, 'FAIL: expected records/parcels in response');

  console.log('PASS');

  await browser.close();
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
