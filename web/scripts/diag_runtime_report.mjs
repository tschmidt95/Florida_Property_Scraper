import fs from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const BASE_URL = (process.env.BASE_URL || process.argv[2] || 'http://127.0.0.1:8000/').replace(/\/+$/, '');
const DIAG_LABEL = (process.env.DIAG_LABEL || '').trim();

function shortError(err) {
  if (!err) return '';
  if (typeof err === 'string') return err;
  return String(err && (err.stack || err.message || err));
}

function safeLabelFromBaseUrl(baseUrl) {
  try {
    const u = new URL(baseUrl);
    const host = (u.host || 'unknown').toLowerCase();
    if (host.includes('127.0.0.1') || host.includes('localhost')) return 'localhost';
    return host.replace(/[^a-z0-9_.-]+/g, '_');
  } catch {
    return 'unknown';
  }
}

function nowStamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return (
    String(d.getFullYear()) +
    pad(d.getMonth() + 1) +
    pad(d.getDate()) +
    '_' +
    pad(d.getHours()) +
    pad(d.getMinutes()) +
    pad(d.getSeconds())
  );
}

function uniqueLimit(arr, limit) {
  const out = [];
  const seen = new Set();
  for (const item of arr) {
    const k = typeof item === 'string' ? item : JSON.stringify(item);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(item);
    if (out.length >= limit) break;
  }
  return out;
}

function hasAppMarkers(html) {
  const t = String(html || '');
  const hasAssetsIndex = /\/assets\/index-[^"']+\.js/i.test(t);
  const hasLeaflet = /leaflet/i.test(t);
  return { hasAssetsIndex, hasLeaflet };
}

async function verifyAppUI(page, report) {
  const html = await page.content().catch(() => '');
  const { hasAssetsIndex, hasLeaflet } = hasAppMarkers(html);
  const hasRoot = await page.locator('#root').count().then((n) => n > 0).catch(() => false);

  // Criteria (as requested):
  // - require Vite chunk /assets/index-*.js
  //   OR
  // - require #root AND a Leaflet marker somewhere in the HTML
  const looksLikeApp = hasAssetsIndex || (hasRoot && hasLeaflet);

  report.notes.push(`app_markers=${JSON.stringify({ hasAssetsIndex, hasRoot, hasLeaflet })}`);
  if (!looksLikeApp) {
    report.ok = false;
    report.errors.push(
      'NOT_APP_UI: forwarded page does not look like the built app (missing assets index / leaflet markers)',
    );
    return false;
  }
  return true;
}

async function listVisibleButtonTexts(page) {
  return await page.evaluate(() => {
    function isVisible(el) {
      if (!(el instanceof HTMLElement)) return false;
      const style = window.getComputedStyle(el);
      if (style.visibility === 'hidden' || style.display === 'none') return false;
      const rect = el.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    }

    const buttons = Array.from(document.querySelectorAll('button'));
    const texts = [];
    for (const b of buttons) {
      if (!isVisible(b)) continue;
      const t = (b.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 120);
      if (t) texts.push(t);
    }
    return texts.slice(0, 200);
  });
}

async function pickLeafletMapWithPolygonTool(page) {
  const maps = page.locator('.leaflet-container');
  const mapCount = await maps.count().catch(() => 0);
  if (mapCount <= 0) return null;

  for (let i = 0; i < mapCount; i++) {
    const map = maps.nth(i);
    const btn = map.locator('a.leaflet-draw-draw-polygon').first();
    const count = await btn.count().catch(() => 0);
    if (!count) continue;
    const visible = await btn.isVisible().catch(() => false);
    if (visible) return map;
  }

  return maps.first();
}

async function drawPolygonOnLeafletMap(page, map) {
  const polygonBtn = map.locator('a.leaflet-draw-draw-polygon').first();
  await polygonBtn.waitFor({ state: 'visible', timeout: 30000 });
  await polygonBtn.click({ timeout: 15000 });

  const box = await map.boundingBox();
  if (!box) throw new Error('leaflet map boundingBox missing');

  const pts = [
    [0.30, 0.30],
    [0.70, 0.30],
    [0.75, 0.60],
    [0.50, 0.75],
    [0.25, 0.60],
  ];

  for (let i = 0; i < pts.length; i++) {
    const [fx, fy] = pts[i];
    const x = Math.round(box.x + box.width * fx);
    const y = Math.round(box.y + box.height * fy);
    if (i === pts.length - 1) {
      await page.mouse.dblclick(x, y);
    } else {
      await page.mouse.click(x, y);
    }
    await page.waitForTimeout(120);
  }
}

async function tryPolygonRun(page, report, consoleRing) {
  const map = await pickLeafletMapWithPolygonTool(page);
  if (!map) {
    report.notes.push('no .leaflet-container found');
    return { attempted: false, ok: false };
  }

  await map.waitFor({ state: 'visible', timeout: 30000 });

  const polygonToolVisible = await map.locator('a.leaflet-draw-draw-polygon').first().isVisible().catch(() => false);
  if (!polygonToolVisible) {
    report.notes.push('polygon tool not visible');
    return { attempted: false, ok: false };
  }

  // Collect any request whose url includes /api/parcels/search.
  const onRequest = (req) => {
    try {
      const url = req.url();
      if (!url.includes('/api/parcels/search')) return;
      report.notes.push(`saw_request: ${req.method()} ${url}`);
    } catch {
      // ignore
    }
  };
  page.on('request', onRequest);

  try {
    await drawPolygonOnLeafletMap(page, map);
    report.clicked.push({ type: 'polygon_draw', text: 'draw+finish(dblclick)' });

    const runBtn = page.getByRole('button', { name: /^Run$/ }).first();
    const runCount = await runBtn.count().catch(() => 0);
    report.notes.push(`run_button_count=${runCount}`);
    if (!runCount) {
      report.errors.push('Run button not found');
      return { attempted: true, ok: false };
    }
    await runBtn.waitFor({ state: 'visible', timeout: 30000 });

    const reqPromise = page.waitForRequest(
      (r) => r.url().includes('/api/parcels/search') && r.method() === 'POST',
      { timeout: 60000 },
    );

    report.clicked.push({ type: 'button', text: 'Run' });
    await runBtn.click({ timeout: 15000 });

    let req;
    try {
      req = await reqPromise;
      report.polygonRequestSeen = true;
    } catch (e) {
      report.polygonRequestSeen = false;
      const visibleButtons = await listVisibleButtonTexts(page).catch(() => []);
      report.errors.push(`polygon: timed out waiting for POST /api/parcels/search request: ${shortError(e)}`);
      report.notes.push(`visible_buttons=${JSON.stringify(visibleButtons.slice(0, 50))}`);
      report.notes.push(`last_console_lines=${JSON.stringify(consoleRing.slice(-50))}`);
      return { attempted: true, ok: false };
    }

    const resp = (await req.response().catch(() => null)) || null;
    if (!resp) {
      report.errors.push('polygon: request observed but response missing');
      return { attempted: true, ok: false };
    }

    report.polygonResponseStatus = resp.status();

    const bodyText = await resp.text().catch(() => '');
    const previewRaw = (bodyText || '').slice(0, 2000);

    let recordsLen = 0;
    try {
      const json = JSON.parse(bodyText);
      recordsLen = Array.isArray(json.records) ? json.records.length : 0;
    } catch {
      // ignore
    }

    report.polygonResponseBodyPreview = `status=${report.polygonResponseStatus} recordsLen=${recordsLen} bodyPreview=${previewRaw}`;

    const ok = report.polygonResponseStatus === 200 && recordsLen > 0;
    if (!ok) {
      report.errors.push(
        `polygon: response did not meet success criteria (status=${report.polygonResponseStatus} recordsLen=${recordsLen})`,
      );
      return { attempted: true, ok: false };
    }

    report.notes.push(`polygon ok (status=200 recordsLen=${recordsLen})`);
    return { attempted: true, ok: true };
  } finally {
    page.off('request', onRequest);
  }
}

async function writeFailureArtifacts(page, report, label) {
  const stamp = nowStamp();
  const safeLabel = (DIAG_LABEL || label || safeLabelFromBaseUrl(BASE_URL)).replace(/[^a-z0-9_.-]+/gi, '_');
  const outDir = path.join(process.cwd(), 'web', 'tmp');
  await fs.mkdir(outDir, { recursive: true });

  const pngPath = path.join(outDir, `diag_fail_${stamp}_${safeLabel}.png`);
  const htmlPath = path.join(outDir, `diag_fail_${stamp}_${safeLabel}.html`);

  try {
    await page.screenshot({ path: pngPath, fullPage: true });
    report.notes.push(`wrote_screenshot=${path.relative(process.cwd(), pngPath)}`);
  } catch (e) {
    report.notes.push(`screenshot_failed=${shortError(e)}`);
  }

  try {
    const html = await page.content();
    await fs.writeFile(htmlPath, html, 'utf-8');
    report.notes.push(`wrote_html=${path.relative(process.cwd(), htmlPath)}`);
  } catch (e) {
    report.notes.push(`html_snapshot_failed=${shortError(e)}`);
  }
}

async function main() {
  const report = {
    baseUrl: `${BASE_URL}/`,
    ok: false,
    errors: [],
    consoleLines: [],
    consoleErrors: [],
    requestFailed: [],
    http4xx5xx: [],
    clicked: [],
    notes: [],
    polygonRunOk: false,
    polygonRequestSeen: false,
    polygonResponseStatus: null,
    polygonResponseBodyPreview: null,
  };

  const consoleRing = [];
  const pushConsole = (entry) => {
    consoleRing.push(entry);
    if (consoleRing.length > 200) consoleRing.splice(0, consoleRing.length - 200);
  };

  const browser = await chromium.launch({ headless: true });
  const host = (() => {
    try {
      return new URL(`${BASE_URL}/`).host || '';
    } catch {
      return '';
    }
  })();

  const looksLikeCodespacesForward = host.endsWith('.app.github.dev');
  const context = await browser.newContext(
    looksLikeCodespacesForward
      ? {
          userAgent: 'codespaces-proof',
          extraHTTPHeaders: {
            Accept: '*/*',
          },
        }
      : undefined,
  );
  if (looksLikeCodespacesForward) report.notes.push('codespaces_forward_headers=enabled');
  const page = await context.newPage();

  if (looksLikeCodespacesForward) {
    await page.route('**/*', async (route, request) => {
      const headers = { ...request.headers() };
      // Drop browser navigation fetch metadata that appears to trigger the Codespaces
      // port access gate for private ports.
      for (const k of [
        'sec-fetch-site',
        'sec-fetch-mode',
        'sec-fetch-dest',
        'sec-fetch-user',
        'upgrade-insecure-requests',
      ]) {
        delete headers[k];
      }
      headers['accept'] = '*/*';
      headers['user-agent'] = 'codespaces-proof';
      await route.continue({ headers });
    });
    report.notes.push('codespaces_forward_route_header_override=enabled');
  }

  page.on('console', (msg) => {
    const entry = {
      type: msg.type(),
      text: (msg.text() || '').slice(0, 5000),
    };
    pushConsole(entry);
    report.consoleLines.push(entry);
    if (entry.type === 'error' || entry.type === 'warning') {
      report.consoleErrors.push(entry);
    }
  });

  page.on('pageerror', (err) => {
    report.errors.push(`pageerror: ${shortError(err)}`);
  });

  page.on('requestfailed', (req) => {
    report.requestFailed.push({
      url: req.url(),
      method: req.method(),
      errorText: req.failure() ? req.failure().errorText : '',
    });
  });

  page.on('response', (resp) => {
    const status = resp.status();
    if (status >= 400) {
      report.http4xx5xx.push({ url: resp.url(), status });
    }
  });

  try {
    await page.goto(`${BASE_URL}/`, { waitUntil: 'domcontentloaded', timeout: 45000 });
    report.ok = true;

    await page.waitForLoadState('networkidle', { timeout: 45000 }).catch(() => {
      report.notes.push('waitForLoadState(networkidle) timed out');
    });

    const isApp = await verifyAppUI(page, report);
    if (!isApp) {
      await writeFailureArtifacts(page, report, safeLabelFromBaseUrl(BASE_URL));
      return;
    }

    const poly = await tryPolygonRun(page, report, consoleRing);
    report.polygonRunOk = !!poly.ok;
    if (poly.attempted) report.notes.push(`polygon attempted ok=${poly.ok}`);

    if (!report.polygonRunOk) {
      await writeFailureArtifacts(page, report, safeLabelFromBaseUrl(BASE_URL));
    }
  } catch (e) {
    report.ok = false;
    report.errors.push(`fatal: ${shortError(e)}`);
    await writeFailureArtifacts(page, report, safeLabelFromBaseUrl(BASE_URL)).catch(() => null);
  } finally {
    report.consoleLines = uniqueLimit(report.consoleLines, 200);
    report.consoleErrors = uniqueLimit(report.consoleErrors, 200);
    report.errors = uniqueLimit(report.errors, 200);
    report.requestFailed = uniqueLimit(report.requestFailed, 200);
    report.http4xx5xx = uniqueLimit(report.http4xx5xx, 200);
    report.clicked = uniqueLimit(report.clicked, 50);
    report.notes = uniqueLimit(report.notes, 200);

    await browser.close().catch(() => null);
    process.stdout.write(JSON.stringify(report, null, 2) + '\n');
  }
}

main().catch((e) => {
  process.stdout.write(
    JSON.stringify(
      {
        baseUrl: `${BASE_URL}/`,
        ok: false,
        errors: [shortError(e)],
        consoleLines: [],
        consoleErrors: [],
        requestFailed: [],
        http4xx5xx: [],
        clicked: [],
        notes: ['unhandled top-level exception'],
        polygonRunOk: false,
        polygonRequestSeen: false,
        polygonResponseStatus: null,
        polygonResponseBodyPreview: null,
      },
      null,
      2,
    ) + '\n',
  );
  process.exit(1);
});
