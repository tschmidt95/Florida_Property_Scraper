const map = new maplibregl.Map({
  container: "map",
  style: "https://demotiles.maplibre.org/style.json",
  center: [-81.5, 27.8],
  zoom: 6,
});

const stateSelect = document.getElementById("stateSelect");
const countySelect = document.getElementById("countySelect");
const tooltip = document.getElementById("tooltip");

const drawer = document.getElementById("drawer");
const drawerBody = document.getElementById("drawerBody");
const drawerMeta = document.getElementById("drawerMeta");
const drawerClose = document.getElementById("drawerClose");

const metaStar = document.getElementById("metaStar");
const metaTags = document.getElementById("metaTags");
const metaLists = document.getElementById("metaLists");
const metaNotes = document.getElementById("metaNotes");
const metaSave = document.getElementById("metaSave");
const metaStatus = document.getElementById("metaStatus");

function debounce(fn, ms) {
  let t = null;
  return (...args) => {
    if (t) clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

function emptyFeatureCollection() {
  return { type: "FeatureCollection", features: [] };
}

const parcelsCache = new Map();

class LRUCache {
  constructor(limit) {
    this.limit = limit;
    this.map = new Map();
  }
  get(key) {
    if (!this.map.has(key)) return undefined;
    const v = this.map.get(key);
    this.map.delete(key);
    this.map.set(key, v);
    return v;
  }
  set(key, value) {
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, value);
    if (this.map.size > this.limit) {
      const oldestKey = this.map.keys().next().value;
      this.map.delete(oldestKey);
    }
  }
  has(key) {
    return this.map.has(key);
  }
}

let hoveredFeatureId = null;
let selectedFeatureId = null;
let selectedParcelId = null;
let selectedCounty = null;

const detailCache = new LRUCache(200);

async function fetchCounties() {
  // Geometry counties for /api/parcels (not the scraper's enabled counties).
  const counties = [
    { value: "seminole", label: "Seminole" },
    { value: "orange", label: "Orange" },
  ];
  countySelect.innerHTML = "";
  counties.forEach((c) => {
    const opt = document.createElement("option");
    opt.value = c.value;
    opt.textContent = c.label;
    countySelect.appendChild(opt);
  });
  if (!countySelect.value) {
    countySelect.value = "seminole";
  }
}

async function fetchParcels() {
  if (!countySelect.value) {
    return;
  }
  const county = countySelect.value;

  const zoom = Math.round(map.getZoom());
  if (zoom < 15) {
    if (map.getSource("parcels")) {
      map.getSource("parcels").setData(emptyFeatureCollection());
    }
    return;
  }

  const bounds = map.getBounds();
  const bbox = [
    bounds.getWest(),
    bounds.getSouth(),
    bounds.getEast(),
    bounds.getNorth(),
  ].join(",");

  const key = `${county}|${zoom}|${bbox
    .split(",")
    .map((v) => Number(v).toFixed(5))
    .join(",")}`;

  if (parcelsCache.has(key)) {
    const cached = parcelsCache.get(key);
    if (map.getSource("parcels")) {
      map.getSource("parcels").setData(cached);
    }
    return;
  }

  const resp = await fetch(
    `/api/parcels?county=${encodeURIComponent(county)}&bbox=${encodeURIComponent(
      bbox
    )}&zoom=${zoom}`
  );
  const data = await resp.json();
  parcelsCache.set(key, data);
  if (map.getSource("parcels")) {
    map.getSource("parcels").setData(data);
  } else {
    map.addSource("parcels", {
      type: "geojson",
      data,
    });
    map.addLayer({
      id: "parcel-fill",
      type: "fill",
      source: "parcels",
      paint: {
        "fill-color": [
          "case",
          ["boolean", ["feature-state", "selected"], false],
          "#111",
          ["boolean", ["feature-state", "hover"], false],
          "#00bcd4",
          "#ff6f00",
        ],
        "fill-opacity": 0.25,
      },
    });
    map.addLayer({
      id: "parcel-outline",
      type: "line",
      source: "parcels",
      paint: {
        "line-color": "#333",
        "line-width": [
          "case",
          ["boolean", ["feature-state", "selected"], false],
          4,
          ["boolean", ["feature-state", "hover"], false],
          3,
          1,
        ],
      },
    });
  }
}

map.on("load", async () => {
  await fetchCounties();
  await fetchParcels();
});

map.on("moveend", debounce(fetchParcels, 200));

countySelect.addEventListener("change", () => {
  parcelsCache.clear();
  tooltip.style.display = "none";

  closeDrawer();

  if (hoveredFeatureId) {
    try {
      map.setFeatureState({ source: "parcels", id: hoveredFeatureId }, { hover: false });
    } catch (_) {}
  }
  if (selectedFeatureId) {
    try {
      map.setFeatureState({ source: "parcels", id: selectedFeatureId }, { selected: false });
    } catch (_) {}
  }
  hoveredFeatureId = null;
  selectedFeatureId = null;

  if (map.getSource("parcels")) {
    map.getSource("parcels").setData(emptyFeatureCollection());
  }
  fetchParcels();
});

function formatMoney(v) {
  if (v === null || v === undefined) return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return "";
  if (n === 0) return "$0";
  return `$${n.toLocaleString()}`;
}

function renderHoverPopupFromProps(props, pageX, pageY) {
  tooltip.style.display = "block";
  tooltip.style.left = `${pageX + 12}px`;
  tooltip.style.top = `${pageY + 12}px`;

  // Tooltip must use ONLY these 5 list fields:
  // situs_address, owner_name, last_sale_date, last_sale_price, mortgage_amount
  const situs = String(props?.situs_address || "").trim();
  const owner = String(props?.owner_name || "").trim();
  const saleDate = String(props?.last_sale_date || "").trim();
  const salePrice = formatMoney(props?.last_sale_price);
  const mortgageRaw = props?.mortgage_amount;
  const mortgageAmt = mortgageRaw === null || mortgageRaw === undefined ? null : formatMoney(mortgageRaw);

  const lines = [];
  if (situs) lines.push(`Situs: ${situs}`);
  if (owner) lines.push(`Owner: ${owner}`);
  if (saleDate || salePrice) lines.push(`Sale: ${[saleDate, salePrice].filter(Boolean).join(" ")}`);
  if (mortgageAmt === null) {
    lines.push("Mortgage: null");
  } else if (mortgageAmt) {
    lines.push(`Mortgage: ${mortgageAmt}`);
  }
  tooltip.textContent = lines.join("\n");
}

function parseCsv(value) {
  return String(value || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function closeDrawer() {
  drawer.style.display = "none";
  drawerBody.innerHTML = '<div class="muted">Click a parcel</div>';
  drawerMeta.style.display = "none";
  metaStatus.textContent = "";
  selectedParcelId = null;
  selectedCounty = null;
}

drawerClose.addEventListener("click", () => {
  closeDrawer();
  if (selectedFeatureId) {
    try {
      map.setFeatureState({ source: "parcels", id: selectedFeatureId }, { selected: false });
    } catch (_) {}
  }
  selectedFeatureId = null;
});

function renderDetailToDrawer(detail) {
  const pa = detail?.pa;
  const meta = detail?.user_meta;

  const parcelId = escapeHtml(detail?.parcel_id || "");
  const county = escapeHtml(detail?.county || "");

  let owner = "";
  let situs = "";
  let saleDate = "";
  let salePrice = "";
  if (pa) {
    situs = String(pa.situs_address || "").trim();
    owner = Array.isArray(pa.owner_names) ? pa.owner_names.filter(Boolean).join("; ") : "";
    saleDate = String(pa.last_sale_date || "").trim();
    salePrice = formatMoney(pa.last_sale_price);
  }

  drawerBody.innerHTML = `
    <div class="card">
      <div class="row"><b>${parcelId}</b> <span class="muted">(${county})</span></div>
      <div class="row">Situs: ${escapeHtml(situs || "")}</div>
      <div class="row">Owner: ${escapeHtml(owner || "")}</div>
      <div class="row">Sale: ${escapeHtml([saleDate, salePrice].filter(Boolean).join(" "))}</div>
      ${pa ? "" : '<div class="row muted">No PA record found for this parcel_id.</div>'}
    </div>

    <details>
      <summary>Sales</summary>
      <pre>${escapeHtml(JSON.stringify({
        last_sale_date: pa?.last_sale_date ?? null,
        last_sale_price: pa?.last_sale_price ?? 0,
      }, null, 2))}</pre>
    </details>
    <details>
      <summary>Structure</summary>
      <pre>${escapeHtml(JSON.stringify({
        land_use_code: pa?.land_use_code ?? "",
        year_built: pa?.year_built ?? 0,
        building_sf: pa?.building_sf ?? 0,
      }, null, 2))}</pre>
    </details>
    <details>
      <summary>Valuation</summary>
      <pre>${escapeHtml(JSON.stringify({
        assessed_value: pa?.assessed_value ?? 0,
      }, null, 2))}</pre>
    </details>
    <details>
      <summary>All PA (JSON)</summary>
      <pre>${escapeHtml(JSON.stringify(pa, null, 2))}</pre>
    </details>
  `;

  // Populate meta editor (never mutates PA).
  drawerMeta.style.display = "block";
  metaStar.checked = Boolean(meta?.starred);
  metaTags.value = Array.isArray(meta?.tags) ? meta.tags.join(", ") : "";
  metaLists.value = Array.isArray(meta?.lists) ? meta.lists.join(", ") : "";
  metaNotes.value = String(meta?.notes || "");
}

async function fetchDetailAndOpenDrawer(county, parcelId) {
  drawer.style.display = "block";
  drawerBody.innerHTML = '<div class="muted">Loading...</div>';
  drawerMeta.style.display = "none";
  metaStatus.textContent = "";

  const cacheKey = `${county}::${parcelId}`;
  const cached = detailCache.get(cacheKey);
  if (cached) {
    renderDetailToDrawer(cached);
    return;
  }

  try {
    const resp = await fetch(
      `/api/parcels/${encodeURIComponent(parcelId)}?county=${encodeURIComponent(county)}`
    );
    if (!resp.ok) {
      drawerBody.innerHTML = `<div class="muted">Failed to load detail (${resp.status}).</div>`;
      return;
    }
    const detail = await resp.json();
    detailCache.set(cacheKey, detail);
    renderDetailToDrawer(detail);
  } catch (e) {
    drawerBody.innerHTML = `<div class="muted">Failed to load detail.</div>`;
  }
}

metaSave.addEventListener("click", async () => {
  if (!selectedParcelId || !selectedCounty) return;
  metaStatus.textContent = "Saving...";
  const payload = {
    starred: Boolean(metaStar.checked),
    tags: parseCsv(metaTags.value),
    lists: parseCsv(metaLists.value),
    notes: String(metaNotes.value || ""),
  };
  let saved = null;
  try {
    const resp = await fetch(
      `/api/parcels/${encodeURIComponent(selectedParcelId)}/meta?county=${encodeURIComponent(
        selectedCounty
      )}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }
    );
    if (!resp.ok) {
      metaStatus.textContent = `Save failed (${resp.status})`;
      return;
    }
    saved = await resp.json();
  } catch (e) {
    metaStatus.textContent = "Save failed";
    return;
  }
  metaStatus.textContent = "Saved";

  // Update cached detail for the selected parcel.
  const cacheKey = `${selectedCounty}::${selectedParcelId}`;
  const cached = detailCache.get(cacheKey);
  if (cached) {
    cached.user_meta = saved;
    detailCache.set(cacheKey, cached);
  }
});

map.on("mousemove", "parcel-fill", (e) => {
  const feature = e.features[0];
  if (!feature) return;

  const county = countySelect.value;
  const parcelId = feature.properties.parcel_id;
  const featureId = feature.id || `${county}:${parcelId}`;

  if (hoveredFeatureId && hoveredFeatureId !== featureId) {
    try {
      map.setFeatureState({ source: "parcels", id: hoveredFeatureId }, { hover: false });
    } catch (_) {}
  }
  hoveredFeatureId = featureId;
  try {
    map.setFeatureState({ source: "parcels", id: featureId }, { hover: true });
  } catch (_) {}

  renderHoverPopupFromProps(feature.properties, e.originalEvent.pageX, e.originalEvent.pageY);
});

map.on("mouseleave", "parcel-fill", () => {
  if (hoveredFeatureId) {
    try {
      map.setFeatureState({ source: "parcels", id: hoveredFeatureId }, { hover: false });
    } catch (_) {}
  }
  hoveredFeatureId = null;
  tooltip.style.display = "none";
});

map.on("click", "parcel-fill", async (e) => {
  const feature = e.features[0];
  if (!feature) return;
  const county = countySelect.value;
  const parcelId = feature.properties.parcel_id;
  const featureId = feature.id || `${county}:${parcelId}`;

  if (selectedFeatureId && selectedFeatureId !== featureId) {
    try {
      map.setFeatureState({ source: "parcels", id: selectedFeatureId }, { selected: false });
    } catch (_) {}
  }
  selectedFeatureId = featureId;
  try {
    map.setFeatureState({ source: "parcels", id: featureId }, { selected: true });
  } catch (_) {}

  selectedCounty = county;
  selectedParcelId = parcelId;
  await fetchDetailAndOpenDrawer(county, parcelId);
});

// Start closed; only open on click.
closeDrawer();
