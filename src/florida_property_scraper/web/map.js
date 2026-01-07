const map = new maplibregl.Map({
  container: "map",
  style: "https://demotiles.maplibre.org/style.json",
  center: [-81.5, 27.8],
  zoom: 6,
});

const stateSelect = document.getElementById("stateSelect");
const countySelect = document.getElementById("countySelect");
const details = document.getElementById("details");
const tooltip = document.getElementById("tooltip");

async function fetchCounties() {
  const state = stateSelect.value;
  const resp = await fetch(`/counties?state=${state}`);
  const counties = await resp.json();
  countySelect.innerHTML = "";
  counties.forEach((slug) => {
    const opt = document.createElement("option");
    opt.value = slug;
    opt.textContent = slug.replace(/_/g, " ");
    countySelect.appendChild(opt);
  });
}

async function fetchParcels() {
  if (!countySelect.value) {
    return;
  }
  const state = stateSelect.value;
  const county = countySelect.value;
  const bounds = map.getBounds();
  const bbox = [
    bounds.getWest(),
    bounds.getSouth(),
    bounds.getEast(),
    bounds.getNorth(),
  ].join(",");
  const zoom = Math.round(map.getZoom());
  const resp = await fetch(
    `/parcels?state=${state}&county=${county}&bbox=${bbox}&zoom=${zoom}`
  );
  const data = await resp.json();
  if (map.getSource("parcels")) {
    map.getSource("parcels").setData(data);
  } else {
    map.addSource("parcels", {
      type: "geojson",
      data,
    });
    map.addLayer({
      id: "parcel-fill",
      type: "circle",
      source: "parcels",
      paint: {
        "circle-radius": 5,
        "circle-color": "#ff6f00",
        "circle-stroke-color": "#333",
        "circle-stroke-width": 1,
      },
    });
    map.addLayer({
      id: "parcel-hover",
      type: "circle",
      source: "parcels",
      paint: {
        "circle-radius": 8,
        "circle-color": "#00bcd4",
        "circle-stroke-color": "#111",
        "circle-stroke-width": 2,
      },
      filter: ["==", "parcel_id", ""],
    });
  }
}

map.on("load", async () => {
  await fetchCounties();
  await fetchParcels();
});

map.on("moveend", fetchParcels);

countySelect.addEventListener("change", fetchParcels);

map.on("mousemove", "parcel-fill", (e) => {
  const feature = e.features[0];
  if (!feature) return;
  map.setFilter("parcel-hover", ["==", "parcel_id", feature.properties.parcel_id]);
  tooltip.style.display = "block";
  tooltip.style.left = `${e.originalEvent.pageX + 12}px`;
  tooltip.style.top = `${e.originalEvent.pageY + 12}px`;
  tooltip.textContent = `${feature.properties.parcel_id} ${feature.properties.address || ""}`;
});

map.on("mouseleave", "parcel-fill", () => {
  map.setFilter("parcel-hover", ["==", "parcel_id", ""]);
  tooltip.style.display = "none";
});

map.on("click", "parcel-fill", async (e) => {
  const feature = e.features[0];
  if (!feature) return;
  const state = stateSelect.value;
  const county = countySelect.value;
  const parcelId = feature.properties.parcel_id;
  const resp = await fetch(
    `/parcels/${encodeURIComponent(parcelId)}?state=${state}&county=${county}`
  );
  const data = await resp.json();
  details.textContent = JSON.stringify(data, null, 2);
});
