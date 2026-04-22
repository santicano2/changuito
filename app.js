const state = {
  products: [],
  stores: [],
  histories: {},
  selectedProductId: null,
  selectedStoreId: null,
  chart: null
};

const statusText = document.getElementById("statusText");
const productSearch = document.getElementById("productSearch");
const storeSelect = document.getElementById("storeSelect");
const locateBtn = document.getElementById("locateBtn");
const summary = document.getElementById("summary");

async function loadJson(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`No se pudo cargar ${path}`);
  return response.json();
}

function setStatus(message) {
  statusText.textContent = message;
}

function initChart() {
  const chartEl = document.getElementById("chart");
  state.chart = echarts.init(chartEl);
  window.addEventListener("resize", () => state.chart.resize());
}

function renderStoreOptions(stores) {
  const baseOption = '<option value="">Selecciona una sucursal</option>';
  const options = stores
    .slice(0, 200)
    .map(
      (store) =>
        `<option value="${store.id}">${store.chain} - ${store.name} (${store.province})</option>`
    )
    .join("");
  storeSelect.innerHTML = baseOption + options;
}

function getChainFromStore(storeId) {
  if (!storeId || storeId === "all") return null;
  const store = state.stores.find((item) => item.id === storeId);
  return store ? store.chain : null;
}

function getSeriesForSelection(productId, storeId) {
  if (!productId) return [];
  if (!storeId) {
    return state.histories[`${productId}::all`] || [];
  }

  const chain = getChainFromStore(storeId);
  if (!chain) return state.histories[`${productId}::all`] || [];

  const storesInChain = state.stores.filter((store) => store.chain === chain).map((store) => store.id);
  const byDate = new Map();

  for (const id of storesInChain) {
    const points = state.histories[`${productId}::${id}`] || [];
    for (const point of points) {
      const existing = byDate.get(point.date) || [];
      existing.push(point.price);
      byDate.set(point.date, existing);
    }
  }

  if (byDate.size === 0) {
    return state.histories[`${productId}::all`] || [];
  }

  return [...byDate.entries()]
    .map(([date, prices]) => ({
      date,
      price: Number((prices.reduce((acc, value) => acc + value, 0) / prices.length).toFixed(2))
    }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (value) => (value * Math.PI) / 180;
  const r = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return 2 * r * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function pickProductBySearch(term) {
  if (!term || term.length < 2) return null;
  const normalized = term.toLowerCase();
  return (
    state.products.find((product) =>
      `${product.name} ${product.brand}`.toLowerCase().includes(normalized)
    ) || null
  );
}

function renderChartAndSummary() {
  if (!state.selectedProductId) {
    summary.innerHTML = "Busca un producto para ver su evolucion.";
    state.chart.clear();
    return;
  }

  const points = getSeriesForSelection(state.selectedProductId, state.selectedStoreId);

  if (points.length === 0) {
    summary.innerHTML = "No hay historial para la combinacion seleccionada.";
    state.chart.clear();
    return;
  }

  const prices = points.map((point) => point.price);
  const first = prices[0];
  const last = prices[prices.length - 1];
  const deltaPct = first ? ((last - first) / first) * 100 : 0;

  state.chart.setOption({
    tooltip: { trigger: "axis" },
    xAxis: { type: "category", data: points.map((point) => point.date) },
    yAxis: { type: "value" },
    series: [
      {
        type: "line",
        data: prices,
        smooth: true,
        areaStyle: { opacity: 0.2 },
        lineStyle: { width: 3 }
      }
    ]
  });

  summary.innerHTML = `
    <ul class="summary-list">
      <li>Observaciones: <strong>${points.length}</strong></li>
      <li>Precio inicial: <strong>$${first.toFixed(2)}</strong></li>
      <li>Precio actual: <strong>$${last.toFixed(2)}</strong></li>
      <li>Variacion: <strong>${deltaPct.toFixed(2)}%</strong></li>
      <li>Minimo: <strong>$${Math.min(...prices).toFixed(2)}</strong></li>
      <li>Maximo: <strong>$${Math.max(...prices).toFixed(2)}</strong></li>
    </ul>
  `;
}

async function bootstrap() {
  try {
    setStatus("Cargando productos y sucursales...");
    initChart();

    const [products, stores, histories] = await Promise.all([
      loadJson("./data/products.json"),
      loadJson("./data/stores.json"),
      loadJson("./data/histories.json")
    ]);

    state.products = products;
    state.stores = stores;
    state.histories = histories;
    renderStoreOptions(stores);

    setStatus(`Listo. ${products.length} productos, ${stores.length} sucursales.`);
  } catch (error) {
    setStatus(`Error al cargar datos: ${error.message}`);
  }
}

productSearch.addEventListener("input", () => {
  const product = pickProductBySearch(productSearch.value);
  state.selectedProductId = product ? product.id : null;
  renderChartAndSummary();
});

storeSelect.addEventListener("change", () => {
  state.selectedStoreId = storeSelect.value || null;
  renderChartAndSummary();
});

locateBtn.addEventListener("click", () => {
  if (!navigator.geolocation) {
    setStatus("Tu navegador no soporta geolocalizacion.");
    return;
  }

  navigator.geolocation.getCurrentPosition(
    (position) => {
      const { latitude, longitude } = position.coords;
      const near = state.stores
        .filter((store) => Number.isFinite(store.lat) && Number.isFinite(store.lon))
        .map((store) => ({
          ...store,
          km: haversineKm(latitude, longitude, store.lat, store.lon)
        }))
        .sort((a, b) => a.km - b.km)
        .slice(0, 50);

      renderStoreOptions(near);
      setStatus(`Mostrando sucursales cercanas (hasta 50).`);
    },
    () => setStatus("No se pudo obtener tu ubicacion.")
  );
});

bootstrap();
