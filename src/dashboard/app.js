const MAX_ROWS = 100;
const TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"];

const state = {
  total: 0,
  peak: 0,
  byTicker: Object.fromEntries(TICKERS.map((t) => [t, 0])),
  times: [],
};

const $ = (id) => document.getElementById(id);
const fmt = new Intl.NumberFormat("en-US");

function severity(volume) {
  if (volume >= 200000) return ["HIGH", "high"];
  if (volume >= 50000) return ["MED", "med"];
  return ["LOW", "low"];
}

function bump(el, value) {
  el.textContent = value;
  el.classList.remove("bump");
  void el.offsetWidth;
  el.classList.add("bump");
}

function renderBars() {
  const max = Math.max(1, ...Object.values(state.byTicker));
  $("ticker-bars").innerHTML = Object.entries(state.byTicker)
    .sort((a, b) => b[1] - a[1])
    .map(([t, c]) => `
      <div class="bar-row">
        <span>${t}</span>
        <div class="bar-track"><div class="bar-fill" style="width:${(c / max) * 100}%"></div></div>
        <span class="bar-count">${c}</span>
      </div>`)
    .join("");
}

function renderStats() {
  const now = Date.now();
  state.times = state.times.filter((t) => now - t < 60000);
  bump($("stat-total"), fmt.format(state.total));
  $("stat-rate").textContent = state.times.length;
  $("stat-peak").textContent = fmt.format(state.peak);
  const top = Object.entries(state.byTicker).sort((a, b) => b[1] - a[1])[0];
  $("stat-top").textContent = top && top[1] > 0 ? top[0] : "—";
}

function addAlert(a, animate = true) {
  state.total += 1;
  state.peak = Math.max(state.peak, a.volume || 0);
  state.byTicker[a.ticker] = (state.byTicker[a.ticker] || 0) + 1;
  if (animate) state.times.push(Date.now());

  const tbody = $("alert-rows");
  tbody.querySelector(".empty")?.remove();

  const [label, cls] = severity(a.volume);
  const sentCls = a.sentiment >= 0 ? "pos" : "neg";
  const tr = document.createElement("tr");
  if (animate) tr.className = "new";
  tr.innerHTML = `
    <td>${new Date(a.timestamp * 1000).toLocaleTimeString()}</td>
    <td class="ticker">${a.ticker}</td>
    <td>${a.price != null ? "$" + a.price.toFixed(2) : "—"}</td>
    <td>${fmt.format(a.volume)}</td>
    <td class="${sentCls}">${a.sentiment.toFixed(2)}</td>
    <td><span class="badge badge--${cls}">${label}</span></td>`;
  tbody.prepend(tr);
  while (tbody.children.length > MAX_ROWS) tbody.lastChild.remove();
}

function setStatus(kind, text) {
  $("connection-status").className = `status status--${kind}`;
  $("status-text").textContent = text;
}

async function loadRecent() {
  try {
    const res = await fetch("/api/alerts/recent");
    const alerts = await res.json();
    alerts.reverse().forEach((a) => addAlert(a, false));
    renderBars();
    renderStats();
  } catch (e) {
    console.error("Failed to load recent alerts", e);
  }
}

function connect() {
  const es = new EventSource("/api/alerts/stream");
  es.onopen = () => setStatus("live", "Live");
  es.onmessage = (e) => {
    addAlert(JSON.parse(e.data));
    renderBars();
    renderStats();
  };
  es.onerror = () => setStatus("down", "Reconnecting…");
}

$("clear-btn").addEventListener("click", () => {
  state.total = 0;
  state.peak = 0;
  state.times = [];
  TICKERS.forEach((t) => (state.byTicker[t] = 0));
  $("alert-rows").innerHTML = '<tr class="empty"><td colspan="6">Waiting for anomalies…</td></tr>';
  renderBars();
  renderStats();
});

setInterval(renderStats, 5000);
renderBars();
loadRecent().then(connect);
