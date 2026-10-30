
(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const state = { summary: null, hours: [], records: [], filtered: [], selected: null, visible: 10, fetchedAt: null };
  let toastTimer;

  const fmt = (n) => new Intl.NumberFormat().format(n ?? 0);
  const pct = (n) => `${(Number(n || 0) * 100).toFixed(2)}%`;
  const scorePct = (n) => `${(Number(n || 0) * 100).toFixed(1)}%`;
  const val = (v) => v === null || v === undefined || v === "" ? "—" : String(v);
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, c => ({
    "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;"
  })[c]);
  const dateText = (v) => {
    if (!v) return "—";
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? String(v) : d.toLocaleString();
  };

  async function api(path) {
    const response = await fetch(path, { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
    return response.json();
  }

  async function refresh() {
    $("refresh").disabled = true;
    setConnection("Connecting…", "");
    try {
      const [summary, hourly, predictions] = await Promise.all([
        api("/api/v1/analytics/summary"),
        api("/api/v1/analytics/hourly?limit=168"),
        api("/api/v1/predictions?limit=1000")
      ]);
      state.summary = summary;
      state.hours = Array.isArray(hourly.hours) ? hourly.hours : [];
      state.records = Array.isArray(predictions.predictions) ? predictions.predictions : [];
      state.fetchedAt = new Date();
      setConnection("API connected", "ok");
      render();
    } catch (error) {
      setConnection("API unavailable", "bad");
      toast(`${error.message}. Check that the API and analytics containers are running.`);
      if (!state.summary) {
        $("trend").textContent = "Unable to load trend data.";
        $("records").innerHTML = '<tr><td colspan="7" class="empty">API data unavailable. Check the API container logs.</td></tr>';
      }
    } finally {
      $("refresh").disabled = false;
    }
  }

  function setConnection(text, cls) {
    $("connection").textContent = text;
    $("connection").className = `pill ${cls}`;
  }

  function filteredRecords() {
    let rows = state.records.slice();
    const minimum = Number($("riskFilter").value || 0);
    rows = rows.filter(r => Number(r.fraud_probability || 0) >= minimum);

    const hours = $("timeRange").value;
    if (hours !== "all") {
      const cutoff = Date.now() - Number(hours) * 3600000;
      rows = rows.filter(r => {
        const time = new Date(r.timestamp || r.event_time || "").getTime();
        return Number.isFinite(time) && time >= cutoff;
      });
    }
    return rows;
  }

  function render() {
    state.filtered = filteredRecords();
    renderKpis();
    renderTrend();
    renderTable();
    $("freshness").textContent =
      `Fetched ${state.fetchedAt.toLocaleTimeString()} · latest stored event: ${dateText(state.summary.latest_timestamp)}`;
  }

  function renderKpis() {
    const s = state.summary || {};
    const total = Number(s.records_loaded || 0);
    const flagged = Number(s.flagged_count || 0);
    const rate = total ? flagged / total : 0;

    let label = "NORMAL", cls = "normal";
    if (rate >= 0.05 && rate <= 0.15) { label = "WARNING"; cls = "warning"; }
    if (rate > 0.15) { label = "CRITICAL"; cls = "critical"; }

    $("flagRate").textContent = pct(rate);
    $("riskBadge").textContent = label;
    $("riskBadge").className = `risk ${cls}`;
    $("rateCaption").textContent = `${fmt(flagged)} flagged / ${fmt(total)} loaded records`;
    $("flagged").textContent = fmt(flagged);
    $("flaggedCaption").textContent = `${pct(rate)} of loaded records · model predictions, not confirmed fraud`;
    $("average").textContent = pct(s.average_probability);
    $("averageMeter").style.width = `${Math.max(0, Math.min(100, Number(s.average_probability || 0) * 100))}%`;
    $("maximum").textContent = pct(s.maximum_probability);
  }

  function renderTrend() {
    const hours = state.hours.slice();
    if (!hours.length) {
      $("trend").innerHTML = '<p class="muted">No hourly records returned by the API.</p>';
      return;
    }

    const range = $("timeRange").value;
    let displayed = hours;
    if (range !== "all") {
      const cutoff = Date.now() - Number(range) * 3600000;
      displayed = hours.filter(h => {
        const time = new Date(String(h.hour || "").replace(" ", "T") + ":00:00Z").getTime();
        return !Number.isFinite(time) || time >= cutoff;
      });
    }
    if (!displayed.length) {
      $("trend").innerHTML = '<p class="muted">No hourly buckets in this time range.</p>';
      return;
    }

    const max = Math.max(1, ...displayed.map(h => Number(h.events || 0)));
    const w = 700, h = 190, left = 36, right = 10, top = 12, bottom = 29;
    const pw = w - left - right, ph = h - top - bottom;
    const x = i => left + (displayed.length === 1 ? pw / 2 : i * pw / (displayed.length - 1));
    const y = n => top + ph - Number(n || 0) / max * ph;

    const grid = [0, .25, .5, .75, 1].map(f => {
      const gy = top + ph - ph * f;
      return `<line x1="${left}" y1="${gy}" x2="${w-right}" y2="${gy}" stroke="#26364d"/><text x="${left-7}" y="${gy+3}" text-anchor="end" fill="#91a2ba" font-size="9">${Math.round(max*f)}</text>`;
    }).join("");

    const totalLine = displayed.map((r,i) => `${x(i)},${y(r.events)}`).join(" ");
    const flaggedLine = displayed.map((r,i) => `${x(i)},${y(r.flagged)}`).join(" ");
    const dots = displayed.map((r,i) =>
      `<circle cx="${x(i)}" cy="${y(r.events)}" r="2.5" fill="#80adff"><title>${esc(r.hour)} · ${fmt(r.events)} predictions</title></circle>
       <circle cx="${x(i)}" cy="${y(r.flagged)}" r="2.5" fill="#ff8088"><title>${esc(r.hour)} · ${fmt(r.flagged)} flagged</title></circle>`
    ).join("");
    const labels = displayed.map((r,i) => {
      if (displayed.length > 12 && i % Math.ceil(displayed.length / 7) !== 0 && i !== displayed.length-1) return "";
      return `<text x="${x(i)}" y="${h-7}" text-anchor="middle" fill="#91a2ba" font-size="9">${esc(String(r.hour || "").slice(5))}</text>`;
    }).join("");

    $("trend").innerHTML = `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" role="img" aria-label="Hourly total and flagged predictions">${grid}<polyline points="${totalLine}" fill="none" stroke="#80adff" stroke-width="2.3" stroke-linejoin="round"/> <polyline points="${flaggedLine}" fill="none" stroke="#ff8088" stroke-width="2" stroke-linejoin="round"/>${dots}${labels}</svg>`;
    $("trendNote").textContent = `${displayed.length} hourly buckets returned. Counts reflect stored prediction records, not independently verified click traffic.`;
  }

  function renderTable() {
    const rows = state.filtered.slice(0, state.visible);
    $("recordCount").textContent = `${fmt(state.filtered.length)} matching records`;
    $("tableNote").textContent = `Showing ${fmt(rows.length)} of ${fmt(state.filtered.length)} loaded records. API response sample: ${fmt(state.records.length)}.`;
    $("more").disabled = rows.length >= state.filtered.length;
    $("more").textContent = rows.length >= state.filtered.length ? "All loaded rows shown" : "Show more →";

    if (!rows.length) {
      $("records").innerHTML = '<tr><td colspan="7" class="empty">No matching records. Try a wider time range or lower risk score.</td></tr>';
      return;
    }

    $("records").innerHTML = rows.map((r,i) => {
      const score = Math.max(0, Math.min(1, Number(r.fraud_probability || 0)));
      const flagged = Number(r.prediction) === 1;
      return `<tr data-row="${i}" tabindex="0">
        <td class="event-id">${esc(String(r.event_id || "unknown").slice(0,13))}…</td>
        <td>${esc(dateText(r.timestamp || r.event_time))}</td>
        <td>${esc(val(r.ip))}</td>
        <td>D${esc(val(r.device))} / A${esc(val(r.app))}</td>
        <td><div class="score"><b>${scorePct(score)}</b><span class="score-track"><span class="${score >= .5 ? "high" : ""}" style="width:${score*100}%"></span></span></div></td>
        <td><span class="prediction ${flagged ? "flagged" : "clear"}">${flagged ? "FLAGGED" : "NOT FLAGGED"}</span></td>
        <td>›</td>
      </tr>`;
    }).join("");

    $("records").querySelectorAll("[data-row]").forEach(row => {
      const open = () => openDetails(rows[Number(row.dataset.row)]);
      row.addEventListener("click", open);
      row.addEventListener("keydown", e => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
      });
    });
  }

  function openDetails(record) {
    state.selected = record;
    const flagged = Number(record.prediction) === 1;
    $("drawerTitle").textContent = `Event ${String(record.event_id || "unknown").slice(0,20)}`;
    const fields = [
      ["Event ID",record.event_id],["Timestamp",record.timestamp || record.event_time],
      ["IP feature",record.ip],["App feature",record.app],["Device feature",record.device],
      ["OS feature",record.os],["Channel feature",record.channel],["Prediction",flagged ? "Flagged (1)" : "Not flagged (0)"]
    ];
    $("details").innerHTML = `
      <section class="detail-section"><h3>MODEL RISK ASSESSMENT</h3><div class="detail-score">${pct(record.fraud_probability)}</div><p class="detail-muted">${flagged ? "Model classified this record as flagged." : "Model did not flag this record."} This is a prediction, not ground-truth fraud.</p></section>
      <section class="detail-section"><h3>AVAILABLE EVENT FIELDS</h3><div class="detail-grid">${fields.map(([k,v]) => `<div class="detail-field"><span>${esc(k)}</span><b>${esc(val(v))}</b></div>`).join("")}</div></section>
      <section class="detail-section"><h3>RAW PREDICTION PAYLOAD</h3><pre class="json">${esc(JSON.stringify(record,null,2))}</pre></section>
      <section class="detail-section"><h3>ADDITIONAL INVESTIGATION DATA</h3><div class="detail-field"><span>HTTP headers, user agent, reverse DNS, ASN, campaign, publisher, session latency</span><b>Not included in current API record</b></div></section>`;

    $("backdrop").hidden = false;
    $("drawer").classList.add("open");
    $("drawer").setAttribute("aria-hidden","false");
  }

  function closeDetails() {
    $("drawer").classList.remove("open");
    $("drawer").setAttribute("aria-hidden","true");
    setTimeout(() => { if (!$("drawer").classList.contains("open")) $("backdrop").hidden = true; }, 220);
  }

  function exportCsv(rows, filename) {
    if (!rows.length) { toast("No records available to export."); return; }
    const keys = [...new Set(rows.flatMap(r => Object.keys(r)))];
    const cell = v => {
      const s = v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
      return `"${s.replace(/"/g,'""')}"`;
    };
    const csv = [keys.map(cell).join(","), ...rows.map(r => keys.map(k => cell(r[k])).join(","))].join("\r\n");
    const url = URL.createObjectURL(new Blob(["\ufeff",csv],{type:"text/csv;charset=utf-8"}));
    const a = document.createElement("a");
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
    toast(`Exported ${rows.length} record(s).`);
  }

  function toast(message) {
    $("toast").textContent = message;
    $("toast").classList.add("show");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => $("toast").classList.remove("show"), 3500);
  }

  $("refresh").addEventListener("click", refresh);
  $("timeRange").addEventListener("change", () => { state.visible = 10; render(); });
  $("riskFilter").addEventListener("change", () => { state.visible = 10; render(); });
  $("reset").addEventListener("click", () => {
    $("timeRange").value = "all";
    $("riskFilter").value = "0";
    state.visible = 10;
    render();
  });
  $("more").addEventListener("click", () => { state.visible += 10; renderTable(); });
  $("export").addEventListener("click", () => exportCsv(state.filtered, "ad-fraud-evidence.csv"));
  $("exportOne").addEventListener("click", () => {
    if (state.selected) exportCsv([state.selected], `evidence-${String(state.selected.event_id || "event").replace(/[^a-z0-9_-]/gi,"_")}.csv`);
  });
  $("closeDrawer").addEventListener("click", closeDetails);
  $("backdrop").addEventListener("click", closeDetails);
  document.addEventListener("keydown", e => { if (e.key === "Escape") closeDetails(); });

  refresh();
})();
