(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const HOUR = 3600000;
  const DIMS = { channel: "Channel", app: "App", device: "Device", os: "OS" };
  const WARN = 0.05, CRIT = 0.15;
  const MIN_SEGMENT = 10;      // min records for a segment to count as "highest-risk"
  const MIN_SHOWN = 5;         // min records for a segment to appear in the breakdown
  const MAX_ROWS = 8;

  const DEFAULT_FILTERS = { range: "all", minScore: 0, status: "all", query: "", segment: null, hour: null };
  const state = {
    summary: null, hours: [], records: [],
    base: [], filtered: [], sorted: [], shown: [],
    selected: null, topRecord: null, topSegment: null,
    visible: 10, fetchedAt: null, dim: "channel",
    filters: { ...DEFAULT_FILTERS },
    sort: { key: "score", dir: "desc" }
  };
  let toastTimer, searchTimer, resizeTimer, lastFocus;

  /* ---------- helpers ---------- */
  const fmt = (n) => new Intl.NumberFormat().format(n ?? 0);
  const pct = (n) => `${(Number(n || 0) * 100).toFixed(1)}%`;
  const val = (v) => (v === null || v === undefined || v === "" ? "—" : String(v));
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  const isFlagged = (r) => Number(r.prediction) === 1;
  const score = (r) => Number(r.fraud_probability || 0);
  const bandOf = (rate) => (rate > CRIT ? "critical" : rate >= WARN ? "warning" : "normal");
  const bandLabel = { normal: "Normal", warning: "Warning", critical: "Critical" };

  const parseTime = (v) => {
    if (!v) return NaN;
    let s = String(v).trim();
    if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}(:\d{2}){0,2}(\.\d+)?$/.test(s)) s = s.replace(" ", "T") + "Z"; // no zone → UTC
    return new Date(s).getTime();
  };
  const recTime = (r) => parseTime(r.timestamp || r.event_time);
  const hourMs = (h) => {
    const m = String(h || "").match(/(\d{4}-\d{2}-\d{2})[ T](\d{2})/);
    return m ? Date.parse(`${m[1]}T${m[2]}:00:00Z`) : NaN;
  };
  const hourLabel = (ms) => new Date(ms).toISOString().slice(5, 13).replace("T", " ") + ":00";
  const dateText = (v) => {
    const t = parseTime(v);
    return Number.isNaN(t) ? val(v) : new Date(t).toLocaleString();
  };

  /* Time ranges are relative to the newest event, not to "now",
     so "Last 24 h" still works when the data is a few days old. */
  function anchorTime() {
    const t = parseTime(state.summary && state.summary.latest_timestamp);
    if (Number.isFinite(t)) return t;
    const times = state.records.map(recTime).filter(Number.isFinite);
    return times.length ? Math.max(...times) : Date.now();
  }
  const rangeCutoff = () => (state.filters.range === "all" ? null : anchorTime() - Number(state.filters.range) * HOUR);

  const isFiltered = () => {
    const f = state.filters;
    return f.range !== "all" || f.minScore > 0 || f.status !== "all" || !!f.query || !!f.segment || f.hour !== null;
  };

  /* ---------- data ---------- */
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
        $("trend").innerHTML = '<p class="empty">Unable to load trend data.</p>';
        $("breakdown").innerHTML = '<p class="empty">Unable to load segment data.</p>';
        $("records").innerHTML = '<tr><td colspan="9" class="empty">API data unavailable. Check the API container logs.</td></tr>';
      }
    } finally {
      $("refresh").disabled = false;
    }
  }

  function setConnection(text, cls) {
    $("connection").textContent = text;
    $("connection").className = `status ${cls}`;
  }

  /* ---------- filtering ---------- */
  function baseRecords() {
    const f = state.filters;
    const cutoff = rangeCutoff();
    const q = f.query.trim().toLowerCase();
    return state.records.filter((r) => {
      if (score(r) < f.minScore) return false;
      if (f.status === "flagged" && !isFlagged(r)) return false;
      if (f.status === "clear" && isFlagged(r)) return false;
      const t = recTime(r);
      if (cutoff !== null && !(t >= cutoff)) return false;
      if (f.hour !== null && Math.floor(t / HOUR) * HOUR !== f.hour) return false;
      if (q && !(String(r.event_id || "").toLowerCase().includes(q) || String(r.ip ?? "").includes(q))) return false;
      return true;
    });
  }

  function sortRows(rows) {
    const m = state.sort.dir === "asc" ? 1 : -1;
    return rows.slice().sort((a, b) =>
      state.sort.key === "score" ? (score(a) - score(b)) * m : ((recTime(a) || 0) - (recTime(b) || 0)) * m
    );
  }

  /* ---------- aggregates ---------- */
  function stats(rows) {
    let flagged = 0, max = null;
    for (const r of rows) {
      if (isFlagged(r)) flagged++;
      if (!max || score(r) > score(max)) max = r;
    }
    return { total: rows.length, flagged, rate: rows.length ? flagged / rows.length : 0, max };
  }

  function segmentsFor(rows, key) {
    const m = new Map();
    for (const r of rows) {
      const v = r[key];
      if (v === null || v === undefined || v === "") continue;
      const k = String(v);
      const e = m.get(k) || { key, value: k, n: 0, flagged: 0 };
      e.n++;
      if (isFlagged(r)) e.flagged++;
      m.set(k, e);
    }
    return [...m.values()].map((e) => ({ ...e, rate: e.flagged / e.n }));
  }

  function topSegment(rows) {
    let best = null;
    for (const key of Object.keys(DIMS)) {
      for (const s of segmentsFor(rows, key)) {
        if (s.n < MIN_SEGMENT || !s.flagged) continue;
        if (!best || s.rate > best.rate || (s.rate === best.rate && s.n > best.n)) best = s;
      }
    }
    return best;
  }

  /* ---------- render ---------- */
  function render() {
    state.base = baseRecords();
    const seg = state.filters.segment;
    state.filtered = seg ? state.base.filter((r) => String(r[seg.key]) === seg.value) : state.base;
    state.sorted = sortRows(state.filtered);
    syncControls();
    renderChips();
    renderKpis();
    renderTrend();
    renderBreakdown();
    renderTable();
    const latest = state.summary ? dateText(state.summary.latest_timestamp) : "—";
    $("freshness").textContent = `Updated ${state.fetchedAt.toLocaleTimeString()} · latest event ${latest}`;
  }

  function syncControls() {
    const f = state.filters;
    $("timeRange").value = f.range;
    $("riskFilter").value = String(f.minScore);
    $("statusFilter").value = f.status;
    if ($("search").value !== f.query) $("search").value = f.query;
    document.querySelectorAll("#dimTabs [data-dim]").forEach((b) =>
      b.setAttribute("aria-selected", String(b.dataset.dim === state.dim)));
  }

  function renderChips() {
    const f = state.filters, chips = [];
    if (f.range !== "all") chips.push(["range", `Last ${f.range === "24" ? "24 h" : f.range === "72" ? "3 days" : "7 days"}`]);
    if (f.minScore > 0) chips.push(["minScore", `Score ≥ ${pct(f.minScore)}`]);
    if (f.status !== "all") chips.push(["status", f.status === "flagged" ? "Flagged only" : "Not flagged"]);
    if (f.query) chips.push(["query", `Search: ${f.query}`]);
    if (f.hour !== null) chips.push(["hour", `Hour ${hourLabel(f.hour)} UTC`]);
    if (f.segment) chips.push(["segment", `${DIMS[f.segment.key]} ${f.segment.value}`]);
    $("chips").hidden = !chips.length;
    $("chips").innerHTML = chips.map(([k, t]) =>
      `<button class="chip" data-clear="${k}" aria-label="Remove filter ${esc(t)}">${esc(t)} ×</button>`).join("");
  }

  function renderKpis() {
    const s = state.summary || {};
    const filtered = isFiltered();
    const st = stats(state.filtered);
    const useSummary = !filtered && s.records_loaded != null;   // summary covers all stored records
    const total = useSummary ? Number(s.records_loaded) : st.total;
    const flagged = useSummary ? Number(s.flagged_count || 0) : st.flagged;
    const rate = total ? flagged / total : 0;
    const band = bandOf(rate);

    $("flagRate").textContent = total ? pct(rate) : "—";
    $("riskBadge").textContent = total ? bandLabel[band] : "—";
    $("riskBadge").className = `badge ${total ? band : ""}`;
    $("rateCaption").textContent = `${fmt(flagged)} of ${fmt(total)} ${filtered ? "matching " : ""}records flagged`;

    // latest-hour reading + direction (hourly API is unfiltered, so only shown when no filters)
    let trend = "";
    if (!filtered) {
      const hrs = state.hours
        .map((h) => ({ ms: hourMs(h.hour), e: Number(h.events || 0), f: Number(h.flagged || 0) }))
        .filter((h) => Number.isFinite(h.ms) && h.e > 0)
        .sort((a, b) => a.ms - b.ms);
      const cur = hrs[hrs.length - 1], prev = hrs[hrs.length - 2];
      if (cur) {
        trend = `Latest hour: ${pct(cur.f / cur.e)} (${fmt(cur.f)} of ${fmt(cur.e)})`;
        if (prev) {
          const d = (cur.f / cur.e - prev.f / prev.e) * 100;
          if (Math.abs(d) >= 0.05) trend += ` · ${d > 0 ? "▲" : "▼"} ${Math.abs(d).toFixed(1)} pp vs previous active hour`;
        }
      }
    }
    $("rateTrend").textContent = trend;

    // highest-risk event
    const top = st.max;
    const maxScore = useSummary ? Number(s.maximum_probability || 0) : top ? score(top) : 0;
    $("maximum").textContent = useSummary || top ? pct(maxScore) : "—";
    const inSample = top && Math.abs(score(top) - maxScore) < 1e-6;
    state.topRecord = inSample ? top : null;
    $("topEvent").disabled = !inSample;
    $("topEvent").textContent = inSample
      ? `Event ${String(top.event_id || "unknown").slice(0, 8)}… · inspect →`
      : top || useSummary ? "Outside the loaded sample" : "No records";

    // highest-risk segment (ignores the segment filter so it stays useful while drilling)
    const seg = topSegment(state.base);
    state.topSegment = seg;
    $("segmentRate").textContent = seg ? pct(seg.rate) : "—";
    $("segmentBtn").disabled = !seg;
    $("segmentBtn").textContent = seg
      ? `${DIMS[seg.key]} ${seg.value} · ${fmt(seg.flagged)} of ${fmt(seg.n)} flagged →`
      : `No segment with ≥ ${MIN_SEGMENT} records and flags`;
  }

  function renderTrend() {
    const el = $("trend");
    const cutoff = rangeCutoff();
    let hrs = state.hours
      .map((h) => ({ ms: hourMs(h.hour), e: Number(h.events || 0), f: Number(h.flagged || 0) }))
      .filter((h) => Number.isFinite(h.ms))
      .sort((a, b) => a.ms - b.ms);
    if (cutoff !== null) hrs = hrs.filter((h) => h.ms + HOUR > cutoff);
    if (!hrs.length) { el.innerHTML = '<p class="empty">No hourly data in this range.</p>'; return; }

    const w = Math.max(320, el.clientWidth - 8), h = 230, L = 44, R = 40, T = 12, B = 30;
    const pw = w - L - R, ph = h - T - B;
    const rates = hrs.map((x) => (x.e ? x.f / x.e : 0));
    const ymax = Math.max(0.2, Math.ceil(Math.max(...rates) * 20) / 20);
    const vmax = Math.max(1, ...hrs.map((x) => x.e));
    const slot = pw / hrs.length;
    const cx = (i) => L + slot * (i + 0.5);
    const y = (r) => T + ph - (r / ymax) * ph;
    const bw = Math.min(56, slot * 0.6);

    let grid = "";
    for (let k = 0; k <= Math.round(ymax / 0.05); k++) {
      const t = k * 0.05, gy = y(t);
      const cls = Math.abs(t - WARN) < 1e-9 ? "th-warn" : Math.abs(t - CRIT) < 1e-9 ? "th-crit" : "grid-line";
      grid += `<line class="${cls}" x1="${L}" x2="${w - R}" y1="${gy}" y2="${gy}"/>`;
      grid += `<text x="${L - 8}" y="${gy + 4}" text-anchor="end">${Math.round(t * 100)}%</text>`;
      if (cls === "th-warn") grid += `<text class="th-label-warn" x="${w - R + 6}" y="${gy + 4}">Warn</text>`;
      if (cls === "th-crit") grid += `<text class="th-label-crit" x="${w - R + 6}" y="${gy + 4}">Crit</text>`;
    }

    const bars = hrs.map((x, i) => {
      const bh = (x.e / vmax) * ph * 0.5;
      return `<rect class="bar${x.ms === state.filters.hour ? " sel" : ""}" x="${cx(i) - bw / 2}" y="${T + ph - bh}" width="${bw}" height="${bh}" rx="3"/>`;
    }).join("");

    const pts = hrs.map((x, i) => ({ x: cx(i), y: y(rates[i]), ok: x.e > 0 })).filter((p) => p.ok);
    const line = pts.length > 1 ? `<polyline class="line" points="${pts.map((p) => `${p.x},${p.y}`).join(" ")}"/>` : "";
    const dots = pts.map((p) => `<circle class="dot" cx="${p.x}" cy="${p.y}" r="3.5"/>`).join("");

    const step = Math.ceil(hrs.length / Math.max(2, Math.floor(pw / 90)));
    const labels = hrs.map((x, i) =>
      i % step === 0 ? `<text x="${cx(i)}" y="${h - 8}" text-anchor="middle">${hourLabel(x.ms)}</text>` : "").join("");

    const hits = hrs.map((x, i) => {
      const tip = `${hourLabel(x.ms)} UTC · ${fmt(x.e)} predictions · ${fmt(x.f)} flagged (${pct(rates[i])})`;
      return `<rect class="hit" data-ms="${x.ms}" tabindex="0" role="button" aria-label="${esc(tip)}" x="${L + slot * i}" y="${T}" width="${slot}" height="${ph}"><title>${esc(tip)}</title></rect>`;
    }).join("");

    el.innerHTML = `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="group" aria-label="Hourly flag rate and volume">${grid}${bars}${line}${dots}${labels}${hits}</svg>`;
  }

  function renderBreakdown() {
    const el = $("breakdown");
    const all = segmentsFor(state.base, state.dim);
    let list = all.filter((s) => s.n >= MIN_SHOWN);
    const hidden = all.length - list.length;
    if (!list.length) list = all;
    list.sort((a, b) => b.rate - a.rate || b.n - a.n);
    const shown = list.slice(0, MAX_ROWS);
    if (!shown.length) { el.innerHTML = '<p class="empty">No records match the current filters.</p>'; return; }

    const scale = Math.max(0.2, ...shown.map((s) => s.rate));
    const active = state.filters.segment;
    el.innerHTML = shown.map((s) => {
      const isActive = active && active.key === s.key && active.value === s.value;
      return `<button class="bd-row${isActive ? " active" : ""}" data-key="${s.key}" data-value="${esc(s.value)}" aria-pressed="${!!isActive}">
        <span>${esc(DIMS[s.key])} ${esc(s.value)}</span>
        <span class="bd-bar"><i class="${bandOf(s.rate)}" style="width:${Math.min(100, (s.rate / scale) * 100)}%"></i></span>
        <span class="bd-val">${pct(s.rate)}</span>
        <span class="bd-n">${fmt(s.flagged)} / ${fmt(s.n)}</span>
      </button>`;
    }).join("") +
      `<p class="bd-note">Top ${shown.length} of ${list.length} by flag rate${hidden > 0 ? ` · ${hidden} segment(s) with fewer than ${MIN_SHOWN} records hidden` : ""}.</p>`;
  }

  function renderTable() {
    state.shown = state.sorted.slice(0, state.visible);
    const sampled = state.summary && Number(state.summary.records_loaded) > state.records.length;
    $("recordCount").textContent = `${fmt(state.filtered.length)} matching records`;
    $("tableNote").textContent =
      `Showing ${fmt(state.shown.length)} of ${fmt(state.filtered.length)}` +
      (sampled ? ` · loaded sample: latest ${fmt(state.records.length)} of ${fmt(state.summary.records_loaded)} stored records` : "");
    $("more").hidden = state.shown.length >= state.filtered.length;

    document.querySelectorAll("th[data-th]").forEach((th) => {
      const on = th.dataset.th === state.sort.key;
      th.setAttribute("aria-sort", on ? (state.sort.dir === "asc" ? "ascending" : "descending") : "none");
      th.querySelector(".arrow").textContent = on ? (state.sort.dir === "asc" ? "↑" : "↓") : "";
    });

    if (!state.shown.length) {
      $("records").innerHTML = '<tr><td colspan="9" class="empty">No matching records. Clear a filter to widen the view.</td></tr>';
      return;
    }
    $("records").innerHTML = state.shown.map((r, i) => {
      const sc = Math.max(0, Math.min(1, score(r))), fl = isFlagged(r);
      return `<tr data-row="${i}" tabindex="0">
        <td><div class="score"><b>${pct(sc)}</b><span class="track"><i class="${fl ? "hi" : ""}" style="width:${sc * 100}%"></i></span></div></td>
        <td class="mono">${esc(String(r.event_id || "unknown").slice(0, 12))}…</td>
        <td>${esc(dateText(r.timestamp || r.event_time))}</td>
        <td>${esc(val(r.ip))}</td>
        <td>${esc(val(r.channel))}</td>
        <td>${esc(val(r.app))}</td>
        <td>${esc(val(r.device))}</td>
        <td>${esc(val(r.os))}</td>
        <td><span class="tag ${fl ? "flagged" : ""}">${fl ? "Flagged" : "Not flagged"}</span></td>
      </tr>`;
    }).join("");
  }

  /* ---------- details on demand ---------- */
  function openDetails(record) {
    if (!record) return;
    state.selected = record;
    lastFocus = document.activeElement;
    const fl = isFlagged(record);
    const all = state.records;
    const overall = stats(all).rate;
    const sameIp = record.ip === null || record.ip === undefined ? [] : all.filter((r) => String(r.ip) === String(record.ip));
    const ipFlagged = sameIp.filter(isFlagged).length;

    const ctx = Object.keys(DIMS).map((key) => {
      const v = record[key];
      if (v === null || v === undefined || v === "") return "";
      const s = segmentsFor(all, key).find((x) => x.value === String(v));
      return s ? `<div class="ctx"><span>${DIMS[key]} ${esc(v)}</span><b>${pct(s.rate)}</b><small>${fmt(s.flagged)} of ${fmt(s.n)} flagged</small></div>` : "";
    }).join("");

    const fields = [
      ["Event ID", record.event_id], ["Time", dateText(record.timestamp || record.event_time)],
      ["IP", record.ip], ["Channel", record.channel], ["App", record.app],
      ["Device", record.device], ["OS", record.os], ["Prediction", fl ? "Flagged (1)" : "Not flagged (0)"]
    ];

    $("drawerTitle").textContent = `Event ${String(record.event_id || "unknown").slice(0, 20)}`;
    $("details").innerHTML = `
      <section class="detail-section">
        <span class="label">Model risk score</span>
        <div class="detail-score">${pct(score(record))}<span class="tag ${fl ? "flagged" : ""}">${fl ? "Flagged" : "Not flagged"}</span></div>
        <p class="muted">A model prediction, not confirmed fraud.</p>
      </section>
      <section class="detail-section">
        <span class="label">Context · overall flag rate ${pct(overall)}</span>
        ${ctx}
        <div class="ctx"><span>Same IP in loaded records</span><b>${fmt(sameIp.length)}</b><small>${fmt(ipFlagged)} flagged</small></div>
      </section>
      <section class="detail-section">
        <span class="label">Event fields</span>
        <div class="detail-grid">${fields.map(([k, v]) => `<div class="detail-field"><span>${esc(k)}</span><b>${esc(val(v))}</b></div>`).join("")}</div>
      </section>
      <details><summary>Raw payload</summary><pre class="json">${esc(JSON.stringify(record, null, 2))}</pre></details>`;

    $("backdrop").hidden = false;
    $("drawer").classList.add("open");
    $("drawer").setAttribute("aria-hidden", "false");
    $("closeDrawer").focus();
  }

  function closeDetails() {
    $("drawer").classList.remove("open");
    $("drawer").setAttribute("aria-hidden", "true");
    setTimeout(() => { if (!$("drawer").classList.contains("open")) $("backdrop").hidden = true; }, 220);
    if (lastFocus && lastFocus.focus) lastFocus.focus();
  }

  /* ---------- export / toast ---------- */
  function exportCsv(rows, filename) {
    if (!rows.length) { toast("No records available to export."); return; }
    const keys = [...new Set(rows.flatMap((r) => Object.keys(r)))];
    const cell = (v) => {
      const s = v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
      return `"${s.replace(/"/g, '""')}"`;
    };
    const csv = [keys.map(cell).join(","), ...rows.map((r) => keys.map((k) => cell(r[k])).join(","))].join("\r\n");
    const url = URL.createObjectURL(new Blob(["\ufeff", csv], { type: "text/csv;charset=utf-8" }));
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

  /* ---------- events ---------- */
  const update = () => { state.visible = 10; render(); };

  $("refresh").addEventListener("click", refresh);
  $("timeRange").addEventListener("change", (e) => { state.filters.range = e.target.value; update(); });
  $("riskFilter").addEventListener("change", (e) => { state.filters.minScore = Number(e.target.value); update(); });
  $("statusFilter").addEventListener("change", (e) => { state.filters.status = e.target.value; update(); });
  $("search").addEventListener("input", (e) => {
    clearTimeout(searchTimer);
    const v = e.target.value;
    searchTimer = setTimeout(() => { state.filters.query = v; update(); }, 200);
  });
  $("reset").addEventListener("click", () => { state.filters = { ...DEFAULT_FILTERS }; $("search").value = ""; update(); });

  $("chips").addEventListener("click", (e) => {
    const b = e.target.closest("[data-clear]");
    if (!b) return;
    const k = b.dataset.clear;
    state.filters[k] = DEFAULT_FILTERS[k];
    update();
  });

  // trend: click an hour to zoom
  const toggleHour = (target) => {
    const t = target.closest("[data-ms]");
    if (!t) return;
    const ms = Number(t.dataset.ms);
    state.filters.hour = state.filters.hour === ms ? null : ms;
    update();
  };
  $("trend").addEventListener("click", (e) => toggleHour(e.target));
  $("trend").addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggleHour(e.target); }
  });

  // breakdown: tabs + click a segment to filter
  $("dimTabs").addEventListener("click", (e) => {
    const b = e.target.closest("[data-dim]");
    if (!b) return;
    state.dim = b.dataset.dim;
    syncControls();
    renderBreakdown();
  });
  $("breakdown").addEventListener("click", (e) => {
    const row = e.target.closest(".bd-row");
    if (!row) return;
    const cur = state.filters.segment;
    const same = cur && cur.key === row.dataset.key && cur.value === row.dataset.value;
    state.filters.segment = same ? null : { key: row.dataset.key, value: row.dataset.value };
    update();
  });

  // KPI shortcuts
  $("topEvent").addEventListener("click", () => openDetails(state.topRecord));
  $("segmentBtn").addEventListener("click", () => {
    const s = state.topSegment;
    if (!s) return;
    state.dim = s.key;
    state.filters.segment = { key: s.key, value: s.value };
    update();
  });

  // table
  $("records").addEventListener("click", (e) => {
    const tr = e.target.closest("[data-row]");
    if (tr) openDetails(state.shown[Number(tr.dataset.row)]);
  });
  $("records").addEventListener("keydown", (e) => {
    if (e.key !== "Enter" && e.key !== " ") return;
    const tr = e.target.closest("[data-row]");
    if (tr) { e.preventDefault(); openDetails(state.shown[Number(tr.dataset.row)]); }
  });
  document.querySelectorAll(".sort").forEach((b) => b.addEventListener("click", () => {
    const key = b.dataset.sort;
    state.sort = { key, dir: state.sort.key === key && state.sort.dir === "desc" ? "asc" : "desc" };
    state.visible = 10;
    render();
  }));
  $("more").addEventListener("click", () => { state.visible += 10; renderTable(); });
  $("export").addEventListener("click", () => exportCsv(state.sorted, "ad-fraud-evidence.csv"));

  // drawer
  $("exportOne").addEventListener("click", () => {
    if (state.selected) exportCsv([state.selected], `evidence-${String(state.selected.event_id || "event").replace(/[^a-z0-9_-]/gi, "_")}.csv`);
  });
  $("closeDrawer").addEventListener("click", closeDetails);
  $("backdrop").addEventListener("click", closeDetails);
  document.addEventListener("keydown", (e) => { if (e.key === "Escape" && $("drawer").classList.contains("open")) closeDetails(); });

  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => { if (state.fetchedAt) renderTrend(); }, 150);
  });

  refresh();
})();