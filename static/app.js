const state = {
  apps: [],
  languages: [],
  countries: [],
  parentCountries: [],
  activeTab: "review",
  review: {
    appMode: "__WITH_APP__",
    apps: new Set(),
    languages: new Set(),
    sort: "median_views",
    dir: "desc",
    offset: 0,
    total: 0,
    limit: 50,
  },
  scraper: {
    apps: new Set(),
    countries: new Set(),
    queueView: "handles",
    queueOffset: 0,
    queueTotal: 0,
    policyMode: "all",
  },
  contacts: {
    status: "to_contact",
    apps: new Set(),
    languages: new Set(),
    countries: new Set(),
    offset: 0,
    total: 0,
    limit: 50,
  },
  classificationHandle: null,
  emailHandle: null,
};

const $ = (id) => document.getElementById(id);
const fmt = new Intl.NumberFormat("en", { maximumFractionDigits: 0 });
const compact = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const countryNames = typeof Intl.DisplayNames === "function"
  ? new Intl.DisplayNames(["en"], { type: "region" })
  : null;

const LANGUAGE_META = {
  ENG: ["English", "🇬🇧"], SPA: ["Spanish", "🇪🇸"], ITA: ["Italian", "🇮🇹"],
  POL: ["Polish", "🇵🇱"], DEU: ["German", "🇩🇪"], SWE: ["Swedish", "🇸🇪"],
  FRA: ["French", "🇫🇷"], POR: ["Portuguese", "🇵🇹"], NOR: ["Norwegian", "🇳🇴"],
  NOB: ["Norwegian Bokmål", "🇳🇴"], HRV: ["Croatian", "🇭🇷"], CES: ["Czech", "🇨🇿"],
  SLK: ["Slovak", "🇸🇰"], SLV: ["Slovenian", "🇸🇮"], RON: ["Romanian", "🇷🇴"],
  NLD: ["Dutch", "🇳🇱"], FIN: ["Finnish", "🇫🇮"], DAN: ["Danish", "🇩🇰"],
  EST: ["Estonian", "🇪🇪"], TUR: ["Turkish", "🇹🇷"], JPN: ["Japanese", "🇯🇵"],
  KOR: ["Korean", "🇰🇷"], VIE: ["Vietnamese", "🇻🇳"], UKR: ["Ukrainian", "🇺🇦"],
  RUS: ["Russian", "🇷🇺"], SRP: ["Serbian", "🇷🇸"], BUL: ["Bulgarian", "🇧🇬"],
  SQI: ["Albanian", "🇦🇱"], IND: ["Indonesian", "🇮🇩"], HAT: ["Haitian Creole", "🇭🇹"],
  UND: ["Undetermined", "🌐"], ZXX: ["No language", "🌐"], UNKNOWN: ["Unknown", "🌐"],
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const text = await response.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`Invalid response (${response.status})`);
  }
  if (!response.ok) throw new Error(data.error || response.statusText);
  return data;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function icons() {
  if (window.lucide) window.lucide.createIcons({ attrs: { "aria-hidden": "true" } });
  hydrateTooltips();
}

function hydrateTooltips(root = document) {
  root.querySelectorAll("button, summary, a.icon-button, input, select, textarea, [title], [data-tooltip]").forEach((node) => {
    const label = node.closest("label")?.querySelector("span")?.textContent.trim();
    const tooltip = node.dataset.tooltip
      || node.getAttribute("title")
      || node.getAttribute("aria-label")
      || label
      || node.getAttribute("placeholder")
      || (node.matches("button") ? node.textContent.trim().replace(/\s+/g, " ") : "");
    if (tooltip) node.dataset.tooltip = tooltip;
    node.removeAttribute("title");
  });
}

function positionTooltip(target) {
  const tooltip = $("hoverTooltip");
  const rect = target.getBoundingClientRect();
  tooltip.classList.add("show");
  const tooltipRect = tooltip.getBoundingClientRect();
  const left = Math.min(
    window.innerWidth - tooltipRect.width / 2 - 8,
    Math.max(tooltipRect.width / 2 + 8, rect.left + rect.width / 2),
  );
  const below = rect.bottom + 8;
  const top = below + tooltipRect.height <= window.innerHeight - 8
    ? below
    : Math.max(8, rect.top - tooltipRect.height - 8);
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function showTooltip(target) {
  const tooltip = $("hoverTooltip");
  clearTimeout(showTooltip.timer);
  showTooltip.timer = setTimeout(() => {
    tooltip.textContent = target.dataset.tooltip;
    tooltip.dataset.for = target.id || "";
    positionTooltip(target);
  }, 300);
}

function hideTooltip() {
  clearTimeout(showTooltip.timer);
  $("hoverTooltip").classList.remove("show");
}

function toast(message, tone = "ok") {
  const node = $("toast");
  node.textContent = message;
  node.dataset.tone = tone;
  node.classList.add("show");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => node.classList.remove("show"), 3200);
}

function showError(error) {
  console.error(error);
  toast(error.message || "Request failed", "error");
}

function flagForCountry(code) {
  const normalized = String(code || "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(normalized)) return "🌐";
  return String.fromCodePoint(...[...normalized].map((char) => 127397 + char.charCodeAt(0)));
}

function languageMeta(code) {
  const normalized = String(code || "UNKNOWN").toUpperCase();
  return LANGUAGE_META[normalized] || [normalized, "🌐"];
}

function languageHtml(code) {
  const [name, flag] = languageMeta(code);
  return `<span class="language-label" title="${escapeHtml(code)}"><span class="flag">${flag}</span>${escapeHtml(name)}</span>`;
}

function countryHtml(code) {
  if (!code) return `<span class="muted-value">Unknown</span>`;
  const normalized = code.toUpperCase();
  let name = normalized;
  try { name = countryNames?.of(normalized) || normalized; } catch {}
  return `<span class="language-label"><span class="flag">${flagForCountry(normalized)}</span>${escapeHtml(name)}</span>`;
}

function formatNumber(value) {
  return value == null ? "—" : compact.format(value);
}

function queueCountsHtml(counts) {
  if (!counts?.length) return `<span><b>0</b> queued</span>`;
  return counts.map((item) => `<span><b>${fmt.format(item.count)}</b> ${escapeHtml(item.status)}</span>`).join("");
}

function bindSegmented(id, onChange) {
  const root = $(id);
  root.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-value]");
    if (!button) return;
    root.querySelectorAll("button").forEach((item) => item.classList.toggle("selected", item === button));
    onChange(button.dataset.value);
  });
}

function renderMultiSelect({ id, options, selected, placeholder, label, onChange, emptyText = "No matches" }) {
  const root = $(id);
  const menu = root.querySelector("[data-options]");
  const summary = root.querySelector("summary span");
  const search = root.querySelector("[data-search]");
  const query = search?.value.trim().toLowerCase() || "";
  const visible = options.filter((option) => label(option).toLowerCase().includes(query));
  menu.innerHTML = visible.map((option) => {
    const text = label(option);
    return `<label class="select-option"><input type="checkbox" value="${escapeHtml(option)}" ${selected.has(option) ? "checked" : ""} /><span>${escapeHtml(text)}</span></label>`;
  }).join("") || `<span class="select-option muted-value">${escapeHtml(emptyText)}</span>`;

  const selectedValues = [...selected];
  summary.textContent = selectedValues.length === 0
    ? placeholder
    : selectedValues.length === 1
      ? label(selectedValues[0])
      : `${selectedValues.length} selected`;

  menu.querySelectorAll('input[type="checkbox"]').forEach((input) => {
    input.addEventListener("change", () => {
      if (input.checked) selected.add(input.value); else selected.delete(input.value);
      renderMultiSelect({ id, options, selected, placeholder, label, onChange, emptyText });
      onChange();
    });
  });

  if (search && !search.dataset.bound) {
    search.dataset.bound = "true";
    search.addEventListener("input", () => renderMultiSelect({ id, options, selected, placeholder, label, onChange, emptyText }));
  }
  icons();
}

function renderAllMultiSelects() {
  const languageLabel = (code) => `${languageMeta(code)[1]} ${languageMeta(code)[0]}`;
  const appLabel = (name) => name;
  const countryLabel = (code) => `${flagForCountry(code)} ${countryNames?.of(code) || code}`;
  renderMultiSelect({ id: "reviewLanguageFilter", options: state.languages, selected: state.review.languages, placeholder: "All languages", label: languageLabel, onChange: resetAndRefreshReview });
  renderMultiSelect({ id: "reviewAppFilter", options: state.apps.map((app) => app.name), selected: state.review.apps, placeholder: "All apps", label: appLabel, onChange: resetAndRefreshReview });
  renderMultiSelect({ id: "scraperCountryFilter", options: state.parentCountries, selected: state.scraper.countries, placeholder: "All parent countries", label: countryLabel, onChange: () => {}, emptyText: "No parent country data yet" });
  renderMultiSelect({ id: "scraperAppFilter", options: state.apps.filter((app) => app.policy !== "blacklist").map((app) => app.name), selected: state.scraper.apps, placeholder: "Whitelist only", label: appLabel, onChange: () => {} });
  renderMultiSelect({ id: "contactCountryFilter", options: state.countries, selected: state.contacts.countries, placeholder: "All countries", label: countryLabel, onChange: resetAndRefreshContacts });
  renderMultiSelect({ id: "contactLanguageFilter", options: state.languages, selected: state.contacts.languages, placeholder: "All languages", label: languageLabel, onChange: resetAndRefreshContacts });
  renderMultiSelect({ id: "contactAppFilter", options: state.apps.map((app) => app.name), selected: state.contacts.apps, placeholder: "All apps", label: appLabel, onChange: resetAndRefreshContacts });
}

async function refreshMetadata() {
  const [appsData, languagesData, countriesData, parentCountriesData] = await Promise.all([
    api("/api/apps"), api("/api/languages"), api("/api/countries"), api("/api/queue/countries"),
  ]);
  state.apps = appsData.apps || [];
  state.languages = languagesData.languages || [];
  state.countries = countriesData.countries || [];
  state.parentCountries = parentCountriesData.countries || [];
  $("appNames").innerHTML = state.apps.map((app) => `<option value="${escapeHtml(app.name)}"></option>`).join("");
  renderAllMultiSelects();
  renderPolicyList();
  icons();
}

async function refreshParentCountries() {
  const data = await api("/api/queue/countries");
  state.parentCountries = data.countries || [];
  renderAllMultiSelects();
}

function reviewParams() {
  const params = new URLSearchParams({
    app_mode: state.review.appMode,
    sort: state.review.sort,
    dir: state.review.dir,
    limit: String(state.review.limit),
    offset: String(state.review.offset),
  });
  if (state.review.appMode === "__WITH_APP__" && state.review.apps.size) params.set("apps", [...state.review.apps].join(","));
  if (state.review.languages.size) params.set("languages", [...state.review.languages].join(","));
  setParam(params, "email", $("reviewEmail").value);
  setParam(params, "min_followers", $("reviewMinFollowers").value);
  setParam(params, "max_followers", $("reviewMaxFollowers").value);
  setParam(params, "min_median_views", $("reviewMinMedian").value);
  setParam(params, "max_median_views", $("reviewMaxMedian").value);
  setParam(params, "min_avg_views", $("reviewMinAvg").value);
  setParam(params, "max_avg_views", $("reviewMaxAvg").value);
  return params;
}

function setParam(params, key, value) {
  const normalized = String(value || "").trim();
  if (normalized) params.set(key, normalized);
}

async function refreshReview() {
  const data = await api(`/api/creators?${reviewParams()}`);
  state.review.total = data.total || 0;
  state.review.offset = data.offset || 0;
  $("reviewTotal").textContent = fmt.format(state.review.total);
  $("reviewRows").innerHTML = (data.creators || []).map(reviewRowHtml).join("");
  $("reviewEmpty").hidden = (data.creators || []).length > 0;
  updatePager("review", data, data.creators?.length || 0);
  bindReviewRows();
  icons();
}

function reviewRowHtml(creator) {
  const app = creator.promoted_app_name;
  const appCell = `<button class="editable-cell app-cell" data-edit-app="${escapeHtml(creator.handle)}" data-app="${escapeHtml(app || "")}" data-tooltip="Edit promoted app" type="button">${app ? `<span class="app-badge">${escapeHtml(app)}</span>` : `<span class="muted-value">No app</span>`}</button>`;
  const email = `<button class="editable-cell email-cell" data-edit-email="${escapeHtml(creator.handle)}" data-email="${escapeHtml(creator.email || "")}" data-tooltip="Edit email address" type="button">${creator.email ? escapeHtml(creator.email) : `<span class="muted-value">Unknown</span>`}</button>`;
  const contactState = creator.contact_status || "unselected";
  const contactLabel = contactState === "to_contact" ? "Queued" : contactState === "contacted" ? "Contacted" : "Contact";
  return `<tr>
    <td><div class="creator-identity"><a href="https://www.tiktok.com/@${escapeHtml(creator.handle)}" target="_blank" rel="noreferrer">${escapeHtml(creator.contact_name || creator.display_name || `@${creator.handle}`)}</a><small>@${escapeHtml(creator.handle)}</small></div></td>
    <td>${appCell}</td>
    <td>${languageHtml(creator.language_code)}</td>
    <td class="number-cell">${formatNumber(creator.follower_count)}</td>
    <td class="number-cell">${formatNumber(creator.median_views)}</td>
    <td class="number-cell">${formatNumber(creator.avg_views)}</td>
    <td>${email}</td>
    <td><div class="row-actions"><button class="contact-button ${contactState === "to_contact" ? "queued" : ""}" data-contact="${escapeHtml(creator.handle)}" data-contact-state="${escapeHtml(contactState)}" data-tooltip="${contactState === "to_contact" ? "Remove from Contact Queue" : contactState === "contacted" ? "Open Contact Queue" : "Add to Contact Queue"}" type="button">${escapeHtml(contactLabel)}</button><button class="icon-button" data-bucket="${escapeHtml(creator.handle)}" type="button" title="Add to frontier" aria-label="Add to frontier"><i data-lucide="network"></i></button></div></td>
  </tr>`;
}

function bindEditableCreatorFields(root = document) {
  root.querySelectorAll("[data-edit-app]").forEach((button) => {
    if (button.dataset.bound) return;
    button.dataset.bound = "true";
    button.addEventListener("click", () => openClassification(button.dataset.editApp, button.dataset.app));
  });
  root.querySelectorAll("[data-edit-email]").forEach((button) => {
    if (button.dataset.bound) return;
    button.dataset.bound = "true";
    button.addEventListener("click", () => openEmailEditor(button.dataset.editEmail, button.dataset.email));
  });
}

function bindReviewRows() {
  document.querySelectorAll("[data-contact]").forEach((button) => button.addEventListener("click", async () => {
    if (button.dataset.contactState === "contacted") {
      switchTab("contacts");
      return;
    }
    const next = button.dataset.contactState === "to_contact" ? "unselected" : "to_contact";
    await setContactStatus(button.dataset.contact, next);
  }));
  document.querySelectorAll("[data-bucket]").forEach((button) => button.addEventListener("click", () => addFrontierSeed(button.dataset.bucket).catch(showError)));
  bindEditableCreatorFields($("reviewRows"));
}

function resetAndRefreshReview() {
  state.review.offset = 0;
  refreshReview().catch(showError);
}

function contactParams(limit = state.contacts.limit) {
  const statuses = state.contacts.status === "all" ? ["to_contact", "contacted"] : [state.contacts.status];
  const params = new URLSearchParams({
    contact_statuses: statuses.join(","),
    sort: "contact_priority",
    dir: "desc",
    limit: String(limit),
    offset: String(state.contacts.offset),
  });
  if (state.contacts.apps.size) params.set("apps", [...state.contacts.apps].join(","));
  if (state.contacts.languages.size) params.set("languages", [...state.contacts.languages].join(","));
  if (state.contacts.countries.size) params.set("countries", [...state.contacts.countries].join(","));
  return params;
}

async function refreshContacts() {
  const data = await api(`/api/creators?${contactParams()}`);
  state.contacts.total = data.total || 0;
  state.contacts.offset = data.offset || 0;
  $("contactTotal").textContent = fmt.format(state.contacts.total);
  $("contactRows").innerHTML = (data.creators || []).map(contactRowHtml).join("");
  $("contactEmpty").hidden = (data.creators || []).length > 0;
  updatePager("contact", data, data.creators?.length || 0);
  bindContactRows();
  icons();
}

function contactRowHtml(creator) {
  const contacted = creator.contact_status === "contacted";
  const app = creator.promoted_app_name;
  return `<tr>
    <td><div class="creator-identity"><a href="https://www.tiktok.com/@${escapeHtml(creator.handle)}" target="_blank" rel="noreferrer">${escapeHtml(creator.contact_name || creator.display_name || `@${creator.handle}`)}</a><small>@${escapeHtml(creator.handle)} · ${languageMeta(creator.language_code)[1]} ${escapeHtml(languageMeta(creator.language_code)[0])}</small></div></td>
    <td><button class="editable-cell app-cell" data-edit-app="${escapeHtml(creator.handle)}" data-app="${escapeHtml(app || "")}" data-tooltip="Edit promoted app" type="button">${app ? `<span class="app-badge">${escapeHtml(app)}</span>` : `<span class="muted-value">No app</span>`}</button></td>
    <td>${countryHtml(creator.country_code)}</td>
    <td class="number-cell">${formatNumber(creator.follower_count)}</td>
    <td class="number-cell">${formatNumber(creator.median_views)}</td>
    <td><button class="editable-cell email-cell" data-edit-email="${escapeHtml(creator.handle)}" data-email="${escapeHtml(creator.email || "")}" data-tooltip="Edit email address" type="button">${creator.email ? escapeHtml(creator.email) : `<span class="muted-value">Unknown</span>`}</button></td>
    <td><span class="state-badge" data-state="${escapeHtml(creator.contact_status)}">${contacted ? "Contacted" : "To contact"}</span></td>
    <td><div class="row-actions">${creator.email ? `<a class="icon-button" href="mailto:${escapeHtml(creator.email)}" title="Write email" aria-label="Write email"><i data-lucide="mail"></i></a>` : ""}<button class="secondary-button" data-contact-action="${escapeHtml(creator.handle)}" data-next="${contacted ? "to_contact" : "contacted"}" type="button">${contacted ? "Requeue" : "Mark contacted"}</button><button class="icon-button" data-contact-remove="${escapeHtml(creator.handle)}" type="button" title="Remove from contact queue" aria-label="Remove from contact queue"><i data-lucide="x"></i></button></div></td>
  </tr>`;
}

function bindContactRows() {
  document.querySelectorAll("[data-contact-action]").forEach((button) => button.addEventListener("click", () => setContactStatus(button.dataset.contactAction, button.dataset.next)));
  document.querySelectorAll("[data-contact-remove]").forEach((button) => button.addEventListener("click", () => setContactStatus(button.dataset.contactRemove, "unselected")));
  bindEditableCreatorFields($("contactRows"));
}

async function setContactStatus(handle, status) {
  await api(`/api/creators/${encodeURIComponent(handle)}/contact`, { method: "PATCH", body: JSON.stringify({ status }) });
  toast(status === "to_contact" ? "Added to Contact Queue" : status === "contacted" ? "Marked as contacted" : "Removed from Contact Queue");
  await Promise.all([refreshContactCount(), state.activeTab === "review" ? refreshReview() : Promise.resolve(), state.activeTab === "contacts" ? refreshContacts() : Promise.resolve()]);
}

async function refreshContactCount() {
  const params = new URLSearchParams({ contact_statuses: "to_contact", limit: "1", offset: "0" });
  const data = await api(`/api/creators?${params}`);
  $("contactNavCount").textContent = compact.format(data.total || 0);
}

function updatePager(prefix, data, shown) {
  const start = shown ? (data.offset || 0) + 1 : 0;
  const end = (data.offset || 0) + shown;
  $(`${prefix}Range`).textContent = `${fmt.format(start)}–${fmt.format(end)} of ${fmt.format(data.total || 0)}`;
  $(`${prefix}Prev`).disabled = !data.has_prev;
  $(`${prefix}Next`).disabled = !data.has_next;
}

async function refreshRunStatus() {
  const status = await api("/api/run/status");
  const running = status.running;
  const stopping = status.stopping;
  const label = stopping ? "Stopping" : running ? "Running" : "Idle";
  $("runPill").lastChild.textContent = label;
  $("runPill").dataset.state = stopping ? "stopping" : running ? "running" : "idle";
  $("runButton").disabled = running;
  $("stopButton").disabled = !running || stopping;
  $("sidebarRunState").textContent = label;
  $("sidebarLiveDot").classList.toggle("running", running);
  const summary = status.last_summary;
  let line = "Ready";
  if (running) {
    if (status.handles?.length) line = `Running ${status.handles.length} selected handles`;
    else if (status.app_names?.length) line = `Running ${status.app_names.length} selected apps`;
    else line = "Running app whitelist";
  } else if (summary) {
    line = `${summary.succeeded} complete · ${summary.failed} failed · ${summary.skipped} skipped`;
  }
  if (status.last_error) line = "Last run failed";
  $("runLine").textContent = line;
  $("sidebarRunLine").textContent = line;
  $("queueCounts").innerHTML = queueCountsHtml(status.queue_counts || []);
}

async function startRun() {
  const handles = $("runHandles").value.split(/[\s,]+/).map((value) => value.trim()).filter(Boolean);
  const explicitBatch = handles.length > 0;
  const limit = $("runLimit").value.trim();
  await api("/api/run", {
    method: "POST",
    body: JSON.stringify({
      concurrency: Number($("runConcurrency").value || 10),
      limit: limit ? Number(limit) : null,
      handles,
      apps: explicitBatch ? [] : [...state.scraper.apps],
      countries: explicitBatch ? [] : [...state.scraper.countries],
      whitelist_only: !explicitBatch && state.scraper.apps.size === 0,
    }),
  });
  toast(explicitBatch ? `Started ${handles.length} selected handles` : "Scraper started");
  await refreshRunStatus();
}

async function stopRun() {
  await api("/api/run/stop", { method: "POST", body: "{}" });
  toast("Stop requested");
  await refreshRunStatus();
}

async function refreshQueue() {
  const limit = Number($("queueLimit").value || 50);
  const params = new URLSearchParams({ status: $("queueStatus").value, limit: String(limit), offset: String(state.scraper.queueOffset) });
  const sourceOnly = state.scraper.queueView === "sources";
  const data = await api(`${sourceOnly ? "/api/queue/sources" : "/api/queue"}?${params}`);
  state.scraper.queueOffset = data.offset || 0;
  state.scraper.queueTotal = data.total || 0;
  $("queueTable").dataset.view = sourceOnly ? "sources" : "handles";
  $("queueHead").innerHTML = sourceOnly
    ? `<tr><th>Source</th><th title="Parent market inferred from the source creator's classified language">Parent country</th><th>App</th><th title="App queue policy">Priority</th><th title="Queue items shown for this source">Queued handles</th><th class="source-action-col"></th></tr>`
    : `<tr><th>Handle</th><th title="Parent creator that discovered this handle">Source</th><th title="Parent market inferred from the source creator's classified language">Parent country</th><th>App</th><th title="App queue policy">Priority</th><th>Status</th><th title="Number of crawl attempts">Attempts</th></tr>`;
  $("queueRows").innerHTML = sourceOnly
    ? (data.items || []).map((item) => `<tr>
      <td><div class="creator-identity"><a href="https://www.tiktok.com/@${escapeHtml(item.source_handle)}" target="_blank" rel="noreferrer">@${escapeHtml(item.source_handle)}</a></div></td>
      <td>${countryHtml(item.country_code)}</td>
      <td>${item.app_name ? `<span class="app-badge">${escapeHtml(item.app_name)}</span>` : `<span class="muted-value">Unknown</span>`}</td>
      <td><span class="policy-badge" data-policy="${escapeHtml(item.app_policy)}">${escapeHtml(item.app_policy)}</span></td>
      <td class="number-cell">${fmt.format(item.item_count)}</td>
      <td>${item.removable_count ? sourceRemoveButtonHtml(item.source_handle) : ""}</td>
    </tr>`).join("")
    : (data.items || []).map((item) => `<tr>
      <td><div class="creator-identity"><a href="https://www.tiktok.com/@${escapeHtml(item.handle)}" target="_blank" rel="noreferrer">@${escapeHtml(item.handle)}</a></div></td>
      <td>${item.discovered_from ? `<div class="queue-source"><a class="queue-source-link" href="https://www.tiktok.com/@${escapeHtml(item.discovered_from)}" target="_blank" rel="noreferrer">@${escapeHtml(item.discovered_from)}</a>${item.status !== "done" ? sourceRemoveButtonHtml(item.discovered_from) : ""}</div>` : `<span class="muted-value">Manual</span>`}</td>
      <td>${countryHtml(item.inferred_country_code)}</td>
      <td>${item.inferred_app_name ? `<span class="app-badge">${escapeHtml(item.inferred_app_name)}</span>` : `<span class="muted-value">Unknown</span>`}</td>
      <td><span class="policy-badge" data-policy="${escapeHtml(item.app_policy)}">${escapeHtml(item.app_policy)}</span></td>
      <td><span class="status-label" data-status="${escapeHtml(item.status)}">${escapeHtml(item.status)}</span></td>
      <td class="number-cell">${fmt.format(item.attempts)}</td>
    </tr>`).join("");
  document.querySelectorAll("[data-remove-source]").forEach((button) => button.addEventListener("click", () => removeQueueSource(button.dataset.removeSource).catch(showError)));
  updatePager("queue", data, data.items?.length || 0);
  icons();
}

function sourceRemoveButtonHtml(source) {
  return `<button class="source-remove" data-remove-source="${escapeHtml(source)}" type="button" data-tooltip="Immediately remove every unprocessed queue item from @${escapeHtml(source)}" aria-label="Remove source @${escapeHtml(source)}"><i data-lucide="x"></i></button>`;
}

async function removeQueueSource(source) {
  const result = await api(`/api/queue/source/${encodeURIComponent(source)}`, { method: "DELETE" });
  state.scraper.queueOffset = 0;
  toast(`Removed ${fmt.format(result.removed || 0)} accounts from @${source}`);
  await Promise.all([refreshQueue(), refreshRunStatus(), refreshParentCountries()]);
}

function renderPolicyList() {
  const query = $("policySearch")?.value.trim().toLowerCase() || "";
  const apps = state.apps.filter((app) => (state.scraper.policyMode === "all" || app.policy === state.scraper.policyMode) && app.name.toLowerCase().includes(query));
  $("policyList").innerHTML = apps.map((app) => `<div class="policy-row"><div><strong>${escapeHtml(app.name)}</strong><small>${fmt.format(app.creator_count)} creators</small></div><div class="policy-control" aria-label="${escapeHtml(app.name)} policy"><button class="${app.policy === "whitelist" ? "selected" : ""}" data-policy-app="${escapeHtml(app.name)}" data-policy="whitelist" type="button" title="Whitelist" aria-label="Whitelist"><i data-lucide="star"></i></button><button class="${app.policy === "neutral" ? "selected" : ""}" data-policy-app="${escapeHtml(app.name)}" data-policy="neutral" type="button" title="Neutral" aria-label="Neutral"><i data-lucide="minus"></i></button><button class="${app.policy === "blacklist" ? "selected" : ""}" data-policy-app="${escapeHtml(app.name)}" data-policy="blacklist" type="button" title="Blacklist" aria-label="Blacklist"><i data-lucide="ban"></i></button></div></div>`).join("");
  document.querySelectorAll("[data-policy-app]").forEach((button) => button.addEventListener("click", () => updateAppPolicy(button.dataset.policyApp, button.dataset.policy).catch(showError)));
  icons();
}

async function updateAppPolicy(name, policy) {
  await api(`/api/apps/${encodeURIComponent(name)}/policy`, { method: "PATCH", body: JSON.stringify({ policy }) });
  toast(`${name} set to ${policy}`);
  await Promise.all([refreshMetadata(), refreshQueue(), refreshRunStatus()]);
}

async function addApp(event) {
  event.preventDefault();
  const name = $("newAppName").value.trim();
  if (!name) return;
  await api("/api/apps", { method: "POST", body: JSON.stringify({ name }) });
  $("newAppName").value = "";
  toast("App added");
  await refreshMetadata();
}

async function seedCreator(event) {
  event.preventDefault();
  const handle = $("seedHandle").value.trim();
  if (!handle) return;
  await api("/api/seed", { method: "POST", body: JSON.stringify({ handle, app_name: $("seedApp").value.trim() || null }) });
  $("seedHandle").value = "";
  $("seedApp").value = "";
  toast("Creator added to queue");
  state.scraper.queueOffset = 0;
  await Promise.all([refreshQueue(), refreshRunStatus(), refreshMetadata()]);
}

function openClassification(handle, app) {
  state.classificationHandle = handle;
  $("classificationHandle").textContent = `@${handle}`;
  $("classificationApp").value = app || "";
  $("classificationDialog").showModal();
}

function openEmailEditor(handle, email) {
  state.emailHandle = handle;
  $("emailHandle").textContent = `@${handle}`;
  $("creatorEmail").value = email || "";
  $("emailDialog").showModal();
}

async function saveClassification(clear = false) {
  if (!state.classificationHandle) return;
  const appName = clear ? null : $("classificationApp").value.trim() || null;
  await api(`/api/creators/${encodeURIComponent(state.classificationHandle)}/classification`, { method: "PATCH", body: JSON.stringify({ app_name: appName }) });
  $("classificationDialog").close();
  toast(clear ? "Marked as no app" : "Classification saved");
  await Promise.all([refreshReview(), refreshMetadata(), refreshQueue()]);
}

async function saveEmail(clear = false) {
  if (!state.emailHandle) return;
  const field = $("creatorEmail");
  if (!clear && field.value.trim() && !field.checkValidity()) {
    field.reportValidity();
    return;
  }
  const email = clear ? null : field.value.trim() || null;
  await api(`/api/creators/${encodeURIComponent(state.emailHandle)}/email`, { method: "PATCH", body: JSON.stringify({ email }) });
  $("emailDialog").close();
  toast(clear ? "Email cleared" : "Email saved");
  await Promise.all([
    state.activeTab === "review" ? refreshReview() : Promise.resolve(),
    state.activeTab === "contacts" ? refreshContacts() : Promise.resolve(),
  ]);
}

async function refreshFrontierStatus() {
  const status = await api("/api/frontier/run/status");
  $("frontierRunPill").textContent = status.stopping ? "Stopping" : status.running ? "Running" : "Idle";
  $("frontierRunButton").disabled = status.running;
  $("frontierStopButton").disabled = !status.running || status.stopping;
  $("frontierBucketCount").textContent = `${fmt.format(status.bucket_count || 0)} seeds`;
  $("frontierRunCounts").innerHTML = queueCountsHtml(status.item_counts || []);
}

async function refreshFrontierBucket() {
  const data = await api("/api/frontier/bucket");
  $("frontierBucketCount").textContent = `${fmt.format(data.total || 0)} seeds`;
  $("frontierBucketRows").innerHTML = (data.items || []).map((item) => `<tr><td><a class="email-link" href="https://www.tiktok.com/@${escapeHtml(item.handle)}" target="_blank" rel="noreferrer">@${escapeHtml(item.handle)}</a></td><td>${escapeHtml(item.promoted_app_name || "—")}</td><td><button class="icon-button" data-frontier-remove="${escapeHtml(item.handle)}" type="button" title="Remove seed" aria-label="Remove seed"><i data-lucide="x"></i></button></td></tr>`).join("");
  document.querySelectorAll("[data-frontier-remove]").forEach((button) => button.addEventListener("click", () => removeFrontierSeed(button.dataset.frontierRemove).catch(showError)));
  icons();
}

async function refreshFrontierItems() {
  const data = await api("/api/frontier/items?limit=100");
  $("frontierRunCounts").innerHTML = queueCountsHtml(data.counts || []);
  $("frontierRows").innerHTML = (data.items || []).map((item) => `<tr><td><a class="email-link" href="https://www.tiktok.com/@${escapeHtml(item.handle)}" target="_blank" rel="noreferrer">@${escapeHtml(item.handle)}</a><span class="cell-secondary">${item.discovered_from ? `from @${escapeHtml(item.discovered_from)}` : "seed"}</span></td><td>${fmt.format(item.depth)}</td><td><span class="status-label" data-status="${escapeHtml(item.status)}">${escapeHtml(item.status)}</span></td><td><button class="icon-button" data-frontier-add="${escapeHtml(item.handle)}" type="button" title="Add to bucket" aria-label="Add to bucket"><i data-lucide="plus"></i></button></td></tr>`).join("");
  document.querySelectorAll("[data-frontier-add]").forEach((button) => button.addEventListener("click", () => addFrontierSeed(button.dataset.frontierAdd).catch(showError)));
  icons();
}

async function addFrontierSeed(handle, source = "dashboard") {
  if (!handle) return;
  await api("/api/frontier/bucket", { method: "POST", body: JSON.stringify({ handle, source }) });
  toast("Frontier seed added");
  await Promise.all([refreshFrontierBucket(), refreshFrontierStatus()]);
}

async function removeFrontierSeed(handle) {
  await api(`/api/frontier/bucket/${encodeURIComponent(handle)}`, { method: "DELETE" });
  toast("Frontier seed removed");
  await Promise.all([refreshFrontierBucket(), refreshFrontierStatus()]);
}

async function startFrontierRun() {
  const limit = $("frontierLimit").value.trim();
  await api("/api/frontier/run", { method: "POST", body: JSON.stringify({ concurrency: Number($("frontierConcurrency").value || 10), limit: limit ? Number(limit) : null, refresh_seeds: $("frontierRefreshSeeds").checked }) });
  toast("Frontier crawl started");
  await Promise.all([refreshFrontierStatus(), refreshFrontierItems()]);
}

async function stopFrontierRun() {
  await api("/api/frontier/run/stop", { method: "POST", body: "{}" });
  toast("Frontier stop requested");
  await refreshFrontierStatus();
}

function switchTab(tab) {
  state.activeTab = tab;
  document.querySelectorAll("[data-tab-target]").forEach((button) => button.classList.toggle("active", button.dataset.tabTarget === tab));
  document.querySelectorAll("[data-tab-page]").forEach((page) => page.classList.toggle("active", page.dataset.tabPage === tab));
  history.replaceState(null, "", `#${tab}`);
  if (tab === "review") refreshReview().catch(showError);
  if (tab === "scraper") Promise.all([refreshRunStatus(), refreshQueue(), refreshFrontierStatus(), refreshFrontierBucket(), refreshFrontierItems()]).catch(showError);
  if (tab === "contacts") refreshContacts().catch(showError);
  window.scrollTo({ top: 0 });
}

function clearReviewFilters() {
  state.review.apps.clear();
  state.review.languages.clear();
  ["reviewMinFollowers", "reviewMaxFollowers", "reviewMinMedian", "reviewMaxMedian", "reviewMinAvg", "reviewMaxAvg"].forEach((id) => { $(id).value = ""; });
  $("reviewEmail").value = "";
  renderAllMultiSelects();
  resetAndRefreshReview();
}

function bindEvents() {
  document.querySelectorAll("[data-tab-target]").forEach((button) => button.addEventListener("click", () => switchTab(button.dataset.tabTarget)));
  bindSegmented("reviewAppMode", (value) => {
    state.review.appMode = value;
    $("reviewAppFilter").hidden = value === "__NO_APP__";
    resetAndRefreshReview();
  });
  bindSegmented("reviewSortDir", (value) => { state.review.dir = value; resetAndRefreshReview(); });
  bindSegmented("contactStatusFilter", (value) => { state.contacts.status = value; resetAndRefreshContacts(); });
  bindSegmented("policyMode", (value) => { state.scraper.policyMode = value; renderPolicyList(); });
  bindSegmented("queueViewMode", (value) => { state.scraper.queueView = value; state.scraper.queueOffset = 0; refreshQueue().catch(showError); });

  $("reviewSort").addEventListener("change", () => { state.review.sort = $("reviewSort").value; resetAndRefreshReview(); });
  $("advancedToggle").addEventListener("click", () => { $("advancedFilters").hidden = !$("advancedFilters").hidden; });
  $("clearReviewFilters").addEventListener("click", clearReviewFilters);
  ["reviewMinFollowers", "reviewMaxFollowers", "reviewMinMedian", "reviewMaxMedian", "reviewMinAvg", "reviewMaxAvg", "reviewEmail"].forEach((id) => $(id).addEventListener("change", resetAndRefreshReview));
  $("reviewPrev").addEventListener("click", () => { state.review.offset = Math.max(0, state.review.offset - state.review.limit); refreshReview().catch(showError); });
  $("reviewNext").addEventListener("click", () => { state.review.offset += state.review.limit; refreshReview().catch(showError); });

  $("contactPrev").addEventListener("click", () => { state.contacts.offset = Math.max(0, state.contacts.offset - state.contacts.limit); refreshContacts().catch(showError); });
  $("contactNext").addEventListener("click", () => { state.contacts.offset += state.contacts.limit; refreshContacts().catch(showError); });

  $("runButton").addEventListener("click", () => startRun().catch(showError));
  $("stopButton").addEventListener("click", () => stopRun().catch(showError));
  $("refreshQueue").addEventListener("click", () => refreshQueue().catch(showError));
  $("queueStatus").addEventListener("change", () => { state.scraper.queueOffset = 0; refreshQueue().catch(showError); });
  $("queueLimit").addEventListener("change", () => { state.scraper.queueOffset = 0; refreshQueue().catch(showError); });
  $("queuePrev").addEventListener("click", () => { state.scraper.queueOffset = Math.max(0, state.scraper.queueOffset - Number($("queueLimit").value || 50)); refreshQueue().catch(showError); });
  $("queueNext").addEventListener("click", () => { state.scraper.queueOffset += Number($("queueLimit").value || 50); refreshQueue().catch(showError); });
  $("policySearch").addEventListener("input", renderPolicyList);
  $("appForm").addEventListener("submit", (event) => addApp(event).catch(showError));
  $("seedForm").addEventListener("submit", (event) => seedCreator(event).catch(showError));

  $("saveClassification").addEventListener("click", () => saveClassification(false).catch(showError));
  $("clearClassification").addEventListener("click", () => saveClassification(true).catch(showError));
  $("saveEmail").addEventListener("click", () => saveEmail(false).catch(showError));
  $("clearEmail").addEventListener("click", () => saveEmail(true).catch(showError));
  $("emailForm").addEventListener("submit", (event) => { event.preventDefault(); saveEmail(false).catch(showError); });

  $("frontierSeedForm").addEventListener("submit", (event) => { event.preventDefault(); const handle = $("frontierHandle").value.trim(); addFrontierSeed(handle, "manual").then(() => { $("frontierHandle").value = ""; }).catch(showError); });
  $("frontierRunButton").addEventListener("click", () => startFrontierRun().catch(showError));
  $("frontierStopButton").addEventListener("click", () => stopFrontierRun().catch(showError));

  document.addEventListener("click", (event) => {
    document.querySelectorAll("details.multi-select[open], details.sort-menu[open]").forEach((details) => {
      if (!details.contains(event.target)) details.removeAttribute("open");
    });
  });
  document.addEventListener("pointerover", (event) => {
    const target = event.target.closest?.("[data-tooltip]");
    if (target) showTooltip(target);
  });
  document.addEventListener("pointerout", (event) => {
    const target = event.target.closest?.("[data-tooltip]");
    if (target && !target.contains(event.relatedTarget)) hideTooltip();
  });
  document.addEventListener("focusin", (event) => {
    const target = event.target.closest?.("[data-tooltip]");
    if (target) showTooltip(target);
  });
  document.addEventListener("focusout", hideTooltip);
}

function resetAndRefreshContacts() {
  state.contacts.offset = 0;
  refreshContacts().catch(showError);
}

async function initialize() {
  bindEvents();
  icons();
  await Promise.all([refreshMetadata(), refreshRunStatus(), refreshContactCount()]);
  const requested = location.hash.slice(1);
  switchTab(["review", "scraper", "contacts"].includes(requested) ? requested : "review");
}

initialize().catch(showError);
setInterval(() => {
  refreshRunStatus()
    .then(() => state.activeTab === "scraper" ? Promise.all([refreshQueue(), refreshParentCountries(), refreshFrontierStatus(), refreshFrontierItems()]) : null)
    .catch(showError);
}, 5000);
setInterval(() => refreshContactCount().catch(showError), 15000);
