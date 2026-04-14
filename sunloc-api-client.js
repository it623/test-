/**
 * SUNLOC API CLIENT
 * Embedded in both Planning App and DPR App.
 * Handles all server communication with localStorage fallback.
 *
 * Usage: paste this block inside a <script> tag in each app.
 * Set window.SUNLOC_SERVER_URL before this runs, e.g.:
 *   <script>window.SUNLOC_SERVER_URL = 'https://your-app.railway.app';</script>
 */

const SunlocAPI = (() => {
  // ── Config ─────────────────────────────────────────────────
  // Server URL: set via window.SUNLOC_SERVER_URL or prompt on first load
  function getServerUrl() {
    let url = window.SUNLOC_SERVER_URL
           || localStorage.getItem('sunloc_server_url')
           || '';
    if (!url) {
      url = prompt(
        '🔗 Enter your Sunloc Server URL\n(e.g. https://sunloc.up.railway.app)\n\nLeave blank to work offline with local storage only.',
        ''
      );
      if (url) {
        url = url.replace(/\/$/, ''); // strip trailing slash
        localStorage.setItem('sunloc_server_url', url);
      }
    }
    return url || null;
  }

  let _serverUrl = null;
  let _online = false;
  let _lastCheck = 0;

  async function serverUrl() {
    if (!_serverUrl) _serverUrl = getServerUrl();
    return _serverUrl;
  }

  // ── Online check (cached 30s) ──────────────────────────────
  async function isOnline() {
    const now = Date.now();
    if (now - _lastCheck < 30000) return _online;
    _lastCheck = now;
    const url = await serverUrl();
    if (!url) { _online = false; return false; }
    try {
      const r = await fetch(url + '/api/health', { signal: AbortSignal.timeout(3000) });
      _online = r.ok;
    } catch { _online = false; }
    return _online;
  }

  // ── Generic fetch with fallback ────────────────────────────
  async function apiFetch(path, options = {}) {
    const url = await serverUrl();
    if (!url) throw new Error('No server configured');
    const res = await fetch(url + path, {
      ...options,
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
      signal: options.signal || AbortSignal.timeout(8000),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  // ══════════════════════════════════════════════════════════
  // PLANNING APP METHODS
  // ══════════════════════════════════════════════════════════

  async function savePlanningState(state) {
    if (!(await isOnline())) {
      localStorage.setItem('sunloc_state', JSON.stringify(state));
      return { ok: true, offline: true };
    }
    try {
      // Save to server
      const res = await apiFetch('/api/planning/state', {
        method: 'POST',
        body: JSON.stringify({ state }),
      });
      // Also keep localStorage in sync as backup
      localStorage.setItem('sunloc_state', JSON.stringify(state));
      return res;
    } catch (err) {
      // Fallback to localStorage
      localStorage.setItem('sunloc_state', JSON.stringify(state));
      return { ok: true, offline: true, error: err.message };
    }
  }

  async function loadPlanningState() {
    if (!(await isOnline())) {
      const local = localStorage.getItem('sunloc_state');
      return { ok: true, offline: true, state: local ? JSON.parse(local) : null };
    }
    try {
      const res = await apiFetch('/api/planning/state');
      // Update localStorage backup
      if (res.state) localStorage.setItem('sunloc_state', JSON.stringify(res.state));
      return res;
    } catch (err) {
      // Fallback to localStorage
      const local = localStorage.getItem('sunloc_state');
      return { ok: true, offline: true, state: local ? JSON.parse(local) : null };
    }
  }

  async function getActiveOrdersForMachine(machineId) {
    if (!(await isOnline())) return { ok: true, offline: true, orders: [] };
    try {
      return await apiFetch(`/api/orders/machine/${encodeURIComponent(machineId)}`);
    } catch { return { ok: true, offline: true, orders: [] }; }
  }

  async function getAllActiveOrders() {
    if (!(await isOnline())) {
      // Read from local planning state
      try {
        const local = localStorage.getItem('sunloc_state');
        if (local) {
          const state = JSON.parse(local);
          const orders = (state.orders || []).filter(o => o.status !== 'closed' && !o.deleted);
          return { ok: true, offline: true, orders };
        }
      } catch {}
      return { ok: true, offline: true, orders: [] };
    }
    try {
      return await apiFetch('/api/orders/active');
    } catch { return { ok: true, offline: true, orders: [] }; }
  }

  async function getOrderActuals(orderId) {
    if (!(await isOnline())) return { ok: true, offline: true, actuals: [], total: 0 };
    try {
      return await apiFetch(`/api/actuals/order/${encodeURIComponent(orderId)}`);
    } catch { return { ok: true, offline: true, actuals: [], total: 0 }; }
  }

  // ══════════════════════════════════════════════════════════
  // DPR APP METHODS
  // ══════════════════════════════════════════════════════════

  async function saveDPRRecord(floor, date, data) {
    // Always save to localStorage as backup
    const localKey = `dpr3_${floor}_${date}`;
    localStorage.setItem(localKey, JSON.stringify(data));

    if (!(await isOnline())) return { ok: true, offline: true };
    try {
      return await apiFetch('/api/dpr/save', {
        method: 'POST',
        body: JSON.stringify({ floor, date, data }),
      });
    } catch (err) {
      return { ok: true, offline: true, error: err.message };
    }
  }

  async function loadDPRRecord(floor, date) {
    if (!(await isOnline())) {
      const local = localStorage.getItem(`dpr3_${floor}_${date}`);
      return { ok: true, offline: true, data: local ? JSON.parse(local) : null };
    }
    try {
      const res = await apiFetch(`/api/dpr/${encodeURIComponent(floor)}/${encodeURIComponent(date)}`);
      // If server has data, update localStorage backup
      if (res.data) localStorage.setItem(`dpr3_${floor}_${date}`, JSON.stringify(res.data));
      // If server has no data but localStorage does, use that and push to server
      if (!res.data) {
        const local = localStorage.getItem(`dpr3_${floor}_${date}`);
        if (local) {
          res.data = JSON.parse(local);
          // Push to server silently
          apiFetch('/api/dpr/save', {
            method: 'POST',
            body: JSON.stringify({ floor, date, data: res.data }),
          }).catch(() => {});
        }
      }
      return res;
    } catch (err) {
      const local = localStorage.getItem(`dpr3_${floor}_${date}`);
      return { ok: true, offline: true, data: local ? JSON.parse(local) : null };
    }
  }

  async function getDPRDates(floor) {
    if (!(await isOnline())) {
      // Reconstruct from localStorage keys
      const keys = Object.keys(localStorage).filter(k => k.startsWith(`dpr3_${floor}_`));
      const dates = keys.map(k => k.replace(`dpr3_${floor}_`, '')).sort().reverse();
      return { ok: true, offline: true, dates };
    }
    try {
      return await apiFetch(`/api/dpr/dates/${encodeURIComponent(floor)}`);
    } catch {
      const keys = Object.keys(localStorage).filter(k => k.startsWith(`dpr3_${floor}_`));
      const dates = keys.map(k => k.replace(`dpr3_${floor}_`, '')).sort().reverse();
      return { ok: true, offline: true, dates };
    }
  }

  // ══════════════════════════════════════════════════════════
  // SERVER CONFIG UI
  // ══════════════════════════════════════════════════════════

  function getConfiguredUrl() {
    return _serverUrl || localStorage.getItem('sunloc_server_url') || null;
  }

  function setServerUrl(url) {
    _serverUrl = url ? url.replace(/\/$/, '') : null;
    _online = false;
    _lastCheck = 0;
    if (url) localStorage.setItem('sunloc_server_url', _serverUrl);
    else localStorage.removeItem('sunloc_server_url');
  }

  async function checkHealth() {
    const url = await serverUrl();
    if (!url) return null;
    try {
      const r = await fetch(url + '/api/health', { signal: AbortSignal.timeout(5000) });
      return r.ok ? await r.json() : null;
    } catch { return null; }
  }

  // ── Status indicator helper ────────────────────────────────
  // Call this to render a small sync status badge in your app's UI
  // Pass the element ID where the badge should appear
  async function renderStatusBadge(elementId) {
    const el = document.getElementById(elementId);
    if (!el) return;
    el.innerHTML = '<span style="color:#888;font-size:11px">Checking...</span>';
    const online = await isOnline();
    const url = getConfiguredUrl();
    if (!url) {
      el.innerHTML = '<span style="background:#444;color:#ccc;padding:2px 8px;border-radius:10px;font-size:11px;cursor:pointer" onclick="SunlocAPI.promptServerUrl()">⚙️ Set Server URL</span>';
    } else if (online) {
      el.innerHTML = `<span style="background:rgba(0,230,118,0.15);color:#00e676;border:1px solid rgba(0,230,118,0.3);padding:2px 10px;border-radius:10px;font-size:11px">🟢 Synced</span>`;
    } else {
      el.innerHTML = `<span style="background:rgba(255,171,0,0.15);color:#ffab00;border:1px solid rgba(255,171,0,0.3);padding:2px 10px;border-radius:10px;font-size:11px;cursor:pointer" title="${url}" onclick="SunlocAPI.promptServerUrl()">🟡 Offline — click to reconfigure</span>`;
    }
  }

  function promptServerUrl() {
    const current = getConfiguredUrl() || '';
    const newUrl = prompt('Enter Sunloc Server URL:', current);
    if (newUrl !== null) setServerUrl(newUrl.trim());
    _lastCheck = 0; // force recheck
  }

  // ── Public API ─────────────────────────────────────────────
  return {
    // Planning
    savePlanningState,
    loadPlanningState,
    getActiveOrdersForMachine,
    getAllActiveOrders,
    getOrderActuals,
    // DPR
    saveDPRRecord,
    loadDPRRecord,
    getDPRDates,
    // Utils
    isOnline,
    checkHealth,
    getConfiguredUrl,
    setServerUrl,
    renderStatusBadge,
    promptServerUrl,
  };
})();
