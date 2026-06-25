// ═══════════════════════════════════════════════════════════════════════════
// sap-client.js — SAP Business One Service Layer integration for Sunloc v39
// ═══════════════════════════════════════════════════════════════════════════
//
// PURPOSE
//   Provides a single SapClient class that talks to SAP B1 Service Layer
//   (OData REST API at https://sapcore.shl.com:50000/b1s/v1/) for:
//     - Login / session management (B1SESSION cookie)
//     - Pulling open Sales Orders (indents) for Unplanned Orders page
//     - Pushing AR Invoice trigger to SAP when Sunloc dispatch manager fires
//     - Pulling generated invoices back into Sunloc for dispatch flow
//
// DESIGN NOTES
//   - All public methods return { ok: true, data } on success or
//     { ok: false, error: '...', degraded: true } on failure.
//   - degraded === true means SAP unreachable / not configured / down — caller
//     should display a soft warning, not a hard error. The system keeps running.
//   - Sessions cached in sap_config row 1. Auto-refresh on 401 / timeout.
//   - Every API call logged to sap_audit_log (rolling 5000 rows).
//   - Password encrypted in DB using SAP_ENCRYPT_KEY env var (AES-256-GCM).
//   - Self-signed cert tolerance via `rejectUnauthorized: false` since the
//     factory's SAP server uses a self-signed cert (per screenshots).
//
// USAGE (from server.js)
//   const { SapClient } = require('./sap-client');
//   const sap = new SapClient({ pgPool, db, log: console });
//   const { ok, indents } = await sap.fetchOpenSalesOrders();
//   if (!ok) { /* show degraded UI */ }
// ═══════════════════════════════════════════════════════════════════════════

const crypto = require('crypto');
const https = require('https');
const http = require('http');
const { URL } = require('url');

// AES-256-GCM with a 32-byte key derived from env var
function _getEncKey() {
  const raw = process.env.SAP_ENCRYPT_KEY || 'sunloc-dev-fallback-key-not-for-production-use';
  // Derive a 32-byte key via SHA-256 so any-length env var works
  return crypto.createHash('sha256').update(raw).digest();
}

function encryptPassword(plain) {
  if (!plain) return '';
  try {
    const key = _getEncKey();
    const iv = crypto.randomBytes(12); // GCM standard nonce size
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const enc = Buffer.concat([cipher.update(plain, 'utf8'), cipher.final()]);
    const tag = cipher.getAuthTag();
    // Format: base64(iv | tag | ciphertext)
    return Buffer.concat([iv, tag, enc]).toString('base64');
  } catch (e) {
    return '';
  }
}

function decryptPassword(encoded) {
  if (!encoded) return '';
  try {
    const key = _getEncKey();
    const buf = Buffer.from(encoded, 'base64');
    if (buf.length < 28) return ''; // iv (12) + tag (16) minimum
    const iv = buf.subarray(0, 12);
    const tag = buf.subarray(12, 28);
    const ct = buf.subarray(28);
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(tag);
    const plain = Buffer.concat([decipher.update(ct), decipher.final()]).toString('utf8');
    return plain;
  } catch (e) {
    return '';
  }
}

class SapClient {
  /**
   * @param {object} opts
   * @param {object|null} opts.pgPool - PostgreSQL pool, if running in PG mode
   * @param {object|null} opts.db - better-sqlite3 instance, if running in SQLite mode
   * @param {object} opts.log - logger (defaults to console)
   */
  constructor({ pgPool = null, db = null, log = console } = {}) {
    this.pgPool = pgPool;
    this.db = db;
    this.log = log;
    this._sessionCache = null; // { cookie, routeId, expiresAt }
    this._configCache = null;  // last read from sap_config
    this._configCacheAt = 0;
    this._inflightLogin = null; // dedupe concurrent login attempts
  }

  // ── Config access ──────────────────────────────────────────────────────────
  async getConfig({ forceRefresh = false } = {}) {
    const now = Date.now();
    if (!forceRefresh && this._configCache && (now - this._configCacheAt) < 30_000) {
      return this._configCache;
    }
    let row;
    if (this.pgPool) {
      const r = await this.pgPool.query(`SELECT * FROM sap_config WHERE id = 1`);
      row = r.rows[0];
    } else {
      row = this.db.prepare(`SELECT * FROM sap_config WHERE id = 1`).get();
    }
    this._configCache = row || null;
    this._configCacheAt = now;
    return this._configCache;
  }

  async saveConfig({ url, username, companyDb, password, updatedBy }) {
    const passEnc = password ? encryptPassword(password) : undefined;
    const fields = [];
    const values = [];
    if (url !== undefined) { fields.push('sap_url'); values.push(url); }
    if (username !== undefined) { fields.push('sap_username'); values.push(username); }
    if (companyDb !== undefined) { fields.push('sap_company_db'); values.push(companyDb); }
    if (passEnc !== undefined && password !== '') { fields.push('sap_password_encrypted'); values.push(passEnc); }
    if (!fields.length) return;
    fields.push('updated_by'); values.push(updatedBy || 'admin');
    if (this.pgPool) {
      const setClauses = fields.map((f, i) => `${f} = $${i+1}`).concat([`updated_at = NOW()::TEXT`]);
      await this.pgPool.query(
        `UPDATE sap_config SET ${setClauses.join(', ')} WHERE id = 1`,
        values
      );
    } else {
      const setClauses = fields.map(f => `${f} = ?`).concat([`updated_at = datetime('now')`]);
      this.db.prepare(`UPDATE sap_config SET ${setClauses.join(', ')} WHERE id = 1`).run(...values);
    }
    // Invalidate cache + session (force re-login with new creds)
    this._configCache = null;
    this._sessionCache = null;
  }

  isConfigured(cfg) {
    if (!cfg) return false;
    return !!(cfg.sap_url && cfg.sap_username && cfg.sap_company_db && cfg.sap_password_encrypted);
  }

  // ── Audit logging ──────────────────────────────────────────────────────────
  async _audit(entry) {
    try {
      const e = {
        method: entry.method || '',
        endpoint: entry.endpoint || '',
        status_code: entry.statusCode || 0,
        duration_ms: entry.durationMs || 0,
        success: entry.success ? 1 : 0,
        error_message: (entry.errorMessage || '').toString().substring(0, 1000),
        request_summary: (entry.requestSummary || '').toString().substring(0, 2000),
        response_summary: (entry.responseSummary || '').toString().substring(0, 2000),
      };
      if (this.pgPool) {
        await this.pgPool.query(
          `INSERT INTO sap_audit_log (method, endpoint, status_code, duration_ms, success, error_message, request_summary, response_summary)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [e.method, e.endpoint, e.status_code, e.duration_ms, !!entry.success, e.error_message, e.request_summary, e.response_summary]
        );
        // Prune: keep only 5000 most recent
        await this.pgPool.query(`
          DELETE FROM sap_audit_log WHERE id NOT IN (
            SELECT id FROM sap_audit_log ORDER BY id DESC LIMIT 5000
          )
        `);
      } else {
        this.db.prepare(
          `INSERT INTO sap_audit_log (method, endpoint, status_code, duration_ms, success, error_message, request_summary, response_summary)
           VALUES (?,?,?,?,?,?,?,?)`
        ).run(e.method, e.endpoint, e.status_code, e.duration_ms, (e.success ? 1 : 0), e.error_message, e.request_summary, e.response_summary);
        // SQLite prune
        this.db.prepare(`
          DELETE FROM sap_audit_log WHERE id NOT IN (
            SELECT id FROM sap_audit_log ORDER BY id DESC LIMIT 5000
          )
        `).run();
      }
    } catch (e) {
      // Audit failures should never break the call path
      this.log.warn && this.log.warn('[SAP-AUDIT] failed to write audit row:', e.message);
    }
  }

  // ── HTTPS plumbing ─────────────────────────────────────────────────────────
  /**
   * Low-level HTTP request to SAP. Tolerates self-signed cert. Returns
   * { statusCode, headers, body } or throws on network error.
   * Times out at 20s by default.
   */
  _httpRequest({ url, method = 'GET', headers = {}, body = null, timeoutMs = 20_000 }) {
    return new Promise((resolve, reject) => {
      let parsed;
      try {
        parsed = new URL(url);
      } catch (e) {
        return reject(new Error('Invalid URL: ' + url));
      }
      const isHttps = parsed.protocol === 'https:';
      const lib = isHttps ? https : http;
      const opts = {
        method,
        hostname: parsed.hostname,
        port: parsed.port || (isHttps ? 443 : 80),
        path: parsed.pathname + parsed.search,
        headers: { 'Accept': 'application/json', ...headers },
        // SAP server in factory uses self-signed cert per screenshots
        rejectUnauthorized: false,
      };
      const req = lib.request(opts, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          const buf = Buffer.concat(chunks);
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: buf.toString('utf8'),
          });
        });
      });
      req.on('error', (err) => reject(err));
      req.setTimeout(timeoutMs, () => {
        req.destroy(new Error(`SAP request timed out after ${timeoutMs}ms`));
      });
      if (body) {
        const payload = typeof body === 'string' ? body : JSON.stringify(body);
        req.setHeader('Content-Type', 'application/json');
        req.setHeader('Content-Length', Buffer.byteLength(payload));
        req.write(payload);
      }
      req.end();
    });
  }

  // ── Login / session management ─────────────────────────────────────────────
  /**
   * POST /b1s/v1/Login. Returns session cookie. Updates sap_config.
   * Dedupes concurrent calls so we don't hammer the server when many requests
   * arrive simultaneously after a session expiry.
   */
  async login({ force = false } = {}) {
    if (this._inflightLogin && !force) {
      return this._inflightLogin;
    }
    const promise = (async () => {
      const cfg = await this.getConfig({ forceRefresh: true });
      if (!this.isConfigured(cfg)) {
        return { ok: false, error: 'SAP not configured (URL/username/password/companyDb missing)', degraded: true };
      }
      const password = decryptPassword(cfg.sap_password_encrypted);
      if (!password) {
        return { ok: false, error: 'SAP password decryption failed — set SAP_ENCRYPT_KEY env var and re-enter password', degraded: true };
      }
      const loginUrl = cfg.sap_url.replace(/\/$/, '') + '/b1s/v1/Login';
      const started = Date.now();
      try {
        const res = await this._httpRequest({
          url: loginUrl,
          method: 'POST',
          body: {
            CompanyDB: cfg.sap_company_db,
            UserName: cfg.sap_username,
            Password: password,
          },
        });
        const duration = Date.now() - started;
        if (res.statusCode === 200) {
          // Extract B1SESSION + ROUTEID cookies from Set-Cookie headers
          const setCookieHeaders = res.headers['set-cookie'] || [];
          let b1session = null, routeId = null;
          for (const c of setCookieHeaders) {
            const mB1 = /B1SESSION=([^;]+)/.exec(c);
            const mRoute = /ROUTEID=([^;]+)/.exec(c);
            if (mB1) b1session = mB1[1];
            if (mRoute) routeId = mRoute[1];
          }
          let timeoutMin = 30;
          try {
            const j = JSON.parse(res.body);
            if (j && j.SessionTimeout) timeoutMin = parseInt(j.SessionTimeout, 10) || 30;
          } catch {}
          const expiresAt = new Date(Date.now() + (timeoutMin - 1) * 60 * 1000).toISOString();
          this._sessionCache = { cookie: b1session, routeId, expiresAt };
          // Persist to DB for visibility (and so next process boot can reuse)
          if (this.pgPool) {
            await this.pgPool.query(
              `UPDATE sap_config SET session_cookie=$1, session_route_id=$2, session_expires_at=$3,
                 last_login_at=NOW()::TEXT, last_login_success=TRUE, last_login_error=NULL WHERE id=1`,
              [b1session, routeId, expiresAt]
            );
          } else {
            this.db.prepare(
              `UPDATE sap_config SET session_cookie=?, session_route_id=?, session_expires_at=?,
                 last_login_at=datetime('now'), last_login_success=1, last_login_error=NULL WHERE id=1`
            ).run(b1session, routeId, expiresAt);
          }
          await this._audit({
            method: 'POST', endpoint: '/b1s/v1/Login', statusCode: 200,
            durationMs: duration, success: true,
            requestSummary: `CompanyDB=${cfg.sap_company_db} UserName=${cfg.sap_username}`,
            responseSummary: `SessionTimeout=${timeoutMin}min`,
          });
          this._configCache = null; // force refresh on next read
          return { ok: true, sessionCookie: b1session, routeId, expiresAt };
        } else {
          let errMsg = `SAP login HTTP ${res.statusCode}`;
          try {
            const j = JSON.parse(res.body);
            if (j.error && j.error.message) errMsg = (j.error.message.value || j.error.message);
          } catch {}
          if (this.pgPool) {
            await this.pgPool.query(
              `UPDATE sap_config SET last_login_at=NOW()::TEXT, last_login_success=FALSE, last_login_error=$1 WHERE id=1`,
              [errMsg]
            );
          } else {
            this.db.prepare(
              `UPDATE sap_config SET last_login_at=datetime('now'), last_login_success=0, last_login_error=? WHERE id=1`
            ).run(errMsg);
          }
          await this._audit({
            method: 'POST', endpoint: '/b1s/v1/Login', statusCode: res.statusCode,
            durationMs: duration, success: false, errorMessage: errMsg,
            requestSummary: `CompanyDB=${cfg.sap_company_db}`,
          });
          return { ok: false, error: errMsg, degraded: true };
        }
      } catch (e) {
        const duration = Date.now() - started;
        const errMsg = `Network error: ${e.message}`;
        try {
          if (this.pgPool) {
            await this.pgPool.query(
              `UPDATE sap_config SET last_login_at=NOW()::TEXT, last_login_success=FALSE, last_login_error=$1 WHERE id=1`,
              [errMsg]
            );
          } else {
            this.db.prepare(
              `UPDATE sap_config SET last_login_at=datetime('now'), last_login_success=0, last_login_error=? WHERE id=1`
            ).run(errMsg);
          }
        } catch {}
        await this._audit({
          method: 'POST', endpoint: '/b1s/v1/Login', statusCode: 0,
          durationMs: duration, success: false, errorMessage: errMsg,
        });
        return { ok: false, error: errMsg, degraded: true };
      }
    })();
    this._inflightLogin = promise;
    promise.finally(() => { this._inflightLogin = null; });
    return promise;
  }

  /** Returns a valid session cookie (re-logging in if cache expired). */
  async ensureSession() {
    // Try in-memory cache first
    if (this._sessionCache && this._sessionCache.cookie && this._sessionCache.expiresAt) {
      if (new Date(this._sessionCache.expiresAt).getTime() > Date.now() + 30_000) {
        return { ok: true, cookie: this._sessionCache.cookie, routeId: this._sessionCache.routeId };
      }
    }
    // Try DB cache (cross-process / cross-reboot)
    const cfg = await this.getConfig();
    if (!this.isConfigured(cfg)) {
      return { ok: false, error: 'SAP not configured', degraded: true };
    }
    if (cfg.session_cookie && cfg.session_expires_at) {
      const exp = new Date(cfg.session_expires_at).getTime();
      if (exp > Date.now() + 30_000) {
        this._sessionCache = {
          cookie: cfg.session_cookie,
          routeId: cfg.session_route_id,
          expiresAt: cfg.session_expires_at
        };
        return { ok: true, cookie: cfg.session_cookie, routeId: cfg.session_route_id };
      }
    }
    // Need fresh login
    const r = await this.login();
    if (!r.ok) return { ok: false, error: r.error, degraded: true };
    return { ok: true, cookie: r.sessionCookie, routeId: r.routeId };
  }

  /**
   * Authenticated GET/POST/PATCH/DELETE to SAP. Auto-retries once on 401.
   * Returns { ok, status, data, error, degraded }.
   */
  async call({ method, path, body = null, query = '' }) {
    const sess = await this.ensureSession();
    if (!sess.ok) return { ok: false, error: sess.error, degraded: true };
    const cfg = await this.getConfig();
    const base = cfg.sap_url.replace(/\/$/, '');
    const fullPath = (path.startsWith('/') ? path : '/b1s/v1/' + path) + (query ? (path.includes('?') ? '&' : '?') + query : '');
    const url = base + fullPath;
    const cookieHeader = `B1SESSION=${sess.cookie}` + (sess.routeId ? `; ROUTEID=${sess.routeId}` : '');
    const started = Date.now();
    let firstStatus = 0;
    try {
      let res = await this._httpRequest({
        url,
        method,
        headers: { 'Cookie': cookieHeader, 'B1S-CaseInsensitive': 'true' },
        body,
      });
      firstStatus = res.statusCode;
      if (res.statusCode === 401) {
        // Session expired — relogin once and retry
        this._sessionCache = null;
        const r2 = await this.login({ force: true });
        if (!r2.ok) {
          const dur = Date.now() - started;
          await this._audit({
            method, endpoint: fullPath, statusCode: 401, durationMs: dur,
            success: false, errorMessage: 'Session expired and re-login failed: ' + r2.error,
            requestSummary: body ? JSON.stringify(body).substring(0, 500) : '',
          });
          return { ok: false, error: r2.error, degraded: true };
        }
        const cookie2 = `B1SESSION=${r2.sessionCookie}` + (r2.routeId ? `; ROUTEID=${r2.routeId}` : '');
        res = await this._httpRequest({
          url,
          method,
          headers: { 'Cookie': cookie2, 'B1S-CaseInsensitive': 'true' },
          body,
        });
      }
      const dur = Date.now() - started;
      const ok2xx = res.statusCode >= 200 && res.statusCode < 300;
      let parsed = null;
      try { parsed = res.body ? JSON.parse(res.body) : null; } catch { parsed = res.body; }
      await this._audit({
        method, endpoint: fullPath, statusCode: res.statusCode, durationMs: dur,
        success: ok2xx,
        errorMessage: ok2xx ? '' : (() => {
          try { return parsed?.error?.message?.value || parsed?.error?.message || res.body.substring(0, 500); }
          catch { return res.body.substring(0, 500); }
        })(),
        requestSummary: body ? JSON.stringify(body).substring(0, 500) : '',
        responseSummary: typeof parsed === 'object' ? JSON.stringify(parsed).substring(0, 1000) : String(parsed).substring(0, 1000),
      });
      if (ok2xx) {
        return { ok: true, status: res.statusCode, data: parsed };
      } else {
        const errMsg = (parsed?.error?.message?.value || parsed?.error?.message || `HTTP ${res.statusCode}`);
        return { ok: false, status: res.statusCode, error: errMsg, data: parsed };
      }
    } catch (e) {
      const dur = Date.now() - started;
      await this._audit({
        method, endpoint: fullPath, statusCode: firstStatus, durationMs: dur,
        success: false, errorMessage: 'Network error: ' + e.message,
        requestSummary: body ? JSON.stringify(body).substring(0, 500) : '',
      });
      return { ok: false, error: 'Network error: ' + e.message, degraded: true };
    }
  }

  // ── Public business operations ─────────────────────────────────────────────

  /** Test SAP connection by performing a Login + Logout. */
  async testConnection() {
    const r = await this.login({ force: true });
    if (!r.ok) return { ok: false, error: r.error };
    // Try a simple metadata-light call to confirm session works
    const probe = await this.call({ method: 'GET', path: 'SQLQueries', query: '$top=1' });
    // We don't care if SQLQueries returns data; we just need to see auth pass.
    // A 401 here means session is broken; 404/200 both prove auth.
    if (probe.status === 401) {
      return { ok: false, error: 'Auth probe returned 401 after login — session not honoured' };
    }
    return { ok: true, message: 'SAP connection verified' };
  }

  /**
   * Fetch open Sales Orders (sales indents) from SAP for the Unplanned Orders page.
   * Pulls all Orders with DocumentStatus 'O' (open) and DocDate >= today - N days.
   * Returns { ok, indents } where each indent has { DocEntry, DocNum, CardCode, CardName, DocDate, DocDueDate, DocumentLines: [...] }
   */
  async fetchOpenSalesOrders({ lookbackDays = 365 } = {}) {
    // Open Sales Orders are fetched by DocumentStatus eq 'bost_Open'. The DocDate window is only a
    // sanity guardrail (kept at 365 days). NOTE: the real reason recent open orders (e.g. Alkem #199)
    // were missing was NOT the date window but SAP Service Layer PAGINATION — see the nextLink loop
    // below; the old code read only the first page.
    const lookbackDate = new Date(Date.now() - lookbackDays * 86400_000);
    const dateStr = lookbackDate.toISOString().slice(0, 10);
    // SAP OData v3 filter syntax: DocumentStatus eq 'bost_Open' and DocDate ge 'YYYY-MM-DD'
    // v41z FIX: Remove date filter — fetch ALL open orders regardless of age
    // Low-numbered POs (191, 199, 166, 200) were created years ago but still open in SAP
    // v41z2 FIX: Also fetch bost_Delivered and bost_Close — SAP marks partially delivered orders
    // as bost_Delivered or bost_Close even when lines still have remaining open quantity
    // bost_Partial does NOT exist in this SAP version
    // v41z4 FIX: Use bost_Open only (correct status) + manual $skip pagination
    // SAP B1 Service Layer ignores $top and returns max 20 per page with no nextLink
    // So we manually paginate using $skip=0, $skip=20, $skip=40... until empty page
    const filter = `$filter=DocumentStatus eq 'bost_Open'`;
    const select = `$select=DocEntry,DocNum,CardCode,CardName,DocDate,DocDueDate,DocTotal,DocCurrency,DocumentLines`;
    // v41ZC issue 1: SINGLE, unambiguous pagination strategy. The previous loop mixed manual $skip
    // with @odata.nextLink — when a nextLink was present it fetched that page inline AND advanced
    // skip past it, double-skipping (or dropping) a page and losing orders. SAP B1 Service Layer
    // pages by $skip with a server-set page size (commonly 20). We page purely by $skip, derive the
    // page size from the first page, and stop only on a short or empty page. nextLink is followed
    // ONLY when SAP omits a full page yet provides one (defensive), without also bumping skip.
    let indents = [];
    let complete = true;         // v44O: restore prune-safety signal — false if pagination broke mid-way (partial set), so the caller must NOT prune the indent cache on an incomplete fetch
    let skip = 0;
    let pageSize = 0;            // learned from the first page
    let pageGuard = 0;
    const seen = new Set();      // guard against any duplicate DocEntry across pages
    while (pageGuard < 500) {
      pageGuard++;
      const query = `${filter}&${select}&$skip=${skip}`;
      const r = await this.call({ method: 'GET', path: 'Orders', query });
      if (!r.ok) {
        if (skip === 0) return { ok: false, error: r.error, degraded: r.degraded };
        console.warn('[SAP fetch] page failed at skip=' + skip + ', returning partial set of ' + indents.length + ':', r.error);
        complete = false;        // v44O: partial/paged-failure set — caller must NOT prune (would wrongly delete unfetched open orders)
        break;
      }
      const page = r.data?.value || [];
      if (page.length === 0) break;
      if (pageSize === 0) pageSize = page.length; // first page defines the server page size
      let added = 0;
      for (const o of page) {
        if (o && o.DocEntry != null && seen.has(o.DocEntry)) continue;
        if (o && o.DocEntry != null) seen.add(o.DocEntry);
        indents.push(o); added++;
      }
      console.log('[SAP fetch] skip=' + skip + ' got ' + page.length + ' (new ' + added + '), total ' + indents.length);
      // Stop when SAP returned fewer than a full page → last page reached.
      if (page.length < pageSize) break;
      skip += page.length;
    }
    if (pageGuard >= 500) { complete = false; console.warn('[SAP fetch] hit 500-page guard — treating as INCOMPLETE (no cache prune this cycle)'); }
    console.log('[SAP fetch] DONE — ' + indents.length + ' open Sales Orders across ' + pageGuard + ' page(s)' + (complete ? '' : ' [INCOMPLETE — prune skipped]'));
    return { ok: true, indents, complete };
  }

  /**
   * v40 Phase 18.2: Push a Delivery creation trigger to SAP.
   *
   * IMPORTANT ARCHITECTURE CHANGE FROM v39:
   *   v39 posted to /b1s/v1/Invoices (created A/R Invoice directly).
   *   v40 posts to /b1s/v1/DeliveryNotes — Sunloc creates the Delivery; SAP user
   *   manually converts Delivery → A/R Invoice in SAP via Copy-To.
   *   This matches Sunil Healthcare's actual SAP workflow (Delivery form → Invoice).
   *
   * The Sunloc 5-minute poller picks up the resulting Invoice once SAP user
   * completes the conversion, and routes it to the Tracking → Invoice Queue.
   *
   * @param {object} args
   * @param {string} args.cardCode - SAP customer code from the SO
   * @param {number} args.baseDocEntry - the SAP Sales Order DocEntry (mandatory link)
   * @param {Array<{lineNum, quantity, itemCode?}>} args.lines - which SO lines and quantities to deliver
   * @param {string} args.batchNumber - Sunloc batch reference (stored as UDF)
   * @param {string} args.poNumber - customer PO ref
   * @param {string} args.remarks
   */
  async createDelivery({ cardCode, baseDocEntry, lines, batchNumber, poNumber, remarks }) {
    // Build OData payload for Delivery based on Sales Order
    // SAP's "BaseType" 17 = Sales Order. Each line references the SO line via BaseLine.
    const today = new Date().toISOString().slice(0, 10);
    const documentLines = (lines || []).map((l, i) => ({
      BaseType: 17,             // Sales Order
      BaseEntry: baseDocEntry,
      BaseLine: l.lineNum,
      Quantity: l.quantity,
      // Item code optional — SAP infers from base ref. Pass if we have it.
      ...(l.itemCode ? { ItemCode: l.itemCode } : {}),
    }));
    const payload = {
      CardCode: cardCode,
      DocDate: today,
      DocDueDate: today,
      DocumentLines: documentLines,
      Comments: `Sunloc batch ${batchNumber || ''} PO ${poNumber || ''} ${remarks || ''}`.trim(),
      // Custom fields for Sunloc reference (UDFs must exist in SAP B1 for these to land)
      U_SunlocBatch: batchNumber || '',
      U_SunlocPO: poNumber || '',
    };
    // v40 P18.2: POST to DeliveryNotes endpoint (not Invoices)
    const r = await this.call({ method: 'POST', path: 'DeliveryNotes', body: payload });
    if (!r.ok) {
      return { ok: false, error: r.error, degraded: r.degraded, status: r.status };
    }
    const dlv = r.data || {};
    return {
      ok: true,
      docEntry: dlv.DocEntry,        // Delivery DocEntry — used by poller to match returning Invoice
      docNum: dlv.DocNum,            // Delivery DocNum (NOT Invoice DocNum — that comes later)
      docDate: dlv.DocDate,
      cardCode: dlv.CardCode,
      cardName: dlv.CardName,
      docTotal: dlv.DocTotal,
      objectType: 'Delivery',        // marker so the consumer knows what kind of doc came back
      raw: dlv,
    };
  }

  /**
   * Backward-compat shim: createInvoice() now calls createDelivery() under the hood.
   * Server code that called sap.createInvoice(...) gets the same result shape but
   * the actual SAP-side document created is a Delivery, not an Invoice.
   *
   * @deprecated since v40 — prefer sap.createDelivery() directly.
   */
  async createInvoice(args) {
    const r = await this.createDelivery(args);
    // Map result back to the old "invoice" naming for callers that haven't migrated
    if (r.ok) return { ...r, irn: null };  // IRN only exists on real Invoices, not Deliveries
    return r;
  }

  /**
   * Pull invoices generated in SAP within the last N days. Used by Sunloc's
   * invoice poller to discover both Sunloc-triggered and Direct-SAP invoices.
   */
  async fetchRecentInvoices({ lookbackDays = 60 } = {}) {
    const lookbackDate = new Date(Date.now() - lookbackDays * 86400_000);
    const dateStr = lookbackDate.toISOString().slice(0, 10);
    const filter = `$filter=DocDate ge '${dateStr}'`;
    // v44W FIX (CRITICAL): fetch the FULL invoice entity — NO $select at all. Earlier builds named
    // U_SunlocBatch (and other Sunloc UDFs) in $select; on SAP B1 instances where the A/R Invoice
    // document does not expose that UDF, B1 SL rejects the ENTIRE request ("Property 'U_SunlocBatch'
    // of 'Document' is invalid") and ALL invoice ingestion silently stopped. Omitting $select cannot
    // reference any UDF, so this request can never fail on a missing property. The full entity also
    // returns DocumentLines (so SO-BaseEntry reconciliation + PC/Size/Colour enrichment work) and any
    // UDFs that DO exist on the instance. Heavier per row, but correct and resilient on every instance.
    const invoices = [];
    const seen = new Set();
    let skip = 0, pageSize = 0, pageGuard = 0;
    while (pageGuard < 500) {
      pageGuard++;
      const r = await this.call({ method: 'GET', path: 'Invoices', query: `${filter}&$orderby=DocEntry desc&$skip=${skip}` });
      if (!r.ok) {
        // Partial set (page failure mid-pagination): return what we have, flagged degraded.
        if (invoices.length > 0) return { ok: true, invoices, degraded: true };
        return { ok: false, error: r.error, degraded: r.degraded };
      }
      const page = r.data?.value || [];
      if (page.length === 0) break;
      if (pageSize === 0) pageSize = page.length;
      for (const inv of page) {
        const key = String(inv.DocEntry);
        if (seen.has(key)) continue;
        seen.add(key);
        invoices.push(inv);
      }
      if (page.length < pageSize) break;   // last page reached
      skip += page.length;
    }
    return { ok: true, invoices };
  }

  /** Get a single invoice by DocEntry — used for verifying after creation. */
  async getInvoice(docEntry) {
    const r = await this.call({ method: 'GET', path: `Invoices(${docEntry})` });
    if (!r.ok) return { ok: false, error: r.error, degraded: r.degraded };
    return { ok: true, invoice: r.data };
  }

  /** Returns the most recent N audit log rows. */
  async getAuditLog({ limit = 100 } = {}) {
    let rows;
    if (this.pgPool) {
      const r = await this.pgPool.query(
        `SELECT * FROM sap_audit_log ORDER BY id DESC LIMIT $1`,
        [limit]
      );
      rows = r.rows;
    } else {
      rows = this.db.prepare(`SELECT * FROM sap_audit_log ORDER BY id DESC LIMIT ?`).all(limit);
    }
    return rows;
  }

  /** Health summary for the SAP Status badge. */
  async getStatus() {
    const cfg = await this.getConfig({ forceRefresh: true });
    if (!this.isConfigured(cfg)) {
      return { configured: false, status: 'not_configured', message: 'SAP not configured' };
    }
    // Look at the most recent audit row to assess health
    let last;
    if (this.pgPool) {
      const r = await this.pgPool.query(`SELECT * FROM sap_audit_log ORDER BY id DESC LIMIT 1`);
      last = r.rows[0];
    } else {
      last = this.db.prepare(`SELECT * FROM sap_audit_log ORDER BY id DESC LIMIT 1`).get();
    }
    if (!last) {
      return { configured: true, status: 'unknown', message: 'No SAP calls made yet' };
    }
    const ageMs = Date.now() - new Date(last.called_at).getTime();
    const ageMin = ageMs / 60_000;
    if (last.success) {
      if (ageMin < 5) return { configured: true, status: 'online', message: 'Last call OK', lastCallAt: last.called_at };
      if (ageMin < 15) return { configured: true, status: 'degraded', message: 'Last successful call >5 min ago', lastCallAt: last.called_at };
      return { configured: true, status: 'stale', message: 'No recent successful calls', lastCallAt: last.called_at };
    }
    return { configured: true, status: 'offline', message: last.error_message || 'Last call failed', lastCallAt: last.called_at };
  }
}

module.exports = { SapClient, encryptPassword, decryptPassword };
