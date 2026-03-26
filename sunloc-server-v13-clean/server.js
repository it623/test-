/**
 * SUNLOC INTEGRATED SERVER
 * Shared backend for Planning App + DPR App
 * Stack: Node.js + Express + SQLite (better-sqlite3)
 */

const express = require('express');
const Database = require('better-sqlite3');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

// ─── Middleware ────────────────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── Database Setup ────────────────────────────────────────────
// Priority: 1) DB_PATH env var, 2) /data volume (Railway), 3) local __dirname
function resolveDbPath() {
  if (process.env.DB_PATH) return process.env.DB_PATH;
  // Railway persistent volume — preferred in production
  const volumePath = '/data/sunloc.db';
  try {
    const dir = '/data';
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    // Test we can write to /data
    fs.accessSync(dir, fs.constants.W_OK);
    console.log(`[DB] Using persistent volume: ${volumePath}`);
    return volumePath;
  } catch (e) {
    // /data not available (local dev) — fall back to app directory
    const localPath = path.join(__dirname, 'sunloc.db');
    console.log(`[DB] Volume not available, using local path: ${localPath}`);
    return localPath;
  }
}

const DB_PATH = resolveDbPath();
const db = new Database(DB_PATH);
console.log(`[DB] Database opened at: ${DB_PATH}`);

// Enable WAL mode for better concurrent read/write performance
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// ─── Migration System ──────────────────────────────────────────
// Each migration runs exactly once, tracked in schema_migrations table.
// Adding new tables/columns: just add a new migration — existing data is safe.

// Bootstrap: create migrations tracking table first
db.exec(`
  CREATE TABLE IF NOT EXISTS schema_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL UNIQUE,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
`);

const MIGRATIONS = [
  {
    version: 1,
    name: 'initial_schema',
    sql: `
      CREATE TABLE IF NOT EXISTS planning_state (
        id INTEGER PRIMARY KEY,
        state_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS dpr_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        floor TEXT NOT NULL,
        date TEXT NOT NULL,
        data_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(floor, date)
      );
      CREATE TABLE IF NOT EXISTS production_actuals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT,
        batch_number TEXT,
        machine_id TEXT NOT NULL,
        date TEXT NOT NULL,
        shift TEXT NOT NULL,
        run_index INTEGER NOT NULL DEFAULT 0,
        qty_lakhs REAL DEFAULT 0,
        floor TEXT,
        synced_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(machine_id, date, shift, run_index)
      );
      CREATE INDEX IF NOT EXISTS idx_actuals_order ON production_actuals(order_id);
      CREATE INDEX IF NOT EXISTS idx_actuals_batch ON production_actuals(batch_number);
      CREATE INDEX IF NOT EXISTS idx_actuals_machine ON production_actuals(machine_id, date);
      CREATE INDEX IF NOT EXISTS idx_dpr_date ON dpr_records(date);
    `
  },
  {
    version: 2,
    name: 'tracking_tables',
    sql: `
      CREATE TABLE IF NOT EXISTS tracking_labels (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        label_number INTEGER NOT NULL,
        size TEXT NOT NULL,
        qty REAL NOT NULL,
        is_partial INTEGER DEFAULT 0,
        is_orange INTEGER DEFAULT 0,
        parent_label_id TEXT,
        customer TEXT,
        colour TEXT,
        pc_code TEXT,
        po_number TEXT,
        machine_id TEXT,
        printing_matter TEXT,
        generated TEXT NOT NULL DEFAULT (datetime('now')),
        printed INTEGER DEFAULT 0,
        printed_at TEXT,
        voided INTEGER DEFAULT 0,
        void_reason TEXT,
        voided_at TEXT,
        voided_by TEXT,
        qr_data TEXT,
        UNIQUE(batch_number, label_number, is_orange)
      );
      CREATE TABLE IF NOT EXISTS tracking_scans (
        id TEXT PRIMARY KEY,
        label_id TEXT NOT NULL,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        type TEXT NOT NULL CHECK(type IN ('in','out')),
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        operator TEXT,
        size TEXT,
        qty REAL
      );
      CREATE TABLE IF NOT EXISTS tracking_stage_closure (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        closed INTEGER DEFAULT 1,
        closed_at TEXT NOT NULL DEFAULT (datetime('now')),
        closed_by TEXT,
        UNIQUE(batch_number, dept)
      );
      CREATE TABLE IF NOT EXISTS tracking_wastage (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        type TEXT NOT NULL CHECK(type IN ('salvage','remelt')),
        qty REAL NOT NULL,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        by TEXT
      );
      CREATE TABLE IF NOT EXISTS tracking_dispatch_records (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        customer TEXT,
        qty REAL NOT NULL,
        boxes INTEGER NOT NULL,
        vehicle_no TEXT,
        invoice_no TEXT,
        remarks TEXT,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        by TEXT
      );
      CREATE TABLE IF NOT EXISTS tracking_alerts (
        id TEXT PRIMARY KEY,
        label_id TEXT NOT NULL,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        scan_in_ts TEXT NOT NULL,
        hours_stuck REAL,
        resolved INTEGER DEFAULT 0,
        msg TEXT,
        UNIQUE(label_id, dept)
      );
      CREATE INDEX IF NOT EXISTS idx_scans_batch ON tracking_scans(batch_number, dept);
      CREATE INDEX IF NOT EXISTS idx_labels_batch ON tracking_labels(batch_number);
      CREATE INDEX IF NOT EXISTS idx_wastage_batch ON tracking_wastage(batch_number, dept);
    `
  },
  {
    version: 3,
    name: 'auth_and_audit',
    sql: `
      CREATE TABLE IF NOT EXISTS app_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        pin_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS app_sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        expires_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        ip TEXT,
        ts TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC);
      CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(username);
    `
  },
  // ── ADD FUTURE MIGRATIONS HERE ──
  // { version: 6, name: 'your_migration_name', sql: `ALTER TABLE ...` }
  // Never edit existing migrations — always add new ones
  {
    version: 4,
    name: 'temp_batch_system',
    sql: `
      -- TEMP batches: one per machine per day when no planned order exists
      CREATE TABLE IF NOT EXISTS temp_batches (
        id TEXT PRIMARY KEY,
        machine_id TEXT NOT NULL,
        machine_size TEXT NOT NULL,
        date TEXT NOT NULL,
        daily_cap_lakhs REAL NOT NULL,
        label_count INTEGER NOT NULL,
        pack_size_lakhs REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        reconciled_order_id TEXT,
        reconciled_at TEXT,
        reconciled_by TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(machine_id, date)
      );

      -- Reconciliation requests: Planning Manager proposes, Admin approves
      CREATE TABLE IF NOT EXISTS reconciliation_requests (
        id TEXT PRIMARY KEY,
        proposed_by TEXT NOT NULL,
        proposed_at TEXT NOT NULL DEFAULT (datetime('now')),
        approved_by TEXT,
        approved_at TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        order_id TEXT NOT NULL,
        order_details TEXT NOT NULL,
        back_date TEXT NOT NULL,
        temp_batch_mappings TEXT NOT NULL,
        total_boxes INTEGER NOT NULL,
        rejection_reason TEXT
      );

      -- TEMP batch alerts log
      CREATE TABLE IF NOT EXISTS temp_batch_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        machine_id TEXT NOT NULL,
        temp_batch_id TEXT NOT NULL,
        alert_date TEXT NOT NULL,
        sent_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(machine_id, alert_date)
      );

      CREATE INDEX IF NOT EXISTS idx_temp_batches_machine ON temp_batches(machine_id, date);
      CREATE INDEX IF NOT EXISTS idx_temp_batches_status ON temp_batches(status);
      CREATE INDEX IF NOT EXISTS idx_recon_status ON reconciliation_requests(status);
    `
  },
  {
    version: 5,
    name: 'temp_colour_and_wo_support',
    sql: `
      ALTER TABLE temp_batches ADD COLUMN colour TEXT;
      ALTER TABLE temp_batches ADD COLUMN pc_code TEXT;
      ALTER TABLE temp_batches ADD COLUMN colour_confirmed INTEGER DEFAULT 0;
      ALTER TABLE tracking_labels ADD COLUMN wo_status TEXT;
      CREATE TABLE IF NOT EXISTS wo_reconciliation_requests (
        id TEXT PRIMARY KEY,
        proposed_by TEXT NOT NULL,
        proposed_at TEXT NOT NULL DEFAULT (datetime('now')),
        approved_by TEXT,
        approved_at TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        order_id TEXT NOT NULL,
        customer TEXT NOT NULL,
        po_number TEXT,
        zone TEXT,
        qty_confirmed REAL,
        rejection_reason TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_wo_recon_status ON wo_reconciliation_requests(status);
    `
  }
];

// Run all pending migrations in order
function runMigrations() {
  const applied = new Set(
    db.prepare('SELECT version FROM schema_migrations').all().map(r => r.version)
  );
  let ran = 0;
  for (const m of MIGRATIONS) {
    if (applied.has(m.version)) continue;
    console.log(`[Migration] Running v${m.version}: ${m.name}`);
    db.exec(m.sql);
    db.prepare('INSERT INTO schema_migrations (version, name) VALUES (?, ?)').run(m.version, m.name);
    console.log(`[Migration] v${m.version} applied successfully`);
    ran++;
  }
  if (ran === 0) console.log('[Migration] All migrations up to date');
  else console.log(`[Migration] ${ran} migration(s) applied`);
}

runMigrations();

// ─── Seed default users if none exist ─────────────────────────
const crypto = require('crypto');
function hashPin(pin) { return crypto.createHash('sha256').update(pin + 'sunloc_salt').digest('hex'); }

const seedUsers = [
  // DPR users
  { username: 'GF',     pin: '1111', role: 'gf',      app: 'dpr' },
  { username: 'FF',     pin: '2222', role: 'ff',      app: 'dpr' },
  { username: 'DPR_Admin', pin: '9999', role: 'admin', app: 'dpr' },
  // Planning users
  { username: 'Planning_Manager', pin: '3333', role: 'planning_manager', app: 'planning' },
  { username: 'Printing_Manager', pin: '4444', role: 'printing_manager', app: 'planning' },
  { username: 'Dispatch_Manager', pin: '5555', role: 'dispatch_manager', app: 'planning' },
  { username: 'Plan_Admin',       pin: '9999', role: 'admin',            app: 'planning' },
  // Tracking — admin already built-in but add server auth
  { username: 'Track_Admin', pin: '9999', role: 'admin', app: 'tracking' },
];

const insertUser = db.prepare(`
  INSERT OR IGNORE INTO app_users (username, pin_hash, role, app)
  VALUES (?, ?, ?, ?)
`);
for (const u of seedUsers) {
  insertUser.run(u.username, hashPin(u.pin), u.role, u.app);
}

// Clean expired sessions on startup
db.prepare(`DELETE FROM app_sessions WHERE expires_at < datetime('now')`).run();

// ─── Helper: get latest planning state ────────────────────────
function getPlanningState() {
  const row = db.prepare('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1').get();
  if (!row) return { orders: [], printOrders: [], dispatchPlans: [], dailyPrinting: [], machineMaster: [], printMachineMaster: [], packSizes: {} };
  try { return JSON.parse(row.state_json); } catch { return {}; }
}

// ─── Helper: get active orders for a machine ──────────────────
function getActiveOrdersForMachine(machineId) {
  const state = getPlanningState();
  const orders = state.orders || [];
  return orders.filter(o =>
    o.machineId === machineId &&
    o.status !== 'closed' &&
    !o.deleted
  ).map(o => ({
    id: o.id,
    batchNumber: o.batchNumber || '',
    poNumber: o.poNumber || '',
    customer: o.customer || '',
    size: o.size || '',
    colour: o.colour || '',
    qty: o.qty || 0,
    isPrinted: o.isPrinted || false,
    status: o.status || 'pending',
    zone: o.zone || '',
  }));
}

// Helper: get total actuals for an order (sums all runs across all machines/shifts)
function getOrderActuals(orderId, batchNumber) {
  let rows;
  if (orderId) {
    rows = db.prepare('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE order_id = ?').get(orderId);
    if (!rows?.total && batchNumber) {
      rows = db.prepare('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE batch_number = ?').get(batchNumber);
    }
  } else if (batchNumber) {
    rows = db.prepare('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE batch_number = ?').get(batchNumber);
  }
  return rows?.total || 0;
}

// ═══════════════════════════════════════════════════════════════
// PLANNING APP ROUTES
// ═══════════════════════════════════════════════════════════════

// GET full planning state
app.get('/api/planning/state', (req, res) => {
  try {
    const state = getPlanningState();

    // Enrich orders with live actuals from DPR
    if (state.orders) {
      for (const ord of state.orders) {
        const actual = getOrderActuals(ord.id, ord.batchNumber);
        ord.actualProd = actual;
        if (actual > 0 && ord.status === 'pending') ord.status = 'running';
      }
    }

    res.json({ ok: true, state, savedAt: db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get()?.saved_at });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST save planning state
app.post('/api/planning/state', (req, res) => {
  try {
    const { state } = req.body;
    if (!state) return res.status(400).json({ ok: false, error: 'No state provided' });

    const json = JSON.stringify(state);
    const existing = db.prepare('SELECT id FROM planning_state LIMIT 1').get();
    if (existing) {
      db.prepare('UPDATE planning_state SET state_json = ?, saved_at = datetime(\'now\') WHERE id = ?').run(json, existing.id);
    } else {
      db.prepare('INSERT INTO planning_state (state_json) VALUES (?)').run(json);
    }

    res.json({ ok: true, savedAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET active orders for a machine (used by DPR dropdown)
app.get('/api/orders/machine/:machineId', (req, res) => {
  try {
    const orders = getActiveOrdersForMachine(req.params.machineId);
    res.json({ ok: true, orders });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET all active orders (summary for DPR to cache on load)
app.get('/api/orders/active', (req, res) => {
  try {
    const state = getPlanningState();
    const orders = (state.orders || [])
      .filter(o => o.status !== 'closed' && !o.deleted)
      .map(o => ({
        id: o.id,
        batchNumber: o.batchNumber || '',
        poNumber: o.poNumber || '',
        customer: o.customer || '',
        machineId: o.machineId || '',
        size: o.size || '',
        colour: o.colour || '',
        qty: o.qty || 0,
        actualQty: o.actualQty || 0,
        status: o.status || 'pending',
      }));
    res.json({ ok: true, orders });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// DPR APP ROUTES
// ═══════════════════════════════════════════════════════════════

// GET DPR record for a floor + date
app.get('/api/dpr/:floor/:date', (req, res) => {
  try {
    const { floor, date } = req.params;
    const row = db.prepare('SELECT data_json, saved_at FROM dpr_records WHERE floor = ? AND date = ?').get(floor, date);
    if (!row) return res.json({ ok: true, data: null });
    res.json({ ok: true, data: JSON.parse(row.data_json), savedAt: row.saved_at });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST save DPR record + extract actuals into bridge table
app.post('/api/dpr/save', (req, res) => {
  try {
    const { floor, date, data, actuals } = req.body;
    if (!floor || !date || !data) return res.status(400).json({ ok: false, error: 'Missing floor, date, or data' });

    // Save full DPR record
    db.prepare(`
      INSERT INTO dpr_records (floor, date, data_json)
      VALUES (?, ?, ?)
      ON CONFLICT(floor, date) DO UPDATE SET data_json = excluded.data_json, saved_at = datetime('now')
    `).run(floor, date, JSON.stringify(data));

    // Upsert actuals — supports multi-run (colour change / batch change within same shift)
    const upsertActual = db.prepare(`
      INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET
        order_id = excluded.order_id,
        batch_number = excluded.batch_number,
        qty_lakhs = excluded.qty_lakhs,
        synced_at = datetime('now')
    `);

    // Delete old runs for this floor+date first (clean re-sync)
    const deleteOld = db.prepare(`
      DELETE FROM production_actuals WHERE floor = ? AND date = ?
    `);

    const syncActuals = db.transaction((actualsArr, floor, date) => {
      deleteOld.run(floor, date);
      if (actualsArr && actualsArr.length > 0) {
        // New format: pre-flattened actuals array from DPR app (supports multi-run)
        for (const a of actualsArr) {
          if (!a.qty || a.qty <= 0) continue;
          upsertActual.run(a.orderId || null, a.batchNumber || null, a.machineId, date, a.shift, a.runIndex || 0, a.qty, a.floor || floor);
        }
      } else {
        // Fallback: parse from data.shifts for old single-run format
        const shifts = data.shifts || {};
        for (const [shiftName, shiftData] of Object.entries(shifts)) {
          if (!shiftData.machines) continue;
          for (const [machineId, machineData] of Object.entries(shiftData.machines)) {
            const runs = machineData.runs || [{ orderId: machineData.orderId, batchNumber: machineData.batchNumber, qty: machineData.prod }];
            runs.forEach((run, ri) => {
              const qty = parseFloat(run.qty) || 0;
              if (qty <= 0) return;
              upsertActual.run(run.orderId || null, run.batchNumber || null, machineId, date, shiftName, ri, qty, floor);
            });
          }
        }
      }
    });

    syncActuals(actuals, floor, date);

    // Update actualQty on planning orders (two-way sync: DPR → Planning)
    try {
      const planningState = getPlanningState();
      if (planningState && planningState.orders) {
        // Cumulative actuals per order_id
        const byOrderId = db.prepare(`
          SELECT order_id, SUM(qty_lakhs) as total_qty
          FROM production_actuals
          WHERE order_id IS NOT NULL AND order_id != ''
          GROUP BY order_id
        `).all();
        // Also cumulative by batch_number for rows where order_id not set
        const byBatch = db.prepare(`
          SELECT batch_number, SUM(qty_lakhs) as total_qty
          FROM production_actuals
          WHERE (order_id IS NULL OR order_id = '') AND batch_number IS NOT NULL AND batch_number != ''
          GROUP BY batch_number
        `).all();

        let changed = false;
        // Reset all actuals first to avoid stale data
        for (const ord of planningState.orders) {
          if (ord.actualQty !== undefined) { ord.actualQty = 0; }
        }
        // Apply by orderId (most reliable)
        for (const row of byOrderId) {
          const ord = planningState.orders.find(o => o.id === row.order_id);
          if (ord) { ord.actualQty = parseFloat(row.total_qty.toFixed(3)); changed = true; }
        }
        // Apply by batchNumber for any not matched by orderId
        for (const row of byBatch) {
          const ord = planningState.orders.find(o =>
            o.batchNumber === row.batch_number && (!o.actualQty || o.actualQty === 0)
          );
          if (ord) { ord.actualQty = parseFloat(row.total_qty.toFixed(3)); changed = true; }
        }
        if (changed) {
          db.prepare(`
            INSERT INTO planning_state (id, state_json)
            VALUES (1, ?)
            ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json, updated_at = datetime('now')
          `).run(JSON.stringify(planningState));
        }
      }
    } catch (syncErr) {
      console.error('Planning actualQty sync error:', syncErr.message);
    }

    res.json({ ok: true, savedAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET all DPR dates (for history navigation)
app.get('/api/dpr/dates/:floor', (req, res) => {
  try {
    const rows = db.prepare('SELECT DISTINCT date FROM dpr_records WHERE floor = ? ORDER BY date DESC').all(req.params.floor);
    res.json({ ok: true, dates: rows.map(r => r.date) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET actuals summary for a machine (for DPR to show cumulative vs planned)
app.get('/api/actuals/machine/:machineId', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT date, shift, qty_lakhs, order_id, batch_number
      FROM production_actuals
      WHERE machine_id = ?
      ORDER BY date DESC, shift
      LIMIT 90
    `).all(req.params.machineId);
    res.json({ ok: true, actuals: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET actuals for a specific order
app.get('/api/actuals/order/:orderId', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT date, shift, qty_lakhs, machine_id
      FROM production_actuals
      WHERE order_id = ? OR batch_number = ?
      ORDER BY date, shift
    `).all(req.params.orderId, req.params.orderId);
    const total = rows.reduce((s, r) => s + r.qty_lakhs, 0);
    res.json({ ok: true, actuals: rows, total });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// HEALTH CHECK + INFO
// ═══════════════════════════════════════════════════════════════

// ─── Auth helper ──────────────────────────────────────────────
function generateToken() { return crypto.randomBytes(32).toString('hex'); }

function verifyToken(token) {
  if (!token) return null;
  const session = db.prepare(`
    SELECT * FROM app_sessions WHERE token = ? AND expires_at > datetime('now')
  `).get(token);
  return session || null;
}

function logAudit(username, role, app, action, details, ip) {
  try {
    db.prepare(`INSERT INTO audit_log (username, role, app, action, details, ip) VALUES (?,?,?,?,?,?)`)
      .run(username, role, app, action, details || null, ip || null);
  } catch(e) { console.error('Audit log error:', e.message); }
}

// POST /api/auth/login
app.post('/api/auth/login', (req, res) => {
  try {
    const { username, pin, app: appName } = req.body;
    if (!username || !pin || !appName) return res.status(400).json({ ok: false, error: 'Missing credentials' });
    const user = db.prepare(`SELECT * FROM app_users WHERE username = ? AND app = ?`).get(username, appName);
    if (!user) return res.status(401).json({ ok: false, error: 'User not found' });
    if (user.pin_hash !== hashPin(pin)) return res.status(401).json({ ok: false, error: 'Invalid PIN' });
    // Create session (8 hour expiry)
    const token = generateToken();
    const expires = new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString().replace('T',' ').slice(0,19);
    db.prepare(`INSERT INTO app_sessions (token, user_id, username, role, app, expires_at) VALUES (?,?,?,?,?,?)`)
      .run(token, user.id, user.username, user.role, appName, expires);
    logAudit(user.username, user.role, appName, 'LOGIN', 'Successful login', req.ip);
    res.json({ ok: true, token, username: user.username, role: user.role });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/auth/verify
app.post('/api/auth/verify', (req, res) => {
  const { token } = req.body;
  const session = verifyToken(token);
  if (!session) return res.status(401).json({ ok: false, error: 'Invalid or expired session' });
  res.json({ ok: true, username: session.username, role: session.role, app: session.app });
});

// POST /api/auth/logout
app.post('/api/auth/logout', (req, res) => {
  const { token } = req.body;
  if (token) {
    const session = verifyToken(token);
    if (session) {
      logAudit(session.username, session.role, session.app, 'LOGOUT', null, req.ip);
      db.prepare(`DELETE FROM app_sessions WHERE token = ?`).run(token);
    }
  }
  res.json({ ok: true });
});

// POST /api/auth/change-pin
app.post('/api/auth/change-pin', (req, res) => {
  try {
    const { token, username, newPin } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin' && session.username !== username) {
      return res.status(403).json({ ok: false, error: 'Only admin can change other users PINs' });
    }
    db.prepare(`UPDATE app_users SET pin_hash = ?, updated_at = datetime('now') WHERE username = ? AND app = ?`)
      .run(hashPin(newPin), username, session.app);
    logAudit(session.username, session.role, session.app, 'CHANGE_PIN', `Changed PIN for ${username}`, req.ip);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/audit/log
app.post('/api/audit/log', (req, res) => {
  try {
    const { token, action, details } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    logAudit(session.username, session.role, session.app, action, details, req.ip);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/audit/view — admin only
app.get('/api/audit/view', (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const limit = parseInt(req.query.limit) || 200;
    const app = req.query.app || session.app;
    const rows = db.prepare(`
      SELECT * FROM audit_log WHERE app = ? ORDER BY ts DESC LIMIT ?
    `).all(app, limit);
    res.json({ ok: true, logs: rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/auth/users — admin only, list users for an app
app.get('/api/auth/users', (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const users = db.prepare(`SELECT id, username, role, app, created_at, updated_at FROM app_users WHERE app = ?`)
      .all(req.query.app || session.app);
    res.json({ ok: true, users });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── TEMP Batch Colour/PC Code Update ────────────────────────

// POST /api/temp-batches/update-details — save colour + PC Code (one-time per TEMP batch)
app.post('/api/temp-batches/update-details', (req, res) => {
  try {
    const { tempBatchId, colour, pcCode } = req.body;
    if (!tempBatchId) return res.status(400).json({ ok: false, error: 'Missing tempBatchId' });
    const tb = db.prepare('SELECT * FROM temp_batches WHERE id = ?').get(tempBatchId);
    if (!tb) return res.status(404).json({ ok: false, error: 'TEMP batch not found' });
    db.prepare(`
      UPDATE temp_batches SET colour = ?, pc_code = ?, colour_confirmed = 1 WHERE id = ?
    `).run(colour || null, pcCode || null, tempBatchId);
    logAudit('SYSTEM', 'system', 'dpr', 'TEMP_DETAILS_SET',
      `TEMP batch ${tempBatchId} — Colour: ${colour}, PC Code: ${pcCode}`);
    const updated = db.prepare('SELECT * FROM temp_batches WHERE id = ?').get(tempBatchId);
    res.json({ ok: true, batch: updated });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── W/O (Without Order) Reconciliation ──────────────────────

// POST /api/wo/assign-customer — Planning Manager assigns customer to W/O order
app.post('/api/wo/assign-customer', (req, res) => {
  try {
    const { token, orderId, customer, poNumber, zone, qtyConfirmed } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
    }
    // Update the planning state order
    const planState = getPlanningState();
    const ord = (planState.orders || []).find(o => o.id === orderId);
    if (!ord) return res.status(404).json({ ok: false, error: 'Order not found' });
    if (ord.woStatus !== 'wo') return res.status(400).json({ ok: false, error: 'Order is not a W/O order' });
    ord.customer = customer;
    ord.poNumber = poNumber || ord.poNumber;
    ord.zone = zone || ord.zone;
    if (qtyConfirmed) ord.qty = qtyConfirmed;
    ord.woCustomerAssignedAt = new Date().toISOString();
    ord.woCustomerAssignedBy = session.username;
    // Update dispatch plan customer too
    (planState.dispatchPlans || []).forEach(d => {
      if (d.productionOrderId === orderId) {
        d.customer = customer;
        d.poNumber = poNumber || d.poNumber;
        d.zone = zone || d.zone;
      }
    });
    db.prepare(`
      INSERT INTO planning_state (id, state_json)
      VALUES (1, ?)
      ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json, updated_at = datetime('now')
    `).run(JSON.stringify(planState));
    logAudit(session.username, session.role, 'planning', 'WO_CUSTOMER_ASSIGNED',
      `W/O order ${orderId} assigned to customer: ${customer}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/propose-reconciliation — Planning Manager proposes W/O → real order
app.post('/api/wo/propose-reconciliation', (req, res) => {
  try {
    const { token, orderId, customer, poNumber, zone, qtyConfirmed } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
    }
    if (!customer) return res.status(400).json({ ok: false, error: 'Customer name required' });
    const id = `WORECON-${Date.now()}`;
    db.prepare(`
      INSERT INTO wo_reconciliation_requests
        (id, proposed_by, status, order_id, customer, po_number, zone, qty_confirmed)
      VALUES (?,?,?,?,?,?,?,?)
    `).run(id, session.username, 'pending', orderId, customer, poNumber||null, zone||null, qtyConfirmed||null);
    logAudit(session.username, session.role, 'planning', 'WO_RECON_PROPOSED',
      `W/O reconciliation proposed: ${id} for order ${orderId} → customer ${customer}`);
    res.json({ ok: true, requestId: id, status: 'pending' });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/pending — Admin views pending W/O reconciliation requests
app.get('/api/wo/pending', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const requests = db.prepare(`SELECT * FROM wo_reconciliation_requests WHERE status = 'pending' ORDER BY proposed_at DESC`).all();
    // Enrich with order details from planning state
    const planState = getPlanningState();
    const enriched = requests.map(r => ({
      ...r,
      orderDetails: (planState.orders || []).find(o => o.id === r.order_id) || {}
    }));
    res.json({ ok: true, requests: enriched });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/approve/:id — Admin approves W/O reconciliation
app.post('/api/wo/approve/:id', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const request = db.prepare('SELECT * FROM wo_reconciliation_requests WHERE id = ?').get(req.params.id);
    if (!request) return res.status(404).json({ ok: false, error: 'Request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok: false, error: 'Already processed' });

    const approveWO = db.transaction(() => {
      const now = new Date().toISOString();
      // 1. Update planning state: change woStatus to 'active', add customer
      const planState = getPlanningState();
      const ord = (planState.orders || []).find(o => o.id === request.order_id);
      if (ord) {
        ord.customer = request.customer;
        ord.poNumber = request.po_number || ord.poNumber;
        ord.zone = request.zone || ord.zone;
        if (request.qty_confirmed) ord.qty = request.qty_confirmed;
        ord.woStatus = 'wo-reconciled';
        ord.woReconciledAt = now;
        ord.woReconciledBy = session.username;
        // Update dispatch plans
        (planState.dispatchPlans || []).forEach(d => {
          if (d.productionOrderId === request.order_id) {
            d.customer = request.customer;
            d.poNumber = request.po_number || d.poNumber;
            d.zone = request.zone || d.zone;
          }
        });
        db.prepare(`
          INSERT INTO planning_state (id, state_json) VALUES (1, ?)
          ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json, updated_at = datetime('now')
        `).run(JSON.stringify(planState));
      }
      // 2. Update all tracking labels for this order's batch
      if (ord) {
        db.prepare(`
          UPDATE tracking_labels SET
            customer = ?,
            wo_status = 'wo-reconciled'
          WHERE batch_number = ?
        `).run(request.customer, ord.batchNumber);
      }
      // 3. Mark request approved
      db.prepare(`
        UPDATE wo_reconciliation_requests SET
          status = 'approved', approved_by = ?, approved_at = ?
        WHERE id = ?
      `).run(session.username, now, request.id);
      return { orderId: request.order_id, customer: request.customer };
    });

    const result = approveWO();
    logAudit(session.username, session.role, 'planning', 'WO_RECON_APPROVED',
      `W/O reconciliation ${req.params.id} approved — order ${result.orderId} → ${result.customer}`);
    res.json({ ok: true, result, message: 'W/O reconciliation complete. Replacement labels ready for printing.' });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/reject/:id
app.post('/api/wo/reject/:id', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const { reason } = req.body;
    db.prepare(`UPDATE wo_reconciliation_requests SET status='rejected', approved_by=?, approved_at=datetime('now'), rejection_reason=? WHERE id=?`)
      .run(session.username, reason || 'No reason given', req.params.id);
    logAudit(session.username, session.role, 'planning', 'WO_RECON_REJECTED', `Rejected ${req.params.id}: ${reason}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/history
app.get('/api/wo/history', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    const rows = db.prepare('SELECT * FROM wo_reconciliation_requests ORDER BY proposed_at DESC LIMIT 50').all();
    res.json({ ok: true, requests: rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── Data Export / Import (Admin — for safe migrations) ────────

// GET /api/admin/export — full database export as JSON
app.get('/api/admin/export', (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    // Allow export with admin token OR with a special export key env var
    const exportKey = process.env.EXPORT_KEY || 'sunloc-export-2024';
    const isKeyAuth = req.query.key === exportKey;
    if (!isKeyAuth) {
      const session = verifyToken(token);
      if (!session || session.role !== 'admin') {
        return res.status(403).json({ ok: false, error: 'Admin access required' });
      }
    }

    const tables = [
      'planning_state', 'dpr_records', 'production_actuals',
      'tracking_labels', 'tracking_scans', 'tracking_stage_closure',
      'tracking_wastage', 'tracking_dispatch_records', 'tracking_alerts',
      'app_users', 'audit_log', 'schema_migrations'
    ];

    const exportData = {
      exported_at: new Date().toISOString(),
      db_path: DB_PATH,
      version: 'sunloc-v9',
      tables: {}
    };

    for (const table of tables) {
      try {
        exportData.tables[table] = db.prepare(`SELECT * FROM ${table}`).all();
      } catch (e) {
        exportData.tables[table] = []; // table may not exist yet
      }
    }

    const json = JSON.stringify(exportData, null, 2);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="sunloc-backup-${new Date().toISOString().slice(0,10)}.json"`);
    res.send(json);
    console.log(`[Export] Full database exported — ${Object.values(exportData.tables).reduce((s,t)=>s+t.length,0)} total rows`);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/admin/import — restore database from JSON export
app.post('/api/admin/import', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const exportKey = process.env.EXPORT_KEY || 'sunloc-export-2024';
    const isKeyAuth = req.query.key === exportKey;
    if (!isKeyAuth) {
      const session = verifyToken(token);
      if (!session || session.role !== 'admin') {
        return res.status(403).json({ ok: false, error: 'Admin access required' });
      }
    }

    const { tables, confirm } = req.body;
    if (confirm !== 'IMPORT_CONFIRMED') {
      return res.status(400).json({ ok: false, error: 'Must include confirm: "IMPORT_CONFIRMED"' });
    }
    if (!tables) return res.status(400).json({ ok: false, error: 'No tables data provided' });

    // Run migrations first to ensure schema is up to date
    runMigrations();

    const results = {};
    const importTransaction = db.transaction(() => {
      // Only import data tables — not sessions or migrations
      const importableTables = [
        'planning_state', 'dpr_records', 'production_actuals',
        'tracking_labels', 'tracking_scans', 'tracking_stage_closure',
        'tracking_wastage', 'tracking_dispatch_records', 'tracking_alerts'
      ];

      for (const table of importableTables) {
        const rows = tables[table];
        if (!rows || rows.length === 0) { results[table] = 0; continue; }
        try {
          // Get column names from first row
          const cols = Object.keys(rows[0]);
          const placeholders = cols.map(() => '?').join(',');
          const stmt = db.prepare(
            `INSERT OR REPLACE INTO ${table} (${cols.join(',')}) VALUES (${placeholders})`
          );
          let count = 0;
          for (const row of rows) {
            stmt.run(cols.map(c => row[c]));
            count++;
          }
          results[table] = count;
        } catch (e) {
          results[table] = `ERROR: ${e.message}`;
        }
      }
    });
    importTransaction();

    const totalRows = Object.values(results).reduce((s,v)=>typeof v==='number'?s+v:s, 0);
    console.log(`[Import] Restored ${totalRows} rows across ${Object.keys(results).length} tables`);
    res.json({ ok: true, results, totalRows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/admin/db-status — show DB path, size, migration status
app.get('/api/admin/db-status', (req, res) => {
  try {
    const migrations = db.prepare('SELECT * FROM schema_migrations ORDER BY version').all();
    const tableRowCounts = {};
    const tables = ['planning_state','dpr_records','production_actuals','tracking_labels',
      'tracking_scans','tracking_stage_closure','tracking_wastage','tracking_dispatch_records',
      'tracking_alerts','app_users','audit_log'];
    for (const t of tables) {
      try { tableRowCounts[t] = db.prepare(`SELECT COUNT(*) as c FROM ${t}`).get().c; }
      catch(e) { tableRowCounts[t] = 'N/A'; }
    }
    let dbSizeBytes = 0;
    try { dbSizeBytes = fs.statSync(DB_PATH).size; } catch(e){}
    res.json({
      ok: true,
      db_path: DB_PATH,
      db_size_mb: (dbSizeBytes / 1024 / 1024).toFixed(2),
      migrations_applied: migrations.length,
      migrations,
      table_row_counts: tableRowCounts
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ─── TEMP Batch System ─────────────────────────────────────────

// Helper: calculate label count from daily cap and pack size
function calcTempLabelCount(capLakhs, packSizeLakhs) {
  return Math.ceil(capLakhs / packSizeLakhs);
}

// Helper: generate TEMP batch ID
function tempBatchId(machineId, date) {
  return `TEMP-${machineId}-${date.replace(/-/g,'')}`;
}

// GET /api/temp-batches/check/:machineId — check if machine needs TEMP batch today
app.get('/api/temp-batches/check/:machineId', (req, res) => {
  try {
    const { machineId } = req.params;
    const today = new Date().toISOString().split('T')[0];
    const planState = getPlanningState();

    // Check if machine has any active (non-closed) planned orders
    const activeOrders = (planState.orders || []).filter(o =>
      o.machineId === machineId && o.status !== 'closed' && !o.deleted
    );
    const hasActiveOrder = activeOrders.length > 0;

    // Check if TEMP batch already exists for today
    const existing = db.prepare(
      `SELECT * FROM temp_batches WHERE machine_id = ? AND date = ?`
    ).get(machineId, today);

    // Get machine info from planning state
    const mc = (planState.machineMaster || []).find(m => m.id === machineId);
    const packSizes = planState.packSizes || {};
    const packSizeLakhs = mc ? ((packSizes[mc.size] || 100000) / 100000) : 1;
    const capLakhs = mc ? (mc.cap || 8) : 8;
    const labelCount = mc ? calcTempLabelCount(capLakhs, packSizeLakhs) : 0;

    // Get all active unreconciled TEMP batches for this machine
    const allTemp = db.prepare(
      `SELECT * FROM temp_batches WHERE machine_id = ? AND status = 'active' ORDER BY date DESC`
    ).all(machineId);

    res.json({
      ok: true, machineId,
      hasActiveOrder,
      activeOrders: activeOrders.map(o => ({ id:o.id, batchNumber:o.batchNumber, qty:o.qty, status:o.status })),
      todayTempBatch: existing || null,
      needsTemp: !hasActiveOrder,
      machineInfo: mc ? { size: mc.size, capLakhs, packSizeLakhs, labelCount } : null,
      activeTempBatches: allTemp
    });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/temp-batches/create — create TEMP batch for a machine/date
app.post('/api/temp-batches/create', (req, res) => {
  try {
    const { machineId, date } = req.body;
    const batchDate = date || new Date().toISOString().split('T')[0];
    const id = tempBatchId(machineId, batchDate);

    // Get machine info
    const planState = getPlanningState();
    const mc = (planState.machineMaster || []).find(m => m.id === machineId);
    if (!mc) return res.status(400).json({ ok:false, error:'Machine not found' });

    const packSizes = planState.packSizes || {};
    const packSizeLakhs = (packSizes[mc.size] || 100000) / 100000;
    const capLakhs = mc.cap || 8;
    const labelCount = calcTempLabelCount(capLakhs, packSizeLakhs);

    db.prepare(`
      INSERT OR IGNORE INTO temp_batches
        (id, machine_id, machine_size, date, daily_cap_lakhs, label_count, pack_size_lakhs)
      VALUES (?,?,?,?,?,?,?)
    `).run(id, machineId, mc.size, batchDate, capLakhs, labelCount, packSizeLakhs);

    const batch = db.prepare(`SELECT * FROM temp_batches WHERE id = ?`).get(id);

    // Log alert for today
    db.prepare(`
      INSERT OR IGNORE INTO temp_batch_alerts (machine_id, temp_batch_id, alert_date)
      VALUES (?,?,?)
    `).run(machineId, id, batchDate);

    logAudit('SYSTEM', 'system', 'dpr', 'TEMP_BATCH_CREATED',
      `TEMP batch created: ${id} — ${capLakhs}L → ${labelCount} labels (Size ${mc.size})`);

    res.json({ ok:true, batch });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/temp-batches/active — all active TEMP batches (for alerts)
app.get('/api/temp-batches/active', (req, res) => {
  try {
    const batches = db.prepare(
      `SELECT * FROM temp_batches WHERE status = 'active' ORDER BY machine_id, date DESC`
    ).all();

    // Enrich with days active count
    const today = new Date().toISOString().split('T')[0];
    const enriched = batches.map(b => ({
      ...b,
      daysActive: Math.floor((new Date(today) - new Date(b.date)) / 86400000) + 1
    }));

    res.json({ ok:true, batches: enriched, count: enriched.length });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/reconciliation/propose — Planning Manager proposes reconciliation
app.post('/api/reconciliation/propose', (req, res) => {
  try {
    const { token, orderDetails, backDate, tempBatchMappings } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok:false, error:'Planning Manager or Admin required' });
    }

    // Validate back-date: cannot be before earliest TEMP batch date
    const earliestTempDate = tempBatchMappings.reduce((min, m) => {
      return m.tempDate < min ? m.tempDate : min;
    }, '9999-12-31');

    if (backDate < earliestTempDate) {
      return res.status(400).json({
        ok:false,
        error: `Back-date (${backDate}) cannot be before earliest TEMP batch date (${earliestTempDate})`
      });
    }

    // Validate all TEMP batches exist and are active
    for (const mapping of tempBatchMappings) {
      const tb = db.prepare(`SELECT * FROM temp_batches WHERE id = ?`).get(mapping.tempBatchId);
      if (!tb) return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} not found` });
      if (tb.status !== 'active') return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} is not active` });
    }

    const totalBoxes = tempBatchMappings.reduce((s,m) => s + (m.boxes || 0), 0);
    const id = `RECON-${Date.now()}`;

    db.prepare(`
      INSERT INTO reconciliation_requests
        (id, proposed_by, status, order_id, order_details, back_date, temp_batch_mappings, total_boxes)
      VALUES (?,?,?,?,?,?,?,?)
    `).run(
      id, session.username, 'pending',
      orderDetails.id || `ORDER-${Date.now()}`,
      JSON.stringify(orderDetails),
      backDate,
      JSON.stringify(tempBatchMappings),
      totalBoxes
    );

    logAudit(session.username, session.role, 'planning', 'RECON_PROPOSED',
      `Reconciliation proposed: ${id} — ${tempBatchMappings.length} TEMP batches → Order, ${totalBoxes} boxes`);

    res.json({ ok:true, requestId: id, status:'pending', message:'Awaiting Admin approval' });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/reconciliation/pending — Admin views pending requests
app.get('/api/reconciliation/pending', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') {
      return res.status(403).json({ ok:false, error:'Admin only' });
    }
    const requests = db.prepare(
      `SELECT * FROM reconciliation_requests WHERE status = 'pending' ORDER BY proposed_at DESC`
    ).all().map(r => ({
      ...r,
      order_details: JSON.parse(r.order_details),
      temp_batch_mappings: JSON.parse(r.temp_batch_mappings)
    }));
    res.json({ ok:true, requests });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/reconciliation/approve/:id — Admin approves and executes reconciliation
app.post('/api/reconciliation/approve/:id', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') {
      return res.status(403).json({ ok:false, error:'Admin only' });
    }

    const request = db.prepare(`SELECT * FROM reconciliation_requests WHERE id = ?`).get(req.params.id);
    if (!request) return res.status(404).json({ ok:false, error:'Request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok:false, error:'Request is not pending' });

    const orderDetails = JSON.parse(request.order_details);
    const mappings = JSON.parse(request.temp_batch_mappings);
    const orderId = request.order_id;

    // Execute reconciliation atomically
    const reconcile = db.transaction(() => {
      const now = new Date().toISOString();
      const results = { migratedScans:0, migratedLabels:0, migratedWastage:0, tempBatchesReconciled:0 };

      for (const mapping of mappings) {
        const { tempBatchId: tbId, boxes, startLabelNumber, endLabelNumber } = mapping;
        const tb = db.prepare(`SELECT * FROM temp_batches WHERE id = ?`).get(tbId);
        if (!tb) continue;

        // Determine production month from TEMP batch date (never changes)
        const prodMonth = tb.date.slice(0,7); // YYYY-MM

        // 1. Migrate tracking labels for this TEMP batch (within box range if partial)
        const labelFilter = (startLabelNumber && endLabelNumber)
          ? `batch_number = ? AND label_number >= ? AND label_number <= ?`
          : `batch_number = ?`;
        const labelArgs = (startLabelNumber && endLabelNumber)
          ? [tbId, startLabelNumber, endLabelNumber]
          : [tbId];

        const labelsToMigrate = db.prepare(
          `SELECT * FROM tracking_labels WHERE ${labelFilter}`
        ).all(...labelArgs);

        for (const label of labelsToMigrate) {
          const newLabelId = label.id.replace(tbId, orderId);
          db.prepare(`
            INSERT OR REPLACE INTO tracking_labels SELECT
              replace(id,?,?) as id,
              ? as batch_number,
              label_number, size, qty, is_partial, is_orange,
              parent_label_id, customer, colour, pc_code, po_number, machine_id,
              printing_matter, generated, printed, printed_at,
              voided, void_reason, voided_at, voided_by, qr_data
            FROM tracking_labels WHERE id = ?
          `).run(tbId, orderId, orderId, label.id);

          // Migrate scans for this label
          const scanMigrated = db.prepare(`
            UPDATE tracking_scans SET
              label_id = replace(label_id,?,?),
              batch_number = ?
            WHERE label_id = ?
          `).run(tbId, orderId, orderId, label.id);
          results.migratedScans += scanMigrated.changes;
          results.migratedLabels++;

          // Remove old TEMP label if new one created
          if (newLabelId !== label.id) {
            db.prepare(`DELETE FROM tracking_labels WHERE id = ? AND id != ?`).run(label.id, newLabelId);
          }
        }

        // 2. Migrate wastage records
        const wastage = db.prepare(
          `UPDATE tracking_wastage SET batch_number = ? WHERE batch_number = ?`
        ).run(orderId, tbId);
        results.migratedWastage += wastage.changes;

        // 3. Migrate stage closures
        db.prepare(
          `UPDATE tracking_stage_closure SET batch_number = ? WHERE batch_number = ?`
        ).run(orderId, tbId);

        // 4. Migrate DPR production actuals
        db.prepare(`
          UPDATE production_actuals SET
            order_id = ?, batch_number = ?
          WHERE batch_number = ?
        `).run(orderId, orderDetails.batchNumber || orderId, tbId);

        // 5. Update dispatch records
        db.prepare(`
          UPDATE tracking_dispatch_records SET batch_number = ? WHERE batch_number = ?
        `).run(orderId, tbId);

        // 6. Mark TEMP batch as reconciled (or partially reconciled)
        const isFullReconcile = !startLabelNumber; // full batch
        db.prepare(`
          UPDATE temp_batches SET
            status = ?,
            reconciled_order_id = ?,
            reconciled_at = ?,
            reconciled_by = ?
          WHERE id = ?
        `).run(isFullReconcile ? 'reconciled' : 'partial', orderId, now, session.username, tbId);
        results.tempBatchesReconciled++;
      }

      // 7. Update planning state - add/update order with back-date and correct actualQty
      const planState = getPlanningState();
      if (planState.orders) {
        // Check if order already exists (Planning Manager may have pre-entered it)
        const existingIdx = planState.orders.findIndex(o => o.id === orderId);
        const orderToSave = {
          ...orderDetails,
          id: orderId,
          startDate: request.back_date,
          actualQty: mappings.reduce((s,m) => s + (m.actualLakhs || 0), 0),
          status: 'running'
        };
        if (existingIdx >= 0) {
          planState.orders[existingIdx] = { ...planState.orders[existingIdx], ...orderToSave };
        } else {
          planState.orders.push(orderToSave);
        }
        db.prepare(`
          INSERT INTO planning_state (id, state_json)
          VALUES (1, ?)
          ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json, updated_at = datetime('now')
        `).run(JSON.stringify(planState));
      }

      // 8. Mark reconciliation request as approved
      db.prepare(`
        UPDATE reconciliation_requests SET
          status = 'approved', approved_by = ?, approved_at = ?
        WHERE id = ?
      `).run(session.username, now, request.id);

      return results;
    });

    const results = reconcile();

    logAudit(session.username, session.role, 'planning', 'RECON_APPROVED',
      `Reconciliation ${req.params.id} approved — ${results.migratedLabels} labels, ${results.migratedScans} scans migrated`);

    res.json({ ok:true, results, message:'Reconciliation complete. Replacement labels ready for printing.' });
  } catch(err) {
    res.status(500).json({ ok:false, error:err.message });
  }
});

// POST /api/reconciliation/reject/:id — Admin rejects
app.post('/api/reconciliation/reject/:id', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
    const { reason } = req.body;
    db.prepare(`
      UPDATE reconciliation_requests SET status='rejected', approved_by=?, approved_at=datetime('now'), rejection_reason=?
      WHERE id = ?
    `).run(session.username, reason||'No reason given', req.params.id);
    logAudit(session.username, session.role, 'planning', 'RECON_REJECTED', `Rejected: ${req.params.id} — ${reason}`);
    res.json({ ok:true });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/reconciliation/history — all reconciliation requests
app.get('/api/reconciliation/history', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
    const rows = db.prepare(
      `SELECT * FROM reconciliation_requests ORDER BY proposed_at DESC LIMIT 100`
    ).all().map(r => ({
      ...r,
      order_details: JSON.parse(r.order_details),
      temp_batch_mappings: JSON.parse(r.temp_batch_mappings)
    }));
    res.json({ ok:true, requests: rows });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

app.get('/api/health', (req, res) => {
  const planningRow = db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get();
  const dprCount = db.prepare('SELECT COUNT(*) as c FROM dpr_records').get();
  const actualsCount = db.prepare('SELECT COUNT(*) as c FROM production_actuals').get();
  res.json({
    ok: true,
    server: 'Sunloc Integrated Server v1.0',
    db: DB_PATH,
    planningSavedAt: planningRow?.saved_at || null,
    dprRecords: dprCount.c,
    actualsEntries: actualsCount.c,
    uptime: Math.floor(process.uptime()) + 's',
  });
});

// Catch-all: serve index.html for unknown routes (SPA fallback)
app.get('*', (req, res) => {
  const idx = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(idx)) res.sendFile(idx);
  else res.json({ ok: false, error: 'No frontend found. Place Planning App and DPR App in /public folder.' });
});


// ═══════════════════════════════════════════════════════
// TRACKING APP SCHEMA (add to existing server.js)
// ═══════════════════════════════════════════════════════


// ─── TRACKING ROUTES ──────────────────────────────────────────

// GET /api/tracking/state — full tracking state
app.get('/api/tracking/state', (req, res) => {
  try {
    const labels  = db.prepare('SELECT * FROM tracking_labels ORDER BY generated DESC').all();
    const scans   = db.prepare('SELECT * FROM tracking_scans ORDER BY ts ASC').all();
    const closure = db.prepare('SELECT * FROM tracking_stage_closure').all();
    const wastage = db.prepare('SELECT * FROM tracking_wastage ORDER BY ts ASC').all();
    const dispatch= db.prepare('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC').all();
    const alerts  = db.prepare('SELECT * FROM tracking_alerts WHERE resolved = 0').all();
    res.json({ ok: true, state: { labels, scans, stageClosure: closure, wastage, dispatchRecs: dispatch, alerts } });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/tracking/state — save full tracking state
app.post('/api/tracking/state', (req, res) => {
  try {
    const { labels, scans, stageClosure, wastage, dispatchRecs, alerts } = req.body;
    const saveAll = db.transaction(() => {
      if (labels && labels.length) {
        const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_labels
          (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,
          colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,
          voided,void_reason,voided_at,voided_by,qr_data)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
        labels.forEach(l => stmt.run(
          l.id,l.batchNumber,l.labelNumber,l.size,l.qty,l.isPartial?1:0,l.isOrange?1:0,
          l.parentLabelId||null,l.customer||null,l.colour||null,l.pcCode||null,
          l.poNumber||null,l.machineId||null,l.printingMatter||null,
          l.generated||new Date().toISOString(),l.printed?1:0,l.printedAt||null,
          l.voided?1:0,l.voidReason||null,l.voidedAt||null,l.voidedBy||null,l.qrData||null
        ));
      }
      if (scans && scans.length) {
        const stmt = db.prepare(`INSERT OR IGNORE INTO tracking_scans
          (id,label_id,batch_number,dept,type,ts,operator,size,qty) VALUES (?,?,?,?,?,?,?,?,?)`);
        scans.forEach(s => stmt.run(s.id,s.labelId||s.label_id,s.batchNumber||s.batch_number,
          s.dept,s.type,s.ts,s.operator||null,s.size||null,s.qty||null));
      }
      if (stageClosure && stageClosure.length) {
        const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_stage_closure
          (id,batch_number,dept,closed,closed_at,closed_by) VALUES (?,?,?,?,?,?)`);
        stageClosure.forEach(s => stmt.run(s.id,s.batchNumber||s.batch_number,
          s.dept,s.closed?1:0,s.closedAt||s.closed_at,s.closedBy||s.closed_by||null));
      }
      if (wastage && wastage.length) {
        const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_wastage
          (id,batch_number,dept,type,qty,ts,by) VALUES (?,?,?,?,?,?,?)`);
        wastage.forEach(w => stmt.run(w.id,w.batchNumber||w.batch_number,
          w.dept,w.type,w.qty,w.ts,w.by||null));
      }
      if (dispatchRecs && dispatchRecs.length) {
        const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_dispatch_records
          (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,by) VALUES (?,?,?,?,?,?,?,?,?,?)`);
        dispatchRecs.forEach(d => stmt.run(d.id,d.batchNumber||d.batch_number,
          d.customer||null,d.qty,d.boxes,d.vehicleNo||d.vehicle_no||null,
          d.invoiceNo||d.invoice_no||null,d.remarks||null,d.ts,d.by||null));
      }
      if (alerts && alerts.length) {
        const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_alerts
          (id,label_id,batch_number,dept,scan_in_ts,hours_stuck,resolved,msg) VALUES (?,?,?,?,?,?,?,?)`);
        alerts.forEach(a => stmt.run(a.id,a.labelId||a.label_id,
          a.batchNumber||a.batch_number,a.dept,a.scanInTs||a.scan_in_ts,
          a.hoursStuck||a.hours_stuck||null,a.resolved?1:0,a.msg||null));
      }
    });
    saveAll();
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/tracking/batch-summary/:batchNumber
app.get('/api/tracking/batch-summary/:batchNumber', (req, res) => {
  try {
    const { batchNumber } = req.params;
    const labels  = db.prepare('SELECT * FROM tracking_labels WHERE batch_number = ?').all(batchNumber);
    const scans   = db.prepare('SELECT * FROM tracking_scans WHERE batch_number = ? ORDER BY ts').all(batchNumber);
    const wastage = db.prepare('SELECT * FROM tracking_wastage WHERE batch_number = ?').all(batchNumber);
    const dispatch= db.prepare('SELECT * FROM tracking_dispatch_records WHERE batch_number = ?').all(batchNumber);
    const alerts  = db.prepare('SELECT * FROM tracking_alerts WHERE batch_number = ? AND resolved = 0').all(batchNumber);
    const deptMap = {};
    scans.forEach(s => {
      if (!deptMap[s.dept]) deptMap[s.dept] = { in: 0, out: 0 };
      deptMap[s.dept][s.type] = (deptMap[s.dept][s.type] || 0) + 1;
    });
    const labelStats = { total: labels.length, printed: labels.filter(l=>l.printed).length, voided: labels.filter(l=>l.voided).length };
    const dispatched = dispatch.reduce((s,d) => s + d.boxes, 0);
    res.json({ ok: true, deptMap, labelStats, wastage, alerts, dispatched, batchNumber });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/tracking/wip-summary — scan counts for DPR A-grade
app.get('/api/tracking/wip-summary', (req, res) => {
  try {
    const summary = db.prepare(`
      SELECT batch_number, dept, type, COUNT(*) as cnt
      FROM tracking_scans GROUP BY batch_number, dept, type
    `).all();
    res.json({ ok: true, scanSummary: summary });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Start server ────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[Sunloc] Server running on port ${PORT}`);
  console.log(`[Sunloc] DB: ${resolveDbPath()}`);
});
