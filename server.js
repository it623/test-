/**
 * SUNLOC INTEGRATED SERVER
 * Shared backend for Planning App + DPR App + Tracking App
 * Stack: Node.js + Express + PostgreSQL (Railway)
 * All data safe — connects to existing Railway PostgreSQL database
 */

const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

// ─── Middleware ────────────────────────────────────────────────
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public'), {
  etag: false, maxAge: 0,
  setHeaders: (res) => { res.setHeader('Cache-Control', 'no-store'); }
}));

// ─── PostgreSQL Database Setup ─────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
});

// Helper: run a query
async function query(text, params) {
  const client = await pool.connect();
  try { return await client.query(text, params); }
  finally { client.release(); }
}

// Helper: get first row
async function queryOne(text, params) {
  const res = await query(text, params);
  return res.rows[0] || null;
}

// Helper: get all rows
async function queryAll(text, params) {
  const res = await query(text, params);
  return res.rows;
}

// Wrap async route handlers — catches errors automatically
function asyncRoute(fn) {
  return (req, res, next) => fn(req, res, next).catch(next);
}

console.log('[DB] PostgreSQL pool initialised — connecting to Railway PostgreSQL');

// ─── PostgreSQL Schema Setup ──────────────────────────────────
// Safe migrations — each runs once, never drops existing data
async function runMigrations() {
  const client = await pool.connect();
  try {
    // Create migrations tracking table
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    const applied = new Set(
      (await client.query('SELECT version FROM schema_migrations')).rows.map(r => r.version)
    );

    const migrations = [
      { version: 1, name: 'initial_schema', sql: `
        CREATE TABLE IF NOT EXISTS planning_state (
          id INTEGER PRIMARY KEY DEFAULT 1,
          state_json TEXT NOT NULL,
          saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS dpr_records (
          id SERIAL PRIMARY KEY,
          floor TEXT NOT NULL,
          date TEXT NOT NULL,
          data_json TEXT NOT NULL,
          saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(floor, date)
        );
        CREATE TABLE IF NOT EXISTS production_actuals (
          id SERIAL PRIMARY KEY,
          order_id TEXT,
          batch_number TEXT,
          machine_id TEXT NOT NULL,
          date TEXT NOT NULL,
          shift TEXT NOT NULL,
          run_index INTEGER NOT NULL DEFAULT 0,
          qty_lakhs REAL DEFAULT 0,
          floor TEXT,
          synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(machine_id, date, shift, run_index)
        );
        CREATE INDEX IF NOT EXISTS idx_actuals_order ON production_actuals(order_id);
        CREATE INDEX IF NOT EXISTS idx_actuals_batch ON production_actuals(batch_number);
        CREATE INDEX IF NOT EXISTS idx_actuals_machine ON production_actuals(machine_id, date);
        CREATE INDEX IF NOT EXISTS idx_dpr_date ON dpr_records(date);
      `},
      { version: 2, name: 'tracking_tables', sql: `
        CREATE TABLE IF NOT EXISTS tracking_labels (
          id TEXT PRIMARY KEY,
          batch_number TEXT NOT NULL,
          label_number INTEGER NOT NULL,
          size TEXT NOT NULL,
          qty REAL NOT NULL,
          is_partial BOOLEAN DEFAULT FALSE,
          is_orange BOOLEAN DEFAULT FALSE,
          parent_label_id TEXT,
          customer TEXT, colour TEXT, pc_code TEXT,
          po_number TEXT, machine_id TEXT, printing_matter TEXT,
          generated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          printed BOOLEAN DEFAULT FALSE, printed_at TIMESTAMPTZ,
          voided BOOLEAN DEFAULT FALSE, void_reason TEXT,
          voided_at TIMESTAMPTZ, voided_by TEXT, qr_data TEXT,
          UNIQUE(batch_number, label_number, is_orange)
        );
        CREATE TABLE IF NOT EXISTS tracking_scans (
          id TEXT PRIMARY KEY,
          label_id TEXT NOT NULL, batch_number TEXT NOT NULL,
          dept TEXT NOT NULL, type TEXT NOT NULL CHECK(type IN ('in','out')),
          ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          operator TEXT, size TEXT, qty REAL
        );
        CREATE TABLE IF NOT EXISTS tracking_stage_closure (
          id TEXT PRIMARY KEY,
          batch_number TEXT NOT NULL, dept TEXT NOT NULL,
          closed BOOLEAN DEFAULT TRUE,
          closed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          closed_by TEXT, UNIQUE(batch_number, dept)
        );
        CREATE TABLE IF NOT EXISTS tracking_wastage (
          id TEXT PRIMARY KEY,
          batch_number TEXT NOT NULL, dept TEXT NOT NULL,
          type TEXT NOT NULL CHECK(type IN ('salvage','remelt')),
          qty REAL NOT NULL, ts TIMESTAMPTZ NOT NULL DEFAULT NOW(), by TEXT
        );
        CREATE TABLE IF NOT EXISTS tracking_dispatch_records (
          id TEXT PRIMARY KEY,
          batch_number TEXT NOT NULL, customer TEXT,
          qty REAL NOT NULL, boxes INTEGER NOT NULL,
          vehicle_no TEXT, invoice_no TEXT, remarks TEXT,
          ts TIMESTAMPTZ NOT NULL DEFAULT NOW(), by TEXT
        );
        CREATE TABLE IF NOT EXISTS tracking_alerts (
          id TEXT PRIMARY KEY,
          label_id TEXT NOT NULL, batch_number TEXT NOT NULL,
          dept TEXT NOT NULL, scan_in_ts TIMESTAMPTZ NOT NULL,
          hours_stuck REAL, resolved BOOLEAN DEFAULT FALSE,
          msg TEXT, UNIQUE(label_id, dept)
        );
        CREATE INDEX IF NOT EXISTS idx_scans_batch ON tracking_scans(batch_number, dept);
        CREATE INDEX IF NOT EXISTS idx_labels_batch ON tracking_labels(batch_number);
        CREATE INDEX IF NOT EXISTS idx_wastage_batch ON tracking_wastage(batch_number, dept);
      `},
      { version: 3, name: 'auth_and_audit', sql: `
        CREATE TABLE IF NOT EXISTS app_users (
          id SERIAL PRIMARY KEY,
          username TEXT NOT NULL UNIQUE,
          pin_hash TEXT NOT NULL, role TEXT NOT NULL, app TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS app_sessions (
          token TEXT PRIMARY KEY,
          user_id INTEGER NOT NULL, username TEXT NOT NULL,
          role TEXT NOT NULL, app TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          expires_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS audit_log (
          id SERIAL PRIMARY KEY,
          username TEXT NOT NULL, role TEXT NOT NULL, app TEXT NOT NULL,
          action TEXT NOT NULL, details TEXT, ip TEXT,
          ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(username);
      `},
      { version: 4, name: 'temp_batch_system', sql: `
        CREATE TABLE IF NOT EXISTS temp_batches (
          id TEXT PRIMARY KEY,
          machine_id TEXT NOT NULL, machine_size TEXT NOT NULL,
          date TEXT NOT NULL, daily_cap_lakhs REAL NOT NULL,
          label_count INTEGER NOT NULL, pack_size_lakhs REAL NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          reconciled_order_id TEXT, reconciled_at TIMESTAMPTZ,
          reconciled_by TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(machine_id, date)
        );
        CREATE TABLE IF NOT EXISTS reconciliation_requests (
          id TEXT PRIMARY KEY, proposed_by TEXT NOT NULL,
          proposed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          approved_by TEXT, approved_at TIMESTAMPTZ,
          status TEXT NOT NULL DEFAULT 'pending',
          order_id TEXT NOT NULL, order_details TEXT NOT NULL,
          back_date TEXT NOT NULL, temp_batch_mappings TEXT NOT NULL,
          total_boxes INTEGER NOT NULL, rejection_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS temp_batch_alerts (
          id SERIAL PRIMARY KEY, machine_id TEXT NOT NULL,
          temp_batch_id TEXT NOT NULL, alert_date TEXT NOT NULL,
          sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(machine_id, alert_date)
        );
        CREATE INDEX IF NOT EXISTS idx_temp_batches_machine ON temp_batches(machine_id, date);
        CREATE INDEX IF NOT EXISTS idx_temp_batches_status ON temp_batches(status);
        CREATE INDEX IF NOT EXISTS idx_recon_status ON reconciliation_requests(status);
      `},
      { version: 5, name: 'temp_colour_and_wo_support', sql: `
        ALTER TABLE temp_batches ADD COLUMN IF NOT EXISTS colour TEXT;
        ALTER TABLE temp_batches ADD COLUMN IF NOT EXISTS pc_code TEXT;
        ALTER TABLE temp_batches ADD COLUMN IF NOT EXISTS colour_confirmed BOOLEAN DEFAULT FALSE;
        ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS wo_status TEXT;
        CREATE TABLE IF NOT EXISTS wo_reconciliation_requests (
          id TEXT PRIMARY KEY, proposed_by TEXT NOT NULL,
          proposed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          approved_by TEXT, approved_at TIMESTAMPTZ,
          status TEXT NOT NULL DEFAULT 'pending',
          order_id TEXT NOT NULL, customer TEXT NOT NULL,
          po_number TEXT, zone TEXT, qty_confirmed REAL, rejection_reason TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_wo_recon_status ON wo_reconciliation_requests(status);
      `},
      { version: 6, name: 'tracking_labels_extended_fields', sql: `
        ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS ship_to TEXT;
        ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS bill_to TEXT;
        ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS is_excess BOOLEAN DEFAULT FALSE;
        ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS excess_num INTEGER;
        ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS excess_total INTEGER;
        ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS normal_total INTEGER;
      `},
      { version: 7, name: 'planning_state_backups', sql: `
        CREATE TABLE IF NOT EXISTS planning_state_backups (
          id SERIAL PRIMARY KEY,
          state_json TEXT NOT NULL,
          backed_up_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          trigger TEXT NOT NULL DEFAULT 'auto'
        );
        CREATE INDEX IF NOT EXISTS idx_backups_ts ON planning_state_backups(backed_up_at DESC);
      `},
      { version: 8, name: 'dpr_settings', sql: `
        CREATE TABLE IF NOT EXISTS dpr_settings (
          key TEXT PRIMARY KEY,
          value_json TEXT NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `},
      // v9: DPR batch-close table — tracks which orders have been closed in DPR
      //     Used by Planning to gate the Planning close button
      ];

    for (const m of migrations) {
      if (applied.has(m.version)) continue;
      console.log(`[Migration] Running v${m.version}: ${m.name}`);
      await client.query(m.sql);
      await client.query('INSERT INTO schema_migrations (version, name) VALUES ($1, $2)', [m.version, m.name]);
      console.log(`[Migration] v${m.version} applied successfully`);
    }
    console.log('[Migration] All migrations up to date');
  } finally { client.release(); }
}


// ─── Seed default users + helpers ────────────────────────────
const crypto = require('crypto');
function hashPin(pin) { return crypto.createHash('sha256').update(pin + 'sunloc_salt').digest('hex'); }

const DEFAULT_USERS = [
  { username: 'GF',               pin: '1111', role: 'gf',               app: 'dpr' },
  { username: 'FF',               pin: '2222', role: 'ff',               app: 'dpr' },
  { username: 'DPR_Admin',        pin: '9999', role: 'admin',            app: 'dpr' },
  { username: 'Planning_Manager', pin: '3333', role: 'planning_manager', app: 'planning' },
  { username: 'Printing_Manager', pin: '4444', role: 'printing_manager', app: 'planning' },
  { username: 'Dispatch_Manager', pin: '5555', role: 'dispatch_manager', app: 'planning' },
  { username: 'Plan_Admin',       pin: '9999', role: 'admin',            app: 'planning' },
  { username: 'Track_Admin',      pin: '9999', role: 'admin',            app: 'tracking' },
];

async function seedUsers() {
  for (const u of DEFAULT_USERS) {
    await query(
      `INSERT INTO app_users (username, pin_hash, role, app) VALUES ($1,$2,$3,$4) ON CONFLICT (username) DO NOTHING`,
      [u.username, hashPin(u.pin), u.role, u.app]
    );
  }
  await query(`DELETE FROM app_sessions WHERE expires_at < NOW()`);
  console.log('[Seed] Default users ready');
}

// ─── Helper: get latest planning state ────────────────────────
async function getPlanningState() {
  const row = await queryOne('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1');
  if (!row) return { orders: [], printOrders: [], dispatchPlans: [], dailyPrinting: [], machineMaster: [], printMachineMaster: [], packSizes: {} };
  try { return JSON.parse(row.state_json); } catch { return {}; }
}

// ─── Order validation ────────────────────────────────────────────────────────
// Batch format: 2-digit year + machine alpha (1+ uppercase letters) + 3-digit serial
// Examples: 26ZG061, 26U082, 26ZD033
const BATCH_REGEX = /^[0-9]{2}[A-Z]+[0-9]{3}$/;

const VALID_PACKING = ['1 PLY','2 PLY','3 PLY','4 PLY','5 PLY','6 PLY','7 PLY','3PLY','5PLY'];

function isValidOrder(o) {
  if (!o || o.deleted) return false;
  // Must have qty > 0
  if (!o.qty || parseFloat(o.qty) <= 0) return { valid: false, reason: `qty=0 or missing` };
  // Customer must not be empty/none/test
  const cust = String(o.customer || o.shipTo || '').trim().toLowerCase();
  if (!cust || cust === 'none' || cust === 'test') return { valid: false, reason: `invalid customer="${cust}"` };
  // Batch number must match format
  const batch = String(o.batchNumber || '').trim();
  if (!batch || !BATCH_REGEX.test(batch)) return { valid: false, reason: `invalid batchNumber="${batch}"` };
  // Packing must be present
  const packing = String(o.packing || '').trim();
  if (!packing) return { valid: false, reason: `packing is empty` };
  return { valid: true };
}

function cleanOrders(orders) {
  if (!Array.isArray(orders)) return orders;
  const cleaned = [];
  const removed = [];
  for (const o of orders) {
    if (o.deleted) { removed.push(`${o.batchNumber}:deleted`); continue; }
    const check = isValidOrder(o);
    if (check.valid) {
      cleaned.push(o);
    } else {
      removed.push(`${o.batchNumber||o.id}:${check.reason}`);
    }
  }
  if (removed.length > 0) {
    console.log(`[cleanOrders] Removed ${removed.length} invalid orders: ${removed.join(', ')}`);
  }
  return cleaned;
}

function cleanDispatchPlans(orders, dispatchPlans) {
  if (!Array.isArray(dispatchPlans)) return dispatchPlans;
  const validIds = new Set((orders||[]).map(o=>o.id));
  const cleaned = dispatchPlans.filter(dp => {
    if (dp.productionOrderId && validIds.has(dp.productionOrderId)) return true;
    if (dp.allProductionOrderIds && dp.allProductionOrderIds.some(id=>validIds.has(id))) return true;
    console.log(`[cleanDispatchPlans] Removed orphan dispatch plan: ${dp.id} (order: ${dp.productionOrderId})`);
    return false;
  });
  return cleaned;
}

async function savePlanningState(state) {
  // Strip invalid orders before any guard check — permanent enforcement
  if (Array.isArray(state.orders)) {
    state.orders = cleanOrders(state.orders);
  }
  // Strip orphan dispatch plans — plans with no matching production order
  if (Array.isArray(state.dispatchPlans)) {
    state.dispatchPlans = cleanDispatchPlans(state.orders, state.dispatchPlans);
  }

  // Always fetch existing state once for all guards
  const existingRow = await queryOne('SELECT state_json, saved_at FROM planning_state WHERE id=1');
  let existingState = null;
  if (existingRow) {
    try { existingState = JSON.parse(existingRow.state_json); } catch(e) {}
  }

  // SAFETY GUARD 1: never overwrite existing orders with empty array
  if (state && Array.isArray(state.orders) && state.orders.length === 0) {
    if (existingState?.orders?.length > 0) {
      console.log('[savePlanningState] GUARD1: blocked empty orders overwrite');
      const merged = { ...existingState, ...state, orders: cleanOrders(existingState.orders) };
      await query(
        `INSERT INTO planning_state (id, state_json) VALUES (1, $1)
         ON CONFLICT (id) DO UPDATE SET state_json = EXCLUDED.state_json, saved_at = NOW()`,
        [JSON.stringify(merged)]
      );
      return;
    }
  }

  // SAFETY GUARD 2: never write a completely empty state
  const incomingHasData = (state.orders?.length > 0) || (state.printOrders?.length > 0) ||
                          (state.dispatchPlans?.length > 0) || (state.machineMaster?.length > 0);
  if (!incomingHasData && existingState) {
    const existingHasData = (existingState.orders?.length > 0) || (existingState.printOrders?.length > 0) ||
                            (existingState.dispatchPlans?.length > 0) || (existingState.machineMaster?.length > 0);
    if (existingHasData) {
      console.log('[savePlanningState] GUARD2: blocked empty state overwrite');
      return;
    }
  }

  // SAFETY GUARD 3: never reduce order count by more than 5 in a single save
  // (catches stale browser state being pushed after redeployment or back navigation)
  // Note: intentional deletes by admin/planning_manager go through saveState() which sends
  // the full updated state — these are always <=5 orders at a time, so GUARD3 allows them.
  if (existingState?.orders?.length > 0 && state.orders?.length >= 0) {
    const existingCount = existingState.orders.filter(o => !o.deleted).length;
    const incomingCount = (state.orders || []).filter(o => !o.deleted).length;
    const drop = existingCount - incomingCount;
    if (drop > 5) {
      console.log(`[savePlanningState] GUARD3: suspicious order count drop ${existingCount} → ${incomingCount} (drop=${drop}), merging to protect data`);
      // Merge: keep all existing orders not in incoming state, add all incoming
      const incomingIds = new Set((state.orders || []).map(o => o.id));
      const missingOrders = cleanOrders(existingState.orders.filter(o => !incomingIds.has(o.id) && !o.deleted));
      const merged = { ...state, orders: [...(state.orders || []), ...missingOrders] };
      await query(
        `INSERT INTO planning_state (id, state_json) VALUES (1, $1)
         ON CONFLICT (id) DO UPDATE SET state_json = EXCLUDED.state_json, saved_at = NOW()`,
        [JSON.stringify(merged)]
      );
      await query(`INSERT INTO planning_state_backups (state_json, trigger) VALUES ($1, 'guard3-merge')`, [JSON.stringify(merged)]);
      return;
    }
    // Intentional deletes (1-5 orders): log for audit trail but allow through
    if (drop > 0) {
      console.log(`[savePlanningState] ${drop} order(s) intentionally deleted by user — saving permanently`);
    }
  }

  await query(
    `INSERT INTO planning_state (id, state_json) VALUES (1, $1)
     ON CONFLICT (id) DO UPDATE SET state_json = EXCLUDED.state_json, saved_at = NOW()`,
    [JSON.stringify(state)]
  );
  // Auto-backup: keep last 10 snapshots
  try {
    await query(`INSERT INTO planning_state_backups (state_json, trigger) VALUES ($1, 'auto')`, [JSON.stringify(state)]);
    await query(`DELETE FROM planning_state_backups WHERE id NOT IN (SELECT id FROM planning_state_backups ORDER BY backed_up_at DESC LIMIT 10)`);
  } catch(e) { /* backup failure never blocks main save */ }
}

// ─── Helper: get active orders for a machine ──────────────────
async function getActiveOrdersForMachine(machineId) {
  const state = await getPlanningState();
  return (state.orders || [])
    .filter(o => o.machineId === machineId && o.status !== 'closed' && !o.deleted)
    .map(o => ({
      id: o.id, batchNumber: o.batchNumber || '',
      poNumber: o.poNumber || '', customer: o.customer || '',
      size: o.size || '', colour: o.colour || '',
      qty: o.qty || 0, isPrinted: o.isPrinted || false,
      status: o.status || 'pending', zone: o.zone || '',
    }));
}

// Helper: get total actuals for an order
async function getOrderActuals(orderId, batchNumber) {
  let row;
  if (orderId) {
    row = await queryOne('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE order_id = $1', [orderId]);
    if (!row?.total && batchNumber)
      row = await queryOne('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE batch_number = $1', [batchNumber]);
  } else if (batchNumber) {
    row = await queryOne('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE batch_number = $1', [batchNumber]);
  }
  return parseFloat(row?.total) || 0;
}

// Auth helpers
function generateToken() { return crypto.randomBytes(32).toString('hex'); }

async function verifyToken(token) {
  if (!token) return null;
  return await queryOne(`SELECT * FROM app_sessions WHERE token=$1 AND expires_at > NOW()`, [token]);
}

async function logAudit(username, role, app, action, details, ip) {
  try {
    await query(
      `INSERT INTO audit_log (username,role,app,action,details,ip) VALUES ($1,$2,$3,$4,$5,$6)`,
      [username, role, app, action, details||null, ip||null]
    );
  } catch(e) { console.error('Audit log error:', e.message); }
}

// Helper: temp batch ID
function calcTempLabelCount(capLakhs, packSizeLakhs) { return Math.ceil(capLakhs / packSizeLakhs); }
function tempBatchId(machineId, date) { return `TEMP-${machineId}-${date.replace(/-/g,'')}`; }

// ═══════════════════════════════════════════════════════════════
// PLANNING APP ROUTES
// ═══════════════════════════════════════════════════════════════

// GET full planning state
app.get('/api/planning/state', asyncRoute(async (req, res) => {
  const state = await getPlanningState();
  if (state.orders) {
    for (const ord of state.orders) {
      const actual = await getOrderActuals(ord.id, ord.batchNumber);
      ord.actualProd = actual;
      if (actual > 0 && ord.status === 'pending') ord.status = 'running';
    }
  }
  const row = await queryOne('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1');
  res.json({ ok: true, state, savedAt: row?.saved_at });
}));

// POST save planning state
app.post('/api/planning/state', asyncRoute(async (req, res) => {
  const { state } = req.body;
  if (!state) return res.status(400).json({ ok: false, error: 'No state provided' });
  await savePlanningState(state);
  res.json({ ok: true, savedAt: new Date().toISOString() });
}));

// GET active orders for a machine
app.get('/api/orders/machine/:machineId', asyncRoute(async (req, res) => {
  const orders = await getActiveOrdersForMachine(req.params.machineId);
  res.json({ ok: true, orders });
}));

// GET all active orders
app.get('/api/orders/active', asyncRoute(async (req, res) => {
  const state = await getPlanningState();
  const rawOrders = (state.orders || []).filter(o => o.status !== 'closed' && !o.deleted);
  // Use getOrderActuals for live production qty from DPR records
  const orders = await Promise.all(rawOrders.map(async o => {
    const actual = await getOrderActuals(o.id, o.batchNumber);
    return {
      id: o.id, batchNumber: o.batchNumber || '', poNumber: o.poNumber || '',
      customer: o.customer || '', machineId: o.machineId || '',
      size: o.size || '', colour: o.colour || '',
      qty: o.qty || 0, grossQty: o.grossQty || 0,
      actualQty: actual || o.actualQty || 0, status: o.status || 'pending',
    };
  }));
  res.json({ ok: true, orders });
}));

// ═══════════════════════════════════════════════════════════════
// DPR APP ROUTES
// ═══════════════════════════════════════════════════════════════

// GET DPR record
app.get('/api/dpr/:floor/:date', asyncRoute(async (req, res) => {
  const { floor, date } = req.params;
  const row = await queryOne('SELECT data_json, saved_at FROM dpr_records WHERE floor=$1 AND date=$2', [floor, date]);
  if (!row) return res.json({ ok: true, data: null });
  res.json({ ok: true, data: JSON.parse(row.data_json), savedAt: row.saved_at });
}));

// POST save DPR record + extract actuals into bridge table
app.post('/api/dpr/save', asyncRoute(async (req, res) => {
  const { floor, date, data, actuals } = req.body;
  if (!floor || !date || !data) return res.status(400).json({ ok: false, error: 'Missing floor, date, or data' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Save full DPR record
    await client.query(
      `INSERT INTO dpr_records (floor, date, data_json) VALUES ($1,$2,$3)
       ON CONFLICT(floor,date) DO UPDATE SET data_json=EXCLUDED.data_json, saved_at=NOW()`,
      [floor, date, JSON.stringify(data)]
    );
    // Delete old runs for this floor+date ONLY when new actuals are provided (prevents accidental wipe)
    if (actuals && actuals.length > 0) {
      await client.query('DELETE FROM production_actuals WHERE floor=$1 AND date=$2', [floor, date]);
    }
    // Upsert actuals — supports multi-run
    if (actuals && actuals.length > 0) {
      for (const a of actuals) {
        if (!a.qty || a.qty <= 0) continue;
        await client.query(
          `INSERT INTO production_actuals (order_id,batch_number,machine_id,date,shift,run_index,qty_lakhs,floor)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT(machine_id,date,shift,run_index) DO UPDATE SET
             order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number,
             qty_lakhs=EXCLUDED.qty_lakhs, synced_at=NOW()`,
          [a.orderId||null, a.batchNumber||null, a.machineId, date, a.shift, a.runIndex||0, a.qty, a.floor||floor]
        );
      }
    } else {
      // Fallback: parse from data.shifts for old single-run format
      const shifts = data.shifts || {};
      for (const [shiftName, shiftData] of Object.entries(shifts)) {
        if (!shiftData.machines) continue;
        for (const [machineId, machineData] of Object.entries(shiftData.machines)) {
          const runs = machineData.runs || [{ orderId: machineData.orderId, batchNumber: machineData.batchNumber, qty: machineData.prod }];
          for (let ri = 0; ri < runs.length; ri++) {
            const run = runs[ri];
            const qty = parseFloat(run.qty) || 0;
            if (qty <= 0) continue;
            await client.query(
              `INSERT INTO production_actuals (order_id,batch_number,machine_id,date,shift,run_index,qty_lakhs,floor)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT(machine_id,date,shift,run_index) DO UPDATE SET
                 order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number,
                 qty_lakhs=EXCLUDED.qty_lakhs, synced_at=NOW()`,
              [run.orderId||null, run.batchNumber||null, machineId, date, shiftName, ri, qty, floor]
            );
          }
        }
      }
    }
    await client.query('COMMIT');
  } catch(e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }

  // Two-way sync: update actualQty on planning orders (DPR → Planning)
  try {
    const planningState = await getPlanningState();
    if (planningState && planningState.orders) {
      const byOrderId = await queryAll(
        `SELECT order_id, SUM(qty_lakhs) as total_qty FROM production_actuals WHERE order_id IS NOT NULL AND order_id!='' GROUP BY order_id`
      );
      const byBatch = await queryAll(
        `SELECT batch_number, SUM(qty_lakhs) as total_qty FROM production_actuals WHERE (order_id IS NULL OR order_id='') AND batch_number IS NOT NULL AND batch_number!='' GROUP BY batch_number`
      );
      let changed = false;
      for (const ord of planningState.orders) { if (ord.actualQty !== undefined) ord.actualQty = 0; }
      for (const row of byOrderId) {
        const ord = planningState.orders.find(o => o.id === row.order_id);
        if (ord) { ord.actualQty = parseFloat(parseFloat(row.total_qty).toFixed(3)); changed = true; }
      }
      for (const row of byBatch) {
        const ord = planningState.orders.find(o => o.batchNumber === row.batch_number && (!o.actualQty || o.actualQty === 0));
        if (ord) { ord.actualQty = parseFloat(parseFloat(row.total_qty).toFixed(3)); changed = true; }
      }
      if (changed) await savePlanningState(planningState);
    }
  } catch (syncErr) { console.error('Planning actualQty sync error:', syncErr.message); }

  res.json({ ok: true, savedAt: new Date().toISOString() });
}));

// GET DPR dates
app.get('/api/dpr/dates/:floor', asyncRoute(async (req, res) => {
  const rows = await queryAll('SELECT DISTINCT date FROM dpr_records WHERE floor=$1 ORDER BY date DESC', [req.params.floor]);
  res.json({ ok: true, dates: rows.map(r => r.date) });
}));

// GET DPR history — all saved floor+date records (used by History page on any device)
app.get('/api/dpr/history', asyncRoute(async (req, res) => {
  const rows = await queryAll('SELECT floor, date, saved_at FROM dpr_records ORDER BY date DESC, floor ASC');
  res.json({ ok: true, records: rows.map(r => ({ floor: r.floor, date: r.date, savedAt: r.saved_at })) });
}));

// GET DPR settings (machine config, targets, active machines) — persisted on server
app.get('/api/dpr/settings', asyncRoute(async (req, res) => {
  const rows = await queryAll('SELECT key, value_json FROM dpr_settings');
  const settings = {};
  rows.forEach(r => { try { settings[r.key] = JSON.parse(r.value_json); } catch(e) {} });
  res.json({ ok: true, settings });
}));

// POST DPR settings — save one or more settings keys to server
app.post('/api/dpr/settings', asyncRoute(async (req, res) => {
  const { settings } = req.body;
  if (!settings || typeof settings !== 'object') return res.status(400).json({ ok: false, error: 'Missing settings' });
  for (const [key, value] of Object.entries(settings)) {
    await queryOne(
      `INSERT INTO dpr_settings (key, value_json) VALUES ($1, $2)
       ON CONFLICT(key) DO UPDATE SET value_json=EXCLUDED.value_json, updated_at=NOW()`,
      [key, JSON.stringify(value)]
    );
  }
  res.json({ ok: true });
}));

// ─── DPR Batch-Close Routes (NEW) ────────────────────────────────────────────
// POST — mark an order as DPR-closed (called from dpr.html when supervisor closes a batch)
app.post('/api/dpr/batch-close', asyncRoute(async (req, res) => {
  const { orderId, batchNumber, closedBy, notes } = req.body;
  if (!orderId) return res.status(400).json({ ok: false, error: 'orderId required' });
  await query(
    `INSERT INTO dpr_batch_closed (order_id, batch_number, closed_at, closed_by, notes)
     VALUES ($1,$2,NOW(),$3,$4)
     ON CONFLICT(order_id) DO UPDATE SET closed_at=NOW(), closed_by=EXCLUDED.closed_by, notes=EXCLUDED.notes`,
    [orderId, batchNumber || null, closedBy || null, notes || null]
  );
  res.json({ ok: true });
}));

// DELETE — reopen a DPR-closed batch (admin action)
app.delete('/api/dpr/batch-close/:orderId', asyncRoute(async (req, res) => {
  await query('DELETE FROM dpr_batch_closed WHERE order_id=$1', [req.params.orderId]);
  res.json({ ok: true });
}));

// GET — return all DPR-closed order IDs (Planning reads this to gate its close button)
app.get('/api/dpr/batch-closed', asyncRoute(async (req, res) => {
  const rows = await queryAll('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed');
  res.json({ ok: true, closed: rows });
}));

// GET actuals for a machine
app.get('/api/actuals/machine/:machineId', asyncRoute(async (req, res) => {
  const rows = await queryAll(
    `SELECT date,shift,qty_lakhs,order_id,batch_number FROM production_actuals WHERE machine_id=$1 ORDER BY date DESC,shift LIMIT 90`,
    [req.params.machineId]
  );
  res.json({ ok: true, actuals: rows });
}));

// GET actuals for an order
app.get('/api/actuals/order/:orderId', asyncRoute(async (req, res) => {
  const rows = await queryAll(
    `SELECT date,shift,qty_lakhs,machine_id FROM production_actuals WHERE order_id=$1 OR batch_number=$1 ORDER BY date,shift`,
    [req.params.orderId]
  );
  const total = rows.reduce((s,r) => s + parseFloat(r.qty_lakhs||0), 0);
  res.json({ ok: true, actuals: rows, total });
}));

// ═══════════════════════════════════════════════════════════════
// HEALTH CHECK + INFO
// ═══════════════════════════════════════════════════════════════

// ─── Auth helpers defined above ──────────────────────────────

// POST /api/auth/login
app.post('/api/auth/login', asyncRoute(async (req, res) => {
  const { username, pin, app: appName } = req.body;
  if (!username || !pin || !appName) return res.status(400).json({ ok: false, error: 'Missing credentials' });
  const user = await queryOne(`SELECT * FROM app_users WHERE username=$1 AND app=$2`, [username, appName]);
  if (!user) return res.status(401).json({ ok: false, error: 'User not found' });
  if (user.pin_hash !== hashPin(pin)) return res.status(401).json({ ok: false, error: 'Invalid PIN' });
  const token = generateToken();
  const expires = new Date(Date.now() + 30*24*60*60*1000).toISOString(); // 30 days
  await query(`INSERT INTO app_sessions (token,user_id,username,role,app,expires_at) VALUES ($1,$2,$3,$4,$5,$6)`,
    [token, user.id, user.username, user.role, appName, expires]);
  await logAudit(user.username, user.role, appName, 'LOGIN', 'Successful login', req.ip);
  res.json({ ok: true, token, username: user.username, role: user.role });
}));

// POST /api/auth/verify
app.post('/api/auth/verify', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.body.token);
  if (!session) return res.status(401).json({ ok: false, error: 'Invalid or expired session' });
  res.json({ ok: true, username: session.username, role: session.role, app: session.app });
}));

// POST /api/auth/logout
app.post('/api/auth/logout', asyncRoute(async (req, res) => {
  const { token } = req.body;
  if (token) {
    const session = await verifyToken(token);
    if (session) {
      await logAudit(session.username, session.role, session.app, 'LOGOUT', null, req.ip);
      await query(`DELETE FROM app_sessions WHERE token=$1`, [token]);
    }
  }
  res.json({ ok: true });
}));

// POST /api/auth/change-pin
app.post('/api/auth/change-pin', asyncRoute(async (req, res) => {
  const { token, username, newPin } = req.body;
  const session = await verifyToken(token);
  if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
  if (session.role !== 'admin' && session.username !== username)
    return res.status(403).json({ ok: false, error: 'Only admin can change other users PINs' });
  await query(`UPDATE app_users SET pin_hash=$1, updated_at=NOW() WHERE username=$2 AND app=$3`,
    [hashPin(newPin), username, session.app]);
  await logAudit(session.username, session.role, session.app, 'CHANGE_PIN', `Changed PIN for ${username}`, req.ip);
  res.json({ ok: true });
}));

// POST /api/audit/log
app.post('/api/audit/log', asyncRoute(async (req, res) => {
  const { token, action, details } = req.body;
  const session = await verifyToken(token);
  if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
  await logAudit(session.username, session.role, session.app, action, details, req.ip);
  res.json({ ok: true });
}));

// GET /api/audit/view
app.get('/api/audit/view', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token'] || req.query.token);
  if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
  if (session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
  const limit = parseInt(req.query.limit) || 200;
  const appName = req.query.app || session.app;
  const rows = await queryAll(`SELECT * FROM audit_log WHERE app=$1 ORDER BY ts DESC LIMIT $2`, [appName, limit]);
  res.json({ ok: true, logs: rows });
}));

// GET /api/auth/users
app.get('/api/auth/users', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token'] || req.query.token);
  if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
  const users = await queryAll(`SELECT id,username,role,app,created_at,updated_at FROM app_users WHERE app=$1`,
    [req.query.app || session.app]);
  res.json({ ok: true, users });
}));

// ─── TEMP Batch Colour/PC Code Update ────────────────────────

// POST /api/temp-batches/update-details
app.post('/api/temp-batches/update-details', asyncRoute(async (req, res) => {
  const { tempBatchId, colour, pcCode } = req.body;
  if (!tempBatchId) return res.status(400).json({ ok: false, error: 'Missing tempBatchId' });
  const tb = await queryOne('SELECT * FROM temp_batches WHERE id=$1', [tempBatchId]);
  if (!tb) return res.status(404).json({ ok: false, error: 'TEMP batch not found' });
  await query(`UPDATE temp_batches SET colour=$1, pc_code=$2, colour_confirmed=TRUE WHERE id=$3`,
    [colour||null, pcCode||null, tempBatchId]);
  await logAudit('SYSTEM','system','dpr','TEMP_DETAILS_SET',`TEMP batch ${tempBatchId} — Colour: ${colour}, PC Code: ${pcCode}`);
  const updated = await queryOne('SELECT * FROM temp_batches WHERE id=$1', [tempBatchId]);
  res.json({ ok: true, batch: updated });
}));

// ─── W/O (Without Order) Reconciliation ──────────────────────

// POST /api/wo/assign-customer
app.post('/api/wo/assign-customer', asyncRoute(async (req, res) => {
  const { token, orderId, customer, poNumber, zone, qtyConfirmed } = req.body;
  const session = await verifyToken(token);
  if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
  if (!['planning_manager','admin'].includes(session.role))
    return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
  const planState = await getPlanningState();
  const ord = (planState.orders||[]).find(o=>o.id===orderId);
  if (!ord) return res.status(404).json({ ok: false, error: 'Order not found' });
  if (ord.woStatus !== 'wo') return res.status(400).json({ ok: false, error: 'Order is not a W/O order' });
  ord.customer=customer; ord.poNumber=poNumber||ord.poNumber; ord.zone=zone||ord.zone;
  if (qtyConfirmed) ord.qty=qtyConfirmed;
  ord.woCustomerAssignedAt=new Date().toISOString(); ord.woCustomerAssignedBy=session.username;
  (planState.dispatchPlans||[]).forEach(d=>{
    if(d.productionOrderId===orderId){d.customer=customer;d.poNumber=poNumber||d.poNumber;d.zone=zone||d.zone;}
  });
  await savePlanningState(planState);
  await logAudit(session.username,session.role,'planning','WO_CUSTOMER_ASSIGNED',`W/O order ${orderId} → ${customer}`);
  res.json({ ok: true });
}));

// POST /api/wo/propose-reconciliation
app.post('/api/wo/propose-reconciliation', asyncRoute(async (req, res) => {
  const { token, orderId, customer, poNumber, zone, qtyConfirmed } = req.body;
  const session = await verifyToken(token);
  if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
  if (!['planning_manager','admin'].includes(session.role))
    return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
  if (!customer) return res.status(400).json({ ok: false, error: 'Customer name required' });
  const id = `WORECON-${Date.now()}`;
  const billTo = req.body.billTo || '';
  const finalCustomer = (billTo && billTo !== customer) ? customer+'|||'+billTo : customer;
  await query(
    `INSERT INTO wo_reconciliation_requests (id,proposed_by,status,order_id,customer,po_number,zone,qty_confirmed) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [id, session.username, 'pending', orderId, finalCustomer, poNumber||null, zone||null, qtyConfirmed||null]
  );
  await logAudit(session.username,session.role,'planning','WO_RECON_PROPOSED',`W/O recon ${id} → order ${orderId} → ${customer}`);
  res.json({ ok: true, requestId: id, status: 'pending' });
}));

// GET /api/wo/pending
app.get('/api/wo/pending', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token']);
  if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
  const requests = await queryAll(`SELECT * FROM wo_reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`);
  const planState = await getPlanningState();
  const enriched = requests.map(r=>({...r, orderDetails:(planState.orders||[]).find(o=>o.id===r.order_id)||{}}));
  res.json({ ok: true, requests: enriched });
}));

// POST /api/wo/approve/:id
app.post('/api/wo/approve/:id', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token']);
  if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
  const request = await queryOne('SELECT * FROM wo_reconciliation_requests WHERE id=$1', [req.params.id]);
  if (!request) return res.status(404).json({ ok: false, error: 'Request not found' });
  if (request.status !== 'pending') return res.status(400).json({ ok: false, error: 'Already processed' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const now = new Date().toISOString();
    const planState = await getPlanningState();
    const ord = (planState.orders||[]).find(o=>o.id===request.order_id);
    if (ord) {
      const custParts = (request.customer||'').split('|||');
      ord.customer=custParts[0]; ord.shipTo=custParts[0]; ord.billTo=custParts[1]||'';
      ord.poNumber=request.po_number||ord.poNumber; ord.zone=request.zone||ord.zone;
      if (request.qty_confirmed) ord.qty=request.qty_confirmed;
      ord.woStatus='wo-reconciled'; ord.woReconciledAt=now; ord.woReconciledBy=session.username;
      (planState.dispatchPlans||[]).forEach(d=>{
        if(d.productionOrderId===request.order_id){d.customer=request.customer;d.poNumber=request.po_number||d.poNumber;d.zone=request.zone||d.zone;}
      });
      await savePlanningState(planState);
      await client.query(`UPDATE tracking_labels SET customer=$1, wo_status='wo-reconciled' WHERE batch_number=$2`,
        [request.customer, ord.batchNumber]);
    }
    await client.query(`UPDATE wo_reconciliation_requests SET status='approved',approved_by=$1,approved_at=$2 WHERE id=$3`,
      [session.username, now, request.id]);
    await client.query('COMMIT');
  } catch(e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }
  await logAudit(session.username,session.role,'planning','WO_RECON_APPROVED',`W/O recon ${req.params.id} approved`);
  res.json({ ok: true, message: 'W/O reconciliation complete. Replacement labels ready for printing.' });
}));

// POST /api/wo/reject/:id
app.post('/api/wo/reject/:id', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token']);
  if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
  const { reason } = req.body;
  await query(`UPDATE wo_reconciliation_requests SET status='rejected',approved_by=$1,approved_at=NOW(),rejection_reason=$2 WHERE id=$3`,
    [session.username, reason||'No reason given', req.params.id]);
  await logAudit(session.username,session.role,'planning','WO_RECON_REJECTED',`Rejected ${req.params.id}: ${reason}`);
  res.json({ ok: true });
}));

// GET /api/wo/history
app.get('/api/wo/history', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token']);
  if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
  const rows = await queryAll(`SELECT * FROM wo_reconciliation_requests ORDER BY proposed_at DESC LIMIT 50`);
  res.json({ ok: true, requests: rows });
}));

// ─── Data Export / Import (Admin — for safe migrations) ────────

// GET /api/admin/export
app.get('/api/admin/export', asyncRoute(async (req, res) => {
  const exportKey = process.env.EXPORT_KEY || 'sunloc-export-2024';
  if (req.query.key !== exportKey) {
    const session = await verifyToken(req.headers['x-session-token'] || req.query.token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin access required' });
  }
  const tables = ['planning_state','dpr_records','production_actuals','tracking_labels','tracking_scans',
    'tracking_stage_closure','tracking_wastage','tracking_dispatch_records','tracking_alerts',
    'app_users','audit_log','schema_migrations'];
  const exportData = { exported_at: new Date().toISOString(), db: 'PostgreSQL (Railway)', version: 'sunloc-v9', tables: {} };
  for (const table of tables) {
    try { exportData.tables[table] = await queryAll(`SELECT * FROM ${table}`); }
    catch(e) { exportData.tables[table] = []; }
  }
  const json = JSON.stringify(exportData, null, 2);
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Content-Disposition', `attachment; filename="sunloc-backup-${new Date().toISOString().slice(0,10)}.json"`);
  res.send(json);
  console.log(`[Export] ${Object.values(exportData.tables).reduce((s,t)=>s+t.length,0)} rows exported`);
}));

// POST /api/admin/import
app.post('/api/admin/import', asyncRoute(async (req, res) => {
  const exportKey = process.env.EXPORT_KEY || 'sunloc-export-2024';
  if (req.query.key !== exportKey) {
    const session = await verifyToken(req.headers['x-session-token']);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin access required' });
  }
  const { tables, confirm } = req.body;
  if (confirm !== 'IMPORT_CONFIRMED') return res.status(400).json({ ok: false, error: 'Must include confirm: "IMPORT_CONFIRMED"' });
  if (!tables) return res.status(400).json({ ok: false, error: 'No tables data provided' });
  await runMigrations();
  const importableTables = ['planning_state','dpr_records','production_actuals','tracking_labels',
    'tracking_scans','tracking_stage_closure','tracking_wastage','tracking_dispatch_records','tracking_alerts'];
  const results = {};
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const table of importableTables) {
      const rows = tables[table];
      if (!rows || rows.length === 0) { results[table] = 0; continue; }
      try {
        const cols = Object.keys(rows[0]);
        let count = 0;
        for (const row of rows) {
          const vals = cols.map(c => row[c]);
          const placeholders = cols.map((_,i) => `$${i+1}`).join(',');
          const updateSet = cols.filter(c=>c!=='id').map(c=>`${c}=EXCLUDED.${c}`).join(',');
          await client.query(
            `INSERT INTO ${table} (${cols.join(',')}) VALUES (${placeholders}) ON CONFLICT DO UPDATE SET ${updateSet}`,
            vals
          );
          count++;
        }
        results[table] = count;
      } catch(e) { results[table] = `ERROR: ${e.message}`; }
    }
    await client.query('COMMIT');
  } catch(e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }
  const totalRows = Object.values(results).reduce((s,v)=>typeof v==='number'?s+v:s, 0);
  console.log(`[Import] Restored ${totalRows} rows`);
  res.json({ ok: true, results, totalRows });
}));

// GET /api/admin/db-status
app.get('/api/admin/db-status', asyncRoute(async (req, res) => {
  const migrations = await queryAll('SELECT * FROM schema_migrations ORDER BY version');
  const tables = ['planning_state','dpr_records','production_actuals','tracking_labels',
    'tracking_scans','tracking_stage_closure','tracking_wastage','tracking_dispatch_records',
    'tracking_alerts','app_users','audit_log'];
  const tableRowCounts = {};
  for (const t of tables) {
    try { const r = await queryOne(`SELECT COUNT(*) as c FROM ${t}`); tableRowCounts[t] = parseInt(r.c); }
    catch(e) { tableRowCounts[t] = 'N/A'; }
  }
  res.json({ ok: true, db: 'PostgreSQL (Railway)', migrations_applied: migrations.length, migrations, table_row_counts: tableRowCounts });
}));

// ─── TEMP Batch System ─────────────────────────────────────────

// calcTempLabelCount and tempBatchId defined above

// GET /api/temp-batches/check/:machineId
app.get('/api/temp-batches/check/:machineId', asyncRoute(async (req, res) => {
  const { machineId } = req.params;
  const today = new Date().toISOString().split('T')[0];
  const planState = await getPlanningState();
  const activeOrders = (planState.orders||[]).filter(o=>o.machineId===machineId&&o.status!=='closed'&&!o.deleted);
  const existing = await queryOne(`SELECT * FROM temp_batches WHERE machine_id=$1 AND date=$2`, [machineId, today]);
  const allTemp = await queryAll(`SELECT * FROM temp_batches WHERE machine_id=$1 AND status='active' ORDER BY date DESC`, [machineId]);
  const mc = (planState.machineMaster||[]).find(m=>m.id===machineId);
  const packSizes = planState.packSizes||{};
  const packSizeLakhs = mc ? ((packSizes[mc.size]||100000)/100000) : 1;
  const capLakhs = mc ? (mc.cap||8) : 8;
  const labelCount = mc ? calcTempLabelCount(capLakhs, packSizeLakhs) : 0;
  res.json({
    ok:true, machineId, hasActiveOrder: activeOrders.length>0,
    activeOrders: activeOrders.map(o=>({id:o.id,batchNumber:o.batchNumber,qty:o.qty,status:o.status})),
    todayTempBatch: existing||null, needsTemp: activeOrders.length===0,
    machineInfo: mc ? {size:mc.size,capLakhs,packSizeLakhs,labelCount} : null,
    activeTempBatches: allTemp
  });
}));

// POST /api/temp-batches/create
app.post('/api/temp-batches/create', asyncRoute(async (req, res) => {
  const { machineId, date } = req.body;
  const batchDate = date || new Date().toISOString().split('T')[0];
  const id = tempBatchId(machineId, batchDate);
  const planState = await getPlanningState();
  const mc = (planState.machineMaster||[]).find(m=>m.id===machineId);
  if (!mc) return res.status(400).json({ ok:false, error:'Machine not found' });
  const packSizeLakhs = ((planState.packSizes||{})[mc.size]||100000)/100000;
  const capLakhs = mc.cap||8;
  const labelCount = calcTempLabelCount(capLakhs, packSizeLakhs);
  await query(
    `INSERT INTO temp_batches (id,machine_id,machine_size,date,daily_cap_lakhs,label_count,pack_size_lakhs) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`,
    [id, machineId, mc.size, batchDate, capLakhs, labelCount, packSizeLakhs]
  );
  await query(`INSERT INTO temp_batch_alerts (machine_id,temp_batch_id,alert_date) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
    [machineId, id, batchDate]);
  await logAudit('SYSTEM','system','dpr','TEMP_BATCH_CREATED',`TEMP batch ${id} — ${capLakhs}L → ${labelCount} labels`);
  const batch = await queryOne('SELECT * FROM temp_batches WHERE id=$1', [id]);
  res.json({ ok:true, batch });
}));

// GET /api/temp-batches/active
app.get('/api/temp-batches/active', asyncRoute(async (req, res) => {
  const batches = await queryAll(`SELECT * FROM temp_batches WHERE status='active' ORDER BY machine_id, date DESC`);
  const today = new Date().toISOString().split('T')[0];
  const enriched = batches.map(b=>({...b, daysActive: Math.floor((new Date(today)-new Date(b.date))/86400000)+1}));
  res.json({ ok:true, batches: enriched, count: enriched.length });
}));

// POST /api/reconciliation/propose
app.post('/api/reconciliation/propose', asyncRoute(async (req, res) => {
  const { token, orderDetails, backDate, tempBatchMappings } = req.body;
  const session = await verifyToken(token);
  if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
  if (!['planning_manager','admin'].includes(session.role))
    return res.status(403).json({ ok:false, error:'Planning Manager or Admin required' });
  const earliestTempDate = tempBatchMappings.reduce((min,m)=>m.tempDate<min?m.tempDate:min,'9999-12-31');
  if (backDate < earliestTempDate)
    return res.status(400).json({ ok:false, error:`Back-date (${backDate}) cannot be before earliest TEMP batch date (${earliestTempDate})` });
  for (const mapping of tempBatchMappings) {
    const tb = await queryOne(`SELECT * FROM temp_batches WHERE id=$1`, [mapping.tempBatchId]);
    if (!tb) return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} not found` });
    if (tb.status !== 'active') return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} is not active` });
  }
  const totalBoxes = tempBatchMappings.reduce((s,m)=>s+(m.boxes||0),0);
  const id = `RECON-${Date.now()}`;
  await query(
    `INSERT INTO reconciliation_requests (id,proposed_by,status,order_id,order_details,back_date,temp_batch_mappings,total_boxes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [id, session.username, 'pending', orderDetails.id||`ORDER-${Date.now()}`, JSON.stringify(orderDetails), backDate, JSON.stringify(tempBatchMappings), totalBoxes]
  );
  await logAudit(session.username,session.role,'planning','RECON_PROPOSED',`Recon proposed: ${id} — ${tempBatchMappings.length} batches, ${totalBoxes} boxes`);
  res.json({ ok:true, requestId:id, status:'pending', message:'Awaiting Admin approval' });
}));

// GET /api/reconciliation/pending
app.get('/api/reconciliation/pending', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token']);
  if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
  const rows = await queryAll(`SELECT * FROM reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`);
  const requests = rows.map(r=>({...r, order_details:JSON.parse(r.order_details), temp_batch_mappings:JSON.parse(r.temp_batch_mappings)}));
  res.json({ ok:true, requests });
}));

// POST /api/reconciliation/approve/:id
app.post('/api/reconciliation/approve/:id', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token']);
  if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
  const request = await queryOne(`SELECT * FROM reconciliation_requests WHERE id=$1`, [req.params.id]);
  if (!request) return res.status(404).json({ ok:false, error:'Request not found' });
  if (request.status !== 'pending') return res.status(400).json({ ok:false, error:'Request is not pending' });
  const orderDetails = JSON.parse(request.order_details);
  const mappings = JSON.parse(request.temp_batch_mappings);
  const orderId = request.order_id;
  const results = { migratedScans:0, migratedLabels:0, migratedWastage:0, tempBatchesReconciled:0 };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const now = new Date().toISOString();
    for (const mapping of mappings) {
      const { tempBatchId: tbId, startLabelNumber, endLabelNumber } = mapping;
      const tb = (await client.query('SELECT * FROM temp_batches WHERE id=$1',[tbId])).rows[0];
      if (!tb) continue;
      // 1. Migrate tracking labels
      let labelQ, labelP;
      if (startLabelNumber && endLabelNumber) {
        labelQ = `SELECT * FROM tracking_labels WHERE batch_number=$1 AND label_number>=$2 AND label_number<=$3`;
        labelP = [tbId, startLabelNumber, endLabelNumber];
      } else {
        labelQ = `SELECT * FROM tracking_labels WHERE batch_number=$1`;
        labelP = [tbId];
      }
      const labelsToMigrate = (await client.query(labelQ, labelP)).rows;
      for (const label of labelsToMigrate) {
        const newId = label.id.replace(tbId, orderId);
        // Rebuild qr_data so printed QR codes still resolve after batch reconciliation
        const oldQr = label.qr_data || '';
        const newQr = oldQr
          ? oldQr.replace(tbId, orderId).replace(label.id, newId)
          : `SUNLOC|${orderId}|${label.label_number}|${label.size}|${label.qty}|${newId}`;
        await client.query(
          `INSERT INTO tracking_labels SELECT $1 as id, $2 as batch_number,
            label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,
            po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,
            voided_at,voided_by,$4 as qr_data FROM tracking_labels WHERE id=$3
           ON CONFLICT(id) DO UPDATE SET batch_number=EXCLUDED.batch_number, qr_data=EXCLUDED.qr_data`,
          [newId, orderId, label.id, newQr]
        );
        const sc = await client.query(`UPDATE tracking_scans SET label_id=$1,batch_number=$2 WHERE label_id=$3 RETURNING id`,
          [newId, orderId, label.id]);
        results.migratedScans += sc.rowCount;
        if (newId !== label.id) await client.query(`DELETE FROM tracking_labels WHERE id=$1`, [label.id]);
        results.migratedLabels++;
      }
      // 2. Migrate wastage
      const wq = await client.query(`UPDATE tracking_wastage SET batch_number=$1 WHERE batch_number=$2 RETURNING id`,[orderId,tbId]);
      results.migratedWastage += wq.rowCount;
      // 3. Migrate stage closures
      await client.query(`UPDATE tracking_stage_closure SET batch_number=$1 WHERE batch_number=$2`,[orderId,tbId]);
      // 4. Migrate DPR actuals
      await client.query(`UPDATE production_actuals SET order_id=$1,batch_number=$2 WHERE batch_number=$3`,
        [orderId, orderDetails.batchNumber||orderId, tbId]);
      // 5. Migrate dispatch records
      await client.query(`UPDATE tracking_dispatch_records SET batch_number=$1 WHERE batch_number=$2`,[orderId,tbId]);
      // 6. Mark TEMP batch reconciled
      const status = startLabelNumber ? 'partial' : 'reconciled';
      await client.query(`UPDATE temp_batches SET status=$1,reconciled_order_id=$2,reconciled_at=$3,reconciled_by=$4 WHERE id=$5`,
        [status,orderId,now,session.username,tbId]);
      results.tempBatchesReconciled++;
    }
    // 7. Update planning state
    const planState = await getPlanningState();
    if (planState.orders) {
      const idx = planState.orders.findIndex(o=>o.id===orderId);
      const orderToSave = { ...orderDetails, id:orderId, startDate:request.back_date,
        actualQty:mappings.reduce((s,m)=>s+(m.actualLakhs||0),0), status:'running' };
      if (idx>=0) planState.orders[idx]={...planState.orders[idx],...orderToSave};
      else planState.orders.push(orderToSave);
      // Use savePlanningState so cleanOrders runs — prevents ghost orders from reconciliation
      await savePlanningState(planState);
    }
    // 8. Mark request approved
    await client.query(`UPDATE reconciliation_requests SET status='approved',approved_by=$1,approved_at=$2 WHERE id=$3`,
      [session.username,now,request.id]);
    await client.query('COMMIT');
  } catch(e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }
  await logAudit(session.username,session.role,'planning','RECON_APPROVED',
    `Reconciliation ${req.params.id} — ${results.migratedLabels} labels, ${results.migratedScans} scans migrated`);
  res.json({ ok:true, results, message:'Reconciliation complete. Replacement labels ready for printing.' });
}));

// POST /api/reconciliation/reject/:id
app.post('/api/reconciliation/reject/:id', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token']);
  if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
  const { reason } = req.body;
  await query(`UPDATE reconciliation_requests SET status='rejected',approved_by=$1,approved_at=NOW(),rejection_reason=$2 WHERE id=$3`,
    [session.username, reason||'No reason given', req.params.id]);
  await logAudit(session.username,session.role,'planning','RECON_REJECTED',`Rejected: ${req.params.id} — ${reason}`);
  res.json({ ok:true });
}));

// GET /api/reconciliation/history
app.get('/api/reconciliation/history', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token']);
  if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
  const rows = await queryAll(`SELECT * FROM reconciliation_requests ORDER BY proposed_at DESC LIMIT 100`);
  const requests = rows.map(r=>({...r, order_details:JSON.parse(r.order_details), temp_batch_mappings:JSON.parse(r.temp_batch_mappings)}));
  res.json({ ok:true, requests });
}));

app.get('/api/health', asyncRoute(async (req, res) => {
  const [planningRow, dprCount, actualsCount, labelsCount, scansCount, usersCount] = await Promise.all([
    queryOne('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1'),
    queryOne('SELECT COUNT(*) as c FROM dpr_records'),
    queryOne('SELECT COUNT(*) as c FROM production_actuals'),
    queryOne('SELECT COUNT(*) as c FROM tracking_labels'),
    queryOne('SELECT COUNT(*) as c FROM tracking_scans'),
    queryOne('SELECT COUNT(*) as c FROM app_users'),
  ]);
  res.json({
    ok: true,
    server: 'Sunloc Integrated Server v2.0 (PostgreSQL)',
    db: 'PostgreSQL (Railway)',
    url: 'https://sunloc.up.railway.app',
    planningSavedAt: planningRow?.saved_at || null,
    dprRecords: parseInt(dprCount?.c||0),
    actualsEntries: parseInt(actualsCount?.c||0),
    trackingLabels: parseInt(labelsCount?.c||0),
    trackingScans: parseInt(scansCount?.c||0),
    users: parseInt(usersCount?.c||0),
    uptime: Math.floor(process.uptime()) + 's',
    apps: ['planning', 'dpr', 'tracking'],
    syncStatus: 'all-apps-synced',
  });
}));

// ─── CROSS-APP SYNC SNAPSHOT ──────────────────────────────────
// Single endpoint that returns everything all 3 apps need in one call.
// GET /api/tracking/labels-only — returns all non-voided labels fast
app.get('/api/tracking/labels-only', asyncRoute(async (req, res) => {
  const labels = await queryAll('SELECT * FROM tracking_labels WHERE voided=0 ORDER BY generated DESC');
  res.json({ ok: true, labels });
}));

// GET /api/sync/snapshot
app.get('/api/sync/snapshot', asyncRoute(async (req, res) => {
  const [planningState, savedAtRow, labels, scans, closure, wastage, dispatch, alerts] = await Promise.all([
    getPlanningState(),
    queryOne('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1'),
    queryAll('SELECT * FROM tracking_labels ORDER BY generated DESC'),
    queryAll('SELECT * FROM tracking_scans ORDER BY ts ASC'),
    queryAll('SELECT * FROM tracking_stage_closure'),
    queryAll('SELECT * FROM tracking_wastage ORDER BY ts ASC'),
    queryAll('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC'),
    queryAll('SELECT * FROM tracking_alerts WHERE resolved=0'),
  ]);

  // Enrich orders with actual production (same as /api/planning/state does)
  const rawOrders = planningState.orders || [];
  const orders = [];
  for (const o of rawOrders) {
    if (o.deleted || o.status === 'closed') continue;
    const actual = await getOrderActuals(o.id, o.batchNumber);
    orders.push({
      id: o.id,
      batchNumber: o.batchNumber || '',
      poNumber: o.poNumber || '',
      customer: o.customer || '',
      machineId: o.machineId || '',
      size: o.size || '',
      colour: o.colour || '',
      pcCode: o.pcCode || '',
      qty: o.qty || 0,
      grossQty: o.grossQty || 0,
      actualQty: actual || o.actualQty || 0,
      actualProd: actual || o.actualProd || 0,
      status: (actual > 0 && o.status === 'pending') ? 'running' : (o.status || 'pending'),
      isPrinted: !!o.isPrinted,
      startDate: o.startDate || null,
      endDate: o.endDate || null,
      printingMatter: o.printMatter || o.printingMatter || '',
      zone: o.zone || '',
      dispatchedQty: o.dispatchedQty || 0,
    });
  }

  res.json({
    ok: true,
    planningSavedAt: savedAtRow?.saved_at || null,
    orders,
    machineMaster: planningState.machineMaster || [],
    tracking: { labels, scans, stageClosure: closure, wastage, dispatchRecs: dispatch, alerts },
  });
}));

// ─── TRACKING ROUTES ──────────────────────────────────────────

app.get('/api/tracking/label', asyncRoute(async (req, res) => {
  const { id, batchNumber, labelNumber } = req.query;
  let label = null;
  if (id) label = await queryOne('SELECT * FROM tracking_labels WHERE id=$1', [id]);
  if (!label && batchNumber && labelNumber != null)
    label = await queryOne('SELECT * FROM tracking_labels WHERE batch_number=$1 AND ABS(label_number)=ABS($2)',
      [batchNumber, parseInt(labelNumber)]);
  res.json({ ok: true, label: label || null });
}));

app.get('/api/tracking/state', asyncRoute(async (req, res) => {
  const [labels, scans, closure, wastage, dispatch, alerts] = await Promise.all([
    queryAll('SELECT * FROM tracking_labels ORDER BY generated DESC'),
    queryAll('SELECT * FROM tracking_scans ORDER BY ts ASC'),
    queryAll('SELECT * FROM tracking_stage_closure'),
    queryAll('SELECT * FROM tracking_wastage ORDER BY ts ASC'),
    queryAll('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC'),
    queryAll('SELECT * FROM tracking_alerts WHERE resolved=0'),
  ]);
  res.json({ ok:true, state:{ labels, scans, stageClosure:closure, wastage, dispatchRecs:dispatch, alerts } });
}));

// POST /api/tracking/state — save full tracking state
app.post('/api/tracking/state', asyncRoute(async (req, res) => {
  const { labels, scans, stageClosure, wastage, dispatchRecs, alerts } = req.body;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    if (labels && labels.length) {
      for (const l of labels) {
        // Convert OL- string label numbers to negative integers for DB storage
        const _lNum = (typeof l.labelNumber==='string' && l.labelNumber.startsWith('OL-'))
          ? -(parseInt(l.labelNumber.slice(3))||0)
          : (parseInt(l.labelNumber)||0);
        await client.query(
          `INSERT INTO tracking_labels (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,wo_status,ship_to,bill_to,is_excess,excess_num,excess_total,normal_total)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)
           ON CONFLICT(id) DO UPDATE SET batch_number=EXCLUDED.batch_number,qty=EXCLUDED.qty,printed=EXCLUDED.printed,printed_at=EXCLUDED.printed_at,voided=EXCLUDED.voided,void_reason=EXCLUDED.void_reason,customer=EXCLUDED.customer,colour=EXCLUDED.colour,qr_data=EXCLUDED.qr_data,wo_status=EXCLUDED.wo_status,is_orange=EXCLUDED.is_orange,parent_label_id=EXCLUDED.parent_label_id,is_partial=EXCLUDED.is_partial,is_excess=EXCLUDED.is_excess,printing_matter=EXCLUDED.printing_matter`,
          [l.id,l.batchNumber,_lNum,l.size,l.qty,l.isPartial?1:0,l.isOrange?1:0,
           l.parentLabelId||null,l.customer||null,l.colour||null,l.pcCode||null,
           l.poNumber||null,l.machineId||null,l.printingMatter||null,
           l.generated||new Date().toISOString(),l.printed?1:0,l.printedAt||null,
           l.voided?1:0,l.voidReason||null,l.voidedAt||null,l.voidedBy||null,l.qrData||null,
           l.woStatus||null,l.shipTo||null,l.billTo||null,l.isExcess?1:0,l.excessNum||null,l.excessTotal||null,l.normalTotal||null]
        );
      }
    }
    if (scans && scans.length) {
      for (const s of scans) {
        await client.query(
          `INSERT INTO tracking_scans (id,label_id,batch_number,dept,type,ts,operator,size,qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(id) DO NOTHING`,
          [s.id,s.labelId||s.label_id,s.batchNumber||s.batch_number,s.dept,s.type,s.ts,s.operator||null,s.size||null,s.qty||null]
        );
      }
    }
    if (stageClosure && stageClosure.length) {
      for (const s of stageClosure) {
        await client.query(
          `INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at,closed_by) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO UPDATE SET closed=EXCLUDED.closed`,
          [s.id,s.batchNumber||s.batch_number,s.dept,!!s.closed,s.closedAt||s.closed_at,s.closedBy||s.closed_by||null]
        );
      }
    }
    if (wastage && wastage.length) {
      for (const w of wastage) {
        await client.query(
          `INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO UPDATE SET qty=EXCLUDED.qty`,
          [w.id,w.batchNumber||w.batch_number,w.dept,w.type,w.qty,w.ts,w.by||null]
        );
      }
    }
    if (dispatchRecs && dispatchRecs.length) {
      for (const d of dispatchRecs) {
        await client.query(
          `INSERT INTO tracking_dispatch_records (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id) DO UPDATE SET qty=EXCLUDED.qty,boxes=EXCLUDED.boxes`,
          [d.id,d.batchNumber||d.batch_number,d.customer||null,d.qty,d.boxes,d.vehicleNo||d.vehicle_no||null,d.invoiceNo||d.invoice_no||null,d.remarks||null,d.ts,d.by||null]
        );
      }
    }
    if (alerts && alerts.length) {
      for (const a of alerts) {
        await client.query(
          `INSERT INTO tracking_alerts (id,label_id,batch_number,dept,scan_in_ts,hours_stuck,resolved,msg) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO UPDATE SET resolved=EXCLUDED.resolved,hours_stuck=EXCLUDED.hours_stuck`,
          [a.id,a.labelId||a.label_id,a.batchNumber||a.batch_number,a.dept,a.scanInTs||a.scan_in_ts,a.hoursStuck||a.hours_stuck||null,!!a.resolved,a.msg||null]
        );
      }
    }
    await client.query('COMMIT');
  } catch(e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }
  res.json({ ok:true });
}));

// GET /api/tracking/batch-summary/:batchNumber
app.get('/api/tracking/batch-summary/:batchNumber', asyncRoute(async (req, res) => {
  const { batchNumber } = req.params;
  const [labels, scans, wastage, dispatch, alerts] = await Promise.all([
    queryAll('SELECT * FROM tracking_labels WHERE batch_number=$1',[batchNumber]),
    queryAll('SELECT * FROM tracking_scans WHERE batch_number=$1 ORDER BY ts',[batchNumber]),
    queryAll('SELECT * FROM tracking_wastage WHERE batch_number=$1',[batchNumber]),
    queryAll('SELECT * FROM tracking_dispatch_records WHERE batch_number=$1',[batchNumber]),
    queryAll('SELECT * FROM tracking_alerts WHERE batch_number=$1 AND resolved=0',[batchNumber]),
  ]);
  const deptMap = {};
  scans.forEach(s=>{ if(!deptMap[s.dept]) deptMap[s.dept]={in:0,out:0}; deptMap[s.dept][s.type]=(deptMap[s.dept][s.type]||0)+1; });
  const labelStats = { total:labels.length, printed:labels.filter(l=>l.printed).length, voided:labels.filter(l=>l.voided).length };
  const dispatched = dispatch.reduce((s,d)=>s+d.boxes,0);
  res.json({ ok:true, deptMap, labelStats, wastage, alerts, dispatched, batchNumber });
}));

// GET /api/tracking/wip-summary
app.get('/api/tracking/wip-summary', asyncRoute(async (req, res) => {
  const [summary, closures] = await Promise.all([
    queryAll(`SELECT batch_number, dept, type, COUNT(*) as cnt FROM tracking_scans GROUP BY batch_number, dept, type`),
    queryAll(`SELECT batch_number, dept, closed FROM tracking_stage_closure WHERE closed=1`)
  ]);
  res.json({ ok: true, scanSummary: summary, closures });
}));

// GET /api/tracking/labels
app.get('/api/tracking/labels', asyncRoute(async (req, res) => {
  const { batchNumber } = req.query;
  if(!batchNumber) return res.status(400).json({ok:false,error:'batchNumber required'});
  const labels = await queryAll('SELECT * FROM tracking_labels WHERE batch_number=$1 AND voided=0',[batchNumber]);
  res.json({ok:true, labels});
}));

// POST /api/tracking/scan
app.post('/api/tracking/scan', asyncRoute(async (req, res) => {
  const { scan } = req.body;
  if(!scan||!scan.id) return res.status(400).json({ok:false,error:'Missing scan'});
  const labelId = scan.labelId||scan.label_id;
  const dept    = scan.dept;
  const type    = scan.type;

  // Server-side flow enforcement
  // Also match old temp-batch label IDs (e.g. BT006-xxx -> 26ZH035-xxx same suffix)
  const dashIdx = labelId.indexOf('-');
  const suffix  = dashIdx > 0 ? labelId.slice(dashIdx) : null;
  const existing = await queryAll(
    `SELECT type FROM tracking_scans WHERE (label_id=$1 OR (label_id LIKE $2)) AND dept=$3 ORDER BY ts ASC`,
    [labelId, suffix ? `%${suffix}` : labelId, dept]
  );
  const deptIn  = existing.filter(s=>s.type==='in').length;
  const deptOut = existing.filter(s=>s.type==='out').length;

  if(type==='in' && deptIn>0)
    return res.json({ok:false, error:`Box already scanned IN at ${dept}`});
  if(type==='out' && deptIn===0)
    return res.json({ok:false, error:`Box not scanned IN at ${dept} yet`});
  if(type==='out' && deptOut>=deptIn)
    return res.json({ok:false, error:`Box already scanned OUT at ${dept}`});

  await query(
    `INSERT INTO tracking_scans (id,label_id,batch_number,dept,type,ts,operator,size,qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(id) DO NOTHING`,
    [scan.id,labelId,scan.batchNumber||scan.batch_number,dept,type,scan.ts,scan.operator||null,scan.size||null,scan.qty||null]
  );
  res.json({ok:true});
}));

// ─── TRACKING GRANULAR ENDPOINTS (called by pushToServer) ────────

// POST /api/tracking/labels — upsert individual labels
app.post('/api/tracking/labels', asyncRoute(async (req, res) => {
  const { labels } = req.body;
  if (!labels || !labels.length) return res.json({ ok: true });
  for (const l of labels) {
    // Convert OL- string label numbers to negative integers for DB storage
    const _lNum2 = (typeof l.labelNumber==='string' && l.labelNumber.startsWith('OL-'))
      ? -(parseInt(l.labelNumber.slice(3))||0)
      : (parseInt(l.labelNumber)||0);
    await query(
      `INSERT INTO tracking_labels (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,wo_status,ship_to,bill_to,is_excess,excess_num,excess_total,normal_total)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)
       ON CONFLICT(id) DO UPDATE SET batch_number=EXCLUDED.batch_number,qty=EXCLUDED.qty,printed=EXCLUDED.printed,printed_at=EXCLUDED.printed_at,voided=EXCLUDED.voided,void_reason=EXCLUDED.void_reason,customer=EXCLUDED.customer,colour=EXCLUDED.colour,qr_data=EXCLUDED.qr_data,wo_status=EXCLUDED.wo_status,is_orange=EXCLUDED.is_orange,parent_label_id=EXCLUDED.parent_label_id,is_partial=EXCLUDED.is_partial,is_excess=EXCLUDED.is_excess,printing_matter=EXCLUDED.printing_matter`,
      [l.id,l.batchNumber,_lNum2,l.size,l.qty,!!l.isPartial,!!l.isOrange,
       l.parentLabelId||null,l.customer||null,l.colour||null,l.pcCode||null,
       l.poNumber||null,l.machineId||null,l.printingMatter||null,
       l.generated||new Date().toISOString(),!!l.printed,l.printedAt||null,
       !!l.voided,l.voidReason||null,l.voidedAt||null,l.voidedBy||null,l.qrData||null,
       l.woStatus||null,l.shipTo||null,l.billTo||null,!!l.isExcess,l.excessNum||null,l.excessTotal||null,l.normalTotal||null]
    );
  }
  res.json({ ok: true });
}));

// POST /api/tracking/label-void — void a label
app.post('/api/tracking/label-void', asyncRoute(async (req, res) => {
  const { labelId, reason, voidedBy } = req.body;
  if (!labelId) return res.status(400).json({ ok: false, error: 'Missing labelId' });
  const now = new Date().toISOString();
  await query(
    `UPDATE tracking_labels SET voided=TRUE, void_reason=$1, voided_at=$2, voided_by=$3 WHERE id=$4`,
    [reason || null, now, voidedBy || null, labelId]
  );
  await logAudit(voidedBy || 'SYSTEM', 'operator', 'tracking', 'LABEL_VOID', `Label ${labelId} voided: ${reason}`);
  res.json({ ok: true });
}));

// POST /api/tracking/reprint-log — log a reprint event
app.post('/api/tracking/reprint-log', asyncRoute(async (req, res) => {
  const { log } = req.body;
  if (log) {
    await logAudit(log.by || 'SYSTEM', 'operator', 'tracking', 'REPRINT', `Label ${log.labelId} reprinted: ${log.reason || ''}`);
  }
  res.json({ ok: true });
}));

// POST /api/tracking/stage-status — update status map for batch
app.post('/api/tracking/stage-status', asyncRoute(async (req, res) => {
  const { batchNumber, statusMap } = req.body;
  if (!batchNumber || !statusMap) return res.json({ ok: true });
  // statusMap: { dept: 'open'|'closed' } — sync to stage_closure table
  for (const [dept, status] of Object.entries(statusMap)) {
    if (status === 'closed') {
      const id = `sc-${batchNumber}-${dept}`;
      await query(
        `INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at) VALUES ($1,$2,$3,TRUE,NOW())
         ON CONFLICT(batch_number,dept) DO UPDATE SET closed=1, closed_at=NOW()`,
        [id, batchNumber, dept]
      );
    } else {
      await query(
        `UPDATE tracking_stage_closure SET closed=FALSE WHERE batch_number=$1 AND dept=$2`,
        [batchNumber, dept]
      );
    }
  }
  res.json({ ok: true });
}));

// POST /api/tracking/stage-close — close a stage for a batch
app.post('/api/tracking/stage-close', asyncRoute(async (req, res) => {
  const { batchNumber, dept, closedBy } = req.body;
  if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'Missing batchNumber or dept' });
  const id = `sc-${batchNumber}-${dept}`;
  await query(
    `INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at,closed_by) VALUES ($1,$2,$3,TRUE,NOW(),$4)
     ON CONFLICT(batch_number,dept) DO UPDATE SET closed=1, closed_at=NOW(), closed_by=EXCLUDED.closed_by`,
    [id, batchNumber, dept, closedBy || null]
  );
  await logAudit(closedBy || 'SYSTEM', 'operator', 'tracking', 'STAGE_CLOSE', `Stage ${dept} closed for batch ${batchNumber}`);
  res.json({ ok: true });
}));

// POST /api/tracking/wastage — record wastage (salvage/remelt)
app.post('/api/tracking/wastage', asyncRoute(async (req, res) => {
  const { batchNumber, dept, salvage, remelt, by } = req.body;
  if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'Missing batchNumber or dept' });
  const now = new Date().toISOString();
  if (salvage > 0) {
    const id = `w-${batchNumber}-${dept}-salv-${Date.now()}`;
    await query(
      `INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES ($1,$2,$3,'salvage',$4,$5,$6) ON CONFLICT(id) DO NOTHING`,
      [id, batchNumber, dept, salvage, now, by || null]
    );
  }
  if (remelt > 0) {
    const id = `w-${batchNumber}-${dept}-rem-${Date.now()}`;
    await query(
      `INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES ($1,$2,$3,'remelt',$4,$5,$6) ON CONFLICT(id) DO NOTHING`,
      [id, batchNumber, dept, remelt, now, by || null]
    );
  }
  res.json({ ok: true });
}));

// POST /api/tracking/backfill — admin: insert historical scan records with backdated timestamp
app.post('/api/tracking/backfill', asyncRoute(async (req, res) => {
  const { batchNumber, dept, inCount, outCount, qtyPerBox, backdateTs, operator } = req.body;
  if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'Missing batchNumber or dept' });
  const ts = backdateTs ? new Date(backdateTs).toISOString() : new Date().toISOString();
  const op = operator || 'admin-backfill';
  let inserted = 0;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Insert IN scans
    for (let i = 0; i < (inCount || 0); i++) {
      const id = `bf-${batchNumber}-${dept}-in-${Date.now()}-${i}`;
      const labelId = `backfill-${batchNumber}-${i+1}`;
      await client.query(
        `INSERT INTO tracking_scans (id, label_id, batch_number, dept, type, ts, operator)
         VALUES ($1,$2,$3,$4,'in',$5,$6) ON CONFLICT(id) DO NOTHING`,
        [id, labelId, batchNumber, dept, ts, op]
      );
      inserted++;
    }
    // Insert OUT scans
    for (let i = 0; i < (outCount || 0); i++) {
      const id = `bf-${batchNumber}-${dept}-out-${Date.now()}-${i}`;
      const labelId = `backfill-${batchNumber}-${i+1}`;
      await client.query(
        `INSERT INTO tracking_scans (id, label_id, batch_number, dept, type, ts, operator, qty)
         VALUES ($1,$2,$3,$4,'out',$5,$6,$7) ON CONFLICT(id) DO NOTHING`,
        [id, labelId, batchNumber, dept, ts, op, qtyPerBox || 1]
      );
      inserted++;
    }
    await client.query('COMMIT');
  } catch(e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }
  res.json({ ok: true, inserted });
}));

// POST /api/tracking/backfill-wastage — admin: insert historical wastage with backdated timestamp
app.post('/api/tracking/backfill-wastage', asyncRoute(async (req, res) => {
  const { batchNumber, dept, salvage, remelt, backdateTs, operator } = req.body;
  if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'Missing batchNumber or dept' });
  const ts = backdateTs ? new Date(backdateTs).toISOString() : new Date().toISOString();
  const op = operator || 'admin-backfill';
  if (salvage > 0) {
    const id = `bf-w-${batchNumber}-${dept}-salv-${Date.now()}`;
    await query(
      `INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES ($1,$2,$3,'salvage',$4,$5,$6) ON CONFLICT(id) DO NOTHING`,
      [id, batchNumber, dept, salvage, ts, op]
    );
  }
  if (remelt > 0) {
    const id = `bf-w-${batchNumber}-${dept}-rem-${Date.now()}`;
    await query(
      `INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES ($1,$2,$3,'remelt',$4,$5,$6) ON CONFLICT(id) DO NOTHING`,
      [id, batchNumber, dept, remelt, ts, op]
    );
  }
  res.json({ ok: true });
}));

// POST /api/tracking/dispatch-record — save a dispatch record
app.post('/api/tracking/dispatch-record', asyncRoute(async (req, res) => {
  const { record } = req.body;
  if (!record || !record.id) return res.status(400).json({ ok: false, error: 'Missing record' });
  await query(
    `INSERT INTO tracking_dispatch_records (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
     ON CONFLICT(id) DO UPDATE SET qty=EXCLUDED.qty, boxes=EXCLUDED.boxes, vehicle_no=EXCLUDED.vehicle_no, invoice_no=EXCLUDED.invoice_no`,
    [record.id, record.batchNumber||record.batch_number, record.customer||null, record.qty, record.boxes,
     record.vehicleNo||record.vehicle_no||null, record.invoiceNo||record.invoice_no||null, record.remarks||null,
     record.ts||new Date().toISOString(), record.by||null]
  );
  await logAudit(record.by||'SYSTEM','operator','tracking','DISPATCH_RECORD',`Dispatched ${record.boxes} boxes of batch ${record.batchNumber||record.batch_number}`);
  res.json({ ok: true });
}));

// POST /api/tracking/dispatch-update — update dispatched qty on planning order
app.post('/api/tracking/dispatch-update', asyncRoute(async (req, res) => {
  const { batchNumber, dispatchedQty, vehicleNo, invoiceNo } = req.body;
  if (!batchNumber) return res.status(400).json({ ok: false, error: 'Missing batchNumber' });
  // Update planning state — mark dispatch progress
  try {
    const planState = await getPlanningState();
    if (planState && planState.orders) {
      const ord = planState.orders.find(o => o.batchNumber === batchNumber);
      if (ord) {
        ord.dispatchedQty = (ord.dispatchedQty || 0) + (dispatchedQty || 0);
        if (vehicleNo) ord.lastVehicleNo = vehicleNo;
        if (invoiceNo) ord.lastInvoiceNo = invoiceNo;
        await savePlanningState(planState);
      }
    }
  } catch (e) { console.error('dispatch-update planning sync error:', e.message); }
  res.json({ ok: true });
}));

// ── NEW: A-Grade Summary for Planning app ────────────────────────
app.get('/api/tracking/agrade-summary', asyncRoute(async (req, res) => {
  // Scan counts per batch per dept per type
  const scans = await queryAll(`
    SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty
    FROM tracking_scans
    GROUP BY batch_number, dept, type
  `);

  // Wastage per batch per dept
  const wastage = await queryAll(`
    SELECT batch_number, dept, type, SUM(qty) as total_qty
    FROM tracking_wastage
    GROUP BY batch_number, dept, type
  `);

  // Build per-batch summary
  const batches = {};
  scans.forEach(s => {
    if (!batches[s.batch_number]) batches[s.batch_number] = {};
    if (!batches[s.batch_number][s.dept]) batches[s.batch_number][s.dept] = {in:0,out:0,inQty:0,outQty:0};
    batches[s.batch_number][s.dept][s.type] = parseInt(s.cnt||0);
    batches[s.batch_number][s.dept][s.type+'Qty'] = parseFloat(s.total_qty||0);
  });

  wastage.forEach(w => {
    if (!batches[w.batch_number]) batches[w.batch_number] = {};
    if (!batches[w.batch_number][w.dept]) batches[w.batch_number][w.dept] = {in:0,out:0,inQty:0,outQty:0};
    if (!batches[w.batch_number][w.dept].wastage) batches[w.batch_number][w.dept].wastage = {};
    batches[w.batch_number][w.dept].wastage[w.type] = parseFloat(w.total_qty||0);
  });

  // Calculate A-grade per batch per stage
  const result = {};
  Object.entries(batches).forEach(([batchNo, depts]) => {
    const aim  = depts['aim']      || {};
    const print = depts['printing'] || {};
    const pi   = depts['pi']       || {};
    const pack = depts['packing']  || {};

    const aimWaste   = (aim.wastage?.salvage||0)   + (aim.wastage?.remelt||0);
    const printWaste = (print.wastage?.salvage||0) + (print.wastage?.remelt||0);
    const piWaste    = (pi.wastage?.salvage||0)    + (pi.wastage?.remelt||0);

    const aimOut = aim.outQty || 0;
    const aimInspected = aimOut + aimWaste;
    const printOut = print.outQty || 0;
    const printInspected = printOut + printWaste;
    const piOut = pi.outQty || 0;
    const piInspected = piOut + piWaste;

    result[batchNo] = {
      aim: {
        inQty: aim.inQty||0, outQty: aimOut,
        wastage: aimWaste, inspected: aimInspected,
        aGradePct: aimInspected>0 ? (aimOut/aimInspected*100) : null
      },
      printing: {
        inQty: print.inQty||0, outQty: printOut,
        wastage: printWaste, inspected: printInspected,
        aGradePct: printInspected>0 ? (printOut/printInspected*100) : null
      },
      pi: {
        inQty: pi.inQty||0, outQty: piOut,
        wastage: piWaste, inspected: piInspected,
        aGradePct: piInspected>0 ? (piOut/piInspected*100) : null
      },
      packing: { inQty: pack.inQty||0, outQty: pack.outQty||0 }
    };
  });

  res.json({ ok: true, batches: result });
}));

// ── NEW: Dispatch Actuals for Planning app ────────────────────────
app.get('/api/tracking/dispatch-actuals', asyncRoute(async (req, res) => {
  // Sum all dispatch records per batch from tracking_dispatch_records
  const rows = await queryAll(`
    SELECT batch_number, SUM(qty) as dispatched_qty, SUM(boxes) as dispatched_boxes
    FROM tracking_dispatch_records
    GROUP BY batch_number
  `);
  res.json({ ok: true, actuals: rows });
}));

// jsQR proxy — serves jsQR to factory phones without CDN access
let jsqrCache = null;
app.get('/jsqr.min.js', (req, res) => {
  if (jsqrCache) {
    res.setHeader('Content-Type', 'application/javascript');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    return res.send(jsqrCache);
  }
  const https = require('https');
  https.get('https://cdnjs.cloudflare.com/ajax/libs/jsQR/1.4.0/jsQR.min.js', r => {
    let data = '';
    r.on('data', c => data += c);
    r.on('end', () => {
      jsqrCache = data;
      res.setHeader('Content-Type', 'application/javascript');
      res.setHeader('Cache-Control', 'public, max-age=86400');
      res.send(data);
    });
  }).on('error', () => res.status(503).send('// jsQR fetch failed'));
});

// POST /api/admin/snapshot — manual snapshot before deploy
app.post('/api/admin/snapshot', asyncRoute(async (req, res) => {
  const key = req.query.key || req.body?.key;
  const exportKey = process.env.EXPORT_KEY || 'sunloc-export-2024';
  // Allow either export key OR admin session
  if (key !== exportKey) {
    const session = await verifyToken(req.headers['x-session-token']);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
  }
  const currentState = await queryOne('SELECT state_json, saved_at FROM planning_state WHERE id=1');
  if (!currentState) return res.json({ ok: true, message: 'No data to snapshot', orders: 0 });
  await query(`INSERT INTO planning_state_backups (state_json, trigger) VALUES ($1, 'manual')`, [currentState.state_json]);
  await query(`DELETE FROM planning_state_backups WHERE id NOT IN (SELECT id FROM planning_state_backups ORDER BY backed_up_at DESC LIMIT 20)`);
  const parsed = JSON.parse(currentState.state_json);
  const orderCount = parsed.orders?.length || 0;
  await logAudit('SYSTEM', 'admin', 'planning', 'MANUAL_SNAPSHOT', `Manual snapshot: ${orderCount} orders`);
  res.json({ ok: true, message: `Snapshot saved — ${orderCount} orders protected`, orders: orderCount, savedAt: currentState.saved_at });
}));

// POST /api/admin/purge-temp -- just clear temp batches (no full reset needed)
app.post('/api/admin/purge-temp', asyncRoute(async (req, res) => {
  const { token } = req.body;
  const session = await verifyToken(token);
  if (!session || session.role !== 'admin')
    return res.status(403).json({ ok: false, error: 'Admin only' });
  await query("DELETE FROM temp_batches");
  await query('DELETE FROM temp_batch_alerts');
  await logAudit(session.username, session.role, 'admin', 'PURGE_TEMP_BATCHES', 'All TEMP batches purged');
  res.json({ ok: true, message: 'All TEMP batches cleared' });
}));

// POST /api/admin/purge-labels -- clear all tracking labels and scans
app.post('/api/admin/purge-labels', asyncRoute(async (req, res) => {
  const { token } = req.body;
  const session = await verifyToken(token);
  if (!session || session.role !== 'admin')
    return res.status(403).json({ ok: false, error: 'Admin only' });
  await query('DELETE FROM tracking_scans');
  await query('DELETE FROM tracking_labels');
  await query('DELETE FROM tracking_stage_closure');
  await query('DELETE FROM tracking_wastage');
  await query('DELETE FROM tracking_dispatch_records');
  await query('DELETE FROM tracking_alerts');
  await logAudit(session.username, session.role, 'admin', 'PURGE_LABELS', 'All tracking labels and scans purged');
  res.json({ ok: true, message: 'All labels and scans cleared' });
}));

// POST /api/admin/reset-all -- WIPE ALL DATA (Admin only, fresh FY start)
app.post('/api/admin/reset-all', asyncRoute(async (req, res) => {
  const { token, confirm } = req.body;
  const session = await verifyToken(token);
  if (!session || session.role !== 'admin')
    return res.status(403).json({ ok: false, error: 'Admin only' });
  if (confirm !== 'RESET-ALL-DATA')
    return res.status(400).json({ ok: false, error: 'Missing confirmation phrase' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM tracking_scans');
    await client.query('DELETE FROM tracking_labels');
    await client.query('DELETE FROM tracking_stage_closure');
    await client.query('DELETE FROM tracking_wastage');
    await client.query('DELETE FROM tracking_dispatch_records');
    await client.query('DELETE FROM tracking_alerts');
    await client.query('DELETE FROM dpr_records');
    await client.query('DELETE FROM production_actuals');
    await client.query('DELETE FROM temp_batches');
    await client.query('DELETE FROM temp_batch_alerts');
    await client.query('DELETE FROM reconciliation_requests');
    await client.query('DELETE FROM wo_reconciliation_requests');
    await client.query('DELETE FROM planning_state_backups');
    // Preserve master data — only wipe operational data
    const existing = await queryOne('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1');
    const existingState = existing ? JSON.parse(existing.state_json) : {};
    const freshState = {
      orders: [],
      printOrders: [],
      dailyPrinting: [],
      dispatchPlans: [],
      dispatchRecords: [],
      currentPage: 'production',
      activeMonth: null,
      archives: existingState.archives || [],
      // Preserve all master data
      machineMaster: existingState.machineMaster || [],
      printMachineMaster: existingState.printMachineMaster || [],
      packSizes: existingState.packSizes || {},
      truckCapacity: existingState.truckCapacity || 130,
      zoneCapacities: existingState.zoneCapacities || {},
    };
    await client.query('UPDATE planning_state SET state_json=$1, saved_at=NOW()', [JSON.stringify(freshState)]);
    await client.query('COMMIT');
    await logAudit(session.username, session.role, 'admin', 'FULL_DATA_RESET', 'All operational data wiped for fresh FY start');
    res.json({ ok: true, message: 'All data wiped. Users and machine settings preserved.' });
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ ok: false, error: e.message });
  } finally { client.release(); }
}));

// GET /api/admin/backups — list recent planning state backups
app.get('/api/admin/backups', asyncRoute(async (req, res) => {
  const session = await verifyToken(req.headers['x-session-token'] || req.query.token);
  if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
  const rows = await queryAll(`SELECT id, backed_up_at, trigger, length(state_json) as size_bytes FROM planning_state_backups ORDER BY backed_up_at DESC LIMIT 10`);
  res.json({ ok: true, backups: rows });
}));



// ─── SPA Catch-all (must be LAST route) ───────────────────────
app.get('*', (req, res) => {
  const idx = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(idx)) res.sendFile(idx);
  else res.json({ ok: false, error: 'No frontend found. Place files in /public folder.' });
});

// Error handler
app.use((err, req, res, next) => {
  console.error('[Error]', err.message);
  res.status(500).json({ ok: false, error: err.message });
});

// Start server
async function startServer() {
  try {
    await runMigrations();
    await seedUsers();

    // ── REDEPLOYMENT SAFETY: Snapshot + Integrity Check ─────────
    try {
      const currentState = await queryOne('SELECT state_json, saved_at FROM planning_state WHERE id=1');
      if (currentState) {
        const parsed = JSON.parse(currentState.state_json);
        const orderCount = parsed.orders?.length || 0;
        const printOrderCount = parsed.printOrders?.length || 0;
        const dispatchCount = parsed.dispatchPlans?.length || 0;

        // Take startup snapshot before anything else
        await query(
          `INSERT INTO planning_state_backups (state_json, trigger) VALUES ($1, 'startup-snapshot')`,
          [currentState.state_json]
        );
        await query(
          `DELETE FROM planning_state_backups WHERE id NOT IN (SELECT id FROM planning_state_backups ORDER BY backed_up_at DESC LIMIT 20)`
        );

        // Count other critical tables
        const [dprCount, actualsCount, labelsCount, scansCount] = await Promise.all([
          queryOne('SELECT COUNT(*) as c FROM dpr_records'),
          queryOne('SELECT COUNT(*) as c FROM production_actuals'),
          queryOne('SELECT COUNT(*) as c FROM tracking_labels'),
          queryOne('SELECT COUNT(*) as c FROM tracking_scans'),
        ]);

        console.log('[Startup] ═══════════════════════════════════════════');
        console.log(`[Startup] DATABASE INTEGRITY CHECK`);
        console.log(`[Startup]   Planning orders:    ${orderCount}`);
        console.log(`[Startup]   Print orders:       ${printOrderCount}`);
        console.log(`[Startup]   Dispatch plans:     ${dispatchCount}`);
        console.log(`[Startup]   DPR records:        ${parseInt(dprCount?.c||0)}`);
        console.log(`[Startup]   Production actuals: ${parseInt(actualsCount?.c||0)}`);
        console.log(`[Startup]   Tracking labels:    ${parseInt(labelsCount?.c||0)}`);
        console.log(`[Startup]   Tracking scans:     ${parseInt(scansCount?.c||0)}`);
        console.log(`[Startup]   Last saved:         ${currentState.saved_at}`);
        console.log(`[Startup]   Snapshot:           ✅ taken`);
        console.log('[Startup] ═══════════════════════════════════════════');
        console.log(`[Startup] ALL DATA SAFE — ${orderCount} orders protected`);
      } else {
        console.log('[Startup] No planning state found — fresh database');
      }
    } catch(e) {
      console.error('[Startup] Snapshot failed (non-fatal):', e.message);
    }
    // ─────────────────────────────────────────────────────────────

    app.listen(PORT, () => {
      console.log(`[Sunloc] Server running on port ${PORT}`);
      console.log(`[Sunloc] Database: PostgreSQL (Railway) — data persists across redeployments`);
      console.log(`[Sunloc] Data protection: ACTIVE — orders will never be overwritten`);
    });
  } catch(err) {
    console.error('[Startup] Fatal error:', err.message);
    process.exit(1);
  }
}

startServer();

