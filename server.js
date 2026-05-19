/**
 * SUNLOC INTEGRATED SERVER v2
 * Stack: PostgreSQL (production) or SQLite (local dev)
 * Set DATABASE_URL env var to use PostgreSQL automatically.
 * All 107 db.prepare().get/run/all() calls work unchanged via the adapter.
 */

'use strict';

const express = require('express');
const cors    = require('cors');
const path    = require('path');
const fs      = require('fs');
const crypto  = require('crypto');

// v39: SAP B1 Service Layer client — handles login, session, audit, graceful degradation
const { SapClient } = require('./sap-client');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── Database — Dual Mode ──────────────────────────────────────
// When DATABASE_URL is set: uses PostgreSQL via db-pg-sync.js adapter
// (same synchronous db.prepare().get/run/all() API as better-sqlite3)
// When not set: uses better-sqlite3 directly (local dev / Railway SQLite)

let db;
const USE_POSTGRES = !!process.env.DATABASE_URL;

// ── Legacy order cutoff: orders with startDate on or before this date
//    are treated as legacy (already in plant) and exempt from 2-order limit
const LEGACY_CUTOFF = '2026-04-19';

let DB_PATH = 'postgres'; // overwritten below in SQLite mode; must be top-level so health endpoint never throws ReferenceError

if (USE_POSTGRES) {
  const { PgDatabase } = require('./db-pg-sync');
  db = new PgDatabase();
  console.log('[DB] Mode: PostgreSQL');
}

// Direct async pg pool for large queries
let pgPool = null;
if (USE_POSTGRES) {
  try {
    const { Pool } = require('pg');
    pgPool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 });
    console.log('[DB] Direct pg pool ready');
  } catch(e) { console.error('[DB] pg pool error:', e.message); }
} else {
  const Database = require('better-sqlite3');

  function resolveDbPath() {
    if (process.env.DB_PATH) return process.env.DB_PATH;
    const volumePath = '/data/sunloc.db';
    try {
      const dir = '/data';
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.accessSync(dir, fs.constants.W_OK);
      console.log('[DB] Using persistent volume: ' + volumePath);
      return volumePath;
    } catch (e) {
      const localPath = path.join(__dirname, 'sunloc.db');
      console.log('[DB] Volume not available, using local: ' + localPath);
      return localPath;
    }
  }

  DB_PATH = resolveDbPath();
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  console.log('[DB] Mode: SQLite at ' + DB_PATH);
}

// ─── v39: SAP Client singleton ─────────────────────────────────
// One SapClient instance per process, shared across all endpoints.
// In PG mode it uses pgPool for the async DB calls; in SQLite mode
// it uses the same `db` better-sqlite3 instance. SAP_ENCRYPT_KEY env
// var must be set on Railway before configuring SAP credentials.
const sap = new SapClient({ pgPool, db: USE_POSTGRES ? null : db, log: console });
console.log('[SAP] Client initialised — set credentials via /api/sap/config to enable');

// ─── Migration System ──────────────────────────────────────────
// SQLite migration SQL — the PgDatabase.exec() translates to Postgres automatically.

db.exec(`
  CREATE TABLE IF NOT EXISTS schema_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL UNIQUE,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS planning_kv (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
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
  {
    version: 4,
    name: 'temp_batch_system',
    sql: `
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
      CREATE TABLE IF NOT EXISTS pc_codes (
        id SERIAL PRIMARY KEY,
        size TEXT NOT NULL,
        code TEXT NOT NULL,
        colour TEXT NOT NULL,
        pack_size INTEGER DEFAULT 100000,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(size, code)
      );
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
  },
  {
    version: 6,
    name: 'tracking_labels_extended_fields',
    sql: `
      ALTER TABLE tracking_labels ADD COLUMN ship_to TEXT;
      ALTER TABLE tracking_labels ADD COLUMN bill_to TEXT;
      ALTER TABLE tracking_labels ADD COLUMN is_excess INTEGER DEFAULT 0;
      ALTER TABLE tracking_labels ADD COLUMN excess_num INTEGER;
      ALTER TABLE tracking_labels ADD COLUMN excess_total INTEGER;
      ALTER TABLE tracking_labels ADD COLUMN normal_total INTEGER;
    `
  },
  {
    version: 7,
    name: 'dispatch_actuals',
    sql: `
      CREATE TABLE IF NOT EXISTS tracking_dispatch_actuals (
        batch_number TEXT PRIMARY KEY,
        dispatched_qty REAL DEFAULT 0,
        vehicle_no TEXT,
        invoice_no TEXT,
        updated_at TEXT
      );
    `
  },
  {
    version: 8,
    name: 'dpr_batch_closed',
    sql: `
      CREATE TABLE IF NOT EXISTS dpr_batch_closed (
        order_id TEXT PRIMARY KEY,
        batch_number TEXT,
        closed_at TEXT NOT NULL DEFAULT (datetime('now')),
        closed_by TEXT,
        notes TEXT
      );
    `
  },
  {
    version: 9,
    name: 'dpr_settings',
    sql: `
      CREATE TABLE IF NOT EXISTS dpr_settings (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
    `
  },
  {
    version: 10,
    name: 'tracking_scans_label_number',
    sql: `ALTER TABLE tracking_scans ADD COLUMN label_number INTEGER;`
  },
  {
    version: 11,
    name: 'month_archives',
    sql: `CREATE TABLE IF NOT EXISTS month_archives (
      id SERIAL PRIMARY KEY,
      month TEXT NOT NULL UNIQUE,
      archived_at TIMESTAMPTZ DEFAULT NOW(),
      archived_by TEXT,
      snapshot_json JSONB,
      is_auto BOOLEAN DEFAULT TRUE
    );
    CREATE INDEX IF NOT EXISTS idx_month_archives_month ON month_archives(month);`
  },
  {
    // v37I: Migration #12 — dispatch reconciliation alerts
    // Two flow types:
    //   'A' = Flow A: packing-out scan without matching dispatch-in within threshold
    //   'B' = Flow B: dispatch-out scan without manual dispatch record covering it
    // Acknowledge fields support 4-hour expiry so alerts resurface if still unresolved.
    version: 12,
    name: 'dispatch_reconcile_alerts',
    sql: `CREATE TABLE IF NOT EXISTS dispatch_reconcile_alerts (
      id TEXT PRIMARY KEY,
      batch_number TEXT NOT NULL,
      label_id TEXT,
      alert_type TEXT NOT NULL CHECK(alert_type IN ('A','B')),
      triggered_at TEXT NOT NULL DEFAULT (datetime('now')),
      acknowledged_at TEXT,
      acknowledged_by TEXT,
      ack_reason TEXT,
      ack_expires_at TEXT,
      resolved_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_dra_batch ON dispatch_reconcile_alerts(batch_number);
    CREATE INDEX IF NOT EXISTS idx_dra_type_resolved ON dispatch_reconcile_alerts(alert_type, resolved_at);
    CREATE INDEX IF NOT EXISTS idx_dra_label ON dispatch_reconcile_alerts(label_id);
    CREATE TABLE IF NOT EXISTS system_settings (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_by TEXT
    );`
  },
  {
    // v37J Sub-issue 1.1: Migration #13 — customer master.
    // Stores distinct customer names from Ship-to / Bill-to fields so they appear
    // in the autocomplete <datalist> when planners enter new orders. Auto-populated
    // on order save (when a new customer name is encountered) plus seeded from
    // existing production_orders.customer values on first deploy.
    version: 13,
    name: 'customer_master',
    sql: `CREATE TABLE IF NOT EXISTS customer_master (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE COLLATE NOCASE,
      added_at TEXT NOT NULL DEFAULT (datetime('now')),
      added_by TEXT,
      use_count INTEGER NOT NULL DEFAULT 1,
      last_used_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_customer_master_name ON customer_master(name);`
  },
  {
    // v39 Phase 2: invoice_requests — Sunloc-initiated invoice triggers sent to SAP.
    // Each row represents one invoice we asked SAP to generate. Status moves:
    // pending → sent_to_sap → invoice_received (matched back via invoices_received)
    //                       → failed (SAP rejected — admin investigates)
    version: 14,
    name: 'invoice_requests',
    sql: `CREATE TABLE IF NOT EXISTS invoice_requests (
      id TEXT PRIMARY KEY,
      batch_number TEXT,
      customer TEXT,
      card_code TEXT,
      po_number TEXT,
      sap_doc_entry INTEGER,
      size TEXT,
      colour TEXT,
      pc_code TEXT,
      boxes INTEGER,
      qty_lakhs REAL,
      rate_per_lakh REAL,
      selected_labels TEXT,
      selection_mode TEXT,
      truck_number INTEGER,
      status TEXT NOT NULL DEFAULT 'pending',
      sap_response_doc_num TEXT,
      sap_response_doc_entry INTEGER,
      sap_response_irn TEXT,
      sap_error_message TEXT,
      is_admin_override INTEGER NOT NULL DEFAULT 0,
      override_reason TEXT,
      override_by TEXT,
      created_by TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_inv_req_batch ON invoice_requests(batch_number);
    CREATE INDEX IF NOT EXISTS idx_inv_req_status ON invoice_requests(status);
    CREATE INDEX IF NOT EXISTS idx_inv_req_sap_entry ON invoice_requests(sap_doc_entry);`
  },
  {
    // v39 Phase 2: invoices_received — SAP-generated invoices pulled by Sunloc poller.
    // System of record for the dispatch flow: every scan-out must reference a row here.
    // Source: 'sunloc' = matched our invoice_request; 'direct_sap' = SAP created without Sunloc trigger.
    version: 15,
    name: 'invoices_received',
    sql: `CREATE TABLE IF NOT EXISTS invoices_received (
      id TEXT PRIMARY KEY,
      sap_doc_entry INTEGER UNIQUE,
      sap_doc_num TEXT,
      sap_invoice_no TEXT,
      invoice_date TEXT,
      customer TEXT,
      card_code TEXT,
      po_number TEXT,
      batch_number TEXT,
      pc_code TEXT,
      size TEXT,
      colour TEXT,
      total_boxes INTEGER,
      total_qty_lakhs REAL,
      taxable_amount REAL,
      igst_amount REAL,
      total_amount REAL,
      irn TEXT,
      ack_no TEXT,
      ack_date TEXT,
      source TEXT NOT NULL DEFAULT 'sunloc',
      invoice_request_id TEXT,
      dispatch_status TEXT NOT NULL DEFAULT 'pending',
      scanned_boxes INTEGER NOT NULL DEFAULT 0,
      dispatched_at TEXT,
      dispatched_by TEXT,
      vehicle_no TEXT,
      lr_no TEXT,
      remarks TEXT,
      is_deemed_scan_out INTEGER NOT NULL DEFAULT 0,
      deemed_reason TEXT,
      deemed_by TEXT,
      admin_approved_at TEXT,
      admin_approved_by TEXT,
      fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
      payload_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_inv_recv_batch ON invoices_received(batch_number);
    CREATE INDEX IF NOT EXISTS idx_inv_recv_customer ON invoices_received(customer);
    CREATE INDEX IF NOT EXISTS idx_inv_recv_status ON invoices_received(dispatch_status);
    CREATE INDEX IF NOT EXISTS idx_inv_recv_date ON invoices_received(invoice_date);
    CREATE INDEX IF NOT EXISTS idx_inv_recv_source ON invoices_received(source);
    CREATE INDEX IF NOT EXISTS idx_inv_recv_req ON invoices_received(invoice_request_id);`
  },
  {
    // v39 Phase 2: sap_config — single-row table holding SAP Service Layer credentials.
    // Password stored encrypted using SAP_ENCRYPT_KEY env var. Session cookie cached
    // to avoid re-login on every call (SAP sessions last 30 min idle).
    version: 16,
    name: 'sap_config',
    sql: `CREATE TABLE IF NOT EXISTS sap_config (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      sap_url TEXT,
      sap_username TEXT,
      sap_company_db TEXT,
      sap_password_encrypted TEXT,
      session_cookie TEXT,
      session_route_id TEXT,
      session_expires_at TEXT,
      last_login_at TEXT,
      last_login_success INTEGER,
      last_login_error TEXT,
      indent_poll_interval_minutes INTEGER NOT NULL DEFAULT 5,
      invoice_poll_interval_minutes INTEGER NOT NULL DEFAULT 5,
      indent_poll_lookback_days INTEGER NOT NULL DEFAULT 30,
      invoice_poll_lookback_days INTEGER NOT NULL DEFAULT 7,
      last_indent_poll_at TEXT,
      last_invoice_poll_at TEXT,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_by TEXT
    );
    INSERT OR IGNORE INTO sap_config (id, sap_url, sap_username, sap_company_db) VALUES (1, '', '', '');`
  },
  {
    // v39 Phase 2: sap_audit_log — forensic log of every SAP Service Layer call.
    // Rolling 5000 rows (oldest pruned by background job). Critical for diagnosing
    // intermittent failures, session timeouts, and SAP-side rejections.
    version: 17,
    name: 'sap_audit_log',
    sql: `CREATE TABLE IF NOT EXISTS sap_audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      called_at TEXT NOT NULL DEFAULT (datetime('now')),
      method TEXT,
      endpoint TEXT,
      status_code INTEGER,
      duration_ms INTEGER,
      success INTEGER NOT NULL DEFAULT 0,
      error_message TEXT,
      request_summary TEXT,
      response_summary TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_sap_audit_called ON sap_audit_log(called_at);
    CREATE INDEX IF NOT EXISTS idx_sap_audit_success ON sap_audit_log(success);`
  },
  {
    // v39 Phase 2: sap_indent_cache — last-fetched open Sales Orders from SAP.
    // Source for the Unplanned Orders page in Planning App. Refreshed every N min by
    // the indent poller. processed_at is set when planner assigns the indent to a
    // machine (creating a production order in Sunloc).
    version: 18,
    name: 'sap_indent_cache',
    sql: `CREATE TABLE IF NOT EXISTS sap_indent_cache (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sap_doc_entry INTEGER UNIQUE NOT NULL,
      sap_doc_num TEXT,
      card_code TEXT,
      card_name TEXT,
      doc_date TEXT,
      doc_due_date TEXT,
      total_lines INTEGER,
      total_qty REAL,
      payload_json TEXT NOT NULL,
      fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
      processed_at TEXT,
      processed_by TEXT,
      processed_order_id TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_sap_indent_doc_entry ON sap_indent_cache(sap_doc_entry);
    CREATE INDEX IF NOT EXISTS idx_sap_indent_processed ON sap_indent_cache(processed_at);
    CREATE INDEX IF NOT EXISTS idx_sap_indent_customer ON sap_indent_cache(card_code);`
  },
  {
    // v39 Phase 9c: Extend wo_reconciliation_requests with optional SAP refs
    // so a W/O reconciliation proposal can carry SAP DocEntry/DocNum forward.
    // Admin approval applies them to the production order. NULL by default.
    // SQLite lacks ALTER...IF NOT EXISTS, but schema_migrations.applied_at
    // tracking ensures each migration runs at most once.
    version: 19,
    name: 'wo_recon_add_sap_refs',
    sql: `ALTER TABLE wo_reconciliation_requests ADD COLUMN sap_doc_entry INTEGER;
    ALTER TABLE wo_reconciliation_requests ADD COLUMN sap_doc_num TEXT;`
  },
  {
    // v39 Phase 10a: Link invoices_received back to its tracking_dispatch_records
    // entry once dispatch-out completes. Enables clients to find the dispatch
    // record from the invoice id without joining on batch_number alone.
    version: 20,
    name: 'invoices_recv_add_dispatch_rec_id',
    sql: `ALTER TABLE invoices_received ADD COLUMN dispatch_record_id TEXT;`
  },
  {
    // v40 Phase 18.11: Track in-progress truck-level scan-out sessions so workers
    // can resume after modal close, browser crash, or being called away.
    // Session keyed by truck_number (one session per truck at a time).
    // Sessions auto-expire after 24h of no activity (cleaned by background sweep).
    version: 21,
    name: 'truck_scan_session_state',
    sql: `CREATE TABLE IF NOT EXISTS truck_scan_session_state (
      truck_number TEXT PRIMARY KEY,
      invoice_ids_json TEXT NOT NULL,
      scanned_labels_json TEXT NOT NULL DEFAULT '[]',
      vehicle_no TEXT DEFAULT '',
      lr_no TEXT DEFAULT '',
      remarks TEXT DEFAULT '',
      started_by TEXT,
      started_at TEXT DEFAULT (datetime('now')),
      last_updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_truck_scan_last_updated ON truck_scan_session_state(last_updated_at);`
  },
];

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
function hashPin(pin) { return crypto.createHash('sha256').update(pin + 'sunloc_salt').digest('hex'); }

const seedUsers = [
  { username: 'GF',                pin: '1111', role: 'gf',               app: 'dpr'      },
  { username: 'FF',                pin: '2222', role: 'ff',               app: 'dpr'      },
  { username: 'DPR_Admin',         pin: '9999', role: 'admin',            app: 'dpr'      },
  { username: 'Planning_Manager',  pin: '3333', role: 'planning_manager', app: 'planning' },
  { username: 'Printing_Manager',  pin: '4444', role: 'printing_manager', app: 'planning' },
  { username: 'Dispatch_Manager',  pin: '5555', role: 'dispatch_manager', app: 'planning' },
  { username: 'Plan_Admin',        pin: '9999', role: 'admin',            app: 'planning' },
  { username: 'Track_Admin',       pin: '9999', role: 'admin',            app: 'tracking' },
];

const insertUser = db.prepare(`
  INSERT INTO app_users (username, pin_hash, role, app)
  VALUES (?, ?, ?, ?)
  ON CONFLICT (username) DO NOTHING
`);
for (const u of seedUsers) {
  insertUser.run(u.username, hashPin(u.pin), u.role, u.app);
}

// Clean expired sessions on startup (SQLite only — PostgreSQL handles via pgPool)
if (!USE_POSTGRES) {
  try { db.prepare(`DELETE FROM app_sessions WHERE expires_at < datetime('now')`).run(); } catch(e) {}
}


// ─── Helper: get latest planning state ────────────────────────
let _planningStateCache = null;
let _planningStateCacheTime = 0;

async function getPlanningStateAsync() {
  if (pgPool) {
    const r = await pgPool.query('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1');
    if (!r.rows[0]) return { orders: [], printOrders: [], dispatchPlans: [], dailyPrinting: [], machineMaster: [], printMachineMaster: [], packSizes: {} };
    try { return JSON.parse(r.rows[0].state_json); } catch { return {}; }
  }
  return getPlanningState();
}

function getPlanningState() {
  // Return cache if fresh
  if (_planningStateCache && _planningStateCache.orders && _planningStateCache.orders.length > 0 && Date.now() - _planningStateCacheTime < 30000) return _planningStateCache;
  // Try pgPool first (PostgreSQL)
  if (pgPool) {
    // Return cache while async fetch happens — warmPlanningCache() keeps this updated
    if (_planningStateCache) return _planningStateCache;
    return { orders: [], printOrders: [], dispatchPlans: [], dailyPrinting: [], machineMaster: [], printMachineMaster: [], packSizes: {} };
  }
  // SQLite fallback
  const row = db.prepare('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1').get();
  if (!row) return { orders: [], printOrders: [], dispatchPlans: [], dailyPrinting: [], machineMaster: [], printMachineMaster: [], packSizes: {} };
  try {
    _planningStateCache = JSON.parse(row.state_json);
    _planningStateCacheTime = Date.now();
    return _planningStateCache;
  } catch { return {}; }
}

async function ensurePostgresTables() {
  if (!pgPool) return;
  try {
    // Generic key-value store for planning data (dispatch plans, pack sizes, settings etc.)
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS planning_kv (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_labels (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        label_number INTEGER,
        size TEXT,
        qty REAL,
        is_partial INTEGER DEFAULT 0,
        is_orange INTEGER DEFAULT 0,
        parent_label_id TEXT,
        customer TEXT,
        colour TEXT,
        pc_code TEXT,
        po_number TEXT,
        machine_id TEXT,
        printing_matter TEXT,
        generated TEXT NOT NULL DEFAULT NOW()::TEXT,
        printed INTEGER DEFAULT 0,
        printed_at TEXT,
        voided INTEGER DEFAULT 0,
        void_reason TEXT,
        voided_at TEXT,
        voided_by TEXT,
        qr_data TEXT,
        wo_status TEXT,
        ship_to TEXT,
        bill_to TEXT,
        is_excess INTEGER DEFAULT 0,
        excess_num INTEGER,
        excess_total INTEGER,
        normal_total INTEGER
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_scans (
        id TEXT PRIMARY KEY,
        label_id TEXT,
        batch_number TEXT,
        label_number INTEGER,
        dept TEXT NOT NULL,
        type TEXT NOT NULL,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        operator TEXT,
        size TEXT,
        qty REAL
      )
    `);
    // CRITICAL: ensure label_number column exists — missing column causes all scans to fail with 500
    await pgPool.query(`ALTER TABLE tracking_scans ADD COLUMN IF NOT EXISTS label_number INTEGER`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_scans_dept_ts ON tracking_scans(dept, ts DESC)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_scans_batch ON tracking_scans(batch_number, dept)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_labels_batch ON tracking_labels(batch_number)`);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_wastage (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        type TEXT,
        qty REAL,
        by TEXT,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        note TEXT
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_stage_closure (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        closed INTEGER DEFAULT 1,
        closed_at TEXT,
        closed_by TEXT,
        UNIQUE(batch_number, dept)
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_batch_closed (
        order_id TEXT PRIMARY KEY,
        batch_number TEXT,
        closed_at TEXT,
        closed_by TEXT,
        notes TEXT
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_settings (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // planning_state
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS planning_state (
        id SERIAL PRIMARY KEY,
        state_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // dpr_records
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_records (
        id SERIAL PRIMARY KEY,
        floor TEXT NOT NULL,
        date TEXT NOT NULL,
        data_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(floor, date)
      )
    `);

    // production_actuals
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS production_actuals (
        id SERIAL PRIMARY KEY,
        order_id TEXT,
        batch_number TEXT,
        machine_id TEXT NOT NULL,
        date TEXT NOT NULL,
        shift TEXT NOT NULL,
        run_index INTEGER NOT NULL DEFAULT 0,
        qty_lakhs REAL NOT NULL,
        floor TEXT,
        synced_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(machine_id, date, shift, run_index)
      )
    `);

    // app_users
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS app_users (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        pin_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // app_sessions
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS app_sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        expires_at TEXT NOT NULL
      )
    `);

    // audit_log
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        ip TEXT,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // temp_batches
    await pgPool.query(`
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
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(machine_id, date)
      )
    `);

    // temp_batch_alerts
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS temp_batch_alerts (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT NOT NULL,
        machine_id TEXT NOT NULL,
        date TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        message TEXT,
        resolved INTEGER DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // tracking_alerts
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_alerts (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        message TEXT,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved INTEGER DEFAULT 0
      )
    `);

    // tracking_dispatch_records
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_dispatch_records (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        qty REAL,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        operator TEXT,
        note TEXT
      )
    `);

    // tracking_dispatch_actuals
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_dispatch_actuals (
        id SERIAL PRIMARY KEY,
        batch_number TEXT NOT NULL,
        qty REAL,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // reconciliation_requests
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS pc_codes (
        id SERIAL PRIMARY KEY,
        size TEXT NOT NULL,
        code TEXT NOT NULL,
        colour TEXT NOT NULL,
        pack_size INTEGER DEFAULT 100000,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(size, code)
      );
      CREATE TABLE IF NOT EXISTS reconciliation_requests (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT NOT NULL,
        order_id TEXT,
        batch_number TEXT,
        requested_by TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved_at TEXT,
        resolved_by TEXT,
        notes TEXT
      );
      CREATE TABLE IF NOT EXISTS production_orders (
        id TEXT PRIMARY KEY,
        data_json TEXT NOT NULL,
        machine_id TEXT,
        batch_number TEXT,
        status TEXT DEFAULT 'pending',
        deleted BOOLEAN DEFAULT false,
        updated_at TEXT DEFAULT NOW()::TEXT
      );
      CREATE TABLE IF NOT EXISTS print_orders (
        id TEXT PRIMARY KEY,
        machine_id TEXT,
        customer TEXT,
        batch_number TEXT,
        pc_code TEXT,
        size TEXT,
        colour TEXT,
        print_matter TEXT,
        print_type TEXT,
        qty_to_print REAL,
        order_qty REAL,
        printed_to_date REAL DEFAULT 0,
        printed_to_date_manual BOOLEAN DEFAULT false,
        start_date TEXT,
        end_date TEXT,
        status TEXT DEFAULT 'pending',
        zone TEXT,
        remarks TEXT,
        production_order_id TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS machine_master (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        size TEXT,
        cap REAL,
        a_grade REAL,
        preferred_customer TEXT,
        active BOOLEAN DEFAULT true,
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);


    // dispatch_plans — dedicated table for all dispatch plans
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dispatch_plans (
        id TEXT PRIMARY KEY,
        data_json JSONB NOT NULL,
        production_order_id TEXT,
        batch_number TEXT,
        customer TEXT,
        zone TEXT,
        status TEXT DEFAULT 'pending',
        is_auto BOOLEAN DEFAULT false,
        deleted BOOLEAN DEFAULT false,
        updated_at TEXT DEFAULT NOW()::TEXT
      )
    `);

    // daily_printing — dedicated table for all daily printing logs
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS daily_printing (
        id TEXT PRIMARY KEY,
        data_json JSONB NOT NULL,
        print_order_id TEXT,
        machine_id TEXT,
        date TEXT,
        updated_at TEXT DEFAULT NOW()::TEXT
      )
    `);

    // pack_sizes — dedicated table for pack size settings
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS pack_sizes (
        size TEXT PRIMARY KEY,
        value INTEGER NOT NULL,
        updated_at TEXT DEFAULT NOW()::TEXT
      )
    `);

    // wo_reconciliation_requests
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS wo_reconciliation_requests (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT NOT NULL,
        order_id TEXT,
        batch_number TEXT,
        requested_by TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved_at TEXT,
        resolved_by TEXT,
        notes TEXT
      )
    `);

    // schema_migrations (for tracking)
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id SERIAL PRIMARY KEY,
        version INTEGER NOT NULL UNIQUE,
        name TEXT NOT NULL,
        applied_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // Indexes for performance
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC)`).catch(()=>{});
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_prod_actuals_date ON production_actuals(date, machine_id)`).catch(()=>{});
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_dpr_records_date ON dpr_records(date)`).catch(()=>{});

    // v37H: idempotent ALTER for tracking_labels to bring any prod table up to spec
    const labelColumns = [
      'wo_status TEXT', 'ship_to TEXT', 'bill_to TEXT',
      'is_excess INTEGER DEFAULT 0', 'excess_num INTEGER',
      'excess_total INTEGER', 'normal_total INTEGER', 'qr_data TEXT', 'voided_by TEXT'
    ];
    for (const col of labelColumns) {
      const colName = col.split(' ')[0];
      try {
        await pgPool.query(`ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS ${col}`);
      } catch(e) {
        console.warn(`[v37H migration] could not add column ${colName} to tracking_labels:`, e.message);
      }
    }
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_scans (
        id TEXT PRIMARY KEY,
        label_id TEXT,
        batch_number TEXT,
        label_number INTEGER,
        dept TEXT NOT NULL,
        type TEXT NOT NULL,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        operator TEXT,
        size TEXT,
        qty REAL
      )
    `);
    // CRITICAL: ensure label_number column exists — missing column causes all scans to fail with 500
    await pgPool.query(`ALTER TABLE tracking_scans ADD COLUMN IF NOT EXISTS label_number INTEGER`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_scans_dept_ts ON tracking_scans(dept, ts DESC)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_scans_batch ON tracking_scans(batch_number, dept)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_labels_batch ON tracking_labels(batch_number)`);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_wastage (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        type TEXT,
        qty REAL,
        by TEXT,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        note TEXT
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_stage_closure (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        closed INTEGER DEFAULT 1,
        closed_at TEXT,
        closed_by TEXT,
        UNIQUE(batch_number, dept)
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_batch_closed (
        order_id TEXT PRIMARY KEY,
        batch_number TEXT,
        closed_at TEXT,
        closed_by TEXT,
        notes TEXT
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_settings (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // planning_state
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS planning_state (
        id SERIAL PRIMARY KEY,
        state_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // dpr_records
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_records (
        id SERIAL PRIMARY KEY,
        floor TEXT NOT NULL,
        date TEXT NOT NULL,
        data_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(floor, date)
      )
    `);

    // production_actuals
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS production_actuals (
        id SERIAL PRIMARY KEY,
        order_id TEXT,
        batch_number TEXT,
        machine_id TEXT NOT NULL,
        date TEXT NOT NULL,
        shift TEXT NOT NULL,
        run_index INTEGER NOT NULL DEFAULT 0,
        qty_lakhs REAL NOT NULL,
        floor TEXT,
        synced_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(machine_id, date, shift, run_index)
      )
    `);

    // app_users
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS app_users (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        pin_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // app_sessions
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS app_sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        expires_at TEXT NOT NULL
      )
    `);

    // audit_log
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        ip TEXT,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // temp_batches
    // v37H: schema now matches SQLite (was missing colour/pc_code/colour_confirmed)
    // UPDATE statements reference these columns; missing columns caused silent failures.
    await pgPool.query(`
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
        colour TEXT,
        pc_code TEXT,
        colour_confirmed INTEGER DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(machine_id, date)
      )
    `);
    // v37H: ensure all columns exist on previously-created tables
    const tempBatchColumns = ['colour TEXT', 'pc_code TEXT', 'colour_confirmed INTEGER DEFAULT 0'];
    for (const col of tempBatchColumns) {
      const colName = col.split(' ')[0];
      try {
        await pgPool.query(`ALTER TABLE temp_batches ADD COLUMN IF NOT EXISTS ${col}`);
      } catch(e) {
        console.warn(`[v37H migration] could not add column ${colName} to temp_batches:`, e.message);
      }
    }

    // temp_batch_alerts
    // v37G: schema now matches SQLite (was missing alert_date)
    // INSERT statements reference alert_date; missing column caused silent INSERT failures.
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS temp_batch_alerts (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT NOT NULL,
        machine_id TEXT NOT NULL,
        date TEXT,
        alert_date TEXT,
        alert_type TEXT,
        message TEXT,
        resolved INTEGER DEFAULT 0,
        sent_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);
    // v37G: ensure all columns exist on previously-created tables
    const tempBatchAlertColumns = ['alert_date TEXT', 'sent_at TEXT', 'alert_type TEXT', 'message TEXT', 'date TEXT'];
    for (const col of tempBatchAlertColumns) {
      const colName = col.split(' ')[0];
      try {
        await pgPool.query(`ALTER TABLE temp_batch_alerts ADD COLUMN IF NOT EXISTS ${col}`);
      } catch(e) {
        console.warn(`[v37G migration] could not add column ${colName} to temp_batch_alerts:`, e.message);
      }
    }

    // tracking_alerts
    // v37G: schema now matches SQLite (was missing label_id/scan_in_ts/hours_stuck/msg)
    // INSERT statements reference these columns; missing columns caused silent INSERT failures.
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_alerts (
        id TEXT PRIMARY KEY,
        label_id TEXT,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        scan_in_ts TEXT,
        hours_stuck REAL,
        alert_type TEXT,
        message TEXT,
        msg TEXT,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved INTEGER DEFAULT 0
      )
    `);
    // v37G: ensure all columns exist on previously-created tables
    const alertColumns = [
      'label_id TEXT', 'scan_in_ts TEXT', 'hours_stuck REAL', 'msg TEXT', 'alert_type TEXT', 'message TEXT'
    ];
    for (const col of alertColumns) {
      const colName = col.split(' ')[0];
      try {
        await pgPool.query(`ALTER TABLE tracking_alerts ADD COLUMN IF NOT EXISTS ${col}`);
      } catch(e) {
        console.warn(`[v37G migration] could not add column ${colName} to tracking_alerts:`, e.message);
      }
    }

    // tracking_dispatch_records
    // v37G: schema now matches SQLite (was missing customer/boxes/vehicle_no/invoice_no/remarks/by)
    // ALTER TABLE statements added to bring existing prod tables up to schema without data loss.
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_dispatch_records (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        customer TEXT,
        qty REAL,
        boxes INTEGER,
        vehicle_no TEXT,
        invoice_no TEXT,
        remarks TEXT,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        "by" TEXT
      )
    `);
    // v37G: ensure all columns exist on previously-created tables (idempotent — IF NOT EXISTS)
    const dispatchColumns = [
      'customer TEXT', 'boxes INTEGER', 'vehicle_no TEXT', 'invoice_no TEXT',
      'remarks TEXT', '"by" TEXT'
    ];
    for (const col of dispatchColumns) {
      const colName = col.split(' ')[0];
      try {
        await pgPool.query(`ALTER TABLE tracking_dispatch_records ADD COLUMN IF NOT EXISTS ${col}`);
      } catch(e) {
        console.warn(`[v37G migration] could not add column ${colName}:`, e.message);
      }
    }

    // tracking_dispatch_actuals
    // v37G: schema now matches SQLite (was missing dispatched_qty/vehicle_no/invoice_no/updated_at)
    // INSERT statements reference these columns; missing columns caused failures.
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_dispatch_actuals (
        batch_number TEXT PRIMARY KEY,
        dispatched_qty REAL DEFAULT 0,
        vehicle_no TEXT,
        invoice_no TEXT,
        updated_at TEXT,
        qty REAL,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);
    // v37G: ensure all columns exist on previously-created tables
    const dispatchActualColumns = ['dispatched_qty REAL', 'vehicle_no TEXT', 'invoice_no TEXT', 'updated_at TEXT', 'qty REAL'];
    for (const col of dispatchActualColumns) {
      const colName = col.split(' ')[0];
      try {
        await pgPool.query(`ALTER TABLE tracking_dispatch_actuals ADD COLUMN IF NOT EXISTS ${col}`);
      } catch(e) {
        console.warn(`[v37G migration] could not add column ${colName} to tracking_dispatch_actuals:`, e.message);
      }
    }

    // v37I: dispatch_reconcile_alerts — Flow A and Flow B 60-min reconciliation alerts
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dispatch_reconcile_alerts (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        label_id TEXT,
        alert_type TEXT NOT NULL CHECK(alert_type IN ('A','B')),
        triggered_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        acknowledged_at TEXT,
        acknowledged_by TEXT,
        ack_reason TEXT,
        ack_expires_at TEXT,
        resolved_at TEXT
      )
    `);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_dra_batch ON dispatch_reconcile_alerts(batch_number)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_dra_type_resolved ON dispatch_reconcile_alerts(alert_type, resolved_at)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_dra_label ON dispatch_reconcile_alerts(label_id)`);
    // v37I.1: system_settings — generic key/value config (used for fgAgingDaysNonExport, fgAgingDaysExport)
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS system_settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        updated_by TEXT
      )
    `);

    // v37J Sub-issue 1.1: customer_master — distinct customer names for Ship-to/Bill-to autocomplete
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS customer_master (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        added_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        added_by TEXT,
        use_count INTEGER NOT NULL DEFAULT 1,
        last_used_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_customer_master_name_lower ON customer_master(LOWER(name))`);

    // ─── v39 SAP integration tables ─────────────────────────────────
    // invoice_requests — Sunloc-initiated invoice triggers sent to SAP
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS invoice_requests (
        id TEXT PRIMARY KEY,
        batch_number TEXT,
        customer TEXT,
        card_code TEXT,
        po_number TEXT,
        sap_doc_entry INTEGER,
        size TEXT,
        colour TEXT,
        pc_code TEXT,
        boxes INTEGER,
        qty_lakhs DOUBLE PRECISION,
        rate_per_lakh DOUBLE PRECISION,
        selected_labels TEXT,
        selection_mode TEXT,
        truck_number INTEGER,
        status TEXT NOT NULL DEFAULT 'pending',
        sap_response_doc_num TEXT,
        sap_response_doc_entry INTEGER,
        sap_response_irn TEXT,
        sap_error_message TEXT,
        is_admin_override BOOLEAN NOT NULL DEFAULT FALSE,
        override_reason TEXT,
        override_by TEXT,
        created_by TEXT,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_req_batch ON invoice_requests(batch_number)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_req_status ON invoice_requests(status)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_req_sap_entry ON invoice_requests(sap_doc_entry)`);

    // invoices_received — SAP-generated invoices pulled by Sunloc poller
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS invoices_received (
        id TEXT PRIMARY KEY,
        sap_doc_entry INTEGER UNIQUE,
        sap_doc_num TEXT,
        sap_invoice_no TEXT,
        invoice_date TEXT,
        customer TEXT,
        card_code TEXT,
        po_number TEXT,
        batch_number TEXT,
        pc_code TEXT,
        size TEXT,
        colour TEXT,
        total_boxes INTEGER,
        total_qty_lakhs DOUBLE PRECISION,
        taxable_amount DOUBLE PRECISION,
        igst_amount DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        irn TEXT,
        ack_no TEXT,
        ack_date TEXT,
        source TEXT NOT NULL DEFAULT 'sunloc',
        invoice_request_id TEXT,
        dispatch_status TEXT NOT NULL DEFAULT 'pending',
        scanned_boxes INTEGER NOT NULL DEFAULT 0,
        dispatched_at TEXT,
        dispatched_by TEXT,
        vehicle_no TEXT,
        lr_no TEXT,
        remarks TEXT,
        is_deemed_scan_out BOOLEAN NOT NULL DEFAULT FALSE,
        deemed_reason TEXT,
        deemed_by TEXT,
        admin_approved_at TEXT,
        admin_approved_by TEXT,
        fetched_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        payload_json TEXT
      )
    `);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_recv_batch ON invoices_received(batch_number)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_recv_customer ON invoices_received(customer)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_recv_status ON invoices_received(dispatch_status)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_recv_date ON invoices_received(invoice_date)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_recv_source ON invoices_received(source)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_recv_req ON invoices_received(invoice_request_id)`);

    // sap_config — single-row credentials + session state
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS sap_config (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        sap_url TEXT,
        sap_username TEXT,
        sap_company_db TEXT,
        sap_password_encrypted TEXT,
        session_cookie TEXT,
        session_route_id TEXT,
        session_expires_at TEXT,
        last_login_at TEXT,
        last_login_success BOOLEAN,
        last_login_error TEXT,
        indent_poll_interval_minutes INTEGER NOT NULL DEFAULT 5,
        invoice_poll_interval_minutes INTEGER NOT NULL DEFAULT 5,
        indent_poll_lookback_days INTEGER NOT NULL DEFAULT 30,
        invoice_poll_lookback_days INTEGER NOT NULL DEFAULT 7,
        last_indent_poll_at TEXT,
        last_invoice_poll_at TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        updated_by TEXT
      )
    `);
    await pgPool.query(`INSERT INTO sap_config (id, sap_url, sap_username, sap_company_db) VALUES (1, '', '', '') ON CONFLICT (id) DO NOTHING`);

    // sap_audit_log — forensic log of every SAP API call (rolling 5000 rows)
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS sap_audit_log (
        id SERIAL PRIMARY KEY,
        called_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        method TEXT,
        endpoint TEXT,
        status_code INTEGER,
        duration_ms INTEGER,
        success BOOLEAN NOT NULL DEFAULT FALSE,
        error_message TEXT,
        request_summary TEXT,
        response_summary TEXT
      )
    `);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_sap_audit_called ON sap_audit_log(called_at)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_sap_audit_success ON sap_audit_log(success)`);

    // sap_indent_cache — last-fetched open Sales Orders from SAP
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS sap_indent_cache (
        id SERIAL PRIMARY KEY,
        sap_doc_entry INTEGER UNIQUE NOT NULL,
        sap_doc_num TEXT,
        card_code TEXT,
        card_name TEXT,
        doc_date TEXT,
        doc_due_date TEXT,
        total_lines INTEGER,
        total_qty DOUBLE PRECISION,
        payload_json TEXT NOT NULL,
        fetched_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        processed_at TEXT,
        processed_by TEXT,
        processed_order_id TEXT
      )
    `);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_sap_indent_doc_entry ON sap_indent_cache(sap_doc_entry)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_sap_indent_processed ON sap_indent_cache(processed_at)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_sap_indent_customer ON sap_indent_cache(card_code)`);

    // v39 Phase 9c: SAP refs on wo_reconciliation_requests
    // (mirror of migration #19 — PG supports IF NOT EXISTS on ALTER ADD COLUMN)
    try { await pgPool.query(`ALTER TABLE wo_reconciliation_requests ADD COLUMN IF NOT EXISTS sap_doc_entry INTEGER`); } catch (e) { console.warn('[v39 P9c PG] add sap_doc_entry:', e.message); }
    try { await pgPool.query(`ALTER TABLE wo_reconciliation_requests ADD COLUMN IF NOT EXISTS sap_doc_num TEXT`); } catch (e) { console.warn('[v39 P9c PG] add sap_doc_num:', e.message); }
    // v39 Phase 10a: dispatch_record_id link on invoices_received
    try { await pgPool.query(`ALTER TABLE invoices_received ADD COLUMN IF NOT EXISTS dispatch_record_id TEXT`); } catch (e) { console.warn('[v39 P10a PG] add dispatch_record_id:', e.message); }
    // v40 Phase 18.11: truck-level scan-out session persistence
    try {
      await pgPool.query(`
        CREATE TABLE IF NOT EXISTS truck_scan_session_state (
          truck_number TEXT PRIMARY KEY,
          invoice_ids_json TEXT NOT NULL,
          scanned_labels_json TEXT NOT NULL DEFAULT '[]',
          vehicle_no TEXT DEFAULT '',
          lr_no TEXT DEFAULT '',
          remarks TEXT DEFAULT '',
          started_by TEXT,
          started_at TEXT DEFAULT NOW()::TEXT,
          last_updated_at TEXT DEFAULT NOW()::TEXT
        );
      `);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_truck_scan_last_updated ON truck_scan_session_state(last_updated_at)`);
    } catch (e) { console.warn('[v40 P18.11 PG] truck_scan_session_state:', e.message); }
    // ─── end v39 SAP tables ────────────────────────────────────────


    // reconciliation_requests
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS pc_codes (
        id SERIAL PRIMARY KEY,
        size TEXT NOT NULL,
        code TEXT NOT NULL,
        colour TEXT NOT NULL,
        pack_size INTEGER DEFAULT 100000,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(size, code)
      );
      CREATE TABLE IF NOT EXISTS reconciliation_requests (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT,
        order_id TEXT,
        batch_number TEXT,
        proposed_by TEXT,
        proposed_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        approved_by TEXT,
        approved_at TEXT,
        order_details TEXT,
        back_date TEXT,
        temp_batch_mappings TEXT,
        total_boxes INTEGER,
        rejection_reason TEXT,
        requested_by TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved_at TEXT,
        resolved_by TEXT,
        notes TEXT
      );
      CREATE TABLE IF NOT EXISTS production_orders (
        id TEXT PRIMARY KEY,
        data_json TEXT NOT NULL,
        machine_id TEXT,
        batch_number TEXT,
        status TEXT DEFAULT 'pending',
        deleted BOOLEAN DEFAULT false,
        updated_at TEXT DEFAULT NOW()::TEXT
      );
      CREATE TABLE IF NOT EXISTS print_orders (
        id TEXT PRIMARY KEY,
        machine_id TEXT,
        customer TEXT,
        batch_number TEXT,
        pc_code TEXT,
        size TEXT,
        colour TEXT,
        print_matter TEXT,
        print_type TEXT,
        qty_to_print REAL,
        order_qty REAL,
        printed_to_date REAL DEFAULT 0,
        printed_to_date_manual BOOLEAN DEFAULT false,
        start_date TEXT,
        end_date TEXT,
        status TEXT DEFAULT 'pending',
        zone TEXT,
        remarks TEXT,
        production_order_id TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS machine_master (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        size TEXT,
        cap REAL,
        a_grade REAL,
        preferred_customer TEXT,
        active BOOLEAN DEFAULT true,
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // v37G: ensure all columns exist on previously-created reconciliation_requests
    const reconColumns = [
      'proposed_by TEXT', 'proposed_at TEXT', 'approved_by TEXT', 'approved_at TEXT',
      'order_details TEXT', 'back_date TEXT', 'temp_batch_mappings TEXT',
      'total_boxes INTEGER', 'rejection_reason TEXT'
    ];
    for (const col of reconColumns) {
      const colName = col.split(' ')[0];
      try {
        await pgPool.query(`ALTER TABLE reconciliation_requests ADD COLUMN IF NOT EXISTS ${col}`);
      } catch(e) {
        console.warn(`[v37G migration] could not add column ${colName} to reconciliation_requests:`, e.message);
      }
    }

    // wo_reconciliation_requests
    // v37G: schema now matches SQLite (was missing customer/po_number/proposed_by/proposed_at/
    // approved_by/approved_at/rejection_reason/qty_confirmed/zone). INSERT/UPDATE statements
    // reference these columns; missing columns caused PG INSERT failures.
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS wo_reconciliation_requests (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT,
        order_id TEXT,
        batch_number TEXT,
        proposed_by TEXT,
        proposed_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        approved_by TEXT,
        approved_at TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        customer TEXT,
        po_number TEXT,
        zone TEXT,
        qty_confirmed REAL,
        rejection_reason TEXT,
        requested_by TEXT,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved_at TEXT,
        resolved_by TEXT,
        notes TEXT
      )
    `);
    // v37G: ensure all columns exist on previously-created tables
    const woReconColumns = [
      'proposed_by TEXT', 'proposed_at TEXT', 'approved_by TEXT', 'approved_at TEXT',
      'customer TEXT', 'po_number TEXT', 'zone TEXT', 'qty_confirmed REAL', 'rejection_reason TEXT'
    ];
    for (const col of woReconColumns) {
      const colName = col.split(' ')[0];
      try {
        await pgPool.query(`ALTER TABLE wo_reconciliation_requests ADD COLUMN IF NOT EXISTS ${col}`);
      } catch(e) {
        console.warn(`[v37G migration] could not add column ${colName} to wo_reconciliation_requests:`, e.message);
      }
    }

    // schema_migrations (for tracking)
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id SERIAL PRIMARY KEY,
        version INTEGER NOT NULL UNIQUE,
        name TEXT NOT NULL,
        applied_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // Indexes for performance
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC)`).catch(()=>{});
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_prod_actuals_date ON production_actuals(date, machine_id)`).catch(()=>{});
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_dpr_records_date ON dpr_records(date)`).catch(()=>{});


        console.log('[DB] PostgreSQL tables verified/created');
  } catch(e) {
    console.error('[DB] ensurePostgresTables error:', e.message);
  }
}

async function warmPlanningCache() {
  if (!pgPool) return;
  // Refresh cache every 60 seconds
  setInterval(async () => {
    try {
      const r = await pgPool.query('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1');
      if (r.rows[0]) {
        _planningStateCache = JSON.parse(r.rows[0].state_json);
        _planningStateCacheTime = Date.now();
      }
    } catch(e) {}
  }, 60000);
  try {
    const r = await pgPool.query('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1');
    if (r.rows[0]) {
      _planningStateCache = JSON.parse(r.rows[0].state_json);
      _planningStateCacheTime = Date.now();
      console.log('[DB] Planning state cache warmed:', (_planningStateCache.orders||[]).length, 'orders');
    }
  } catch(e) { console.error('[DB] Cache warm error:', e.message); }
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
let _actualsCache = null;
let _actualsCacheTime = 0;
async function warmActualsCache() {
  // Throttle to 60s — prevents DB hammering from every device's 30s auto-sync
  if (Date.now() - _actualsCacheTime < 60000 && _actualsCache) return;
  _actualsCacheTime = Date.now();
  if (!pgPool) return;
  try {
    const r = await pgPool.query('SELECT order_id, batch_number, SUM(qty_lakhs) as total FROM production_actuals GROUP BY order_id, batch_number');
    _actualsCache = {};
    for (const row of r.rows) {
      if (row.order_id) _actualsCache[row.order_id] = parseFloat(row.total) || 0;
      if (row.batch_number) _actualsCache[row.batch_number] = parseFloat(row.total) || 0;
    }
    console.log('[DB] Actuals cache warmed:', r.rows.length, 'entries');
  } catch(e) { console.error('[DB] Actuals cache error:', e.message); }
}

function getOrderActuals(orderId, batchNumber) {
  if (_actualsCache) {
    return _actualsCache[orderId] || _actualsCache[batchNumber] || 0;
  }
  // Falls back to SQLite only — cache should always be warm when pgPool is available
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

// GET /api/pc-codes — load all custom PC codes from DB
app.get('/api/pc-codes', async (req, res) => {
  try {
    let rows = [];
    if (pgPool) {
      try {
        const r = await pgPool.query('SELECT size, code, colour, pack_size FROM pc_codes ORDER BY size, code');
        rows = r.rows;
      } catch(e) {
        // Table may not exist yet — return empty
        return res.json({ ok: true, codes: {}, count: 0 });
      }
    } else {
      try { rows = db.prepare('SELECT size, code, colour, pack_size FROM pc_codes ORDER BY size, code').all(); } catch(e) {}
    }
    const bySize = {};
    rows.forEach(r => {
      if (!bySize[r.size]) bySize[r.size] = [];
      bySize[r.size].push({ c: r.code, n: r.colour, packSize: r.pack_size });
    });
    res.json({ ok: true, codes: bySize, count: rows.length });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/pc-codes — save new PC code to DB permanently
app.post('/api/pc-codes', async (req, res) => {
  try {
    const { size, code, colour, packSize } = req.body;
    if (!size || !code || !colour) return res.status(400).json({ ok: false, error: 'size, code, colour required' });
    if (pgPool) {
      await pgPool.query(
        'INSERT INTO pc_codes (size, code, colour, pack_size) VALUES ($1, $2, $3, $4) ON CONFLICT (size, code) DO UPDATE SET colour=$3, pack_size=$4',
        [size, code, colour, packSize || 100000]
      );
    } else {
      db.prepare('INSERT OR REPLACE INTO pc_codes (size, code, colour, pack_size) VALUES (?, ?, ?, ?)').run(size, code, colour, packSize || 100000);
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// DELETE /api/pc-codes — remove a PC code from DB
app.delete('/api/pc-codes', async (req, res) => {
  try {
    const { size, code } = req.body;
    if (!size || !code) return res.status(400).json({ ok: false, error: 'size, code required' });
    if (pgPool) {
      await pgPool.query('DELETE FROM pc_codes WHERE size=$1 AND code=$2', [size, code]);
    } else {
      db.prepare('DELETE FROM pc_codes WHERE size=? AND code=?').run(size, code);
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// v37J Sub-issue 1.1: Customer master endpoints for Ship-to/Bill-to autocomplete.
// On first call, seeds master from any distinct customer names already present in
// production_orders.data_json (so existing customers don't need manual re-entry).
let _customerMasterSeeded = false;
async function _seedCustomerMasterIfNeeded() {
  if (_customerMasterSeeded) return;
  try {
    if (pgPool) {
      const cnt = await pgPool.query('SELECT COUNT(*) AS c FROM customer_master');
      if (Number(cnt.rows[0].c) > 0) { _customerMasterSeeded = true; return; }
      const r = await pgPool.query(`SELECT DISTINCT data_json::jsonb->>'customer' AS c FROM production_orders WHERE deleted = false`);
      for (const row of r.rows) {
        const name = (row.c || '').trim();
        if (!name) continue;
        await pgPool.query(`INSERT INTO customer_master (name, added_by) VALUES ($1, 'system_seed') ON CONFLICT (name) DO NOTHING`, [name]);
      }
    } else {
      const cnt = db.prepare('SELECT COUNT(*) AS c FROM customer_master').get();
      if (cnt && cnt.c > 0) { _customerMasterSeeded = true; return; }
      const orderRows = db.prepare(`SELECT data_json FROM production_orders WHERE deleted = 0`).all();
      const distinct = new Set();
      for (const o of orderRows) {
        try {
          const d = JSON.parse(o.data_json);
          const c = (d?.customer || '').trim();
          if (c) distinct.add(c);
        } catch(e) {}
      }
      const ins = db.prepare(`INSERT OR IGNORE INTO customer_master (name, added_by) VALUES (?, 'system_seed')`);
      for (const c of distinct) ins.run(c);
    }
    _customerMasterSeeded = true;
    console.log('[CustomerMaster] Seeded from existing production_orders');
  } catch(e) {
    console.warn('[CustomerMaster] Seed failed:', e.message);
  }
}

// GET /api/customers — list customer names sorted by recent use frequency
app.get('/api/customers', async (req, res) => {
  try {
    await _seedCustomerMasterIfNeeded();
    let rows = [];
    if (pgPool) {
      const r = await pgPool.query(`SELECT name, use_count, last_used_at FROM customer_master ORDER BY use_count DESC, last_used_at DESC, name ASC LIMIT 5000`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT name, use_count, last_used_at FROM customer_master ORDER BY use_count DESC, last_used_at DESC, name ASC LIMIT 5000`).all();
    }
    res.json({ ok: true, customers: rows.map(r => r.name), count: rows.length });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/customers — upsert a customer name (called on order save)
// Body: { name, addedBy? }
// If exists, increment use_count and bump last_used_at; if new, insert.
app.post('/api/customers', async (req, res) => {
  try {
    const name = (req.body?.name || '').toString().trim();
    const addedBy = (req.body?.addedBy || 'planning').toString().substring(0, 50);
    if (!name) return res.status(400).json({ ok: false, error: 'name required' });
    if (name.length > 200) return res.status(400).json({ ok: false, error: 'name too long' });
    if (pgPool) {
      await pgPool.query(`
        INSERT INTO customer_master (name, added_by, use_count, last_used_at)
        VALUES ($1, $2, 1, NOW()::TEXT)
        ON CONFLICT (name) DO UPDATE
        SET use_count = customer_master.use_count + 1,
            last_used_at = NOW()::TEXT
      `, [name, addedBy]);
    } else {
      // SQLite: use INSERT ... ON CONFLICT for upsert (works since name is UNIQUE COLLATE NOCASE)
      db.prepare(`
        INSERT INTO customer_master (name, added_by, use_count, last_used_at)
        VALUES (?, ?, 1, datetime('now'))
        ON CONFLICT(name) DO UPDATE SET
          use_count = use_count + 1,
          last_used_at = datetime('now')
      `).run(name, addedBy);
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});


// ════════════════════════════════════════════════════════════════════
// v39 SAP API — direct OData calls to SAP B1 Service Layer
// ════════════════════════════════════════════════════════════════════
// All endpoints below talk to the SAP B1 Service Layer at the URL stored
// in sap_config. Errors return { ok:false, error, degraded? } — never
// crash the server. The SapClient handles session/auth/retry/auditing.
//
// Admin-only endpoints check the X-Sunloc-Role header (set by the
// client app from currentUser.role). This matches the convention used
// by /api/admin/* endpoints elsewhere in this file.
// ════════════════════════════════════════════════════════════════════

function _requireAdmin(req, res) {
  const role = (req.headers['x-sunloc-role'] || req.body?._role || '').toString().toLowerCase();
  if (role !== 'admin') {
    res.status(403).json({ ok: false, error: 'Admin role required' });
    return false;
  }
  return true;
}

// Internal helper — pulls open SOs from SAP and upserts to sap_indent_cache.
// Used by both POST /api/sap/refresh-indents and the background poller.
// Returns { ok, fetched, upserted, error?, degraded? }.
async function _doRefreshSapIndents() {
  const cfg = await sap.getConfig();
  const lookback = (cfg && cfg.indent_poll_lookback_days) || 30;
  const r = await sap.fetchOpenSalesOrders({ lookbackDays: lookback });
  if (!r.ok) return { ok: false, error: r.error, degraded: r.degraded, fetched: 0, upserted: 0 };
  const indents = r.indents || [];
  let upserted = 0;
  for (const ind of indents) {
    const totalQty = (ind.DocumentLines || []).reduce((sum, l) => sum + (parseFloat(l.Quantity) || 0), 0));
    const totalLines = (ind.DocumentLines || []).length;
    const payload = JSON.stringify(ind);
    try {
      if (pgPool) {
        await pgPool.query(`
          INSERT INTO sap_indent_cache (sap_doc_entry, sap_doc_num, card_code, card_name,
            doc_date, doc_due_date, total_lines, total_qty, payload_json, fetched_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()::TEXT)
          ON CONFLICT (sap_doc_entry) DO UPDATE SET
            sap_doc_num=$2, card_code=$3, card_name=$4, doc_date=$5, doc_due_date=$6,
            total_lines=$7, total_qty=$8, payload_json=$9, fetched_at=NOW()::TEXT
        `, [ind.DocEntry, String(ind.DocNum || ''), ind.CardCode || '', ind.CardName || '',
            ind.DocDate || null, ind.DocDueDate || null, totalLines, totalQty, payload]);
      } else {
        db.prepare(`
          INSERT INTO sap_indent_cache (sap_doc_entry, sap_doc_num, card_code, card_name,
            doc_date, doc_due_date, total_lines, total_qty, payload_json, fetched_at)
          VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))
          ON CONFLICT(sap_doc_entry) DO UPDATE SET
            sap_doc_num=excluded.sap_doc_num, card_code=excluded.card_code,
            card_name=excluded.card_name, doc_date=excluded.doc_date,
            doc_due_date=excluded.doc_due_date, total_lines=excluded.total_lines,
            total_qty=excluded.total_qty, payload_json=excluded.payload_json,
            fetched_at=datetime('now')
        `).run(ind.DocEntry, String(ind.DocNum || ''), ind.CardCode || '', ind.CardName || '',
            ind.DocDate || null, ind.DocDueDate || null, totalLines, totalQty, payload);
      }
      upserted++;
    } catch (e) {
      console.warn('[SAP] indent upsert error for DocEntry', ind.DocEntry, ':', e.message);
    }
  }
  // Update last_indent_poll_at
  try {
    if (pgPool) {
      await pgPool.query(`UPDATE sap_config SET last_indent_poll_at = NOW()::TEXT WHERE id=1`);
    } else {
      db.prepare(`UPDATE sap_config SET last_indent_poll_at = datetime('now') WHERE id=1`).run();
    }
  } catch {}
  return { ok: true, fetched: indents.length, upserted };
}

// Internal helper — pulls recent invoices from SAP, upserts to invoices_received,
// auto-matches Sunloc-originated requests by U_SunlocBatch UDF.
// Used by both POST /api/sap/refresh-invoices and the background poller.
async function _doRefreshSapInvoices() {
  const cfg = await sap.getConfig();
  const lookback = (cfg && cfg.invoice_poll_lookback_days) || 7;
  const r = await sap.fetchRecentInvoices({ lookbackDays: lookback });
  if (!r.ok) return { ok: false, error: r.error, degraded: r.degraded, fetched: 0, upserted: 0 };
  const invoices = r.invoices || [];
  let upserted = 0;
  for (const inv of invoices) {
    const batchUdf = inv.U_SunlocBatch || '';
    const poUdf = inv.U_SunlocPO || '';
    let invReqId = null;
    let source = 'direct_sap';
    try {
      if (batchUdf) {
        if (pgPool) {
          const m = await pgPool.query(
            `SELECT id FROM invoice_requests WHERE batch_number=$1 AND status IN ('pending','sent_to_sap') ORDER BY created_at DESC LIMIT 1`,
            [batchUdf]
          );
          if (m.rows[0]) { invReqId = m.rows[0].id; source = 'sunloc'; }
        } else {
          const m = db.prepare(`SELECT id FROM invoice_requests WHERE batch_number=? AND status IN ('pending','sent_to_sap') ORDER BY created_at DESC LIMIT 1`).get(batchUdf);
          if (m) { invReqId = m.id; source = 'sunloc'; }
        }
      }
    } catch (e) { console.warn('[SAP] invoice match error:', e.message); }
    const totalBoxes = Math.round((inv.DocumentLines || []).reduce((sum, l) => sum + (parseFloat(l.Quantity) || 0), 0));
    const docTotal = parseFloat(inv.DocTotal) || 0;
    const vatSum = parseFloat(inv.VatSum) || 0;
    const taxable = docTotal - vatSum;
    const recId = `inv_${inv.DocEntry}`;
    const payload = JSON.stringify(inv);
    try {
      if (pgPool) {
        await pgPool.query(`
          INSERT INTO invoices_received (id, sap_doc_entry, sap_doc_num, sap_invoice_no,
            invoice_date, customer, card_code, po_number, batch_number, total_boxes,
            taxable_amount, igst_amount, total_amount, irn, source, invoice_request_id,
            fetched_at, payload_json)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW()::TEXT,$17)
          ON CONFLICT (sap_doc_entry) DO UPDATE SET
            sap_doc_num=$3, sap_invoice_no=$4, invoice_date=$5, customer=$6,
            card_code=$7, po_number=$8, batch_number=$9, total_boxes=$10,
            taxable_amount=$11, igst_amount=$12, total_amount=$13, irn=$14,
            payload_json=$17, fetched_at=NOW()::TEXT
        `, [recId, inv.DocEntry, String(inv.DocNum || ''), String(inv.DocNum || ''),
            inv.DocDate || null, inv.CardName || '', inv.CardCode || '', poUdf, batchUdf,
            totalBoxes, taxable, vatSum, docTotal, inv.U_IRN || null, source, invReqId, payload]);
      } else {
        db.prepare(`
          INSERT INTO invoices_received (id, sap_doc_entry, sap_doc_num, sap_invoice_no,
            invoice_date, customer, card_code, po_number, batch_number, total_boxes,
            taxable_amount, igst_amount, total_amount, irn, source, invoice_request_id,
            fetched_at, payload_json)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),?)
          ON CONFLICT(sap_doc_entry) DO UPDATE SET
            sap_doc_num=excluded.sap_doc_num, sap_invoice_no=excluded.sap_invoice_no,
            invoice_date=excluded.invoice_date, customer=excluded.customer,
            card_code=excluded.card_code, po_number=excluded.po_number,
            batch_number=excluded.batch_number, total_boxes=excluded.total_boxes,
            taxable_amount=excluded.taxable_amount, igst_amount=excluded.igst_amount,
            total_amount=excluded.total_amount, irn=excluded.irn,
            payload_json=excluded.payload_json, fetched_at=datetime('now')
        `).run(recId, inv.DocEntry, String(inv.DocNum || ''), String(inv.DocNum || ''),
            inv.DocDate || null, inv.CardName || '', inv.CardCode || '', poUdf, batchUdf,
            totalBoxes, taxable, vatSum, docTotal, inv.U_IRN || null, source, invReqId, payload);
      }
      if (invReqId) {
        try {
          if (pgPool) {
            await pgPool.query(
              `UPDATE invoice_requests SET status='invoice_received', sap_response_doc_num=$1, sap_response_doc_entry=$2, sap_response_irn=$3, updated_at=NOW()::TEXT WHERE id=$4`,
              [String(inv.DocNum || ''), inv.DocEntry, inv.U_IRN || null, invReqId]
            );
          } else {
            db.prepare(
              `UPDATE invoice_requests SET status='invoice_received', sap_response_doc_num=?, sap_response_doc_entry=?, sap_response_irn=?, updated_at=datetime('now') WHERE id=?`
            ).run(String(inv.DocNum || ''), inv.DocEntry, inv.U_IRN || null, invReqId);
          }
        } catch (e) { console.warn('[SAP] invoice_requests update error:', e.message); }
      }
      // v39 Phase 9a: also annotate dispatch_plans for this batch so Planning
      // and Tracking apps see the new state without a separate query.
      if (batchUdf) {
        try {
          await _v39_updateDispatchPlansForInvoice(batchUdf, {
            invoice_doc_num: String(inv.DocNum || ''),
            invoice_doc_entry: inv.DocEntry,
            invoice_irn: inv.U_IRN || null,
            invoice_status: 'invoiced',
            invoice_received_at: new Date().toISOString(),
          });
        } catch (e) {
          console.warn('[SAP P9a] dispatch_plans annotate error for batch', batchUdf, ':', e.message);
        }
      }
      upserted++;
    } catch (e) {
      console.warn('[SAP] invoice upsert error for DocEntry', inv.DocEntry, ':', e.message);
    }
  }
  try {
    if (pgPool) {
      await pgPool.query(`UPDATE sap_config SET last_invoice_poll_at = NOW()::TEXT WHERE id=1`);
    } else {
      db.prepare(`UPDATE sap_config SET last_invoice_poll_at = datetime('now') WHERE id=1`).run();
    }
  } catch {}
  return { ok: true, fetched: invoices.length, upserted };
}

// v39 Phase 9a helper: for each dispatch_plans row matching the batch, merge
// invoice annotations into data_json and write back. Sets the row's status
// column to 'invoiced' so downstream filters can find these rows easily.
async function _v39_updateDispatchPlansForInvoice(batchNumber, annotations) {
  let rows;
  if (pgPool) {
    const r = await pgPool.query(
      `SELECT id, data_json FROM dispatch_plans WHERE batch_number=$1 AND deleted=false`,
      [batchNumber]
    );
    rows = r.rows;
  } else {
    rows = db.prepare(
      `SELECT id, data_json FROM dispatch_plans WHERE batch_number=? AND deleted=0`
    ).all(batchNumber);
  }
  let touched = 0;
  for (const row of (rows || [])) {
    try {
      let data;
      try { data = typeof row.data_json === 'string' ? JSON.parse(row.data_json) : row.data_json; }
      catch { data = {}; }
      data = data || {};
      Object.assign(data, annotations);
      const merged = JSON.stringify(data);
      if (pgPool) {
        await pgPool.query(
          `UPDATE dispatch_plans SET data_json=$1::jsonb, status=$2, updated_at=NOW()::TEXT WHERE id=$3`,
          [merged, annotations.invoice_status || 'invoiced', row.id]
        );
      } else {
        db.prepare(
          `UPDATE dispatch_plans SET data_json=?, status=?, updated_at=datetime('now') WHERE id=?`
        ).run(merged, annotations.invoice_status || 'invoiced', row.id);
      }
      touched++;
    } catch (e) {
      console.warn('[v39 P9a] failed annotating dispatch_plan id', row.id, ':', e.message);
    }
  }
  if (touched > 0) console.log(`[v39 P9a] dispatch_plans annotated for batch ${batchNumber}: ${touched} rows`);
  return touched;
}

// GET /api/sap/config — fetch current SAP config (password always masked)
app.get('/api/sap/config', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const cfg = await sap.getConfig({ forceRefresh: true });
    // Mask password — never expose ciphertext or plaintext to client
    const safe = cfg ? {
      sap_url: cfg.sap_url || '',
      sap_username: cfg.sap_username || '',
      sap_company_db: cfg.sap_company_db || '',
      password_is_set: !!cfg.sap_password_encrypted,
      last_login_at: cfg.last_login_at || null,
      last_login_success: cfg.last_login_success ?? null,
      last_login_error: cfg.last_login_error || null,
      session_expires_at: cfg.session_expires_at || null,
      indent_poll_interval_minutes: cfg.indent_poll_interval_minutes || 5,
      invoice_poll_interval_minutes: cfg.invoice_poll_interval_minutes || 5,
      indent_poll_lookback_days: cfg.indent_poll_lookback_days || 30,
      invoice_poll_lookback_days: cfg.invoice_poll_lookback_days || 7,
      last_indent_poll_at: cfg.last_indent_poll_at || null,
      last_invoice_poll_at: cfg.last_invoice_poll_at || null,
      updated_at: cfg.updated_at || null,
      updated_by: cfg.updated_by || null,
    } : null;
    res.json({ ok: true, config: safe });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/sap/config — update SAP config (admin only). Password optional —
// if omitted/empty, existing encrypted password is preserved.
app.post('/api/sap/config', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const { url, username, companyDb, password, updatedBy } = req.body || {};
    if (url !== undefined && typeof url !== 'string') return res.status(400).json({ ok: false, error: 'url must be string' });
    if (username !== undefined && typeof username !== 'string') return res.status(400).json({ ok: false, error: 'username must be string' });
    if (companyDb !== undefined && typeof companyDb !== 'string') return res.status(400).json({ ok: false, error: 'companyDb must be string' });
    if (password !== undefined && typeof password !== 'string') return res.status(400).json({ ok: false, error: 'password must be string' });
    await sap.saveConfig({ url, username, companyDb, password, updatedBy });
    res.json({ ok: true, message: 'SAP config updated' });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/sap/test-connection — performs a Login + probe call
app.post('/api/sap/test-connection', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const r = await sap.testConnection();
    res.json(r);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/sap/audit-log?limit=100 — last N audit rows (admin only)
app.get('/api/sap/audit-log', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 100, 1000);
    const rows = await sap.getAuditLog({ limit });
    res.json({ ok: true, rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/sap/status — health for the topbar badge (all roles can read)
app.get('/api/sap/status', async (req, res) => {
  try {
    const s = await sap.getStatus();
    res.json({ ok: true, ...s });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/sap/refresh-indents — manual trigger to pull open Sales Orders
// from SAP and upsert into sap_indent_cache. Returns count of indents fetched.
app.post('/api/sap/refresh-indents', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const r = await _doRefreshSapIndents();
    res.json(r);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/sap/refresh-invoices — manual trigger to pull recent invoices from SAP
// and upsert into invoices_received. Returns count of new/updated invoices.
// Auto-matches Sunloc-originated invoices by U_SunlocBatch UDF.
app.post('/api/sap/refresh-invoices', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const r = await _doRefreshSapInvoices();
    res.json(r);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/sap/indents?status=unprocessed|all — read cached SAP indents.
// Backed by sap_indent_cache; no SAP roundtrip. The poller keeps this fresh
// every N min (default 5). Used by Planning App's Unplanned Orders page.
// Any logged-in role can read (planners need this).
app.get('/api/sap/indents', async (req, res) => {
  try {
    const filter = (req.query.status || 'unprocessed').toString();
    let rows;
    if (pgPool) {
      const sql = filter === 'all'
        ? `SELECT * FROM sap_indent_cache ORDER BY doc_due_date ASC NULLS LAST, fetched_at DESC LIMIT 500`
        : `SELECT * FROM sap_indent_cache WHERE processed_at IS NULL ORDER BY doc_due_date ASC NULLS LAST, fetched_at DESC LIMIT 500`;
      const r = await pgPool.query(sql);
      rows = r.rows;
    } else {
      const sql = filter === 'all'
        ? `SELECT * FROM sap_indent_cache ORDER BY doc_due_date ASC, fetched_at DESC LIMIT 500`
        : `SELECT * FROM sap_indent_cache WHERE processed_at IS NULL ORDER BY doc_due_date ASC, fetched_at DESC LIMIT 500`;
      rows = db.prepare(sql).all();
    }
    // Parse payload_json so client gets clean structured data
    const indents = rows.map(r => {
      let payload = null;
      try { payload = JSON.parse(r.payload_json); } catch {}
      return {
        sap_doc_entry: r.sap_doc_entry,
        sap_doc_num: r.sap_doc_num,
        card_code: r.card_code,
        card_name: r.card_name,
        doc_date: r.doc_date,
        doc_due_date: r.doc_due_date,
        total_lines: r.total_lines,
        total_qty: r.total_qty,
        fetched_at: r.fetched_at,
        processed_at: r.processed_at,
        processed_by: r.processed_by,
        processed_order_id: r.processed_order_id,
        DocumentLines: payload?.DocumentLines || [],
      };
    });
    // Also include sap config status so client can show "last poll" timestamp
    let lastPoll = null;
    try {
      const cfg = await sap.getConfig();
      lastPoll = cfg?.last_indent_poll_at || null;
    } catch {}
    res.json({ ok: true, count: indents.length, indents, last_indent_poll_at: lastPoll });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/sap/indents/:docEntry/processed — mark an indent as processed
// when planner assigns it to a machine. Stores who processed it and which
// production order id was created. Idempotent — repeat calls just update fields.
app.post('/api/sap/indents/:docEntry/processed', async (req, res) => {
  try {
    const docEntry = parseInt(req.params.docEntry, 10);
    if (!docEntry) return res.status(400).json({ ok: false, error: 'invalid docEntry' });
    const { processedBy, processedOrderId } = req.body || {};
    if (pgPool) {
      await pgPool.query(
        `UPDATE sap_indent_cache SET processed_at=NOW()::TEXT, processed_by=$1, processed_order_id=$2 WHERE sap_doc_entry=$3`,
        [processedBy || 'unknown', processedOrderId || null, docEntry]
      );
    } else {
      db.prepare(
        `UPDATE sap_indent_cache SET processed_at=datetime('now'), processed_by=?, processed_order_id=? WHERE sap_doc_entry=?`
      ).run(processedBy || 'unknown', processedOrderId || null, docEntry);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/sap/indents/:docEntry/unprocess — reverse the above (admin only).
// Used when planner deletes/voids the production order they created from this indent.
app.post('/api/sap/indents/:docEntry/unprocess', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const docEntry = parseInt(req.params.docEntry, 10);
    if (!docEntry) return res.status(400).json({ ok: false, error: 'invalid docEntry' });
    if (pgPool) {
      await pgPool.query(
        `UPDATE sap_indent_cache SET processed_at=NULL, processed_by=NULL, processed_order_id=NULL WHERE sap_doc_entry=$1`,
        [docEntry]
      );
    } else {
      db.prepare(
        `UPDATE sap_indent_cache SET processed_at=NULL, processed_by=NULL, processed_order_id=NULL WHERE sap_doc_entry=?`
      ).run(docEntry);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});


// ════════════════════════════════════════════════════════════════════
// v39 Phase 8: Invoice request endpoints
// ════════════════════════════════════════════════════════════════════
// /api/invoice/request — Dispatch Manager triggers SAP invoice creation
// /api/invoice/requests — list pending/sent invoice requests
// /api/invoice/received — list SAP-generated invoices (system of record)
// /api/invoice/by-batch/:batchNumber — combined view for one batch

// POST /api/invoice/request — Sunloc triggers SAP to create an A/R invoice.
// Body shape:
//   { batchNumber, customer, cardCode, poNumber, sapDocEntry, size, colour,
//     pcCode, boxes, qtyLakhs, selectionMode ('batch'|'truck'|'box'),
//     selectedLabels (array of label IDs), truckNumber, createdBy }
// Flow:
//   1. Insert row in invoice_requests with status='pending'
//   2. Call sap.createInvoice() which POSTs to SAP /b1s/v1/Invoices
//   3. Update row with SAP response (DocNum/DocEntry/IRN) and status='sent_to_sap'
//   4. On SAP error, update row with status='failed', store error message
//   5. Return { ok, request_id, sap_response, status }
// Idempotency: each call creates a new request. Caller is responsible for
//   guarding against double-submit on the client side.
app.post('/api/invoice/request', async (req, res) => {
  try {
    const body = req.body || {};
    const required = ['batchNumber', 'customer', 'boxes', 'qtyLakhs'];
    for (const f of required) {
      if (body[f] === undefined || body[f] === null || body[f] === '') {
        return res.status(400).json({ ok: false, error: `Missing required field: ${f}` });
      }
    }
    // SAP DocEntry is mandatory (per v39 spec — no SAP ref = can't invoice)
    if (!body.sapDocEntry) {
      return res.status(400).json({ ok: false, error: 'sapDocEntry required — cannot trigger SAP invoice without source SO reference' });
    }
    let id = 'invreq_' + crypto.randomBytes(8).toString('hex');
    const selectedLabelsJson = JSON.stringify(body.selectedLabels || []);
    // 1. Insert pending row
    try {
      if (pgPool) {
        await pgPool.query(`
          INSERT INTO invoice_requests (id, batch_number, customer, card_code, po_number,
            sap_doc_entry, size, colour, pc_code, boxes, qty_lakhs, rate_per_lakh,
            selected_labels, selection_mode, truck_number, status, created_by)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'pending',$16)
        `, [id, body.batchNumber, body.customer, body.cardCode || '', body.poNumber || '',
            body.sapDocEntry, body.size || '', body.colour || '', body.pcCode || '',
            parseInt(body.boxes) || 0, parseFloat(body.qtyLakhs) || 0, parseFloat(body.ratePerLakh) || 0,
            selectedLabelsJson, body.selectionMode || 'batch', body.truckNumber || null,
            body.createdBy || 'unknown']);
      } else {
        db.prepare(`
          INSERT INTO invoice_requests (id, batch_number, customer, card_code, po_number,
            sap_doc_entry, size, colour, pc_code, boxes, qty_lakhs, rate_per_lakh,
            selected_labels, selection_mode, truck_number, status, created_by)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?)
        `).run(id, body.batchNumber, body.customer, body.cardCode || '', body.poNumber || '',
            body.sapDocEntry, body.size || '', body.colour || '', body.pcCode || '',
            parseInt(body.boxes) || 0, parseFloat(body.qtyLakhs) || 0, parseFloat(body.ratePerLakh) || 0,
            selectedLabelsJson, body.selectionMode || 'batch', body.truckNumber || null,
            body.createdBy || 'unknown');
      }
    } catch (e) {
      return res.status(500).json({ ok: false, error: 'Failed to write invoice_requests row: ' + e.message });
    }
    // 2. v40 P18.2: Push to SAP — creates a DELIVERY (not an Invoice).
    // SAP user will then manually convert Delivery → A/R Invoice via Copy-To.
    // Sunloc's 5-min poller picks up the resulting Invoice and routes it to Tracking → Invoice Queue.
    // Note: invoice_requests table name retained for historical compat — rows now represent Delivery requests.
    const sapResult = await sap.createDelivery({
      cardCode: body.cardCode,
      baseDocEntry: parseInt(body.sapDocEntry),
      lines: [{
        lineNum: 0,
        quantity: parseFloat(body.qtyLakhs),
        itemCode: body.itemCode || null,
      }],
      batchNumber: body.batchNumber,
      poNumber: body.poNumber || '',
      remarks: `Sunloc dispatch — ${body.boxes} boxes`,
    });
    // 3+4. Update row with result
    try {
      if (sapResult.ok) {
        if (pgPool) {
          await pgPool.query(`
            UPDATE invoice_requests SET status='sent_to_sap',
              sap_response_doc_num=$1, sap_response_doc_entry=$2, sap_response_irn=$3,
              updated_at=NOW()::TEXT WHERE id=$4
          `, [String(sapResult.docNum || ''), sapResult.docEntry, sapResult.irn || null, id]);
        } else {
          db.prepare(`
            UPDATE invoice_requests SET status='sent_to_sap',
              sap_response_doc_num=?, sap_response_doc_entry=?, sap_response_irn=?,
              updated_at=datetime('now') WHERE id=?
          `).run(String(sapResult.docNum || ''), sapResult.docEntry, sapResult.irn || null, id);
        }
        return res.json({
          ok: true,
          request_id: id,
          status: 'sent_to_sap',
          sap_response: {
            docNum: sapResult.docNum,
            docEntry: sapResult.docEntry,
            irn: sapResult.irn,
          }
        });
      } else {
        // SAP rejected or unreachable
        const errMsg = sapResult.error || 'SAP call failed';
        if (pgPool) {
          await pgPool.query(`
            UPDATE invoice_requests SET status='failed',
              sap_error_message=$1, updated_at=NOW()::TEXT WHERE id=$2
          `, [errMsg.toString().substring(0, 1000), id]);
        } else {
          db.prepare(`
            UPDATE invoice_requests SET status='failed',
              sap_error_message=?, updated_at=datetime('now') WHERE id=?
          `).run(errMsg.toString().substring(0, 1000), id);
        }
        return res.json({
          ok: false,
          request_id: id,
          status: 'failed',
          error: errMsg,
          degraded: sapResult.degraded || false,
        });
      }
    } catch (e) {
      // Mark as failed so it doesn't stay stuck as pending
      try {
        if (pgPool) { await pgPool.query(`UPDATE invoice_requests SET status='failed', sap_error_message=$1 WHERE id=$2 AND status='pending'`, ['SAP/DB error: ' + e.message, id]); }
        else { db.prepare(`UPDATE invoice_requests SET status='failed', sap_error_message=? WHERE id=? AND status='pending'`).run('SAP/DB error: ' + e.message, id); }
      } catch(_) {}
      return res.status(500).json({ ok: false, error: 'SAP completed but DB update failed: ' + e.message, request_id: id });
    }
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// v40 Phase 18.1: POST /api/invoice/request-batch
// Consolidated multi-batch Delivery creation. Accepts array of batches, iterates
// server-side calling sap.createDelivery() for each. Returns per-batch results
// so the client can show per-row success/failure in the consolidated approval modal.
// Server-side validation re-enforces eligibility gates (defense in depth):
//   - Must have sapDocEntry + sapDocNum
//   - Must have boxes > 0 AND qtyLakhs > 0
//   - Must not already have a pending or received invoice
// Body: { batches: [{ batchNumber, customer, cardCode, poNumber, sapDocEntry, size, colour, pcCode, boxes, qtyLakhs, truckNumber, itemCode? }], createdBy, remarks }
app.post('/api/invoice/request-batch', async (req, res) => {
  try {
    const body = req.body || {};
    const batches = Array.isArray(body.batches) ? body.batches : [];
    if (batches.length === 0) {
      return res.status(400).json({ ok: false, error: 'batches array is empty or missing' });
    }
    if (batches.length > 50) {
      return res.status(400).json({ ok: false, error: 'too many batches in one request (max 50)' });
    }
    const results = [];
    for (const b of batches) {
      const batchRes = { batchNumber: b.batchNumber, ok: false };
      try {
        // Validate
        if (!b.batchNumber || !b.customer || !b.sapDocEntry) {
          batchRes.error = 'missing required fields (batchNumber/customer/sapDocEntry)';
          results.push(batchRes); continue;
        }
        if (!(parseInt(b.boxes) > 0) || !(parseFloat(b.qtyLakhs) > 0)) {
          batchRes.error = 'invalid boxes or qty (both must be > 0)';
          results.push(batchRes); continue;
        }
        // Check for existing pending/received invoice for this batch
        let existing;
        if (pgPool) {
          const ex = await pgPool.query(
            `SELECT id FROM invoice_requests WHERE batch_number=$1 AND status IN ('pending','sent_to_sap') LIMIT 1`,
            [b.batchNumber]
          );
          existing = ex.rows[0];
        } else {
          existing = db.prepare(`SELECT id FROM invoice_requests WHERE batch_number=? AND status IN ('pending','sent_to_sap') LIMIT 1`).get(b.batchNumber);
        }
        if (existing) {
          batchRes.error = 'already has a pending invoice request (id: ' + existing.id + ')';
          results.push(batchRes); continue;
        }
        // Insert pending row
        let id = 'invreq_' + crypto.randomBytes(8).toString('hex');
        const selectedLabelsJson = JSON.stringify(b.selectedLabels || []);
        if (pgPool) {
          await pgPool.query(`
            INSERT INTO invoice_requests (id, batch_number, customer, card_code, po_number,
              sap_doc_entry, size, colour, pc_code, boxes, qty_lakhs, rate_per_lakh,
              selected_labels, selection_mode, truck_number, status, created_by)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'pending',$16)
          `, [id, b.batchNumber, b.customer, b.cardCode || '', b.poNumber || '',
              b.sapDocEntry, b.size || '', b.colour || '', b.pcCode || '',
              parseInt(b.boxes) || 0, parseFloat(b.qtyLakhs) || 0, parseFloat(b.ratePerLakh) || 0,
              selectedLabelsJson, 'consolidated', b.truckNumber || null,
              body.createdBy || 'unknown']);
        } else {
          db.prepare(`
            INSERT INTO invoice_requests (id, batch_number, customer, card_code, po_number,
              sap_doc_entry, size, colour, pc_code, boxes, qty_lakhs, rate_per_lakh,
              selected_labels, selection_mode, truck_number, status, created_by)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?)
          `).run(id, b.batchNumber, b.customer, b.cardCode || '', b.poNumber || '',
              b.sapDocEntry, b.size || '', b.colour || '', b.pcCode || '',
              parseInt(b.boxes) || 0, parseFloat(b.qtyLakhs) || 0, parseFloat(b.ratePerLakh) || 0,
              selectedLabelsJson, 'consolidated', b.truckNumber || null,
              body.createdBy || 'unknown');
        }
        // Call SAP — creates a Delivery (not Invoice)
        const sapResult = await sap.createDelivery({
          cardCode: b.cardCode,
          baseDocEntry: parseInt(b.sapDocEntry),
          lines: [{
            lineNum: 0,
            quantity: parseFloat(b.qtyLakhs),
            itemCode: b.itemCode || null,
          }],
          batchNumber: b.batchNumber,
          poNumber: b.poNumber || '',
          remarks: body.remarks || `Sunloc consolidated dispatch — ${b.boxes} boxes`,
        });
        // Update row with result
        if (sapResult.ok) {
          if (pgPool) {
            await pgPool.query(`
              UPDATE invoice_requests SET status='sent_to_sap',
                sap_response_doc_num=$1, sap_response_doc_entry=$2,
                updated_at=NOW()::TEXT WHERE id=$3
            `, [String(sapResult.docNum || ''), sapResult.docEntry, id]);
          } else {
            db.prepare(`
              UPDATE invoice_requests SET status='sent_to_sap',
                sap_response_doc_num=?, sap_response_doc_entry=?,
                updated_at=datetime('now') WHERE id=?
            `).run(String(sapResult.docNum || ''), sapResult.docEntry, id);
          }
          batchRes.ok = true;
          batchRes.request_id = id;
          batchRes.delivery_doc_num = sapResult.docNum;
          batchRes.delivery_doc_entry = sapResult.docEntry;
        } else {
          // Mark request as failed (preserve for audit / retry)
          const errMsg = sapResult.error || 'SAP rejected';
          if (pgPool) {
            await pgPool.query(`
              UPDATE invoice_requests SET status='failed', sap_error_message=$1, updated_at=NOW()::TEXT WHERE id=$2
            `, [errMsg, id]);
          } else {
            db.prepare(`
              UPDATE invoice_requests SET status='failed', sap_error_message=?, updated_at=datetime('now') WHERE id=?
            `).run(errMsg, id);
          }
          batchRes.error = errMsg;
          batchRes.degraded = sapResult.degraded || false;
          batchRes.request_id = id;
        }
      } catch (e) {
        batchRes.error = 'server error: ' + e.message;
        // CRITICAL: update DB row to failed so it doesn't stay stuck as 'pending'
        try {
          const errMsg2 = 'server error: ' + e.message;
          if (pgPool) {
            await pgPool.query(`UPDATE invoice_requests SET status='failed', sap_error_message=$1, updated_at=NOW()::TEXT WHERE id=$2`, [errMsg2, id]);
          } else {
            db.prepare(`UPDATE invoice_requests SET status='failed', sap_error_message=?, updated_at=datetime('now') WHERE id=?`).run(errMsg2, id);
          }
        } catch (dbErr) { console.warn('[invoice] failed to update failed status:', dbErr.message); }
      }
      results.push(batchRes);
    }
    const okCount = results.filter(r => r.ok).length;
    const failCount = results.length - okCount;
    res.json({ ok: okCount > 0, results, ok_count: okCount, fail_count: failCount });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/invoice/requests?status=&batch= — list invoice requests (filters optional)
app.get('/api/invoice/requests', async (req, res) => {
  try {
    const status = (req.query.status || '').toString();
    const batch = (req.query.batch || '').toString();
    const limit = Math.min(parseInt(req.query.limit, 10) || 200, 1000);
    const wheres = [];
    const args = [];
    if (status) { wheres.push(pgPool ? `status = $${args.length+1}` : 'status = ?'); args.push(status); }
    if (batch) { wheres.push(pgPool ? `batch_number = $${args.length+1}` : 'batch_number = ?'); args.push(batch); }
    const whereSql = wheres.length ? 'WHERE ' + wheres.join(' AND ') : '';
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM invoice_requests ${whereSql} ORDER BY created_at DESC LIMIT ${limit}`, args);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM invoice_requests ${whereSql} ORDER BY created_at DESC LIMIT ${limit}`).all(...args);
    }
    res.json({ ok: true, count: rows.length, requests: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/invoice/request/:id/cancel — cancel a pending/failed invoice request
app.post('/api/invoice/request/:id/cancel', async (req, res) => {
  try {
    const id = req.params.id;
    const reason = (req.body || {}).reason || 'manual cancel';
    let row;
    if (pgPool) {
      const r = await pgPool.query(`SELECT id, status FROM invoice_requests WHERE id=$1`, [id]);
      row = r.rows[0];
    } else {
      row = db.prepare(`SELECT id, status FROM invoice_requests WHERE id=?`).get(id);
    }
    if (!row) return res.status(404).json({ ok: false, error: 'Invoice request not found' });
    if (['sent_to_sap','invoice_received','delivered'].includes(row.status)) {
      return res.status(400).json({ ok: false, error: `Cannot cancel request in status: ${row.status}` });
    }
    if (pgPool) {
      await pgPool.query(`UPDATE invoice_requests SET status='cancelled', sap_error_message=$1, updated_at=NOW()::TEXT WHERE id=$2`, [reason, id]);
    } else {
      db.prepare(`UPDATE invoice_requests SET status='cancelled', sap_error_message=?, updated_at=datetime('now') WHERE id=?`).run(reason, id);
    }
    res.json({ ok: true, cancelled: id });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/invoice/received — list received invoices (with optional filters)
app.get('/api/invoice/received', async (req, res) => {
  try {
    const status = (req.query.status || '').toString();
    const batch = (req.query.batch || '').toString();
    const customer = (req.query.customer || '').toString();
    const fromDate = (req.query.from_date || '').toString();
    const toDate = (req.query.to_date || '').toString();
    const limit = Math.min(parseInt(req.query.limit, 10) || 200, 1000);
    const wheres = [];
    const args = [];
    if (status) { wheres.push(pgPool ? `dispatch_status = $${args.length+1}` : 'dispatch_status = ?'); args.push(status); }
    if (batch) { wheres.push(pgPool ? `batch_number = $${args.length+1}` : 'batch_number = ?'); args.push(batch); }
    if (customer) { wheres.push(pgPool ? `customer ILIKE $${args.length+1}` : 'customer LIKE ?'); args.push('%' + customer + '%'); }
    if (fromDate) { wheres.push(pgPool ? `invoice_date >= $${args.length+1}` : 'invoice_date >= ?'); args.push(fromDate); }
    if (toDate) { wheres.push(pgPool ? `invoice_date <= $${args.length+1}` : 'invoice_date <= ?'); args.push(toDate); }
    const whereSql = wheres.length ? 'WHERE ' + wheres.join(' AND ') : '';
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM invoices_received ${whereSql} ORDER BY invoice_date DESC, fetched_at DESC LIMIT ${limit}`, args);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM invoices_received ${whereSql} ORDER BY invoice_date DESC, fetched_at DESC LIMIT ${limit}`).all(...args);
    }
    res.json({ ok: true, count: rows.length, invoices: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/invoice/by-batch/:batchNumber — combined view for a specific batch
// Returns { requests: [...], received: [...] } so client can show full state.
app.get('/api/invoice/by-batch/:batchNumber', async (req, res) => {
  try {
    const batch = req.params.batchNumber;
    let requests, received;
    if (pgPool) {
      const r1 = await pgPool.query(`SELECT * FROM invoice_requests WHERE batch_number=$1 ORDER BY created_at DESC`, [batch]);
      const r2 = await pgPool.query(`SELECT * FROM invoices_received WHERE batch_number=$1 ORDER BY invoice_date DESC`, [batch]);
      requests = r1.rows; received = r2.rows;
    } else {
      requests = db.prepare(`SELECT * FROM invoice_requests WHERE batch_number=? ORDER BY created_at DESC`).all(batch);
      received = db.prepare(`SELECT * FROM invoices_received WHERE batch_number=? ORDER BY invoice_date DESC`).all(batch);
    }
    res.json({ ok: true, requests, received });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/invoice/:id/approve-direct-sap — admin approves a direct_sap invoice
// (an invoice that arrived from SAP without a matching Sunloc request).
// Optionally attaches batch_number so the invoice enters the normal dispatch
// flow. Body: { batchNumber?, remarks? }
app.post('/api/invoice/:id/approve-direct-sap', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const id = req.params.id;
    const { batchNumber, remarks } = req.body || {};
    const approver = (req.headers['x-sunloc-user'] || 'admin').toString();
    let row;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM invoices_received WHERE id=$1`, [id]);
      row = r.rows[0];
    } else {
      row = db.prepare(`SELECT * FROM invoices_received WHERE id=?`).get(id);
    }
    if (!row) return res.status(404).json({ ok: false, error: 'Invoice not found' });
    if (row.source !== 'direct_sap') {
      return res.status(400).json({ ok: false, error: 'Only direct_sap invoices need admin approval' });
    }
    // Build update — admin_approved_at, optional batch + remarks
    const setParts = [];
    const args = [];
    if (pgPool) {
      setParts.push(`admin_approved_at=NOW()::TEXT`);
      setParts.push(`admin_approved_by=$${args.length+1}`); args.push(approver);
      if (batchNumber) { setParts.push(`batch_number=$${args.length+1}`); args.push(batchNumber); }
      if (remarks)     { setParts.push(`remarks=$${args.length+1}`);      args.push(remarks); }
      args.push(id);
      await pgPool.query(`UPDATE invoices_received SET ${setParts.join(', ')} WHERE id=$${args.length}`, args);
    } else {
      setParts.push(`admin_approved_at=datetime('now')`);
      setParts.push(`admin_approved_by=?`); args.push(approver);
      if (batchNumber) { setParts.push(`batch_number=?`); args.push(batchNumber); }
      if (remarks)     { setParts.push(`remarks=?`);      args.push(remarks); }
      args.push(id);
      db.prepare(`UPDATE invoices_received SET ${setParts.join(', ')} WHERE id=?`).run(...args);
    }
    // If batch attached, also annotate dispatch_plans
    if (batchNumber) {
      try {
        await _v39_updateDispatchPlansForInvoice(batchNumber, {
          invoice_doc_num: row.sap_doc_num,
          invoice_doc_entry: row.sap_doc_entry,
          invoice_irn: row.irn,
          invoice_status: 'invoiced',
          invoice_received_at: row.fetched_at || new Date().toISOString(),
          invoice_admin_approved: true,
        });
      } catch (e) {
        console.warn('[v39 P9a] post-approve annotate error:', e.message);
      }
    }
    res.json({ ok: true, approved_at: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/invoice/:id/dispatch-out — complete the Scan Out activity for an
// invoice. Called by the Tracking App's new Scan Out panel when:
//   - all boxes expected on the invoice have been scanned, AND
//   - quantities match the invoice's expected total.
// Creates a tracking_dispatch_records row and marks the invoice dispatched.
// Body: { vehicleNo, dispatchedBy, scannedLabelIds: [], remarks }
// Idempotent: if the invoice is already dispatched, returns 409 with the
// existing record id so the client can re-sync state without erroring.
app.post('/api/invoice/:id/dispatch-out', async (req, res) => {
  try {
    const invId = req.params.id;
    const { vehicleNo, dispatchedBy, scannedLabelIds, remarks } = req.body || {};
    // Load invoice
    let inv;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM invoices_received WHERE id=$1`, [invId]);
      inv = r.rows[0];
    } else {
      inv = db.prepare(`SELECT * FROM invoices_received WHERE id=?`).get(invId);
    }
    if (!inv) return res.status(404).json({ ok: false, error: 'Invoice not found' });
    if (!inv.batch_number) {
      return res.status(400).json({ ok: false, error: 'Invoice has no batch_number — admin must approve/attach a batch first' });
    }
    // Block re-dispatch
    if (inv.dispatch_status === 'dispatched') {
      return res.status(409).json({
        ok: false,
        error: 'Invoice already dispatched',
        already_dispatched: true,
        dispatch_record_id: inv.dispatch_record_id || null,
      });
    }
    // direct_sap invoices must be admin-approved before dispatch-out
    if (inv.source === 'direct_sap' && !inv.admin_approved_at) {
      return res.status(403).json({ ok: false, error: 'Direct-SAP invoice must be admin-approved before dispatch-out' });
    }
    // Build dispatch record
    const recId = 'disprec_' + crypto.randomBytes(6).toString('hex');
    const qty = parseFloat(inv.total_amount) > 0
      ? (() => {
          // qty in Lakhs: derive from invoice taxable_amount / rate if available,
          // else fall back to total_boxes × pack-size lookup (approximate).
          // Most accurate path: use total_boxes since SAP invoice line shows boxes.
          // For ledger we record the Lakhs equivalent for downstream reports.
          return parseFloat(inv.total_boxes) > 0 ? parseFloat(inv.total_boxes) / 100 : 0;
        })()
      : 0;
    const boxes = parseInt(inv.total_boxes) || 0;
    const ts = new Date().toISOString();
    // Insert dispatch record
    try {
      if (pgPool) {
        await pgPool.query(
          `INSERT INTO tracking_dispatch_records (id, batch_number, customer, qty, boxes, vehicle_no, invoice_no, remarks, ts, "by")
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
          [recId, inv.batch_number, inv.customer || '', qty, boxes, vehicleNo || '', inv.sap_doc_num || '', remarks || '', ts, dispatchedBy || 'unknown']
        );
      } else {
        db.prepare(
          `INSERT INTO tracking_dispatch_records (id, batch_number, customer, qty, boxes, vehicle_no, invoice_no, remarks, ts, by)
           VALUES (?,?,?,?,?,?,?,?,?,?)`
        ).run(recId, inv.batch_number, inv.customer || '', qty, boxes, vehicleNo || '', inv.sap_doc_num || '', remarks || '', ts, dispatchedBy || 'unknown');
      }
    } catch (e) {
      return res.status(500).json({ ok: false, error: 'Failed to write dispatch record: ' + e.message });
    }
    // Mark invoice dispatched
    try {
      if (pgPool) {
        await pgPool.query(
          `UPDATE invoices_received SET dispatch_status='dispatched', dispatched_at=$1, dispatched_by=$2, vehicle_no=$3, dispatch_record_id=$4 WHERE id=$5`,
          [ts, dispatchedBy || 'unknown', vehicleNo || '', recId, invId]
        );
      } else {
        db.prepare(
          `UPDATE invoices_received SET dispatch_status='dispatched', dispatched_at=?, dispatched_by=?, vehicle_no=?, dispatch_record_id=? WHERE id=?`
        ).run(ts, dispatchedBy || 'unknown', vehicleNo || '', recId, invId);
      }
    } catch (e) {
      console.warn('[v39 P10a] invoice update failed (record was created):', e.message);
    }
    // Recompute dispatch_actuals for this batch — keeps downstream summaries fresh
    try {
      if (typeof _recomputeDispatchActuals === 'function') {
        await _recomputeDispatchActuals(inv.batch_number, vehicleNo || null, inv.sap_doc_num || null);
      }
    } catch (e) {
      console.warn('[v39 P10a] dispatch_actuals recompute failed:', e.message);
    }
    // Annotate dispatch_plans
    try {
      await _v39_updateDispatchPlansForInvoice(inv.batch_number, {
        invoice_dispatched_at: ts,
        invoice_dispatched_by: dispatchedBy || 'unknown',
        invoice_status: 'dispatched',
      });
    } catch (e) {
      console.warn('[v39 P10a] dispatch_plans annotate failed:', e.message);
    }
    res.json({
      ok: true,
      dispatch_record_id: recId,
      invoice_id: invId,
      batch_number: inv.batch_number,
      boxes,
      qty,
      ts,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ─── v40 Phase 18.11: Truck-level scan-out session endpoints ───────────
// Workers scanning multiple batches in one truck need their progress preserved
// across browser close, modal close, or being called away. These endpoints
// persist the scan session keyed by truck_number.

// GET — load session state (or return empty session if none exists)
app.get('/api/invoice/truck-scan-session/:truckNumber', async (req, res) => {
  try {
    const tn = String(req.params.truckNumber);
    let row;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM truck_scan_session_state WHERE truck_number=$1`, [tn]);
      row = r.rows[0];
    } else {
      row = db.prepare(`SELECT * FROM truck_scan_session_state WHERE truck_number=?`).get(tn);
    }
    if (!row) return res.json({ ok: true, exists: false });
    res.json({
      ok: true,
      exists: true,
      session: {
        truckNumber: tn,
        invoiceIds: JSON.parse(row.invoice_ids_json || '[]'),
        scannedLabels: JSON.parse(row.scanned_labels_json || '[]'),
        vehicleNo: row.vehicle_no || '',
        lrNo: row.lr_no || '',
        remarks: row.remarks || '',
        startedBy: row.started_by || '',
        startedAt: row.started_at,
        lastUpdatedAt: row.last_updated_at,
      },
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// PUT — upsert session state. Body: { invoiceIds, scannedLabels, vehicleNo, lrNo, remarks, startedBy }
app.put('/api/invoice/truck-scan-session/:truckNumber', async (req, res) => {
  try {
    const tn = String(req.params.truckNumber);
    const body = req.body || {};
    const invoiceIdsJson = JSON.stringify(body.invoiceIds || []);
    const scannedLabelsJson = JSON.stringify(body.scannedLabels || []);
    const vehicleNo = String(body.vehicleNo || '');
    const lrNo = String(body.lrNo || '');
    const remarks = String(body.remarks || '');
    const startedBy = String(body.startedBy || 'unknown');
    const now = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO truck_scan_session_state (truck_number, invoice_ids_json, scanned_labels_json, vehicle_no, lr_no, remarks, started_by, started_at, last_updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8)
         ON CONFLICT (truck_number) DO UPDATE SET
           invoice_ids_json=EXCLUDED.invoice_ids_json,
           scanned_labels_json=EXCLUDED.scanned_labels_json,
           vehicle_no=EXCLUDED.vehicle_no,
           lr_no=EXCLUDED.lr_no,
           remarks=EXCLUDED.remarks,
           last_updated_at=EXCLUDED.last_updated_at`,
        [tn, invoiceIdsJson, scannedLabelsJson, vehicleNo, lrNo, remarks, startedBy, now]
      );
    } else {
      // SQLite uses ON CONFLICT for upsert
      db.prepare(
        `INSERT INTO truck_scan_session_state (truck_number, invoice_ids_json, scanned_labels_json, vehicle_no, lr_no, remarks, started_by, started_at, last_updated_at)
         VALUES (?,?,?,?,?,?,?,?,?)
         ON CONFLICT (truck_number) DO UPDATE SET
           invoice_ids_json=excluded.invoice_ids_json,
           scanned_labels_json=excluded.scanned_labels_json,
           vehicle_no=excluded.vehicle_no,
           lr_no=excluded.lr_no,
           remarks=excluded.remarks,
           last_updated_at=excluded.last_updated_at`
      ).run(tn, invoiceIdsJson, scannedLabelsJson, vehicleNo, lrNo, remarks, startedBy, now, now);
    }
    res.json({ ok: true, truckNumber: tn, lastUpdatedAt: now });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// DELETE — discard session (after successful dispatch or explicit abandon)
app.delete('/api/invoice/truck-scan-session/:truckNumber', async (req, res) => {
  try {
    const tn = String(req.params.truckNumber);
    if (pgPool) {
      await pgPool.query(`DELETE FROM truck_scan_session_state WHERE truck_number=$1`, [tn]);
    } else {
      db.prepare(`DELETE FROM truck_scan_session_state WHERE truck_number=?`).run(tn);
    }
    res.json({ ok: true, truckNumber: tn, deleted: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// v40 Phase 18.12: GET /api/invoice/active-scan-sessions — list all in-progress
// truck scan sessions (sessions updated within the last 24h). Used by Planning's
// invoice-state loader to mark batches whose dispatch worker is mid-scan, surfacing
// the "🔍 Scanning" badge in Order View.
app.get('/api/invoice/active-scan-sessions', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(
        `SELECT truck_number, invoice_ids_json, scanned_labels_json, last_updated_at
         FROM truck_scan_session_state
         WHERE last_updated_at > (NOW() - interval '24 hours')::TEXT`
      );
      rows = r.rows;
    } else {
      rows = db.prepare(
        `SELECT truck_number, invoice_ids_json, scanned_labels_json, last_updated_at
         FROM truck_scan_session_state
         WHERE last_updated_at > datetime('now','-24 hours')`
      ).all();
    }
    // Flatten: return a map of invoiceId → { truckNumber, scannedCount, lastUpdatedAt }
    const byInvoice = {};
    for (const row of rows) {
      let invoiceIds = [];
      let scannedLabels = [];
      try { invoiceIds = JSON.parse(row.invoice_ids_json || '[]'); } catch {}
      try { scannedLabels = JSON.parse(row.scanned_labels_json || '[]'); } catch {}
      // scannedLabels is array of { invoiceId, labelId, batchNumber, ts } — count by invoice
      const scanCountByInv = {};
      for (const s of scannedLabels) {
        if (s.invoiceId) scanCountByInv[s.invoiceId] = (scanCountByInv[s.invoiceId] || 0) + 1;
      }
      for (const invId of invoiceIds) {
        byInvoice[invId] = {
          truckNumber: row.truck_number,
          scannedCount: scanCountByInv[invId] || 0,
          lastUpdatedAt: row.last_updated_at,
        };
      }
    }
    res.json({ ok: true, sessions: byInvoice });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/invoice/dispatch-out-truck — atomic per-batch processing.
// Body: { truckNumber, vehicleNo, lrNo, remarks, dispatchedBy, batches: [{ invoiceId, scannedBoxes: <count> }] }
// Processes each batch individually using the same logic as /api/invoice/:id/dispatch-out.
// Returns per-batch results — partial failures are surfaced so the worker can retry just the failed batches.
app.post('/api/invoice/dispatch-out-truck', async (req, res) => {
  try {
    const body = req.body || {};
    const truckNumber = String(body.truckNumber || '');
    const vehicleNo = String(body.vehicleNo || '');
    const lrNo = String(body.lrNo || '');
    const remarks = String(body.remarks || '');
    const dispatchedBy = String(body.dispatchedBy || 'unknown');
    const batches = Array.isArray(body.batches) ? body.batches : [];
    if (!truckNumber) return res.status(400).json({ ok: false, error: 'truckNumber required' });
    if (!vehicleNo) return res.status(400).json({ ok: false, error: 'vehicleNo required' });
    if (!lrNo) return res.status(400).json({ ok: false, error: 'lrNo required' });
    if (batches.length === 0) return res.status(400).json({ ok: false, error: 'No batches to dispatch' });
    if (batches.length > 50) return res.status(400).json({ ok: false, error: 'Too many batches in one truck (max 50)' });

    const results = [];
    for (const b of batches) {
      const invId = b.invoiceId;
      const scannedBoxes = parseInt(b.scannedBoxes) || 0;
      if (!invId) {
        results.push({ invoiceId: null, ok: false, error: 'Missing invoiceId' });
        continue;
      }
      // Load invoice
      let inv;
      try {
        if (pgPool) {
          const r = await pgPool.query(`SELECT * FROM invoices_received WHERE id=$1`, [invId]);
          inv = r.rows[0];
        } else {
          inv = db.prepare(`SELECT * FROM invoices_received WHERE id=?`).get(invId);
        }
      } catch (e) {
        results.push({ invoiceId: invId, ok: false, error: 'DB read failed: ' + e.message });
        continue;
      }
      if (!inv) {
        results.push({ invoiceId: invId, ok: false, error: 'Invoice not found' });
        continue;
      }
      if (inv.dispatch_status === 'dispatched' || inv.dispatch_status === 'deemed_dispatched') {
        results.push({ invoiceId: invId, ok: false, error: 'Already dispatched', alreadyDispatched: true });
        continue;
      }
      if (!inv.batch_number) {
        results.push({ invoiceId: invId, ok: false, error: 'No batch_number on invoice — admin must attach first' });
        continue;
      }
      // Build dispatch record
      const recId = 'tdr_' + crypto.randomBytes(8).toString('hex');
      const qty = parseFloat(inv.total_qty_lakhs) > 0
        ? parseFloat(inv.total_qty_lakhs)
        : (parseInt(inv.total_boxes) > 0 ? parseInt(inv.total_boxes) / 100 : 0);
      const boxes = scannedBoxes || parseInt(inv.total_boxes) || 0;
      const ts = new Date().toISOString();
      // Insert dispatch record
      try {
        if (pgPool) {
          await pgPool.query(
            `INSERT INTO tracking_dispatch_records (id, batch_number, customer, qty, boxes, vehicle_no, invoice_no, remarks, ts, "by")
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
            [recId, inv.batch_number, inv.customer || '', qty, boxes, vehicleNo, inv.sap_doc_num || '', remarks, ts, dispatchedBy]
          );
        } else {
          db.prepare(
            `INSERT INTO tracking_dispatch_records (id, batch_number, customer, qty, boxes, vehicle_no, invoice_no, remarks, ts, by)
             VALUES (?,?,?,?,?,?,?,?,?,?)`
          ).run(recId, inv.batch_number, inv.customer || '', qty, boxes, vehicleNo, inv.sap_doc_num || '', remarks, ts, dispatchedBy);
        }
      } catch (e) {
        results.push({ invoiceId: invId, ok: false, error: 'Insert dispatch record failed: ' + e.message });
        continue;
      }
      // Mark invoice dispatched. Also stamp lr_no via remarks suffix since invoices_received has no lr_no col.
      try {
        if (pgPool) {
          await pgPool.query(
            `UPDATE invoices_received SET dispatch_status='dispatched', dispatched_at=$1, dispatched_by=$2, vehicle_no=$3, dispatch_record_id=$4 WHERE id=$5`,
            [ts, dispatchedBy, vehicleNo, recId, invId]
          );
        } else {
          db.prepare(
            `UPDATE invoices_received SET dispatch_status='dispatched', dispatched_at=?, dispatched_by=?, vehicle_no=?, dispatch_record_id=? WHERE id=?`
          ).run(ts, dispatchedBy, vehicleNo, recId, invId);
        }
      } catch (e) {
        console.warn('[v40 P18.11] invoice update failed (record was created):', e.message);
      }
      // Recompute dispatch actuals
      try {
        if (typeof _recomputeDispatchActuals === 'function') {
          await _recomputeDispatchActuals(inv.batch_number, vehicleNo, inv.sap_doc_num || null);
        }
      } catch (e) {
        console.warn('[v40 P18.11] _recomputeDispatchActuals failed:', e.message);
      }
      // Annotate dispatch_plans
      try {
        await _v39_updateDispatchPlansForInvoice(inv.batch_number, {
          invoice_dispatched_at: ts,
          invoice_dispatched_by: dispatchedBy,
          invoice_status: 'dispatched',
        });
      } catch (e) {
        console.warn('[v40 P18.11] dispatch_plans annotate failed:', e.message);
      }
      results.push({
        invoiceId: invId,
        ok: true,
        dispatchRecordId: recId,
        batchNumber: inv.batch_number,
        boxes,
        qty,
        ts,
      });
    }
    // Clear session once truck dispatch attempted (whether all succeeded or not).
    // Worker can retry just failed batches via single-batch dispatch-out endpoint.
    try {
      if (pgPool) {
        await pgPool.query(`DELETE FROM truck_scan_session_state WHERE truck_number=$1`, [truckNumber]);
      } else {
        db.prepare(`DELETE FROM truck_scan_session_state WHERE truck_number=?`).run(truckNumber);
      }
    } catch (e) {
      console.warn('[v40 P18.11] session cleanup failed:', e.message);
    }
    const okCount = results.filter(r => r.ok).length;
    res.json({
      ok: okCount > 0,
      truckNumber,
      totalRequested: batches.length,
      successCount: okCount,
      failureCount: batches.length - okCount,
      results,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/invoice/pending-scan-out — list invoices ready for the Scan Out
// activity. Returns invoices_received where dispatch_status='pending' and
// either source='sunloc' or (source='direct_sap' AND admin_approved_at IS NOT NULL).
// Used by the Tracking App's Invoice Queue panel.
app.get('/api/invoice/pending-scan-out', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    let rows;
    if (pgPool) {
      const r = await pgPool.query(
        `SELECT * FROM invoices_received
         WHERE dispatch_status = 'pending'
           AND (source = 'sunloc' OR (source = 'direct_sap' AND admin_approved_at IS NOT NULL))
         ORDER BY invoice_date DESC, fetched_at DESC
         LIMIT ${limit}`
      );
      rows = r.rows;
    } else {
      rows = db.prepare(
        `SELECT * FROM invoices_received
         WHERE dispatch_status = 'pending'
           AND (source = 'sunloc' OR (source = 'direct_sap' AND admin_approved_at IS NOT NULL))
         ORDER BY invoice_date DESC, fetched_at DESC
         LIMIT ?`
      ).all(limit);
    }
    // v40 Phase 18.5: enrich each invoice with truck_number looked up from
    // dispatch_plans by batch_number. Falls back to null if no plan found.
    const batchNumbers = [...new Set(rows.map(r => r.batch_number).filter(Boolean))];
    const truckByBatch = {};
    if (batchNumbers.length > 0) {
      try {
        let planRows;
        if (pgPool) {
          const r = await pgPool.query(
            `SELECT data_json FROM dispatch_plans WHERE deleted=false AND batch_number = ANY($1)`,
            [batchNumbers]
          );
          planRows = r.rows.map(r => typeof r.data_json === 'string' ? JSON.parse(r.data_json) : r.data_json);
        } else {
          // SQLite — fallback to per-batch query; small N so cheap
          const stmt = db.prepare(`SELECT data_json FROM dispatch_plans WHERE deleted=0 AND batch_number=?`);
          planRows = batchNumbers.flatMap(bn => stmt.all(bn).map(r => JSON.parse(r.data_json)));
        }
        // For each plan with a truck assigned, record first match per batch
        for (const plan of planRows) {
          const bn = plan.batchNumber || '';
          const tn = plan.truckNumber || plan.truck_number || null;
          if (bn && tn && !truckByBatch[bn]) truckByBatch[bn] = tn;
        }
      } catch (e) {
        console.warn('[v40 P18.5] truck lookup failed:', e.message);
      }
    }
    // Annotate response rows
    for (const r of rows) {
      if (r.batch_number && truckByBatch[r.batch_number]) {
        r.suggested_truck_number = truckByBatch[r.batch_number];
      }
    }
    res.json({ ok: true, count: rows.length, invoices: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// v40 Phase 18.7: GET /api/invoice/:id/details
// Returns the parsed Sales Register structure for an invoice — header fields plus
// DocumentLines with ItemCode/Description/Qty/UnitPrice/LineTotal/VAT. Powers
// Scan-Out matching (Phase 18.8), close-the-loop register link (Phase 18.9), and
// the Tracking → Invoice detail view.
app.get('/api/invoice/:id/details', async (req, res) => {
  try {
    const id = req.params.id;
    let row;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM invoices_received WHERE id=$1`, [id]);
      row = r.rows[0];
    } else {
      row = db.prepare(`SELECT * FROM invoices_received WHERE id=?`).get(id);
    }
    if (!row) return res.status(404).json({ ok: false, error: 'invoice not found' });
    // Parse the SAP payload — payload_json holds the full SAP Invoice response
    let payload = {};
    try {
      payload = typeof row.payload_json === 'string' ? JSON.parse(row.payload_json) : (row.payload_json || {});
    } catch (e) { payload = {}; }
    // Header: DocNum, Customer, addresses, dates, totals
    const header = {
      invoice_id: row.id,
      sap_doc_entry: row.sap_doc_entry,
      sap_doc_num: row.sap_doc_num,
      customer: row.customer,
      card_code: row.card_code,
      po_number: row.po_number,
      batch_number: row.batch_number,
      invoice_date: row.invoice_date,
      due_date: payload.DocDueDate || null,
      bill_to_address: payload.Address || '',
      ship_to_address: payload.Address2 || '',
      sales_order_ref: payload.U_SunlocPO || '',
      taxable_amount: parseFloat(row.taxable_amount) || 0,
      igst_amount: parseFloat(row.igst_amount) || 0,
      total_amount: parseFloat(row.total_amount) || 0,
      irn: row.irn || '',
      source: row.source,
      dispatch_status: row.dispatch_status,
      comments: payload.Comments || '',
    };
    // Lines: ItemCode (PC Code), ItemDescription, Quantity, UnitPrice, LineTotal, VAT%, VAT amount
    const lines = (payload.DocumentLines || []).map((ln, idx) => ({
      line_num: ln.LineNum != null ? ln.LineNum : idx,
      item_code: ln.ItemCode || '',
      item_description: ln.ItemDescription || '',
      quantity: parseFloat(ln.Quantity) || 0,
      unit_price: parseFloat(ln.UnitPrice) || 0,
      line_total: parseFloat(ln.LineTotal) || 0,
      vat_percent: parseFloat(ln.VatPercent || ln.TaxPercentagePerRow) || 0,
      vat_amount: parseFloat(ln.VatSum) || 0,
      base_entry: ln.BaseEntry || null,   // Sales Order reference
      base_line: ln.BaseLine,
    }));
    res.json({ ok: true, header, lines, line_count: lines.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// v39 Phase 16: POST /api/invoice/:id/deemed-scan-out
// Admin marks an invoice as dispatched without physical box scanning. Used for:
//   - Documentation-only dispatches
//   - Remote-warehouse handoffs where boxes never pass through Sunloc dispatch
//   - Legacy/manual reconciliation cases
// Records reason + admin user for audit. Creates a tracking_dispatch_records
// entry tagged with "DEEMED: <reason>" so downstream reports surface it.
// Idempotency: 409 if already dispatched.
// Body: { reason (required), vehicleNo (optional), remarks (optional) }
app.post('/api/invoice/:id/deemed-scan-out', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const invId = req.params.id;
    const { reason, vehicleNo, remarks } = req.body || {};
    if (!reason || !reason.trim()) {
      return res.status(400).json({ ok: false, error: 'reason is required for deemed scan-out' });
    }
    const admin = (req.headers['x-sunloc-user'] || 'admin').toString();
    // Load invoice
    let inv;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM invoices_received WHERE id=$1`, [invId]);
      inv = r.rows[0];
    } else {
      inv = db.prepare(`SELECT * FROM invoices_received WHERE id=?`).get(invId);
    }
    if (!inv) return res.status(404).json({ ok: false, error: 'Invoice not found' });
    if (!inv.batch_number) {
      return res.status(400).json({ ok: false, error: 'Invoice has no batch_number — admin must attach one first via approve-direct-sap' });
    }
    if (inv.dispatch_status === 'dispatched') {
      return res.status(409).json({
        ok: false,
        error: 'Invoice already dispatched',
        already_dispatched: true,
        dispatch_record_id: inv.dispatch_record_id || null,
      });
    }
    const recId = 'disprec_deemed_' + crypto.randomBytes(6).toString('hex');
    const boxes = parseInt(inv.total_boxes) || 0;
    const qtyLakhs = parseFloat(inv.total_qty_lakhs) > 0
      ? parseFloat(inv.total_qty_lakhs)
      : (boxes > 0 ? boxes / 100 : 0);
    const ts = new Date().toISOString();
    const dispatchRemarks = `DEEMED: ${reason}${remarks ? ' | ' + remarks : ''}`;
    // Insert dispatch record
    try {
      if (pgPool) {
        await pgPool.query(
          `INSERT INTO tracking_dispatch_records (id, batch_number, customer, qty, boxes, vehicle_no, invoice_no, remarks, ts, "by")
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
          [recId, inv.batch_number, inv.customer || '', qtyLakhs, boxes, vehicleNo || '', inv.sap_doc_num || '', dispatchRemarks, ts, admin]
        );
      } else {
        db.prepare(
          `INSERT INTO tracking_dispatch_records (id, batch_number, customer, qty, boxes, vehicle_no, invoice_no, remarks, ts, by)
           VALUES (?,?,?,?,?,?,?,?,?,?)`
        ).run(recId, inv.batch_number, inv.customer || '', qtyLakhs, boxes, vehicleNo || '', inv.sap_doc_num || '', dispatchRemarks, ts, admin);
      }
    } catch (e) {
      return res.status(500).json({ ok: false, error: 'Failed to write dispatch record: ' + e.message });
    }
    // Mark invoice deemed + dispatched
    try {
      if (pgPool) {
        await pgPool.query(
          `UPDATE invoices_received SET
              dispatch_status='dispatched',
              dispatched_at=$1,
              dispatched_by=$2,
              vehicle_no=$3,
              dispatch_record_id=$4,
              is_deemed_scan_out=TRUE,
              deemed_reason=$5,
              deemed_by=$6
           WHERE id=$7`,
          [ts, admin, vehicleNo || '', recId, reason, admin, invId]
        );
      } else {
        db.prepare(
          `UPDATE invoices_received SET
              dispatch_status='dispatched',
              dispatched_at=?,
              dispatched_by=?,
              vehicle_no=?,
              dispatch_record_id=?,
              is_deemed_scan_out=1,
              deemed_reason=?,
              deemed_by=?
           WHERE id=?`
        ).run(ts, admin, vehicleNo || '', recId, reason, admin, invId);
      }
    } catch (e) {
      console.warn('[v39 P16] invoice deemed update failed (record was created):', e.message);
    }
    // Recompute dispatch_actuals (best-effort)
    try {
      if (typeof _recomputeDispatchActuals === 'function') {
        await _recomputeDispatchActuals(inv.batch_number, vehicleNo || null, inv.sap_doc_num || null);
      }
    } catch (e) {
      console.warn('[v39 P16] dispatch_actuals recompute failed:', e.message);
    }
    // Annotate dispatch_plans (best-effort)
    try {
      await _v39_updateDispatchPlansForInvoice(inv.batch_number, {
        invoice_dispatched_at: ts,
        invoice_dispatched_by: admin,
        invoice_status: 'dispatched',
        invoice_deemed: true,
        invoice_deemed_reason: reason,
      });
    } catch (e) {
      console.warn('[v39 P16] dispatch_plans annotate failed:', e.message);
    }
    res.json({
      ok: true,
      dispatch_record_id: recId,
      invoice_id: invId,
      batch_number: inv.batch_number,
      boxes,
      qty: qtyLakhs,
      ts,
      is_deemed_scan_out: true,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// v39 Phase 16: GET /api/invoice/pending-direct-sap-approval
// List of invoices needing admin review — source='direct_sap' AND not yet
// admin-approved. Used by the admin "Direct-SAP Approval Queue" UI.
app.get('/api/invoice/pending-direct-sap-approval', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    let rows;
    if (pgPool) {
      const r = await pgPool.query(
        `SELECT * FROM invoices_received
         WHERE source = 'direct_sap' AND admin_approved_at IS NULL
         ORDER BY invoice_date DESC, fetched_at DESC
         LIMIT ${limit}`
      );
      rows = r.rows;
    } else {
      rows = db.prepare(
        `SELECT * FROM invoices_received
         WHERE source = 'direct_sap' AND admin_approved_at IS NULL
         ORDER BY invoice_date DESC, fetched_at DESC
         LIMIT ?`
      ).all(limit);
    }
    res.json({ ok: true, count: rows.length, invoices: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});


// ── Print Orders — dedicated table for permanent machine assignments ──────────
// ── Production Orders — dedicated table, each order is its own permanent row ──

// POST /api/orders/upsert — save single order permanently (never lost)
app.post('/api/orders/upsert', async (req, res) => {
  try {
    const ord = req.body;
    if (!ord || !ord.id) return res.status(400).json({ ok: false, error: 'id required' });
    const json = JSON.stringify(ord);
    if (pgPool) {
      await pgPool.query(`
        INSERT INTO production_orders (id, data_json, machine_id, batch_number, status, deleted, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,NOW()::TEXT)
        ON CONFLICT(id) DO UPDATE SET
          data_json=$2, machine_id=$3, batch_number=$4,
          status=$5, deleted=$6, updated_at=NOW()::TEXT
      `, [ord.id, json, ord.machineId||null, ord.batchNumber||null,
          ord.status||'pending', ord.deleted||false]);
    } else {
      db.prepare(`INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at)
        VALUES (?,?,?,?,?,?,datetime('now'))
        ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json,
        machine_id=excluded.machine_id,batch_number=excluded.batch_number,
        status=excluded.status,deleted=excluded.deleted,updated_at=datetime('now')`)
        .run(ord.id, json, ord.machineId||null, ord.batchNumber||null, ord.status||'pending', ord.deleted?1:0);
    }
    res.json({ ok: true, savedAt: new Date().toISOString() });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// DELETE /api/orders/delete-by-batch — permanently delete order by batchNumber + customer match
app.post('/api/orders/delete-by-batch', async (req, res) => {
  try {
    const { batchNumber, customerContains } = req.body;
    if (!batchNumber) return res.status(400).json({ ok: false, error: 'batchNumber required' });
    if (pgPool) {
      if (customerContains) {
        await pgPool.query(
          `DELETE FROM production_orders WHERE batch_number = $1 AND data_json::jsonb->>'customer' LIKE $2`,
          [batchNumber, '%' + customerContains + '%']
        );
      } else {
        await pgPool.query(`DELETE FROM production_orders WHERE batch_number = $1`, [batchNumber]);
      }
    } else {
      db.prepare(`DELETE FROM production_orders WHERE batch_number = ?`).run(batchNumber);
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// v37J Sub-issue 1.3: Detect batches where Planning's production_order customer
// differs from the most recent label customer for the same batch number. This
// commonly happens when a batch is reassigned mid-flight (labels regenerated
// for new customer) but the Planning order wasn't manually updated.
// Returns: { ok: true, mismatches: [{ batchNumber, planningCustomer, labelCustomer, productionOrderId, labelLatestAt }] }
app.get('/api/planning/customer-mismatch', async (req, res) => {
  try {
    const mismatches = [];
    let orderRows = [];
    let labelRows = [];
    if (pgPool) {
      const r1 = await pgPool.query(`SELECT id, batch_number, data_json FROM production_orders WHERE deleted = false AND batch_number IS NOT NULL`);
      orderRows = r1.rows;
      const r2 = await pgPool.query(`SELECT batch_number, customer, generated FROM tracking_labels WHERE voided = false AND customer IS NOT NULL AND customer <> '' ORDER BY generated DESC`);
      labelRows = r2.rows;
    } else {
      orderRows = db.prepare(`SELECT id, batch_number, data_json FROM production_orders WHERE deleted = 0 AND batch_number IS NOT NULL`).all();
      labelRows = db.prepare(`SELECT batch_number, customer, generated FROM tracking_labels WHERE voided = 0 AND customer IS NOT NULL AND customer <> '' ORDER BY generated DESC`).all();
    }
    // Build map: batch_number -> most recent label customer (already DESC-sorted)
    const labelByBatch = {};
    for (const l of labelRows) {
      const bn = (l.batch_number||'').toUpperCase();
      if (!bn) continue;
      if (!labelByBatch[bn]) labelByBatch[bn] = { customer: l.customer, generated: l.generated };
    }
    for (const o of orderRows) {
      const bn = (o.batch_number||'').toUpperCase();
      if (!bn) continue;
      const lb = labelByBatch[bn];
      if (!lb) continue;
      let pcust = '';
      try {
        const data = typeof o.data_json === 'string' ? JSON.parse(o.data_json) : o.data_json;
        pcust = (data?.customer || data?.shipTo || '').toString();
      } catch(e) {}
      // Case-insensitive compare; trim whitespace
      const norm = s => (s||'').toString().trim().toUpperCase();
      if (pcust && lb.customer && norm(pcust) !== norm(lb.customer)) {
        mismatches.push({
          batchNumber: o.batch_number,
          productionOrderId: o.id,
          planningCustomer: pcust,
          labelCustomer: lb.customer,
          labelLatestAt: lb.generated
        });
      }
    }
    res.json({ ok: true, mismatches, count: mismatches.length });
  } catch(err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});
// POST /api/orders/upsert-bulk — save multiple orders at once
app.post('/api/orders/upsert-bulk', async (req, res) => {
  try {
    const { orders } = req.body;
    if (!Array.isArray(orders)) return res.status(400).json({ ok: false, error: 'orders array required' });
    for (const ord of orders) {
      if (!ord.id) continue;
      const json = JSON.stringify(ord);
      if (pgPool) {
        await pgPool.query(`
          INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,NOW()::TEXT)
          ON CONFLICT(id) DO UPDATE SET
            data_json=$2,machine_id=$3,batch_number=$4,
            status=$5,deleted=$6,updated_at=NOW()::TEXT
        `, [ord.id, json, ord.machineId||null, ord.batchNumber||null,
            ord.status||'pending', ord.deleted||false]);
      } else {
        db.prepare(`INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at)
          VALUES (?,?,?,?,?,?,datetime('now'))
          ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json,
          machine_id=excluded.machine_id,batch_number=excluded.batch_number,
          status=excluded.status,deleted=excluded.deleted,updated_at=datetime('now')`)
          .run(ord.id, json, ord.machineId||null, ord.batchNumber||null, ord.status||'pending', ord.deleted?1:0);
      }
    }
    res.json({ ok: true, count: orders.length, savedAt: new Date().toISOString() });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/orders/all — get all orders from dedicated table
app.get('/api/orders/all', async (req, res) => {
  try {
    let rows = [];
    if (pgPool) {
      const r = await pgPool.query('SELECT data_json FROM production_orders ORDER BY updated_at DESC');
      rows = r.rows.map(r => JSON.parse(r.data_json));
    } else {
      rows = db.prepare('SELECT data_json FROM production_orders ORDER BY updated_at DESC').all()
               .map(r => JSON.parse(r.data_json));
    }
    res.json({ ok: true, orders: rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/print-orders
app.get('/api/print-orders', async (req, res) => {
  try {
    if (!pgPool) return res.json({ ok: true, printOrders: [] });
    // DISTINCT ON: one row per (productionOrderId,machineId,printType) — most-recently-updated wins
    // This removes DB duplicates where same batch was saved twice (once assigned, once null)
    const r = await pgPool.query(`
      SELECT DISTINCT ON (
        COALESCE(production_order_id, batch_number, id),
        COALESCE(machine_id, ''),
        COALESCE(print_type, '')
      ) *
      FROM print_orders
      ORDER BY
        COALESCE(production_order_id, batch_number, id),
        COALESCE(machine_id, ''),
        COALESCE(print_type, ''),
        CASE WHEN machine_id IS NOT NULL AND machine_id != '' AND machine_id != 'null' THEN 0 ELSE 1 END,
        updated_at DESC NULLS LAST
    `);
    res.json({ ok: true, printOrders: r.rows.map(row => ({
      id: row.id, machineId: row.machine_id, customer: row.customer,
      batchNumber: row.batch_number, pcCode: row.pc_code, size: row.size,
      colour: row.colour, printMatter: row.print_matter, printType: row.print_type,
      qtyToPrint: parseFloat(row.qty_to_print)||0, orderQty: parseFloat(row.order_qty)||0,
      printedToDate: parseFloat(row.printed_to_date)||0,
      printedToDateManual: row.printed_to_date_manual,
      startDate: row.start_date, endDate: row.end_date, status: row.status,
      zone: row.zone, remarks: row.remarks, productionOrderId: row.production_order_id,
    })) });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/print-orders/bulk
app.post('/api/print-orders/bulk', async (req, res) => {
  try {
    const { printOrders } = req.body;
    if (!Array.isArray(printOrders)) return res.status(400).json({ ok: false, error: 'printOrders array required' });
    if (!pgPool) return res.json({ ok: true, count: 0 });
    for (const p of printOrders) {
      if (!p.id) continue;
      await pgPool.query(`
        INSERT INTO print_orders (id,machine_id,customer,batch_number,pc_code,size,colour,
          print_matter,print_type,qty_to_print,order_qty,printed_to_date,printed_to_date_manual,
          start_date,end_date,status,zone,remarks,production_order_id,updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NOW()::TEXT)
        ON CONFLICT(id) DO UPDATE SET machine_id=$2,customer=$3,batch_number=$4,pc_code=$5,
          size=$6,colour=$7,print_matter=$8,print_type=$9,qty_to_print=$10,order_qty=$11,
          printed_to_date=$12,printed_to_date_manual=$13,start_date=$14,end_date=$15,
          status=$16,zone=$17,remarks=$18,production_order_id=$19,updated_at=NOW()::TEXT
      `, [p.id,p.machineId||null,p.customer||null,p.batchNumber||null,p.pcCode||null,
          p.size||null,p.colour||null,p.printMatter||null,p.printType||null,
          p.qtyToPrint||null,p.orderQty||null,p.printedToDate||0,p.printedToDateManual||false,
          p.startDate||null,p.endDate||null,p.status||'pending',p.zone||null,
          p.remarks||null,p.productionOrderId||null]);
    }
    res.json({ ok: true, count: printOrders.length, savedAt: new Date().toISOString() });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});


// DELETE /api/print-orders/:id — permanently delete a single print order by ID
app.delete("/api/print-orders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ ok: false, error: "id required" });
    if (pgPool) {
      await pgPool.query("DELETE FROM print_orders WHERE id=$1", [id]);
    } else {
      db.prepare("DELETE FROM print_orders WHERE id=?").run(id);
    }
    console.log("[PrintOrders] Deleted:", id);
    res.json({ ok: true, deleted: id });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});
// GET /api/machines/master — get all machines from dedicated table
app.get('/api/machines/master', async (req, res) => {
  try {
    if (!pgPool) return res.json({ ok: true, production: [], print: [] });
    const prod = await pgPool.query('SELECT * FROM machine_master WHERE type=$1 ORDER BY id', ['production']);
    const print = await pgPool.query('SELECT * FROM machine_master WHERE type=$1 ORDER BY id', ['print']);
    const toObj = rows => rows.map(r => ({
      id: r.id, size: r.size, cap: r.cap, aGrade: r.a_grade,
      preferredCustomer: r.preferred_customer, active: r.active
    }));
    res.json({ ok: true, production: toObj(prod.rows), print: toObj(print.rows) });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/machines/master — save all machines permanently
app.post('/api/machines/master', async (req, res) => {
  try {
    const { production, print } = req.body;
    if (!pgPool) return res.json({ ok: true });
    const upsertMachines = async (machines, type) => {
      for (const m of (machines || [])) {
        if (!m.id) continue;
        await pgPool.query(`
          INSERT INTO machine_master (id, type, size, cap, a_grade, preferred_customer, active, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
          ON CONFLICT(id) DO UPDATE SET type=$2,size=$3,cap=$4,a_grade=$5,
            preferred_customer=$6,active=$7,updated_at=NOW()
        `, [m.id, type, m.size||null, m.cap||null, m.aGrade||null, m.preferredCustomer||null, m.active !== false]);
      }
    };
    await upsertMachines(production, 'production');
    await upsertMachines(print, 'print');
    res.json({ ok: true, savedAt: new Date().toISOString() });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET full planning state — uses direct pg pool for large JSON
app.get('/api/planning/state', async (req, res) => {
  try {
    const rawState = await getPlanningStateAsync();
    // CRITICAL: deep clone before mutating — never modify the cached object directly
    // Direct mutation corrupts the cache and causes order count drops (194→175 bug)
    const state = JSON.parse(JSON.stringify(rawState));

    // PERMANENT FIX: Recover orders MISSING from planning_state using production_orders table
    // planning_state is SOURCE OF TRUTH for status/dates/all fields
    // production_orders is used ONLY to recover orders not present in planning_state
    try {
      let dbOrders = [];
      if (pgPool) {
        const r = await pgPool.query('SELECT data_json FROM production_orders WHERE deleted = false ORDER BY updated_at ASC');
        dbOrders = r.rows.map(r => JSON.parse(r.data_json));
      } else {
        dbOrders = db.prepare('SELECT data_json FROM production_orders WHERE deleted = 0 ORDER BY updated_at ASC').all()
                     .map(r => JSON.parse(r.data_json));
      }
      if (dbOrders.length > 0) {
        // Build lookup maps from planning_state orders
        const stateOrderById = new Map((state.orders||[]).map(o => [o.id, o]));
        const stateOrderByBatchMc = new Map();
        (state.orders||[]).forEach(o => {
          if (o.batchNumber && o.machineId) stateOrderByBatchMc.set(`${o.batchNumber}__${o.machineId}`, o);
        });
        // Only add orders from DB that are MISSING from planning_state
        dbOrders.forEach(dbOrd => {
          if (!dbOrd || !dbOrd.id) return;
          if (dbOrd.deleted) return;
          if (dbOrd.batchNumber === '26V049' && (dbOrd.customer||'').includes('SHYAM')) return;
          // Already in planning_state by ID — planning_state wins, skip
          if (stateOrderById.has(dbOrd.id)) return;
          // Same batchNumber+machineId already in planning_state — skip, avoid duplicate
          const bmKey = `${dbOrd.batchNumber}__${dbOrd.machineId}`;
          if (dbOrd.batchNumber && dbOrd.machineId && stateOrderByBatchMc.has(bmKey)) return;
          // Genuinely missing order — recover it
          state.orders = state.orders || [];
          state.orders.push(dbOrd);
          stateOrderById.set(dbOrd.id, dbOrd);
          if (dbOrd.batchNumber && dbOrd.machineId) stateOrderByBatchMc.set(bmKey, dbOrd);
          console.log(`[State] Recovered missing order: ${dbOrd.batchNumber} on ${dbOrd.machineId}`);
        });
      }
    } catch(e) { console.warn('[State] Order recovery failed:', e.message); }

    if (state.orders && _actualsCache) {
      for (const ord of state.orders) {
        const actual = (_actualsCache[ord.id] || _actualsCache[ord.batchNumber] || 0);
        ord.actualProd = actual;
        // Auto-promote pending → running only if machine has fewer than 2 running orders
        // Never auto-promote SAP-imported orders (_noAutoPromote flag)
        if (actual > 0 && ord.status === 'pending' && !ord._noAutoPromote) {
          const runningOnMachine = state.orders.filter(o =>
            o.machineId === ord.machineId &&
            o.status === 'running' &&
            o.id !== ord.id &&
            !o.deleted
          ).length;
          if (runningOnMachine < 2) ord.status = 'running';
        }
      }
    }
    const savedAt = pgPool
      ? (await pgPool.query('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1')).rows[0]?.saved_at
      : db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get()?.saved_at;
    res.json({ ok: true, state, savedAt });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});


// POST save planning state — uses direct pg pool for large JSON
// ── Emergency restore from backup file ─────────────────────────
app.post('/api/planning/restore', async (req, res) => {
  try {
    const state = req.body;
    if (!state || !state.orders) return res.status(400).json({ ok: false, error: 'Invalid backup format' });
    const json = JSON.stringify(state);
    if (pgPool) {
      const existing = await pgPool.query('SELECT id FROM planning_state LIMIT 1');
      if (existing.rows.length > 0) {
        await pgPool.query('UPDATE planning_state SET state_json = $1, saved_at = NOW() WHERE id = $2', [json, existing.rows[0].id]);
      } else {
        await pgPool.query('INSERT INTO planning_state (state_json) VALUES ($1)', [json]);
      }
      _planningStateCache = state;
      _planningStateCacheTime = Date.now();
    }
    res.json({ ok: true, orders: state.orders.length, savedAt: new Date().toISOString() });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/api/planning/state', async (req, res) => {
  try {
    const { state } = req.body;
    if (!state) return res.status(400).json({ ok: false, error: 'No state provided' });

    // Background order merge — runs AFTER response is sent so planning_state save is never blocked
    // With 300+ orders, sequential queries timed out and prevented planning_state from saving
    if (state.orders && state.orders.length > 0) {
      setImmediate(async () => {
        try {
          const orders = state.orders.filter(o => o && o.id &&
            !(o.batchNumber === '26V049' && (o.customer||'').includes('SHYAM')));
          if (!orders.length || !pgPool) return;

          // Fetch ALL existing records in ONE query — no N+1
          const ids = orders.map(o => o.id);
          const existing = await pgPool.query(
            `SELECT id, data_json FROM production_orders WHERE id = ANY($1)`, [ids]
          );
          const existingMap = {};
          existing.rows.forEach(r => {
            try { existingMap[r.id] = typeof r.data_json === 'string'
              ? JSON.parse(r.data_json) : r.data_json; } catch(e) {}
          });

          // Merge preserving manual dates/status, then bulk upsert
          await Promise.all(orders.map(async ord => {
            const ex = existingMap[ord.id];
            let mergedOrd = ord;
            if (ex) {
              const hasManualDate = ex.manualEndDate || ex.manualStartDate;
              mergedOrd = {
                ...ord,
                startDate:       hasManualDate ? ex.startDate   : ord.startDate,
                endDate:         hasManualDate ? ex.endDate     : ord.endDate,
                manualEndDate:   ex.manualEndDate   || ord.manualEndDate,
                manualStartDate: ex.manualStartDate || ord.manualStartDate,
                status: (ex.status && ex.status !== 'pending') ? ex.status : (ord.status||'pending'),
                actualProd: Math.max(ord.actualProd||0, ex.actualProd||0),
              };
            }
            await pgPool.query(`
              INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at)
              VALUES ($1,$2,$3,$4,$5,$6,NOW()::TEXT)
              ON CONFLICT(id) DO UPDATE SET data_json=$2,machine_id=$3,batch_number=$4,
                status=$5,deleted=$6,updated_at=NOW()::TEXT
            `, [mergedOrd.id, JSON.stringify(mergedOrd), mergedOrd.machineId||null,
                mergedOrd.batchNumber||null, mergedOrd.status||'pending', mergedOrd.deleted||false]);
          }));
          console.log(`[State] Background merged ${orders.length} orders into production_orders`);
        } catch(e) { console.warn('[State] Background order merge failed:', e.message); }
      });
    }

    const json = JSON.stringify(state);
    if (pgPool) {
      const existing = await pgPool.query('SELECT id FROM planning_state LIMIT 1');
      if (existing.rows[0]) {
        await pgPool.query('UPDATE planning_state SET state_json = $1, saved_at = NOW() WHERE id = $2', [json, existing.rows[0].id]);
      } else {
        await pgPool.query('INSERT INTO planning_state (state_json) VALUES ($1)', [json]);
      }
      _planningStateCache = state;
      _planningStateCacheTime = Date.now();
    } else {
      const existing = db.prepare('SELECT id FROM planning_state LIMIT 1').get();
      if (existing) {
        db.prepare('UPDATE planning_state SET state_json = ?, saved_at = NOW() WHERE id = ?').run(json, existing.id);
      } else {
        db.prepare('INSERT INTO planning_state (state_json) VALUES (?)').run(json);
      }
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

// GET all active orders (summary for DPR to cache on load) — only 'running' status
app.get('/api/orders/active', async (req, res) => {
  try {
    // Refresh actuals in background — throttled to 60s, non-blocking
    warmActualsCache().catch(()=>{});
    const state = await getPlanningStateAsync();
    const running = (state.orders || []).filter(o => o.status === 'running' && !o.deleted);

    // Helper: extract YYYY-MM-DD from any startDate format (Date object, ISO string, etc.)
    const getDateStr = (d) => {
      if (!d) return '';
      const s = String(d);
      // ISO format: "2026-04-15T00:00:00.000Z" → "2026-04-15"
      if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0,10);
      // Try parsing as Date
      const dt = new Date(s);
      if (!isNaN(dt)) return dt.toISOString().slice(0,10);
      return '';
    };

    const mapOrder = o => {
      // Get actual production from DPR actuals cache — this is the real produced qty
      const actualFromCache = _actualsCache ? (_actualsCache[o.id] || _actualsCache[o.batchNumber] || 0) : 0;
      const actualQty = actualFromCache || o.actualQty || o.actualProd || 0;
      return {
        id: o.id,
        batchNumber: o.batchNumber || '',
        poNumber: o.poNumber || '',
        customer: o.customer || '',
        machineId: o.machineId || '',
        size: o.size || '',
        colour: o.colour || '',
        qty: o.qty || 0,
        grossQty: o.grossQty || o.qty || 0,
        actualQty,
        status: o.status || 'running',
        isPrinted: o.isPrinted || false,
        isLegacy: !o.startDate || getDateStr(o.startDate) <= LEGACY_CUTOFF,
        printMatter: o.printMatter || '',
        printingMatter: o.printMatter || o.printingMatter || '',
      };
    };

    // Separate legacy (startDate <= CUTOFF) and new orders
    const legacyOrders = running.filter(o =>
      !o.startDate || getDateStr(o.startDate) <= LEGACY_CUTOFF
    );
    const newOrders = running.filter(o =>
      o.startDate && getDateStr(o.startDate) > LEGACY_CUTOFF
    );

    // For new orders: max 2 per machine — show LATEST 2 (most recently started)
    // Sort DESC so newest orders are kept, not oldest ones that should be closed
    const newOrdersFiltered = [];
    const newCountPerMachine = {};
    const newSorted = [...newOrders].sort((a,b) => String(b.startDate).localeCompare(String(a.startDate)));
    for (const o of newSorted) {
      const mc = o.machineId || 'unknown';
      if (!newCountPerMachine[mc]) newCountPerMachine[mc] = 0;
      if (newCountPerMachine[mc] < 2) {
        newOrdersFiltered.push(o);
        newCountPerMachine[mc]++;
      }
    }

    // Return ALL legacy orders + filtered new orders
    const orders = [...legacyOrders, ...newOrdersFiltered].map(mapOrder);
    res.json({ ok: true, orders });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// DPR APP ROUTES
// ═══════════════════════════════════════════════════════════════

// POST bulk import DPR records from backup
app.post('/api/dpr/bulk-import', async (req, res) => {
  try {
    const { records } = req.body;
    if (!records || !Array.isArray(records)) return res.status(400).json({ ok: false, error: 'No records provided' });
    let saved = 0;
    if (pgPool) {
      const client = await pgPool.connect();
      try {
        await client.query('BEGIN');
        for (const { floor, date, data } of records) {
          if (!floor || !date || !data) continue;
          await client.query(`INSERT INTO dpr_records (floor, date, data_json) VALUES ($1, $2, $3) ON CONFLICT(floor, date) DO UPDATE SET data_json = EXCLUDED.data_json, saved_at = NOW()`, [floor, date, JSON.stringify(data)]);
          // Extract actuals from DPR data
          await client.query('DELETE FROM production_actuals WHERE floor = $1 AND date = $2', [floor, date]);
          const shifts = data.shifts || {};
          for (const [shiftName, shiftData] of Object.entries(shifts)) {
            if (!shiftData.machines) continue;
            for (const [machineId, machineData] of Object.entries(shiftData.machines)) {
              const runs = machineData.runs || [{ orderId: machineData.orderId, batchNumber: machineData.batchNumber, qty: machineData.prod }];
              for (let ri = 0; ri < runs.length; ri++) {
                const run = runs[ri];
                const qty = parseFloat(run.qty) || 0;
                if (qty <= 0) continue;
                await client.query(`INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                  ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET
                  order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number, qty_lakhs=EXCLUDED.qty_lakhs`,
                  [run.orderId||null, run.batchNumber||null, machineId, date, shiftName, ri, qty, floor]);
              }
            }
          }
          saved++;
        }
        await client.query('COMMIT');
      } catch(e) { await client.query('ROLLBACK'); throw e; }
      finally { client.release(); }
      // Refresh actuals cache
      await warmActualsCache();
    }
    res.json({ ok: true, saved });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET DPR record for a floor + date
// POST save DPR record + extract actuals into bridge table
app.post('/api/dpr/save', async (req, res) => {
  try {
    const { floor, date, data, actuals } = req.body;
    if (!floor || !date || !data) return res.status(400).json({ ok: false, error: 'Missing floor, date, or data' });

    if (pgPool) {
      // Merge incoming shifts with existing DB data to protect shifts filled by other users
      const existingRow = await pgPool.query('SELECT data_json FROM dpr_records WHERE floor=$1 AND date=$2', [floor, date]);
      if (existingRow.rows[0]) {
        try {
          const existing = JSON.parse(existingRow.rows[0].data_json);
          const existingShifts = existing.shifts || {};
          const incomingShifts = data.shifts || {};
          // For each shift: if incoming shift has no machine qty data, keep existing shift data
          for (const shiftKey of ['A', 'B', 'C']) {
            const incomingShift = incomingShifts[shiftKey];
            const existingShift = existingShifts[shiftKey];
            if (!existingShift) continue; // nothing in DB to protect
            if (!incomingShift || !incomingShift.machines) {
              // Incoming has no data for this shift — keep existing
              if (!data.shifts) data.shifts = {};
              data.shifts[shiftKey] = existingShift;
              continue;
            }
            // Check if incoming shift has any actual qty OR staff data entered
            let hasData = false;
            // Check qty in machine runs
            for (const mc of Object.values(incomingShift.machines || {})) {
              const runs = mc.runs || [{ qty: mc.prod }];
              if (runs.some(r => parseFloat(r.qty) > 0)) { hasData = true; break; }
            }
            // Also check staff names (incharge, chemist, fitter etc.)
            if (!hasData) {
              const staffFields = ['incharge', 'chemist', 'fitter', 'electrical', 'utility', 'aim_staff', 'gpr_staff'];
              for (const field of staffFields) {
                const val = incomingShift[field];
                if (Array.isArray(val) && val.some(v => v && v.trim())) { hasData = true; break; }
                if (typeof val === 'string' && val.trim()) { hasData = true; break; }
              }
            }
            if (!hasData) {
              // Incoming shift is completely empty — keep existing shift data
              data.shifts[shiftKey] = existingShift;
            }
          }
        } catch(e) { console.warn('[DPR merge] parse error:', e.message); }
      }

      // Save merged DPR record to PostgreSQL
      await pgPool.query(
        `INSERT INTO dpr_records (floor, date, data_json)
         VALUES ($1, $2, $3)
         ON CONFLICT(floor, date) DO UPDATE SET data_json = EXCLUDED.data_json, saved_at = NOW()`,
        [floor, date, JSON.stringify(data)]
      );

      // Delete old actuals for this floor+date, then re-insert
      await pgPool.query('DELETE FROM production_actuals WHERE floor = $1 AND date = $2', [floor, date]);

      const actualsToSave = [];
      if (actuals && actuals.length > 0) {
        for (const a of actuals) {
          if (!a.qty || a.qty <= 0) continue;
          actualsToSave.push([a.orderId||null, a.batchNumber||null, a.machineId, date, a.shift, a.runIndex||0, a.qty, a.floor||floor]);
        }
      } else {
        const shifts = data.shifts || {};
        for (const [shiftName, shiftData] of Object.entries(shifts)) {
          if (!shiftData.machines) continue;
          for (const [machineId, machineData] of Object.entries(shiftData.machines)) {
            const runs = machineData.runs || [{orderId:machineData.orderId,batchNumber:machineData.batchNumber,qty:machineData.prod}];
            runs.forEach((run,ri) => {
              const qty = parseFloat(run.qty)||0;
              if (qty <= 0) return;
              actualsToSave.push([run.orderId||null, run.batchNumber||null, machineId, date, shiftName, ri, qty, floor]);
            });
          }
        }
      }
      for (const row of actualsToSave) {
        await pgPool.query(
          `INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET
             order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number,
             qty_lakhs=EXCLUDED.qty_lakhs, synced_at=NOW()`,
          row
        );
      }

      // Update planning actuals cache (two-way sync) — warm cache so Planning sees fresh data
      try {
        await warmActualsCache();
      } catch(e) { console.warn('Planning sync error:', e.message); }

    } else {
      // SQLite fallback
      db.prepare(`INSERT INTO dpr_records (floor, date, data_json) VALUES (?, ?, ?) ON CONFLICT(floor, date) DO UPDATE SET data_json = excluded.data_json, saved_at = datetime('now')`).run(floor, date, JSON.stringify(data));
      db.prepare('DELETE FROM production_actuals WHERE floor = ? AND date = ?').run(floor, date);
      const upsert = db.prepare(`INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET order_id=excluded.order_id, batch_number=excluded.batch_number, qty_lakhs=excluded.qty_lakhs, synced_at=datetime('now')`);
      const rows = actuals && actuals.length > 0 ? actuals.filter(a=>a.qty>0).map(a=>[a.orderId||null,a.batchNumber||null,a.machineId,date,a.shift,a.runIndex||0,a.qty,a.floor||floor]) : [];
      db.transaction(rows => rows.forEach(r => upsert.run(...r)))(rows);
    }

    // Refresh actuals cache so Planning sees new DPR data immediately (force — bypass throttle)
    _actualsCacheTime = 0; // bypass 60s throttle so save is visible immediately
    warmActualsCache().catch(e => console.warn('[DPR] cache warm failed:', e.message));
    res.json({ ok: true, savedAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET plant report data — all floors for a single date in one call
// Returns { ok, date, floors: { GF: data|null, '1F': data|null, '2F': data|null } }
app.get('/api/dpr/plant-report/:date', async (req, res) => {
  try {
    const { date } = req.params;
    const FLOOR_KEYS = ['GF', '1F', '2F'];
    const result = {};
    for (const fl of FLOOR_KEYS) {
      if (pgPool) {
        const r = await pgPool.query('SELECT data_json FROM dpr_records WHERE floor=$1 AND date=$2', [fl, date]);
        result[fl] = r.rows[0] ? JSON.parse(r.rows[0].data_json) : null;
      } else {
        const row = db.prepare('SELECT data_json FROM dpr_records WHERE floor = ? AND date = ?').get(fl, date);
        result[fl] = row ? JSON.parse(row.data_json) : null;
      }
    }
    res.json({ ok: true, date, floors: result });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET all DPR dates (for history navigation)
app.get('/api/dpr/dates/:floor', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT DISTINCT date FROM dpr_records WHERE floor=$1 ORDER BY date DESC', [req.params.floor]);
      rows = r.rows;
    } else {
      rows = db.prepare('SELECT DISTINCT date FROM dpr_records WHERE floor = ? ORDER BY date DESC').all(req.params.floor);
    }
    res.json({ ok: true, dates: rows.map(r => r.date) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST close a batch in DPR (manager action)
app.post('/api/dpr/batch-close', async (req, res) => {
  try {
    const { orderId, batchNumber, closedBy, notes } = req.body;
    if (!orderId) return res.status(400).json({ ok: false, error: 'orderId required' });
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO dpr_batch_closed (order_id, batch_number, closed_at, closed_by, notes)
         VALUES ($1,$2,NOW(),$3,$4)
         ON CONFLICT(order_id) DO UPDATE SET batch_number=EXCLUDED.batch_number, closed_at=NOW(), closed_by=EXCLUDED.closed_by, notes=EXCLUDED.notes`,
        [orderId, batchNumber||null, closedBy||null, notes||null]
      );
    } else {
      db.prepare(`INSERT OR REPLACE INTO dpr_batch_closed (order_id, batch_number, closed_at, closed_by, notes)
        VALUES (?, ?, datetime('now'), ?, ?)`).run(orderId, batchNumber||null, closedBy||null, notes||null);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// DELETE reopen a batch in DPR (admin only)
app.delete('/api/dpr/batch-close/:orderId', async (req, res) => {
  try {
    if (pgPool) {
      await pgPool.query('DELETE FROM dpr_batch_closed WHERE order_id = $1', [req.params.orderId]);
    } else {
      db.prepare('DELETE FROM dpr_batch_closed WHERE order_id = ?').run(req.params.orderId);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET all DPR-closed batches (used by Planning to gate close button)
app.get('/api/dpr/batch-closed', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed');
      rows = r.rows;
    } else {
      rows = db.prepare('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed').all();
    }
    res.json({ ok: true, closed: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET DPR record for a floor + date — MUST be after all specific /api/dpr/* routes
app.get('/api/dpr/:floor/:date', async (req, res) => {
  try {
    const { floor, date } = req.params;
    if (pgPool) {
      const r = await pgPool.query('SELECT data_json, saved_at FROM dpr_records WHERE floor = $1 AND date = $2', [floor, date]);
      if (!r.rows.length) return res.json({ ok: true, data: null });
      res.json({ ok: true, data: JSON.parse(r.rows[0].data_json), savedAt: r.rows[0].saved_at });
    } else {
      const row = db.prepare('SELECT data_json, saved_at FROM dpr_records WHERE floor = ? AND date = ?').get(floor, date);
      if (!row) return res.json({ ok: true, data: null });
      res.json({ ok: true, data: JSON.parse(row.data_json), savedAt: row.saved_at });
    }
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/actuals/machine-summary — total qty and distinct days per machine (for Planning avg daily rate)
app.get('/api/actuals/machine-summary', async (req, res) => {
  try {
    // v37E: Per user spec — average production per day must only include FULLY-ENTERED calendar days
    // (all 3 shifts A/B/C present in the saved DPR). Today's partial entry (e.g., only A-shift) is
    // excluded so the average doesn't drop spuriously. The average locks at end of day and updates
    // once all shifts are entered.
    //
    // Completeness signal: a shift is "entered" when its `incharge` field is non-empty.
    // The DPR UI initializes incharge as '' (empty string) on a blank day, and operators fill it
    // when they record the shift. This is a reliable signal that someone actively entered shift
    // data (more reliable than checking shift keys exist — those exist by default in the skeleton).
    // incharge can be a string OR an array of names; both forms count as "filled" if non-empty.
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`
        WITH complete_floor_days AS (
          -- A day's DPR is complete when all 3 shifts have non-empty incharge
          SELECT floor, date FROM dpr_records
          WHERE COALESCE(jsonb_typeof(data_json::jsonb -> 'shifts' -> 'A' -> 'incharge'), 'null') != 'null'
            AND COALESCE(jsonb_typeof(data_json::jsonb -> 'shifts' -> 'B' -> 'incharge'), 'null') != 'null'
            AND COALESCE(jsonb_typeof(data_json::jsonb -> 'shifts' -> 'C' -> 'incharge'), 'null') != 'null'
            AND (
              -- A: array with length > 0  OR  non-empty string
              (jsonb_typeof(data_json::jsonb -> 'shifts' -> 'A' -> 'incharge') = 'array'
                AND jsonb_array_length(data_json::jsonb -> 'shifts' -> 'A' -> 'incharge') > 0)
              OR (jsonb_typeof(data_json::jsonb -> 'shifts' -> 'A' -> 'incharge') = 'string'
                AND length(data_json::jsonb -> 'shifts' -> 'A' ->> 'incharge') > 0)
            )
            AND (
              (jsonb_typeof(data_json::jsonb -> 'shifts' -> 'B' -> 'incharge') = 'array'
                AND jsonb_array_length(data_json::jsonb -> 'shifts' -> 'B' -> 'incharge') > 0)
              OR (jsonb_typeof(data_json::jsonb -> 'shifts' -> 'B' -> 'incharge') = 'string'
                AND length(data_json::jsonb -> 'shifts' -> 'B' ->> 'incharge') > 0)
            )
            AND (
              (jsonb_typeof(data_json::jsonb -> 'shifts' -> 'C' -> 'incharge') = 'array'
                AND jsonb_array_length(data_json::jsonb -> 'shifts' -> 'C' -> 'incharge') > 0)
              OR (jsonb_typeof(data_json::jsonb -> 'shifts' -> 'C' -> 'incharge') = 'string'
                AND length(data_json::jsonb -> 'shifts' -> 'C' ->> 'incharge') > 0)
            )
        ),
        machine_complete_days AS (
          SELECT pa.machine_id, pa.date, SUM(pa.qty_lakhs) AS day_qty
          FROM production_actuals pa
          JOIN complete_floor_days cfd ON cfd.floor = pa.floor AND cfd.date = pa.date
          GROUP BY pa.machine_id, pa.date
        )
        SELECT machine_id,
               SUM(day_qty)        AS total_qty,
               COUNT(*)            AS distinct_days,
               MIN(date)           AS first_date,
               MAX(date)           AS last_date
        FROM machine_complete_days
        GROUP BY machine_id`);
      rows = r.rows;
    } else {
      // SQLite fallback — use json_extract to peek at incharge values.
      // For SQLite we fetch all dpr_records and filter in JS (simpler and SQLite has no jsonb_typeof).
      const allDprRecs = db.prepare('SELECT floor, date, data_json FROM dpr_records').all();
      const completeFloorDays = new Set();
      for (const rec of allDprRecs) {
        try {
          const j = JSON.parse(rec.data_json);
          const shifts = j?.shifts || {};
          const isFilled = (sh) => {
            const inc = shifts[sh]?.incharge;
            if (Array.isArray(inc)) return inc.length > 0;
            if (typeof inc === 'string') return inc.length > 0;
            return false;
          };
          if (isFilled('A') && isFilled('B') && isFilled('C')) {
            completeFloorDays.add(`${rec.floor}|${rec.date}`);
          }
        } catch(e) {}
      }
      // Now sum production_actuals only for those (floor, date) combos
      const allActuals = db.prepare(`SELECT machine_id, floor, date, SUM(qty_lakhs) AS day_qty FROM production_actuals GROUP BY machine_id, floor, date`).all();
      const machineAcc = {};
      allActuals.forEach(a => {
        if (!completeFloorDays.has(`${a.floor}|${a.date}`)) return;
        if (!machineAcc[a.machine_id]) machineAcc[a.machine_id] = { total_qty: 0, distinct_days: 0, first_date: a.date, last_date: a.date };
        machineAcc[a.machine_id].total_qty += parseFloat(a.day_qty || 0);
        machineAcc[a.machine_id].distinct_days += 1;
        if (a.date < machineAcc[a.machine_id].first_date) machineAcc[a.machine_id].first_date = a.date;
        if (a.date > machineAcc[a.machine_id].last_date)  machineAcc[a.machine_id].last_date  = a.date;
      });
      rows = Object.entries(machineAcc).map(([machine_id, v]) => ({ machine_id, ...v }));
    }
    const machines = {};
    rows.forEach(r => {
      const totalQty     = parseFloat(r.total_qty    || 0);
      const distinctDays = parseInt(r.distinct_days  || 0);
      machines[r.machine_id] = {
        totalQty,
        distinctDays,
        avgPerDay: distinctDays > 0 ? parseFloat((totalQty / distinctDays).toFixed(3)) : 0,
        firstDate: r.first_date,
        lastDate:  r.last_date,
        note: 'Only days with all 3 shifts (A/B/C) fully entered (incharge filled) count toward this average.'
      };
    });
    res.json({ ok: true, machines });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET actuals summary for a machine (for DPR to show cumulative vs planned)
app.get('/api/actuals/machine/:machineId', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT date,shift,qty_lakhs,order_id,batch_number FROM production_actuals WHERE machine_id=$1 ORDER BY date DESC, shift LIMIT 90`, [req.params.machineId]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT date,shift,qty_lakhs,order_id,batch_number FROM production_actuals WHERE machine_id=? ORDER BY date DESC, shift LIMIT 90`).all(req.params.machineId);
    }
    res.json({ ok: true, actuals: rows });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET actuals for a specific order
app.get('/api/actuals/order/:orderId', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT date,shift,qty_lakhs,machine_id FROM production_actuals WHERE order_id=$1 OR batch_number=$1 ORDER BY date,shift`, [req.params.orderId]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT date,shift,qty_lakhs,machine_id FROM production_actuals WHERE order_id=? OR batch_number=? ORDER BY date,shift`).all(req.params.orderId, req.params.orderId);
    }
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
  try {
    // db wrapper reads from PostgreSQL when pgPool is active
    // Using simple token lookup (datetime comparison handled by expiry logic below)
    const session = db.prepare(`SELECT * FROM app_sessions WHERE token = ?`).get(token);
    if (!session) return null;
    // Check expiry in JS (works for both SQLite and PostgreSQL datetime formats)
    if (session.expires_at && new Date(session.expires_at) < new Date()) return null;
    return session;
  } catch(e) { return null; }
}

function logAudit(username, role, app, action, details, ip) {
  try {
    if (pgPool) {
      pgPool.query(`INSERT INTO audit_log (username,role,app,action,details,ip) VALUES ($1,$2,$3,$4,$5,$6)`,
        [username, role, app, action, details||null, ip||null]).catch(e=>console.error('Audit log error:',e.message));
    } else {
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details,ip) VALUES (?,?,?,?,?,?)`).run(username,role,app,action,details||null,ip||null);
    }
  } catch(e) { console.error('Audit log error:', e.message); }
}

// POST /api/auth/login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { username, pin, app: appName } = req.body;
    if (!username || !pin || !appName) return res.status(400).json({ ok: false, error: 'Missing credentials' });
    let user;
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM app_users WHERE username=$1 AND app=$2', [username, appName]);
      user = r.rows[0];
    } else {
      user = db.prepare('SELECT * FROM app_users WHERE username=? AND app=?').get(username, appName);
    }
    if (!user) return res.status(401).json({ ok: false, error: 'User not found' });
    if (user.pin_hash !== hashPin(pin)) return res.status(401).json({ ok: false, error: 'Invalid PIN' });
    const token = generateToken();
    const expires = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().replace('T',' ').slice(0,19);
    if (pgPool) {
      await pgPool.query('INSERT INTO app_sessions (token,user_id,username,role,app,expires_at) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(token) DO NOTHING',
        [token, user.id, user.username, user.role, appName, expires]);
    } else {
      db.prepare('INSERT INTO app_sessions (token,user_id,username,role,app,expires_at) VALUES (?,?,?,?,?,?)').run(token, user.id, user.username, user.role, appName, expires);
    }
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
app.post('/api/auth/logout', async (req, res) => {
  const { token } = req.body;
  if (token) {
    const session = verifyToken(token);
    if (session) {
      logAudit(session.username, session.role, session.app, 'LOGOUT', null, req.ip);
      if (pgPool) await pgPool.query('DELETE FROM app_sessions WHERE token=$1', [token]);
      else db.prepare('DELETE FROM app_sessions WHERE token=?').run(token);
    }
  }
  res.json({ ok: true });
});

// POST /api/auth/change-pin
app.post('/api/auth/change-pin', async (req, res) => {
  try {
    const { token, username, newPin } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin' && session.username !== username) {
      return res.status(403).json({ ok: false, error: 'Only admin can change other users PINs' });
    }
    if (pgPool) {
      await pgPool.query('UPDATE app_users SET pin_hash=$1, updated_at=NOW() WHERE username=$2 AND app=$3', [hashPin(newPin), username, session.app]);
    } else {
      db.prepare(`UPDATE app_users SET pin_hash=?, updated_at=datetime('now') WHERE username=? AND app=?`).run(hashPin(newPin), username, session.app);
    }
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
app.get('/api/audit/view', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const limit = parseInt(req.query.limit) || 200;
    const app = req.query.app || session.app;
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM audit_log WHERE app=$1 ORDER BY ts DESC LIMIT $2`, [app, limit]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM audit_log WHERE app = ? ORDER BY ts DESC LIMIT ?`).all(app, limit);
    }
    res.json({ ok: true, logs: rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/auth/users — admin only, list users for an app
app.get('/api/auth/users', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    let users;
    if (pgPool) {
      const r = await pgPool.query(`SELECT id,username,role,app,created_at,updated_at FROM app_users WHERE app=$1`, [req.query.app || session.app]);
      users = r.rows;
    } else {
      users = db.prepare(`SELECT id,username,role,app,created_at,updated_at FROM app_users WHERE app=?`).all(req.query.app || session.app);
    }
    res.json({ ok: true, users });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── TEMP Batch Colour/PC Code Update ────────────────────────

// POST /api/temp-batches/update-details — save colour + PC Code (one-time per TEMP batch)
app.post('/api/temp-batches/update-details', async (req, res) => {
  try {
    const { tempBatchId, colour, pcCode } = req.body;
    if (!tempBatchId) return res.status(400).json({ ok: false, error: 'Missing tempBatchId' });
    let updated;
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [tempBatchId]);
      if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'TEMP batch not found' });
      await pgPool.query(`UPDATE temp_batches SET colour=$1, pc_code=$2, colour_confirmed=1 WHERE id=$3`, [colour||null, pcCode||null, tempBatchId]);
      const r2 = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [tempBatchId]);
      updated = r2.rows[0];
    } else {
      const tb = db.prepare('SELECT * FROM temp_batches WHERE id = ?').get(tempBatchId);
      if (!tb) return res.status(404).json({ ok: false, error: 'TEMP batch not found' });
      db.prepare(`UPDATE temp_batches SET colour = ?, pc_code = ?, colour_confirmed = 1 WHERE id = ?`).run(colour||null, pcCode||null, tempBatchId);
      updated = db.prepare('SELECT * FROM temp_batches WHERE id = ?').get(tempBatchId);
    }
    logAudit('SYSTEM', 'system', 'dpr', 'TEMP_DETAILS_SET', `TEMP batch ${tempBatchId} — Colour: ${colour}, PC Code: ${pcCode}`);
    res.json({ ok: true, batch: updated });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── W/O (Without Order) Reconciliation ──────────────────────

// POST /api/wo/assign-customer — Planning Manager assigns customer to W/O order
app.post('/api/wo/assign-customer', async (req, res) => {
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
    if (pgPool) {
      await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json,saved_at=NOW()::TEXT`, [JSON.stringify(planState)]);
    } else {
      db.prepare(`INSERT INTO planning_state (id, state_json) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json, updated_at = datetime('now')`).run(JSON.stringify(planState));
    }
    _planningStateCache = planState;
    logAudit(session.username, session.role, 'planning', 'WO_CUSTOMER_ASSIGNED',
      `W/O order ${orderId} assigned to customer: ${customer}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/propose-reconciliation — Planning Manager proposes W/O → real order
// v39 Phase 9c: accepts optional sapDocEntry + sapDocNum to carry forward to approval.
app.post('/api/wo/propose-reconciliation', async (req, res) => {
  try {
    const { token, orderId, customer, poNumber, zone, qtyConfirmed, sapDocEntry, sapDocNum } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
    }
    if (!customer) return res.status(400).json({ ok: false, error: 'Customer name required' });
    // v39 Phase 9c: partial SAP link guard — both or neither
    if ((sapDocEntry && !sapDocNum) || (!sapDocEntry && sapDocNum)) {
      return res.status(400).json({ ok: false, error: 'sapDocEntry and sapDocNum must both be provided, or both omitted' });
    }
    const id = `WORECON-${Date.now()}`;
    const billTo = req.body.billTo || '';
    if (pgPool) {
      await pgPool.query(`INSERT INTO wo_reconciliation_requests (id,proposed_by,status,order_id,customer,po_number,zone,qty_confirmed,sap_doc_entry,sap_doc_num) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [id, session.username, 'pending', orderId, customer, poNumber||null, zone||null, qtyConfirmed||null, sapDocEntry||null, sapDocNum||null]);
      if (billTo && billTo !== customer) await pgPool.query('UPDATE wo_reconciliation_requests SET customer=$1 WHERE id=$2', [customer+'|||'+billTo, id]);
    } else {
      db.prepare(`INSERT INTO wo_reconciliation_requests (id,proposed_by,status,order_id,customer,po_number,zone,qty_confirmed,sap_doc_entry,sap_doc_num) VALUES (?,?,?,?,?,?,?,?,?,?)`).run(id, session.username, 'pending', orderId, customer, poNumber||null, zone||null, qtyConfirmed||null, sapDocEntry||null, sapDocNum||null);
      if (billTo && billTo !== customer) db.prepare('UPDATE wo_reconciliation_requests SET customer=? WHERE id=?').run(customer+'|||'+billTo, id);
    }
    logAudit(session.username, session.role, 'planning', 'WO_RECON_PROPOSED',
      `W/O reconciliation proposed: ${id} for order ${orderId} → customer ${customer}${sapDocEntry?` (SAP ${sapDocEntry}/${sapDocNum})`:''}`);
    res.json({ ok: true, requestId: id, status: 'pending' });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/pending — Admin views pending W/O reconciliation requests
app.get('/api/wo/pending', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    let woRows;
    if (pgPool) { const r = await pgPool.query(`SELECT * FROM wo_reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`); woRows=r.rows; }
    else { woRows = db.prepare(`SELECT * FROM wo_reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`).all(); }
    const planState = getPlanningState();
    const enriched = woRows.map(r => ({...r, orderDetails:(planState.orders||[]).find(o=>o.id===r.order_id)||{}}));
    res.json({ ok: true, requests: enriched });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/approve/:id — Admin approves W/O reconciliation
app.post('/api/wo/approve/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const request = pgPool ? (await pgPool.query('SELECT * FROM wo_reconciliation_requests WHERE id=$1',[req.params.id])).rows[0] : db.prepare('SELECT * FROM wo_reconciliation_requests WHERE id=?').get(req.params.id);
    if (!request) return res.status(404).json({ ok: false, error: 'Request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok: false, error: 'Already processed' });

    const approveWO = async () => {
      const now = new Date().toISOString();
      // 1. Update planning state: change woStatus to 'active', add customer
      const planState = getPlanningState();
      const ord = (planState.orders || []).find(o => o.id === request.order_id);
      if (ord) {
        const custParts = (request.customer||'').split('|||');
        ord.customer = custParts[0];
        ord.shipTo   = custParts[0];
        ord.billTo   = custParts[1] || '';
        ord.poNumber = request.po_number || ord.poNumber;
        ord.zone = request.zone || ord.zone;
        if (request.qty_confirmed) ord.qty = request.qty_confirmed;
        ord.woStatus = 'wo-reconciled';
        ord.woReconciledAt = now;
        ord.woReconciledBy = session.username;
        // v39 Phase 9c: apply SAP refs from the reconciliation request, if present
        if (request.sap_doc_entry) {
          ord.sapDocEntry = request.sap_doc_entry;
          ord.sapDocNum = request.sap_doc_num || '';
        }
        // Update dispatch plans
        (planState.dispatchPlans || []).forEach(d => {
          if (d.productionOrderId === request.order_id) {
            d.customer = request.customer;
            d.poNumber = request.po_number || d.poNumber;
            d.zone = request.zone || d.zone;
          }
        });
        if(pgPool){ await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json,saved_at=NOW()::TEXT`,[JSON.stringify(planState)]); _planningStateCache=planState; _planningStateCacheTime=Date.now(); }
        else { db.prepare(`INSERT INTO planning_state (id,state_json) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json,updated_at=datetime('now')`).run(JSON.stringify(planState)); }
      }
      // 2. Update all tracking labels for this order's batch
      if (ord) {
        if(pgPool) await pgPool.query(`UPDATE tracking_labels SET customer=$1,wo_status='wo-reconciled' WHERE batch_number=$2`,[request.customer,ord.batchNumber]);
        else db.prepare(`UPDATE tracking_labels SET customer=?,wo_status='wo-reconciled' WHERE batch_number=?`).run(request.customer,ord.batchNumber);
      }
      // 3. Mark request approved
      if(pgPool) await pgPool.query(`UPDATE wo_reconciliation_requests SET status='approved',approved_by=$1,approved_at=$2 WHERE id=$3`,[session.username,now,request.id]);
      else db.prepare(`UPDATE wo_reconciliation_requests SET status='approved',approved_by=?,approved_at=? WHERE id=?`).run(session.username,now,request.id);
      return { orderId: request.order_id, customer: request.customer };
    };

    const result = await approveWO();
    logAudit(session.username, session.role, 'planning', 'WO_RECON_APPROVED',
      `W/O reconciliation ${req.params.id} approved — order ${result.orderId} → ${result.customer}`);
    res.json({ ok: true, result, message: 'W/O reconciliation complete. Replacement labels ready for printing.' });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/reject/:id
app.post('/api/wo/reject/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const { reason } = req.body;
    if (pgPool) {
      await pgPool.query(`UPDATE wo_reconciliation_requests SET status='rejected',approved_by=$1,approved_at=NOW(),rejection_reason=$2 WHERE id=$3`,
        [session.username, reason||'No reason given', req.params.id]);
    } else {
      db.prepare(`UPDATE wo_reconciliation_requests SET status='rejected',approved_by=?,approved_at=datetime('now'),rejection_reason=? WHERE id=?`).run(session.username, reason||'No reason given', req.params.id);
    }
    logAudit(session.username, session.role, 'planning', 'WO_RECON_REJECTED', `Rejected ${req.params.id}: ${reason}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/history
app.get('/api/wo/history', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    let woHistRows;
    if (pgPool) { const r = await pgPool.query('SELECT * FROM wo_reconciliation_requests ORDER BY proposed_at DESC LIMIT 50'); woHistRows=r.rows; }
    else { woHistRows = db.prepare('SELECT * FROM wo_reconciliation_requests ORDER BY proposed_at DESC LIMIT 50').all(); }
    res.json({ ok: true, requests: woHistRows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── Data Export / Import (Admin — for safe migrations) ────────

// v37E P2: Cleanup orphan/duplicate print orders
// POST /api/admin/cleanup-print-orders
// Removes:
//  (1) print_orders attached to UP (unprinted) production orders — should not exist
//  (2) duplicate print_orders WITHIN the same production_order_id+machine_id pair —
//      keeps the most recently updated one. Legitimate multi-OPM groups (same
//      production_order_id but DIFFERENT machine_ids) are preserved.
//      Also handles: multiple rows with NULL machine_id for the same production_order_id
//      (bug-created skeletons) — keeps the most recent, deletes the rest.
// Call once after deployment to clean legacy data. Safe to re-run.
app.post('/api/admin/cleanup-print-orders', async (req, res) => {
  if (!pgPool) return res.json({ ok: false, error: 'Postgres-only operation' });
  try {
    let deletedOrphans = 0;
    let dedupedCount = 0;

    // (1) Find print orders whose production order is UP — delete them
    const orphans = await pgPool.query(`
      SELECT po.id, po.batch_number, po.production_order_id
      FROM print_orders po
      JOIN production_orders prod ON prod.id = po.production_order_id
      WHERE COALESCE((prod.data_json::jsonb->>'isPrinted')::boolean, false) = false
        AND COALESCE(prod.deleted, false) = false
    `);
    for (const o of orphans.rows) {
      await pgPool.query('DELETE FROM print_orders WHERE id=$1', [o.id]);
      deletedOrphans++;
    }

    // (2) Find duplicate rows that share BOTH production_order_id AND machine_id.
    //     This preserves legitimate multi-OPM groups (same prod ID, different machine IDs).
    //     For NULL machine_id rows: also collapse multiples (these are bug-created skeletons).
    const dupGroups = await pgPool.query(`
      SELECT production_order_id,
             COALESCE(machine_id, '__NULL__') AS machine_key
      FROM print_orders
      WHERE production_order_id IS NOT NULL
      GROUP BY production_order_id, COALESCE(machine_id, '__NULL__')
      HAVING COUNT(*) > 1
    `);
    for (const grp of dupGroups.rows) {
      const machineFilter = grp.machine_key === '__NULL__'
        ? 'machine_id IS NULL'
        : 'machine_id = $2';
      const params = grp.machine_key === '__NULL__'
        ? [grp.production_order_id]
        : [grp.production_order_id, grp.machine_key];
      const dups = await pgPool.query(
        `SELECT id, machine_id, updated_at
         FROM print_orders
         WHERE production_order_id = $1 AND ${machineFilter}
         ORDER BY updated_at DESC NULLS LAST`,
        params
      );
      // Keep first row (most recently updated), delete the rest
      for (let i = 1; i < dups.rows.length; i++) {
        await pgPool.query('DELETE FROM print_orders WHERE id=$1', [dups.rows[i].id]);
        dedupedCount++;
      }
    }

    // (3) Remove orphan NULL-machine rows when a same-productionOrder row WITH a machineId exists
    //     This catches: a "skeleton" auto-created row that never got cleaned up after the user
    //     successfully assigned a machine on a separate code path.
    const skeletons = await pgPool.query(`
      SELECT po1.id
      FROM print_orders po1
      WHERE po1.machine_id IS NULL
        AND po1.production_order_id IS NOT NULL
        AND EXISTS (
          SELECT 1 FROM print_orders po2
          WHERE po2.production_order_id = po1.production_order_id
            AND po2.machine_id IS NOT NULL
            AND po2.id <> po1.id
        )
    `);
    for (const s of skeletons.rows) {
      await pgPool.query('DELETE FROM print_orders WHERE id=$1', [s.id]);
      dedupedCount++;
    }

    res.json({ ok: true, deletedOrphans, dedupedCount });
  } catch (err) {
    console.error('[Admin cleanup-print-orders] failed:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

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
app.get('/api/temp-batches/check/:machineId', async (req, res) => {
  try {
    const { machineId } = req.params;
    const today = new Date().toISOString().split('T')[0];
    const planState = getPlanningState();
    const activeOrders = (planState.orders || []).filter(o =>
      o.machineId === machineId && o.status !== 'closed' && !o.deleted
    );
    const hasActiveOrder = activeOrders.length > 0;
    let existing = null, allTemp = [];
    if (pgPool) {
      const r1 = await pgPool.query(`SELECT * FROM temp_batches WHERE machine_id=$1 AND date=$2`, [machineId, today]);
      existing = r1.rows[0] || null;
      const r2 = await pgPool.query(`SELECT * FROM temp_batches WHERE machine_id=$1 AND status='active' ORDER BY date DESC`, [machineId]);
      allTemp = r2.rows;
    } else {
      existing = db.prepare(`SELECT * FROM temp_batches WHERE machine_id = ? AND date = ?`).get(machineId, today);
      allTemp = db.prepare(`SELECT * FROM temp_batches WHERE machine_id = ? AND status = 'active' ORDER BY date DESC`).all(machineId);
    }
    const mc = (planState.machineMaster || []).find(m => m.id === machineId);
    const packSizes = planState.packSizes || {};
    const packSizeLakhs = mc ? ((packSizes[mc.size] || 100000) / 100000) : 1;
    const capLakhs = mc ? (mc.cap || 8) : 8;
    const labelCount = mc ? calcTempLabelCount(capLakhs, packSizeLakhs) : 0;
    res.json({
      ok: true, machineId, hasActiveOrder,
      activeOrders: activeOrders.map(o => ({ id:o.id, batchNumber:o.batchNumber, qty:o.qty, status:o.status })),
      todayTempBatch: existing || null,
      needsTemp: !hasActiveOrder,
      machineInfo: mc ? { size: mc.size, capLakhs, packSizeLakhs, labelCount } : null,
      activeTempBatches: allTemp
    });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/temp-batches/create — create TEMP batch for a machine/date
app.post('/api/temp-batches/create', async (req, res) => {
  try {
    const { machineId, date } = req.body;
    const batchDate = date || new Date().toISOString().split('T')[0];
    const id = tempBatchId(machineId, batchDate);
    const planState = getPlanningState();
    const mc = (planState.machineMaster || []).find(m => m.id === machineId);
    if (!mc) return res.status(400).json({ ok:false, error:'Machine not found' });
    const packSizes = planState.packSizes || {};
    const packSizeLakhs = (packSizes[mc.size] || 100000) / 100000;
    const capLakhs = mc.cap || 8;
    const labelCount = calcTempLabelCount(capLakhs, packSizeLakhs);
    let batch;
    if (pgPool) {
      await pgPool.query(`INSERT INTO temp_batches (id,machine_id,machine_size,date,daily_cap_lakhs,label_count,pack_size_lakhs) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO NOTHING`,
        [id, machineId, mc.size, batchDate, capLakhs, labelCount, packSizeLakhs]);
      await pgPool.query(`INSERT INTO temp_batch_alerts (machine_id,temp_batch_id,alert_date) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
        [machineId, id, batchDate]);
      const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [id]);
      batch = r.rows[0];
    } else {
      db.prepare(`INSERT OR IGNORE INTO temp_batches (id,machine_id,machine_size,date,daily_cap_lakhs,label_count,pack_size_lakhs) VALUES (?,?,?,?,?,?,?)`).run(id, machineId, mc.size, batchDate, capLakhs, labelCount, packSizeLakhs);
      db.prepare(`INSERT OR IGNORE INTO temp_batch_alerts (machine_id,temp_batch_id,alert_date) VALUES (?,?,?)`).run(machineId, id, batchDate);
      batch = db.prepare(`SELECT * FROM temp_batches WHERE id = ?`).get(id);
    }
    logAudit('SYSTEM','system','dpr','TEMP_BATCH_CREATED',`TEMP batch created: ${id} — ${capLakhs}L → ${labelCount} labels (Size ${mc.size})`);
    res.json({ ok:true, batch });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/temp-batches/active — all active TEMP batches (for alerts)
// ── Delete TEMP batches for a specific date ─────────────────
app.delete('/api/temp-batches/by-date', async (req, res) => {
  try {
    const { date } = req.query; // date = YYYY-MM-DD
    if (!date) return res.status(400).json({ ok: false, error: 'date required' });
    let deleted = 0;
    if (pgPool) {
      const r = await pgPool.query(
        `DELETE FROM temp_batches WHERE date = $1 AND status != 'reconciled'`, [date]
      );
      deleted = r.rowCount;
    } else {
      const r = db.prepare(`DELETE FROM temp_batches WHERE date = ? AND status != 'reconciled'`).run(date);
      deleted = r.changes;
    }
    res.json({ ok: true, deleted, date });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/api/temp-batches/active', async (req, res) => {
  try {
    let batches;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM temp_batches WHERE status='active' ORDER BY machine_id, date DESC`);
      batches = r.rows;
    } else {
      batches = db.prepare(`SELECT * FROM temp_batches WHERE status='active' ORDER BY machine_id, date DESC`).all();
    }
    const today = new Date().toISOString().split('T')[0];
    const TEMP_CUTOFF = '2026-04-27';
    // Ignore TEMP batches created before April 25 2026
    const filtered = batches.filter(b => (b.created_at||b.date||'') >= TEMP_CUTOFF);
    const enriched = filtered.map(b => ({...b, daysActive: Math.floor((new Date(today)-new Date(b.date))/86400000)+1}));
    res.json({ ok:true, batches: enriched, count: enriched.length });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/reconciliation/propose — Planning Manager proposes reconciliation
app.post('/api/reconciliation/propose', async (req, res) => {
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
      let tb;
      if (pgPool) { const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1',[mapping.tempBatchId]); tb=r.rows[0]; }
      else { tb = db.prepare('SELECT * FROM temp_batches WHERE id=?').get(mapping.tempBatchId); }
      if (!tb) return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} not found` });
      if (tb.status !== 'active') return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} is not active` });
    }

    const totalBoxes = tempBatchMappings.reduce((s,m) => s + (m.boxes || 0), 0);
    const id = `RECON-${Date.now()}`;

    if (pgPool) {
      await pgPool.query(`INSERT INTO reconciliation_requests (id,proposed_by,status,order_id,order_details,back_date,temp_batch_mappings,total_boxes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [id, session.username, 'pending', orderDetails.id||`ORDER-${Date.now()}`, JSON.stringify(orderDetails), backDate, JSON.stringify(tempBatchMappings), totalBoxes]);
    } else {
      db.prepare(`INSERT INTO reconciliation_requests (id,proposed_by,status,order_id,order_details,back_date,temp_batch_mappings,total_boxes) VALUES (?,?,?,?,?,?,?,?)`).run(
        id, session.username, 'pending', orderDetails.id||`ORDER-${Date.now()}`, JSON.stringify(orderDetails), backDate, JSON.stringify(tempBatchMappings), totalBoxes);
    }

    logAudit(session.username, session.role, 'planning', 'RECON_PROPOSED',
      `Reconciliation proposed: ${id} — ${tempBatchMappings.length} TEMP batches → Order, ${totalBoxes} boxes`);

    res.json({ ok:true, requestId: id, status:'pending', message:'Awaiting Admin approval' });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/reconciliation/pending — Admin views pending requests
app.get('/api/reconciliation/pending', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`).all();
    }
    const requests = rows.map(r => ({...r, order_details:JSON.parse(r.order_details||'{}'), temp_batch_mappings:JSON.parse(r.temp_batch_mappings||'[]')}));
    res.json({ ok:true, requests });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/reconciliation/approve/:id — Admin approves and executes reconciliation
app.post('/api/reconciliation/approve/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') {
      return res.status(403).json({ ok:false, error:'Admin only' });
    }

    const request = pgPool ? (await pgPool.query('SELECT * FROM reconciliation_requests WHERE id=$1',[req.params.id])).rows[0] : db.prepare('SELECT * FROM reconciliation_requests WHERE id=?').get(req.params.id);
    if (!request) return res.status(404).json({ ok:false, error:'Request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok:false, error:'Request is not pending' });

    const orderDetails = JSON.parse(request.order_details);
    const mappings = JSON.parse(request.temp_batch_mappings);
    const orderId = request.order_id;

    // Execute reconciliation atomically
    const reconcileAsync = async () => {
      const now = new Date().toISOString();
      const results = { migratedScans:0, migratedLabels:0, migratedWastage:0, tempBatchesReconciled:0 };

      for (const mapping of mappings) {
        const { tempBatchId: tbId, boxes, startLabelNumber, endLabelNumber } = mapping;
        const tb = pgPool ? (await pgPool.query('SELECT * FROM temp_batches WHERE id=$1',[tbId])).rows[0] : db.prepare('SELECT * FROM temp_batches WHERE id=?').get(tbId);
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

        const labelsToMigrate = pgPool ? (await pgPool.query(`SELECT * FROM tracking_labels WHERE ${labelFilter.replace('?','$1').replace('?','$2').replace('?','$3')}`, labelArgs)).rows : db.prepare(`SELECT * FROM tracking_labels WHERE ${labelFilter}`).all(...labelArgs);

        for (const label of labelsToMigrate) {
          const newLabelId = label.id.replace(tbId, orderId);
          if(pgPool){ await pgPool.query(`INSERT INTO tracking_labels SELECT replace(id,$1,$2) as id,$2 as batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data FROM tracking_labels WHERE id=$3 ON CONFLICT(id) DO NOTHING`,[tbId,orderId,label.id]); } else { db.prepare(`INSERT OR REPLACE INTO tracking_labels SELECT replace(id,?,?) as id,? as batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data FROM tracking_labels WHERE id=?`).run(tbId,orderId,orderId,label.id); }

          // Migrate scans for this label
          if(pgPool){ const sm=await pgPool.query(`UPDATE tracking_scans SET label_id=replace(label_id,$1,$2),batch_number=$2 WHERE label_id=$3`,[tbId,orderId,label.id]); results.migratedScans+=sm.rowCount||0; } else { const scanMigrated=db.prepare(`UPDATE tracking_scans SET label_id=replace(label_id,?,?),batch_number=? WHERE label_id=?`).run(tbId,orderId,orderId,label.id); results.migratedScans+=scanMigrated.changes; }
          results.migratedLabels++;

          // Remove old TEMP label if new one created
          if (newLabelId !== label.id) {
            if(pgPool) await pgPool.query('DELETE FROM tracking_labels WHERE id=$1 AND id!=$2',[label.id,newLabelId]); else db.prepare('DELETE FROM tracking_labels WHERE id=? AND id!=?').run(label.id,newLabelId);
          }
        }

        // 2. Migrate wastage records
        if(pgPool){ const wm=await pgPool.query('UPDATE tracking_wastage SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); results.migratedWastage+=wm.rowCount||0; } else { const wastage=db.prepare('UPDATE tracking_wastage SET batch_number=? WHERE batch_number=?').run(orderId,tbId); results.migratedWastage+=wastage.changes; }

        // 3. Migrate stage closures
        if(pgPool) await pgPool.query('UPDATE tracking_stage_closure SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); else db.prepare('UPDATE tracking_stage_closure SET batch_number=? WHERE batch_number=?').run(orderId,tbId);

        // 4. Migrate DPR production actuals
        if(pgPool) await pgPool.query('UPDATE production_actuals SET order_id=$1,batch_number=$2 WHERE batch_number=$3',[orderId,orderDetails.batchNumber||orderId,tbId]); else db.prepare('UPDATE production_actuals SET order_id=?,batch_number=? WHERE batch_number=?').run(orderId,orderDetails.batchNumber||orderId,tbId);

        // 5. Update dispatch records
        if(pgPool) await pgPool.query('UPDATE tracking_dispatch_records SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); else db.prepare('UPDATE tracking_dispatch_records SET batch_number=? WHERE batch_number=?').run(orderId,tbId);

        // 6. Mark TEMP batch as reconciled (or partially reconciled)
        const isFullReconcile = !startLabelNumber; // full batch
        if(pgPool) await pgPool.query('UPDATE temp_batches SET status=$1,reconciled_order_id=$2,reconciled_at=$3,reconciled_by=$4 WHERE id=$5',[isFullReconcile?'reconciled':'partial',orderId,now,session.username,tbId]); else db.prepare('UPDATE temp_batches SET status=?,reconciled_order_id=?,reconciled_at=?,reconciled_by=? WHERE id=?').run(isFullReconcile?'reconciled':'partial',orderId,now,session.username,tbId);
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
        await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json, saved_at=NOW()::TEXT`, [JSON.stringify(planState)]);
        _planningStateCache = planState; _planningStateCacheTime = Date.now();
      }

      // 8. Mark reconciliation request as approved
      await pgPool.query(`UPDATE reconciliation_requests SET status='approved',approved_by=$1,approved_at=$2 WHERE id=$3`,
        [session.username, now, request.id]);

      return results;
    };

    const results = await reconcileAsync();

    logAudit(session.username, session.role, 'planning', 'RECON_APPROVED',
      `Reconciliation ${req.params.id} approved — ${results.migratedLabels} labels, ${results.migratedScans} scans migrated`);

    res.json({ ok:true, results, message:'Reconciliation complete. Replacement labels ready for printing.' });
  } catch(err) {
    res.status(500).json({ ok:false, error:err.message });
  }
});

// POST /api/reconciliation/reject/:id — Admin rejects
app.post('/api/reconciliation/reject/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
    const { reason } = req.body;
    if (pgPool) {
      await pgPool.query(`UPDATE reconciliation_requests SET status='rejected',approved_by=$1,approved_at=NOW(),rejection_reason=$2 WHERE id=$3`,
        [session.username, reason||'No reason given', req.params.id]);
    } else {
      db.prepare(`UPDATE reconciliation_requests SET status='rejected',approved_by=?,approved_at=datetime('now'),rejection_reason=? WHERE id=?`).run(session.username, reason||'No reason given', req.params.id);
    }
    logAudit(session.username, session.role, 'planning', 'RECON_REJECTED', `Rejected: ${req.params.id} — ${reason}`);
    res.json({ ok:true });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/reconciliation/history — all reconciliation requests
app.get('/api/reconciliation/history', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM reconciliation_requests ORDER BY proposed_at DESC LIMIT 100`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM reconciliation_requests ORDER BY proposed_at DESC LIMIT 100`).all();
    }
    const requests = rows.map(r => ({...r, order_details:JSON.parse(r.order_details||'{}'), temp_batch_mappings:JSON.parse(r.temp_batch_mappings||'[]')}));
    res.json({ ok:true, requests });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

app.get('/api/health', (req, res) => {
  try {
    const planningRow  = db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get();
    const dprCount     = db.prepare('SELECT COUNT(*) as c FROM dpr_records').get();
    const actualsCount = db.prepare('SELECT COUNT(*) as c FROM production_actuals').get();
    res.json({
      ok: true,
      server: 'Sunloc Integrated Server v1.0',
      db: DB_PATH,
      planningSavedAt: planningRow?.saved_at || null,
      dprRecords: dprCount?.c || 0,
      actualsEntries: actualsCount?.c || 0,
      uptime: Math.floor(process.uptime()) + 's',
    });
  } catch(err) {
    // Server is alive even if DB query fails (e.g. still warming up)
    res.json({ ok: true, server: 'Sunloc Integrated Server v1.0', db: DB_PATH, uptime: Math.floor(process.uptime())+'s', note: 'DB initialising: '+err.message });
  }
});

// NOTE: catch-all SPA fallback moved to END of file (after all API routes)
// so that /api/tracking/* routes are not intercepted by the wildcard.

// ═══════════════════════════════════════════════════════
// TRACKING APP SCHEMA (add to existing server.js)
// ═══════════════════════════════════════════════════════


// ═══════════════════════════════════════════════════════
// DISPATCH PLANS — dedicated table
// ═══════════════════════════════════════════════════════

// GET /api/dispatch-plans
app.get('/api/dispatch-plans', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT data_json FROM dispatch_plans WHERE deleted=false ORDER BY updated_at DESC');
      rows = r.rows.map(r => typeof r.data_json === 'string' ? JSON.parse(r.data_json) : r.data_json);
    } else {
      rows = db.prepare('SELECT data_json FROM dispatch_plans WHERE deleted=0 ORDER BY updated_at DESC').all()
              .map(r => JSON.parse(r.data_json));
    }
    res.json({ ok: true, plans: rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// v39 Phase 13: GET /api/planned-vs-actual?month=YYYY-MM
// Compares dispatch_plans rows (planned) against invoices_received rows
// (actual). Joins on batch_number. When month filter present, restricts to
// plans whose plannedDate falls in that month AND invoices whose
// invoice_date falls in that month. Returns one row per planned dispatch
// with matched actual data alongside; unmatched actuals are returned as
// "unplanned" entries.
//
// Row shape:
//   {
//     batchNumber, customer, poNumber, size, pcCode,
//     planned: { qty, boxes, date, status, hasPlan, planId },
//     actual:  { qty, boxes, invoiceDocNum, invoiceDate, dispatchedAt, dispatchStatus, vehicle, hasInvoice },
//     variance: { qty, boxes, daysLate, status }   // qty positive = over, negative = short
//   }
app.get('/api/planned-vs-actual', async (req, res) => {
  try {
    const month = (req.query.month || '').toString(); // "YYYY-MM" or empty
    // Date bounds (only used if month provided)
    let monthFrom = null, monthTo = null;
    if (/^\d{4}-\d{2}$/.test(month)) {
      const [yr, mo] = month.split('-').map(Number);
      monthFrom = `${yr}-${String(mo).padStart(2,'0')}-01`;
      const last = new Date(yr, mo, 0).getDate();
      monthTo = `${yr}-${String(mo).padStart(2,'0')}-${String(last).padStart(2,'0')}`;
    }
    // Load dispatch plans
    let planRows;
    if (pgPool) {
      const r = await pgPool.query('SELECT data_json FROM dispatch_plans WHERE deleted=false');
      planRows = r.rows.map(r => typeof r.data_json === 'string' ? JSON.parse(r.data_json) : r.data_json);
    } else {
      planRows = db.prepare('SELECT data_json FROM dispatch_plans WHERE deleted=0').all()
                    .map(r => JSON.parse(r.data_json));
    }
    // Filter plans by month if specified — uses plannedDate (top-level field)
    const plans = planRows.filter(p => {
      if (!monthFrom) return true;
      const d = (p.plannedDate || '').toString().slice(0, 10);
      if (!d) return false;
      return d >= monthFrom && d <= monthTo;
    });
    // Load invoices_received (filtered by month if specified)
    let invRows;
    const invSql = monthFrom
      ? (pgPool
          ? `SELECT * FROM invoices_received WHERE invoice_date >= $1 AND invoice_date <= $2 ORDER BY invoice_date DESC`
          : `SELECT * FROM invoices_received WHERE invoice_date >= ? AND invoice_date <= ? ORDER BY invoice_date DESC`)
      : (pgPool
          ? `SELECT * FROM invoices_received ORDER BY invoice_date DESC LIMIT 2000`
          : `SELECT * FROM invoices_received ORDER BY invoice_date DESC LIMIT 2000`);
    if (pgPool) {
      const r = monthFrom
        ? await pgPool.query(invSql, [monthFrom, monthTo])
        : await pgPool.query(invSql);
      invRows = r.rows;
    } else {
      invRows = monthFrom
        ? db.prepare(invSql).all(monthFrom, monthTo)
        : db.prepare(invSql).all();
    }
    // Index invoices by batch_number — handles multi-invoice-per-batch by summing
    const invByBatch = {};
    for (const inv of invRows) {
      const bn = inv.batch_number || '';
      if (!bn) continue;
      if (!invByBatch[bn]) invByBatch[bn] = [];
      invByBatch[bn].push(inv);
    }
    const matchedBatches = new Set();
    // Build joined rows from plans
    const rows = plans.map(p => {
      // Handle consolidated plans (comma-joined batch list) by splitting and using first
      const batches = (p.batchNumber || '').split(',').map(s => s.trim()).filter(Boolean);
      const primaryBatch = batches[0] || '';
      const matchedInvoices = batches.flatMap(bn => {
        matchedBatches.add(bn);
        return invByBatch[bn] || [];
      });
      // Aggregate matched invoices
      const actualBoxes = matchedInvoices.reduce((s, i) => s + (parseInt(i.total_boxes) || 0), 0);
      const actualQty   = matchedInvoices.reduce((s, i) => s + (parseFloat(i.total_qty_lakhs) || (parseInt(i.total_boxes)||0)/100), 0);
      const dispatched  = matchedInvoices.filter(i => i.dispatch_status === 'dispatched');
      const dispatchStatus = matchedInvoices.length === 0 ? 'not_invoiced'
                          : dispatched.length === matchedInvoices.length ? 'dispatched'
                          : dispatched.length > 0 ? 'partial'
                          : 'pending_scan_out';
      const latestInvoice = matchedInvoices[0];
      const latestDispatch = dispatched.sort((a,b) => (b.dispatched_at||'').localeCompare(a.dispatched_at||''))[0];
      // Variance
      const plannedQty   = parseFloat(p.qty) || 0;
      const plannedBoxes = parseInt(p.boxes) || 0;
      let daysLate = 0;
      if (p.plannedDate && latestDispatch?.dispatched_at) {
        const planDt = new Date(p.plannedDate);
        const dispDt = new Date(latestDispatch.dispatched_at);
        daysLate = Math.round((dispDt - planDt) / 86400000);
      }
      const varianceQty = actualQty - plannedQty;
      const varianceBoxes = actualBoxes - plannedBoxes;
      let varianceStatus = 'on_target';
      if (Math.abs(varianceQty) > plannedQty * 0.05) varianceStatus = varianceQty > 0 ? 'over' : 'short';
      if (daysLate > 0) varianceStatus = varianceStatus === 'on_target' ? 'late' : (varianceStatus + '_late');
      return {
        batchNumber: p.batchNumber || '',
        primaryBatch,
        customer: p.customer || '',
        poNumber: p.poNumber || '',
        size: p.size || '',
        pcCode: p.pcCode || '',
        colour: p.colour || '',
        zone: p.zone || '',
        planned: {
          qty: plannedQty,
          boxes: plannedBoxes,
          date: p.plannedDate || '',
          status: p.status || '',
          hasPlan: true,
          planId: p.id || null,
          isExportPlaceholder: !!p.exportPending,
        },
        actual: {
          qty: actualQty,
          boxes: actualBoxes,
          invoiceCount: matchedInvoices.length,
          invoiceDocNum: latestInvoice?.sap_doc_num || '',
          invoiceDate: latestInvoice?.invoice_date || '',
          dispatchedAt: latestDispatch?.dispatched_at || '',
          dispatchStatus,
          vehicle: latestDispatch?.vehicle_no || '',
          hasInvoice: matchedInvoices.length > 0,
        },
        variance: {
          qty: varianceQty,
          boxes: varianceBoxes,
          daysLate,
          status: varianceStatus,
        },
      };
    });
    // Add "unplanned" entries — invoices whose batch_number is NOT in any plan
    const unplanned = [];
    for (const inv of invRows) {
      const bn = inv.batch_number || '';
      if (!bn) continue;
      if (matchedBatches.has(bn)) continue;
      unplanned.push({
        batchNumber: bn,
        primaryBatch: bn,
        customer: inv.customer || '',
        poNumber: inv.po_number || '',
        size: inv.size || '',
        pcCode: inv.pc_code || '',
        colour: inv.colour || '',
        zone: '',
        planned: { qty: 0, boxes: 0, date: '', status: 'unplanned', hasPlan: false, planId: null, isExportPlaceholder: false },
        actual: {
          qty: parseFloat(inv.total_qty_lakhs) || (parseInt(inv.total_boxes)||0)/100,
          boxes: parseInt(inv.total_boxes) || 0,
          invoiceCount: 1,
          invoiceDocNum: inv.sap_doc_num || '',
          invoiceDate: inv.invoice_date || '',
          dispatchedAt: inv.dispatched_at || '',
          dispatchStatus: inv.dispatch_status === 'dispatched' ? 'dispatched' : 'pending_scan_out',
          vehicle: inv.vehicle_no || '',
          hasInvoice: true,
        },
        variance: { qty: parseFloat(inv.total_qty_lakhs) || 0, boxes: parseInt(inv.total_boxes)||0, daysLate: 0, status: 'unplanned' },
      });
    }
    res.json({
      ok: true,
      month: month || 'all',
      count: rows.length + unplanned.length,
      rows: rows.concat(unplanned),
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/dispatch-plans/bulk — save all dispatch plans
app.post('/api/dispatch-plans/bulk', async (req, res) => {
  try {
    const { plans } = req.body;
    if (!Array.isArray(plans)) return res.status(400).json({ ok: false, error: 'plans array required' });
    if (pgPool) {
      for (const p of plans) {
        if (!p || !p.id) continue;
        await pgPool.query(`
          INSERT INTO dispatch_plans (id, data_json, production_order_id, batch_number, customer, zone, status, is_auto, deleted, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()::TEXT)
          ON CONFLICT(id) DO UPDATE SET
            data_json=$2, production_order_id=$3, batch_number=$4, customer=$5,
            zone=$6, status=$7, is_auto=$8, deleted=$9, updated_at=NOW()::TEXT
        `, [p.id, JSON.stringify(p), p.productionOrderId||null, p.batchNumber||null,
            p.customer||null, p.zone||null, p.status||'pending', p.isAuto||false, p.deleted||false]);
      }
    } else {
      const stmt = db.prepare(`INSERT INTO dispatch_plans (id,data_json,production_order_id,batch_number,customer,zone,status,is_auto,deleted,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))
        ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json,production_order_id=excluded.production_order_id,
        batch_number=excluded.batch_number,customer=excluded.customer,zone=excluded.zone,
        status=excluded.status,is_auto=excluded.is_auto,deleted=excluded.deleted,updated_at=datetime('now')`);
      for (const p of plans) {
        if (!p || !p.id) continue;
        stmt.run(p.id, JSON.stringify(p), p.productionOrderId||null, p.batchNumber||null,
                 p.customer||null, p.zone||null, p.status||'pending', p.isAuto?1:0, p.deleted?1:0);
      }
    }
    res.json({ ok: true, saved: plans.length });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ═══════════════════════════════════════════════════════
// DAILY PRINTING — dedicated table
// ═══════════════════════════════════════════════════════

// GET /api/daily-printing
app.get('/api/daily-printing', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT data_json FROM daily_printing ORDER BY date DESC, updated_at DESC');
      rows = r.rows.map(r => typeof r.data_json === 'string' ? JSON.parse(r.data_json) : r.data_json);
    } else {
      rows = db.prepare('SELECT data_json FROM daily_printing ORDER BY date DESC, updated_at DESC').all()
              .map(r => JSON.parse(r.data_json));
    }
    res.json({ ok: true, logs: rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/daily-printing/bulk — save all daily printing logs
app.post('/api/daily-printing/bulk', async (req, res) => {
  try {
    const { logs } = req.body;
    if (!Array.isArray(logs)) return res.status(400).json({ ok: false, error: 'logs array required' });
    if (pgPool) {
      for (const l of logs) {
        if (!l || !l.id) continue;
        await pgPool.query(`
          INSERT INTO daily_printing (id, data_json, print_order_id, machine_id, date, updated_at)
          VALUES ($1,$2,$3,$4,$5,NOW()::TEXT)
          ON CONFLICT(id) DO UPDATE SET
            data_json=$2, print_order_id=$3, machine_id=$4, date=$5, updated_at=NOW()::TEXT
        `, [l.id, JSON.stringify(l), l.printOrderId||null, l.machineId||null, l.date||null]);
      }
    } else {
      const stmt = db.prepare(`INSERT INTO daily_printing (id,data_json,print_order_id,machine_id,date,updated_at)
        VALUES (?,?,?,?,?,datetime('now'))
        ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json,print_order_id=excluded.print_order_id,
        machine_id=excluded.machine_id,date=excluded.date,updated_at=datetime('now')`);
      for (const l of logs) {
        if (!l || !l.id) continue;
        stmt.run(l.id, JSON.stringify(l), l.printOrderId||null, l.machineId||null, l.date||null);
      }
    }
    res.json({ ok: true, saved: logs.length });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ═══════════════════════════════════════════════════════
// PACK SIZES — dedicated table
// ═══════════════════════════════════════════════════════

// GET /api/pack-sizes
app.get('/api/pack-sizes', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT size, value FROM pack_sizes');
      rows = r.rows;
    } else {
      rows = db.prepare('SELECT size, value FROM pack_sizes').all();
    }
    const packSizes = {};
    rows.forEach(r => { packSizes[r.size] = r.value; });
    res.json({ ok: true, packSizes });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/pack-sizes — save pack sizes
app.post('/api/pack-sizes', async (req, res) => {
  try {
    const { packSizes } = req.body;
    if (!packSizes || typeof packSizes !== 'object') return res.status(400).json({ ok: false, error: 'packSizes required' });
    if (pgPool) {
      for (const [size, value] of Object.entries(packSizes)) {
        await pgPool.query(`
          INSERT INTO pack_sizes (size, value, updated_at) VALUES ($1,$2,NOW()::TEXT)
          ON CONFLICT(size) DO UPDATE SET value=$2, updated_at=NOW()::TEXT
        `, [size, value]);
      }
    } else {
      const stmt = db.prepare(`INSERT INTO pack_sizes (size,value,updated_at) VALUES (?,?,datetime('now'))
        ON CONFLICT(size) DO UPDATE SET value=excluded.value,updated_at=datetime('now')`);
      for (const [size, value] of Object.entries(packSizes)) { stmt.run(size, value); }
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET actuals summary for a machine (for DPR to show cumulative vs planned)
app.get('/api/actuals/machine/:machineId', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT date,shift,qty_lakhs,order_id,batch_number FROM production_actuals WHERE machine_id=$1 ORDER BY date DESC, shift LIMIT 90`, [req.params.machineId]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT date,shift,qty_lakhs,order_id,batch_number FROM production_actuals WHERE machine_id=? ORDER BY date DESC, shift LIMIT 90`).all(req.params.machineId);
    }
    res.json({ ok: true, actuals: rows });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET actuals for a specific order
app.get('/api/actuals/order/:orderId', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT date,shift,qty_lakhs,machine_id FROM production_actuals WHERE order_id=$1 OR batch_number=$1 ORDER BY date,shift`, [req.params.orderId]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT date,shift,qty_lakhs,machine_id FROM production_actuals WHERE order_id=? OR batch_number=? ORDER BY date,shift`).all(req.params.orderId, req.params.orderId);
    }
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
  try {
    // db wrapper reads from PostgreSQL when pgPool is active
    // Using simple token lookup (datetime comparison handled by expiry logic below)
    const session = db.prepare(`SELECT * FROM app_sessions WHERE token = ?`).get(token);
    if (!session) return null;
    // Check expiry in JS (works for both SQLite and PostgreSQL datetime formats)
    if (session.expires_at && new Date(session.expires_at) < new Date()) return null;
    return session;
  } catch(e) { return null; }
}

function logAudit(username, role, app, action, details, ip) {
  try {
    if (pgPool) {
      pgPool.query(`INSERT INTO audit_log (username,role,app,action,details,ip) VALUES ($1,$2,$3,$4,$5,$6)`,
        [username, role, app, action, details||null, ip||null]).catch(e=>console.error('Audit log error:',e.message));
    } else {
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details,ip) VALUES (?,?,?,?,?,?)`).run(username,role,app,action,details||null,ip||null);
    }
  } catch(e) { console.error('Audit log error:', e.message); }
}

// POST /api/auth/login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { username, pin, app: appName } = req.body;
    if (!username || !pin || !appName) return res.status(400).json({ ok: false, error: 'Missing credentials' });
    let user;
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM app_users WHERE username=$1 AND app=$2', [username, appName]);
      user = r.rows[0];
    } else {
      user = db.prepare('SELECT * FROM app_users WHERE username=? AND app=?').get(username, appName);
    }
    if (!user) return res.status(401).json({ ok: false, error: 'User not found' });
    if (user.pin_hash !== hashPin(pin)) return res.status(401).json({ ok: false, error: 'Invalid PIN' });
    const token = generateToken();
    const expires = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().replace('T',' ').slice(0,19);
    if (pgPool) {
      await pgPool.query('INSERT INTO app_sessions (token,user_id,username,role,app,expires_at) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(token) DO NOTHING',
        [token, user.id, user.username, user.role, appName, expires]);
    } else {
      db.prepare('INSERT INTO app_sessions (token,user_id,username,role,app,expires_at) VALUES (?,?,?,?,?,?)').run(token, user.id, user.username, user.role, appName, expires);
    }
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
app.post('/api/auth/logout', async (req, res) => {
  const { token } = req.body;
  if (token) {
    const session = verifyToken(token);
    if (session) {
      logAudit(session.username, session.role, session.app, 'LOGOUT', null, req.ip);
      if (pgPool) await pgPool.query('DELETE FROM app_sessions WHERE token=$1', [token]);
      else db.prepare('DELETE FROM app_sessions WHERE token=?').run(token);
    }
  }
  res.json({ ok: true });
});

// POST /api/auth/change-pin
app.post('/api/auth/change-pin', async (req, res) => {
  try {
    const { token, username, newPin } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin' && session.username !== username) {
      return res.status(403).json({ ok: false, error: 'Only admin can change other users PINs' });
    }
    if (pgPool) {
      await pgPool.query('UPDATE app_users SET pin_hash=$1, updated_at=NOW() WHERE username=$2 AND app=$3', [hashPin(newPin), username, session.app]);
    } else {
      db.prepare(`UPDATE app_users SET pin_hash=?, updated_at=datetime('now') WHERE username=? AND app=?`).run(hashPin(newPin), username, session.app);
    }
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
app.get('/api/audit/view', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const limit = parseInt(req.query.limit) || 200;
    const app = req.query.app || session.app;
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM audit_log WHERE app=$1 ORDER BY ts DESC LIMIT $2`, [app, limit]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM audit_log WHERE app = ? ORDER BY ts DESC LIMIT ?`).all(app, limit);
    }
    res.json({ ok: true, logs: rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/auth/users — admin only, list users for an app
app.get('/api/auth/users', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    let users;
    if (pgPool) {
      const r = await pgPool.query(`SELECT id,username,role,app,created_at,updated_at FROM app_users WHERE app=$1`, [req.query.app || session.app]);
      users = r.rows;
    } else {
      users = db.prepare(`SELECT id,username,role,app,created_at,updated_at FROM app_users WHERE app=?`).all(req.query.app || session.app);
    }
    res.json({ ok: true, users });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── TEMP Batch Colour/PC Code Update ────────────────────────

// POST /api/temp-batches/update-details — save colour + PC Code (one-time per TEMP batch)
app.post('/api/temp-batches/update-details', async (req, res) => {
  try {
    const { tempBatchId, colour, pcCode } = req.body;
    if (!tempBatchId) return res.status(400).json({ ok: false, error: 'Missing tempBatchId' });
    let updated;
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [tempBatchId]);
      if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'TEMP batch not found' });
      await pgPool.query(`UPDATE temp_batches SET colour=$1, pc_code=$2, colour_confirmed=1 WHERE id=$3`, [colour||null, pcCode||null, tempBatchId]);
      const r2 = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [tempBatchId]);
      updated = r2.rows[0];
    } else {
      const tb = db.prepare('SELECT * FROM temp_batches WHERE id = ?').get(tempBatchId);
      if (!tb) return res.status(404).json({ ok: false, error: 'TEMP batch not found' });
      db.prepare(`UPDATE temp_batches SET colour = ?, pc_code = ?, colour_confirmed = 1 WHERE id = ?`).run(colour||null, pcCode||null, tempBatchId);
      updated = db.prepare('SELECT * FROM temp_batches WHERE id = ?').get(tempBatchId);
    }
    logAudit('SYSTEM', 'system', 'dpr', 'TEMP_DETAILS_SET', `TEMP batch ${tempBatchId} — Colour: ${colour}, PC Code: ${pcCode}`);
    res.json({ ok: true, batch: updated });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── W/O (Without Order) Reconciliation ──────────────────────

// POST /api/wo/assign-customer — Planning Manager assigns customer to W/O order
app.post('/api/wo/assign-customer', async (req, res) => {
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
    if (pgPool) {
      await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json,saved_at=NOW()::TEXT`, [JSON.stringify(planState)]);
    } else {
      db.prepare(`INSERT INTO planning_state (id, state_json) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json, updated_at = datetime('now')`).run(JSON.stringify(planState));
    }
    _planningStateCache = planState;
    logAudit(session.username, session.role, 'planning', 'WO_CUSTOMER_ASSIGNED',
      `W/O order ${orderId} assigned to customer: ${customer}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/propose-reconciliation — Planning Manager proposes W/O → real order
// v39 Phase 9c: accepts optional sapDocEntry + sapDocNum to carry forward to approval.
app.post('/api/wo/propose-reconciliation', async (req, res) => {
  try {
    const { token, orderId, customer, poNumber, zone, qtyConfirmed, sapDocEntry, sapDocNum } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
    }
    if (!customer) return res.status(400).json({ ok: false, error: 'Customer name required' });
    // v39 Phase 9c: partial SAP link guard — both or neither
    if ((sapDocEntry && !sapDocNum) || (!sapDocEntry && sapDocNum)) {
      return res.status(400).json({ ok: false, error: 'sapDocEntry and sapDocNum must both be provided, or both omitted' });
    }
    const id = `WORECON-${Date.now()}`;
    const billTo = req.body.billTo || '';
    if (pgPool) {
      await pgPool.query(`INSERT INTO wo_reconciliation_requests (id,proposed_by,status,order_id,customer,po_number,zone,qty_confirmed,sap_doc_entry,sap_doc_num) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [id, session.username, 'pending', orderId, customer, poNumber||null, zone||null, qtyConfirmed||null, sapDocEntry||null, sapDocNum||null]);
      if (billTo && billTo !== customer) await pgPool.query('UPDATE wo_reconciliation_requests SET customer=$1 WHERE id=$2', [customer+'|||'+billTo, id]);
    } else {
      db.prepare(`INSERT INTO wo_reconciliation_requests (id,proposed_by,status,order_id,customer,po_number,zone,qty_confirmed,sap_doc_entry,sap_doc_num) VALUES (?,?,?,?,?,?,?,?,?,?)`).run(id, session.username, 'pending', orderId, customer, poNumber||null, zone||null, qtyConfirmed||null, sapDocEntry||null, sapDocNum||null);
      if (billTo && billTo !== customer) db.prepare('UPDATE wo_reconciliation_requests SET customer=? WHERE id=?').run(customer+'|||'+billTo, id);
    }
    logAudit(session.username, session.role, 'planning', 'WO_RECON_PROPOSED',
      `W/O reconciliation proposed: ${id} for order ${orderId} → customer ${customer}${sapDocEntry?` (SAP ${sapDocEntry}/${sapDocNum})`:''}`);
    res.json({ ok: true, requestId: id, status: 'pending' });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/pending — Admin views pending W/O reconciliation requests
app.get('/api/wo/pending', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    let woRows;
    if (pgPool) { const r = await pgPool.query(`SELECT * FROM wo_reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`); woRows=r.rows; }
    else { woRows = db.prepare(`SELECT * FROM wo_reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`).all(); }
    const planState = getPlanningState();
    const enriched = woRows.map(r => ({...r, orderDetails:(planState.orders||[]).find(o=>o.id===r.order_id)||{}}));
    res.json({ ok: true, requests: enriched });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/approve/:id — Admin approves W/O reconciliation
app.post('/api/wo/approve/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const request = pgPool ? (await pgPool.query('SELECT * FROM wo_reconciliation_requests WHERE id=$1',[req.params.id])).rows[0] : db.prepare('SELECT * FROM wo_reconciliation_requests WHERE id=?').get(req.params.id);
    if (!request) return res.status(404).json({ ok: false, error: 'Request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok: false, error: 'Already processed' });

    const approveWO = async () => {
      const now = new Date().toISOString();
      // 1. Update planning state: change woStatus to 'active', add customer
      const planState = getPlanningState();
      const ord = (planState.orders || []).find(o => o.id === request.order_id);
      if (ord) {
        const custParts = (request.customer||'').split('|||');
        ord.customer = custParts[0];
        ord.shipTo   = custParts[0];
        ord.billTo   = custParts[1] || '';
        ord.poNumber = request.po_number || ord.poNumber;
        ord.zone = request.zone || ord.zone;
        if (request.qty_confirmed) ord.qty = request.qty_confirmed;
        ord.woStatus = 'wo-reconciled';
        ord.woReconciledAt = now;
        ord.woReconciledBy = session.username;
        // v39 Phase 9c: apply SAP refs from the reconciliation request, if present
        if (request.sap_doc_entry) {
          ord.sapDocEntry = request.sap_doc_entry;
          ord.sapDocNum = request.sap_doc_num || '';
        }
        // Update dispatch plans
        (planState.dispatchPlans || []).forEach(d => {
          if (d.productionOrderId === request.order_id) {
            d.customer = request.customer;
            d.poNumber = request.po_number || d.poNumber;
            d.zone = request.zone || d.zone;
          }
        });
        if(pgPool){ await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json,saved_at=NOW()::TEXT`,[JSON.stringify(planState)]); _planningStateCache=planState; _planningStateCacheTime=Date.now(); }
        else { db.prepare(`INSERT INTO planning_state (id,state_json) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json,updated_at=datetime('now')`).run(JSON.stringify(planState)); }
      }
      // 2. Update all tracking labels for this order's batch
      if (ord) {
        if(pgPool) await pgPool.query(`UPDATE tracking_labels SET customer=$1,wo_status='wo-reconciled' WHERE batch_number=$2`,[request.customer,ord.batchNumber]);
        else db.prepare(`UPDATE tracking_labels SET customer=?,wo_status='wo-reconciled' WHERE batch_number=?`).run(request.customer,ord.batchNumber);
      }
      // 3. Mark request approved
      if(pgPool) await pgPool.query(`UPDATE wo_reconciliation_requests SET status='approved',approved_by=$1,approved_at=$2 WHERE id=$3`,[session.username,now,request.id]);
      else db.prepare(`UPDATE wo_reconciliation_requests SET status='approved',approved_by=?,approved_at=? WHERE id=?`).run(session.username,now,request.id);
      return { orderId: request.order_id, customer: request.customer };
    };

    const result = await approveWO();
    logAudit(session.username, session.role, 'planning', 'WO_RECON_APPROVED',
      `W/O reconciliation ${req.params.id} approved — order ${result.orderId} → ${result.customer}`);
    res.json({ ok: true, result, message: 'W/O reconciliation complete. Replacement labels ready for printing.' });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/reject/:id
app.post('/api/wo/reject/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const { reason } = req.body;
    if (pgPool) {
      await pgPool.query(`UPDATE wo_reconciliation_requests SET status='rejected',approved_by=$1,approved_at=NOW(),rejection_reason=$2 WHERE id=$3`,
        [session.username, reason||'No reason given', req.params.id]);
    } else {
      db.prepare(`UPDATE wo_reconciliation_requests SET status='rejected',approved_by=?,approved_at=datetime('now'),rejection_reason=? WHERE id=?`).run(session.username, reason||'No reason given', req.params.id);
    }
    logAudit(session.username, session.role, 'planning', 'WO_RECON_REJECTED', `Rejected ${req.params.id}: ${reason}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/history
app.get('/api/wo/history', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    let woHistRows;
    if (pgPool) { const r = await pgPool.query('SELECT * FROM wo_reconciliation_requests ORDER BY proposed_at DESC LIMIT 50'); woHistRows=r.rows; }
    else { woHistRows = db.prepare('SELECT * FROM wo_reconciliation_requests ORDER BY proposed_at DESC LIMIT 50').all(); }
    res.json({ ok: true, requests: woHistRows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── Data Export / Import (Admin — for safe migrations) ────────

// POST /api/admin/cleanup-print-orders
// Removes duplicate/orphan print_orders. Safe to re-run.
app.post('/api/admin/cleanup-print-orders', async (req, res) => {
  if (!pgPool) return res.json({ ok: false, error: 'Postgres-only operation' });
  try {
    let deletedOrphans = 0;
    let dedupedCount = 0;

    // (1) Delete print orders whose production order is UP (unprinted)
    const orphans = await pgPool.query(`
      SELECT po.id FROM print_orders po
      JOIN production_orders prod ON prod.id = po.production_order_id
      WHERE COALESCE((prod.data_json::jsonb->>'isPrinted')::boolean, false) = false
        AND COALESCE(prod.deleted, false) = false
    `);
    for (const o of orphans.rows) {
      await pgPool.query('DELETE FROM print_orders WHERE id=$1', [o.id]);
      deletedOrphans++;
    }

    // (2) Deduplicate: same production_order_id + machine_id — keep most recently updated
    const dupGroups = await pgPool.query(`
      SELECT production_order_id, COALESCE(machine_id, '__NULL__') AS machine_key
      FROM print_orders WHERE production_order_id IS NOT NULL
      GROUP BY production_order_id, COALESCE(machine_id, '__NULL__') HAVING COUNT(*) > 1
    `);
    for (const grp of dupGroups.rows) {
      const machineFilter = grp.machine_key === '__NULL__' ? 'machine_id IS NULL' : 'machine_id = $2';
      const params = grp.machine_key === '__NULL__' ? [grp.production_order_id] : [grp.production_order_id, grp.machine_key];
      const dups = await pgPool.query(
        `SELECT id FROM print_orders WHERE production_order_id = $1 AND ${machineFilter} ORDER BY updated_at DESC NULLS LAST`, params
      );
      for (let i = 1; i < dups.rows.length; i++) {
        await pgPool.query('DELETE FROM print_orders WHERE id=$1', [dups.rows[i].id]);
        dedupedCount++;
      }
    }

    // (3) Remove NULL-machine skeleton rows when assigned row exists for same order
    const skeletons = await pgPool.query(`
      SELECT po1.id FROM print_orders po1
      WHERE po1.machine_id IS NULL AND po1.production_order_id IS NOT NULL
        AND EXISTS (SELECT 1 FROM print_orders po2
          WHERE po2.production_order_id = po1.production_order_id
            AND po2.machine_id IS NOT NULL AND po2.id <> po1.id)
    `);
    for (const s of skeletons.rows) {
      await pgPool.query('DELETE FROM print_orders WHERE id=$1', [s.id]);
      dedupedCount++;
    }

    res.json({ ok: true, deletedOrphans, dedupedCount });
  } catch (err) {
    console.error('[Admin cleanup-print-orders] failed:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

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
app.get('/api/temp-batches/check/:machineId', async (req, res) => {
  try {
    const { machineId } = req.params;
    const today = new Date().toISOString().split('T')[0];
    const planState = getPlanningState();
    const activeOrders = (planState.orders || []).filter(o =>
      o.machineId === machineId && o.status !== 'closed' && !o.deleted
    );
    const hasActiveOrder = activeOrders.length > 0;
    let existing = null, allTemp = [];
    if (pgPool) {
      const r1 = await pgPool.query(`SELECT * FROM temp_batches WHERE machine_id=$1 AND date=$2`, [machineId, today]);
      existing = r1.rows[0] || null;
      const r2 = await pgPool.query(`SELECT * FROM temp_batches WHERE machine_id=$1 AND status='active' ORDER BY date DESC`, [machineId]);
      allTemp = r2.rows;
    } else {
      existing = db.prepare(`SELECT * FROM temp_batches WHERE machine_id = ? AND date = ?`).get(machineId, today);
      allTemp = db.prepare(`SELECT * FROM temp_batches WHERE machine_id = ? AND status = 'active' ORDER BY date DESC`).all(machineId);
    }
    const mc = (planState.machineMaster || []).find(m => m.id === machineId);
    const packSizes = planState.packSizes || {};
    const packSizeLakhs = mc ? ((packSizes[mc.size] || 100000) / 100000) : 1;
    const capLakhs = mc ? (mc.cap || 8) : 8;
    const labelCount = mc ? calcTempLabelCount(capLakhs, packSizeLakhs) : 0;
    res.json({
      ok: true, machineId, hasActiveOrder,
      activeOrders: activeOrders.map(o => ({ id:o.id, batchNumber:o.batchNumber, qty:o.qty, status:o.status })),
      todayTempBatch: existing || null,
      needsTemp: !hasActiveOrder,
      machineInfo: mc ? { size: mc.size, capLakhs, packSizeLakhs, labelCount } : null,
      activeTempBatches: allTemp
    });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/temp-batches/create — create TEMP batch for a machine/date
app.post('/api/temp-batches/create', async (req, res) => {
  try {
    const { machineId, date } = req.body;
    const batchDate = date || new Date().toISOString().split('T')[0];
    const id = tempBatchId(machineId, batchDate);
    const planState = getPlanningState();
    const mc = (planState.machineMaster || []).find(m => m.id === machineId);
    if (!mc) return res.status(400).json({ ok:false, error:'Machine not found' });
    const packSizes = planState.packSizes || {};
    const packSizeLakhs = (packSizes[mc.size] || 100000) / 100000;
    const capLakhs = mc.cap || 8;
    const labelCount = calcTempLabelCount(capLakhs, packSizeLakhs);
    let batch;
    if (pgPool) {
      await pgPool.query(`INSERT INTO temp_batches (id,machine_id,machine_size,date,daily_cap_lakhs,label_count,pack_size_lakhs) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO NOTHING`,
        [id, machineId, mc.size, batchDate, capLakhs, labelCount, packSizeLakhs]);
      await pgPool.query(`INSERT INTO temp_batch_alerts (machine_id,temp_batch_id,alert_date) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
        [machineId, id, batchDate]);
      const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [id]);
      batch = r.rows[0];
    } else {
      db.prepare(`INSERT OR IGNORE INTO temp_batches (id,machine_id,machine_size,date,daily_cap_lakhs,label_count,pack_size_lakhs) VALUES (?,?,?,?,?,?,?)`).run(id, machineId, mc.size, batchDate, capLakhs, labelCount, packSizeLakhs);
      db.prepare(`INSERT OR IGNORE INTO temp_batch_alerts (machine_id,temp_batch_id,alert_date) VALUES (?,?,?)`).run(machineId, id, batchDate);
      batch = db.prepare(`SELECT * FROM temp_batches WHERE id = ?`).get(id);
    }
    logAudit('SYSTEM','system','dpr','TEMP_BATCH_CREATED',`TEMP batch created: ${id} — ${capLakhs}L → ${labelCount} labels (Size ${mc.size})`);
    res.json({ ok:true, batch });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/temp-batches/active — all active TEMP batches (for alerts)
// ── Delete TEMP batches for a specific date ─────────────────
app.delete('/api/temp-batches/by-date', async (req, res) => {
  try {
    const { date } = req.query; // date = YYYY-MM-DD
    if (!date) return res.status(400).json({ ok: false, error: 'date required' });
    let deleted = 0;
    if (pgPool) {
      const r = await pgPool.query(
        `DELETE FROM temp_batches WHERE date = $1 AND status != 'reconciled'`, [date]
      );
      deleted = r.rowCount;
    } else {
      const r = db.prepare(`DELETE FROM temp_batches WHERE date = ? AND status != 'reconciled'`).run(date);
      deleted = r.changes;
    }
    res.json({ ok: true, deleted, date });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/api/temp-batches/active', async (req, res) => {
  try {
    let batches;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM temp_batches WHERE status='active' ORDER BY machine_id, date DESC`);
      batches = r.rows;
    } else {
      batches = db.prepare(`SELECT * FROM temp_batches WHERE status='active' ORDER BY machine_id, date DESC`).all();
    }
    const today = new Date().toISOString().split('T')[0];
    const TEMP_CUTOFF = '2026-04-27';
    // Ignore TEMP batches created before April 25 2026
    const filtered = batches.filter(b => (b.created_at||b.date||'') >= TEMP_CUTOFF);
    const enriched = filtered.map(b => ({...b, daysActive: Math.floor((new Date(today)-new Date(b.date))/86400000)+1}));
    res.json({ ok:true, batches: enriched, count: enriched.length });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/reconciliation/propose — Planning Manager proposes reconciliation
app.post('/api/reconciliation/propose', async (req, res) => {
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
      let tb;
      if (pgPool) { const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1',[mapping.tempBatchId]); tb=r.rows[0]; }
      else { tb = db.prepare('SELECT * FROM temp_batches WHERE id=?').get(mapping.tempBatchId); }
      if (!tb) return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} not found` });
      if (tb.status !== 'active') return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} is not active` });
    }

    const totalBoxes = tempBatchMappings.reduce((s,m) => s + (m.boxes || 0), 0);
    const id = `RECON-${Date.now()}`;

    if (pgPool) {
      await pgPool.query(`INSERT INTO reconciliation_requests (id,proposed_by,status,order_id,order_details,back_date,temp_batch_mappings,total_boxes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [id, session.username, 'pending', orderDetails.id||`ORDER-${Date.now()}`, JSON.stringify(orderDetails), backDate, JSON.stringify(tempBatchMappings), totalBoxes]);
    } else {
      db.prepare(`INSERT INTO reconciliation_requests (id,proposed_by,status,order_id,order_details,back_date,temp_batch_mappings,total_boxes) VALUES (?,?,?,?,?,?,?,?)`).run(
        id, session.username, 'pending', orderDetails.id||`ORDER-${Date.now()}`, JSON.stringify(orderDetails), backDate, JSON.stringify(tempBatchMappings), totalBoxes);
    }

    logAudit(session.username, session.role, 'planning', 'RECON_PROPOSED',
      `Reconciliation proposed: ${id} — ${tempBatchMappings.length} TEMP batches → Order, ${totalBoxes} boxes`);

    res.json({ ok:true, requestId: id, status:'pending', message:'Awaiting Admin approval' });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/reconciliation/pending — Admin views pending requests
app.get('/api/reconciliation/pending', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`).all();
    }
    const requests = rows.map(r => ({...r, order_details:JSON.parse(r.order_details||'{}'), temp_batch_mappings:JSON.parse(r.temp_batch_mappings||'[]')}));
    res.json({ ok:true, requests });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/reconciliation/approve/:id — Admin approves and executes reconciliation
app.post('/api/reconciliation/approve/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') {
      return res.status(403).json({ ok:false, error:'Admin only' });
    }

    const request = pgPool ? (await pgPool.query('SELECT * FROM reconciliation_requests WHERE id=$1',[req.params.id])).rows[0] : db.prepare('SELECT * FROM reconciliation_requests WHERE id=?').get(req.params.id);
    if (!request) return res.status(404).json({ ok:false, error:'Request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok:false, error:'Request is not pending' });

    const orderDetails = JSON.parse(request.order_details);
    const mappings = JSON.parse(request.temp_batch_mappings);
    const orderId = request.order_id;

    // Execute reconciliation atomically
    const reconcileAsync = async () => {
      const now = new Date().toISOString();
      const results = { migratedScans:0, migratedLabels:0, migratedWastage:0, tempBatchesReconciled:0 };

      for (const mapping of mappings) {
        const { tempBatchId: tbId, boxes, startLabelNumber, endLabelNumber } = mapping;
        const tb = pgPool ? (await pgPool.query('SELECT * FROM temp_batches WHERE id=$1',[tbId])).rows[0] : db.prepare('SELECT * FROM temp_batches WHERE id=?').get(tbId);
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

        const labelsToMigrate = pgPool ? (await pgPool.query(`SELECT * FROM tracking_labels WHERE ${labelFilter.replace('?','$1').replace('?','$2').replace('?','$3')}`, labelArgs)).rows : db.prepare(`SELECT * FROM tracking_labels WHERE ${labelFilter}`).all(...labelArgs);

        for (const label of labelsToMigrate) {
          const newLabelId = label.id.replace(tbId, orderId);
          if(pgPool){ await pgPool.query(`INSERT INTO tracking_labels SELECT replace(id,$1,$2) as id,$2 as batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data FROM tracking_labels WHERE id=$3 ON CONFLICT(id) DO NOTHING`,[tbId,orderId,label.id]); } else { db.prepare(`INSERT OR REPLACE INTO tracking_labels SELECT replace(id,?,?) as id,? as batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data FROM tracking_labels WHERE id=?`).run(tbId,orderId,orderId,label.id); }

          // Migrate scans for this label
          if(pgPool){ const sm=await pgPool.query(`UPDATE tracking_scans SET label_id=replace(label_id,$1,$2),batch_number=$2 WHERE label_id=$3`,[tbId,orderId,label.id]); results.migratedScans+=sm.rowCount||0; } else { const scanMigrated=db.prepare(`UPDATE tracking_scans SET label_id=replace(label_id,?,?),batch_number=? WHERE label_id=?`).run(tbId,orderId,orderId,label.id); results.migratedScans+=scanMigrated.changes; }
          results.migratedLabels++;

          // Remove old TEMP label if new one created
          if (newLabelId !== label.id) {
            if(pgPool) await pgPool.query('DELETE FROM tracking_labels WHERE id=$1 AND id!=$2',[label.id,newLabelId]); else db.prepare('DELETE FROM tracking_labels WHERE id=? AND id!=?').run(label.id,newLabelId);
          }
        }

        // 2. Migrate wastage records
        if(pgPool){ const wm=await pgPool.query('UPDATE tracking_wastage SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); results.migratedWastage+=wm.rowCount||0; } else { const wastage=db.prepare('UPDATE tracking_wastage SET batch_number=? WHERE batch_number=?').run(orderId,tbId); results.migratedWastage+=wastage.changes; }

        // 3. Migrate stage closures
        if(pgPool) await pgPool.query('UPDATE tracking_stage_closure SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); else db.prepare('UPDATE tracking_stage_closure SET batch_number=? WHERE batch_number=?').run(orderId,tbId);

        // 4. Migrate DPR production actuals
        if(pgPool) await pgPool.query('UPDATE production_actuals SET order_id=$1,batch_number=$2 WHERE batch_number=$3',[orderId,orderDetails.batchNumber||orderId,tbId]); else db.prepare('UPDATE production_actuals SET order_id=?,batch_number=? WHERE batch_number=?').run(orderId,orderDetails.batchNumber||orderId,tbId);

        // 5. Update dispatch records
        if(pgPool) await pgPool.query('UPDATE tracking_dispatch_records SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); else db.prepare('UPDATE tracking_dispatch_records SET batch_number=? WHERE batch_number=?').run(orderId,tbId);

        // 6. Mark TEMP batch as reconciled (or partially reconciled)
        const isFullReconcile = !startLabelNumber; // full batch
        if(pgPool) await pgPool.query('UPDATE temp_batches SET status=$1,reconciled_order_id=$2,reconciled_at=$3,reconciled_by=$4 WHERE id=$5',[isFullReconcile?'reconciled':'partial',orderId,now,session.username,tbId]); else db.prepare('UPDATE temp_batches SET status=?,reconciled_order_id=?,reconciled_at=?,reconciled_by=? WHERE id=?').run(isFullReconcile?'reconciled':'partial',orderId,now,session.username,tbId);
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
        await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json, saved_at=NOW()::TEXT`, [JSON.stringify(planState)]);
        _planningStateCache = planState; _planningStateCacheTime = Date.now();
      }

      // 8. Mark reconciliation request as approved
      await pgPool.query(`UPDATE reconciliation_requests SET status='approved',approved_by=$1,approved_at=$2 WHERE id=$3`,
        [session.username, now, request.id]);

      return results;
    };

    const results = await reconcileAsync();

    logAudit(session.username, session.role, 'planning', 'RECON_APPROVED',
      `Reconciliation ${req.params.id} approved — ${results.migratedLabels} labels, ${results.migratedScans} scans migrated`);

    res.json({ ok:true, results, message:'Reconciliation complete. Replacement labels ready for printing.' });
  } catch(err) {
    res.status(500).json({ ok:false, error:err.message });
  }
});

// POST /api/reconciliation/reject/:id — Admin rejects
app.post('/api/reconciliation/reject/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
    const { reason } = req.body;
    if (pgPool) {
      await pgPool.query(`UPDATE reconciliation_requests SET status='rejected',approved_by=$1,approved_at=NOW(),rejection_reason=$2 WHERE id=$3`,
        [session.username, reason||'No reason given', req.params.id]);
    } else {
      db.prepare(`UPDATE reconciliation_requests SET status='rejected',approved_by=?,approved_at=datetime('now'),rejection_reason=? WHERE id=?`).run(session.username, reason||'No reason given', req.params.id);
    }
    logAudit(session.username, session.role, 'planning', 'RECON_REJECTED', `Rejected: ${req.params.id} — ${reason}`);
    res.json({ ok:true });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/reconciliation/history — all reconciliation requests
app.get('/api/reconciliation/history', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM reconciliation_requests ORDER BY proposed_at DESC LIMIT 100`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM reconciliation_requests ORDER BY proposed_at DESC LIMIT 100`).all();
    }
    const requests = rows.map(r => ({...r, order_details:JSON.parse(r.order_details||'{}'), temp_batch_mappings:JSON.parse(r.temp_batch_mappings||'[]')}));
    res.json({ ok:true, requests });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

app.get('/api/health', (req, res) => {
  try {
    const planningRow  = db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get();
    const dprCount     = db.prepare('SELECT COUNT(*) as c FROM dpr_records').get();
    const actualsCount = db.prepare('SELECT COUNT(*) as c FROM production_actuals').get();
    res.json({
      ok: true,
      server: 'Sunloc Integrated Server v1.0',
      db: DB_PATH,
      planningSavedAt: planningRow?.saved_at || null,
      dprRecords: dprCount?.c || 0,
      actualsEntries: actualsCount?.c || 0,
      uptime: Math.floor(process.uptime()) + 's',
    });
  } catch(err) {
    // Server is alive even if DB query fails (e.g. still warming up)
    res.json({ ok: true, server: 'Sunloc Integrated Server v1.0', db: DB_PATH, uptime: Math.floor(process.uptime())+'s', note: 'DB initialising: '+err.message });
  }
});

// NOTE: catch-all SPA fallback moved to END of file (after all API routes)
// so that /api/tracking/* routes are not intercepted by the wildcard.

// ═══════════════════════════════════════════════════════
// TRACKING APP SCHEMA (add to existing server.js)
// ═══════════════════════════════════════════════════════


// ─── TRACKING ROUTES ──────────────────────────────────────────

// GET /api/tracking/state — full tracking state
app.get('/api/tracking/label', async (req, res) => {
  // Direct label lookup by id or by batchNumber+labelNumber
  try {
    const { id, batchNumber, labelNumber } = req.query;
    let label = null;
    if (id) {
      if (pgPool) {
        label = (await pgPool.query('SELECT * FROM tracking_labels WHERE id=$1',[id])).rows[0] || null;
      } else {
        label = db.prepare('SELECT * FROM tracking_labels WHERE id=?').get(id);
      }
    }
    if (!label && batchNumber && labelNumber != null) {
      if (pgPool) {
        label = (await pgPool.query('SELECT * FROM tracking_labels WHERE batch_number=$1 AND ABS(label_number)=ABS($2)',[batchNumber, parseInt(labelNumber)])).rows[0] || null;
      } else {
        label = db.prepare('SELECT * FROM tracking_labels WHERE batch_number=? AND ABS(label_number)=ABS(?)').get(batchNumber, parseInt(labelNumber));
      }
    }
    res.json({ ok: true, label: label || null });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/api/tracking/state', async (req, res) => {
  try {
    if (pgPool) {
      const [labels, scans, closure, wastage, dispatch, alerts] = await Promise.all([
        pgPool.query('SELECT * FROM tracking_labels ORDER BY generated DESC'),
        pgPool.query('SELECT * FROM tracking_scans ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_stage_closure'),
        pgPool.query('SELECT * FROM tracking_wastage ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_alerts WHERE resolved = 0'),
      ]);
      const mapLabel = r => ({ ...r, batchNumber: r.batch_number, labelNumber: r.label_number, isPartial: r.is_partial, isOrange: r.is_orange, parentLabelId: r.parent_label_id, pcCode: r.pc_code, poNumber: r.po_number, machineId: r.machine_id, printingMatter: r.printing_matter, printedAt: r.printed_at, voidReason: r.void_reason, voidedAt: r.voided_at, voidedBy: r.voided_by, qrData: r.qr_data, woStatus: r.wo_status, shipTo: r.ship_to, billTo: r.bill_to, isExcess: r.is_excess, excessNum: r.excess_num, excessTotal: r.excess_total, normalTotal: r.normal_total });
      const mapScan = r => ({ ...r, labelId: r.label_id, batchNumber: r.batch_number, labelNumber: r.label_number });
      const mapClosure = r => ({ ...r, batchNumber: r.batch_number, closedAt: r.closed_at, closedBy: r.closed_by });
      const mapWastage = r => ({ ...r, batchNumber: r.batch_number });
      const mapDispatch = r => ({ ...r, batchNumber: r.batch_number, vehicleNo: r.vehicle_no, invoiceNo: r.invoice_no });
      const mapAlert = r => ({ ...r, labelId: r.label_id, batchNumber: r.batch_number, scanInTs: r.scan_in_ts, hoursStuck: r.hours_stuck });
      res.json({ ok: true, state: {
        labels: labels.rows.map(mapLabel), scans: scans.rows.map(mapScan),
        stageClosure: closure.rows.map(mapClosure), wastage: wastage.rows.map(mapWastage),
        dispatchRecs: dispatch.rows.map(mapDispatch), alerts: alerts.rows.map(mapAlert)
      }});
    } else {
      const labels  = db.prepare('SELECT * FROM tracking_labels ORDER BY generated DESC').all();
      const scans   = db.prepare('SELECT * FROM tracking_scans ORDER BY ts ASC').all();
      const closure = db.prepare('SELECT * FROM tracking_stage_closure').all();
      const wastage = db.prepare('SELECT * FROM tracking_wastage ORDER BY ts ASC').all();
      const dispatch= db.prepare('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC').all();
      const alerts  = db.prepare('SELECT * FROM tracking_alerts WHERE resolved = 0').all();
      res.json({ ok: true, state: { labels, scans, stageClosure: closure, wastage, dispatchRecs: dispatch, alerts } });
    }
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/tracking/state — save full tracking state
app.post('/api/tracking/state', async (req, res) => {
  try {
    const { labels, scans, stageClosure, wastage, dispatchRecs, alerts } = req.body;
    if (pgPool) {
      const client = await pgPool.connect();
      try {
        await client.query('BEGIN');
        if (labels && labels.length) {
          for (const l of labels) {
            await client.query(`INSERT INTO tracking_labels
              (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,wo_status,ship_to,bill_to,is_excess,excess_num,excess_total,normal_total)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)
              ON CONFLICT (id) DO UPDATE SET batch_number=EXCLUDED.batch_number,label_number=EXCLUDED.label_number,printed=EXCLUDED.printed,printed_at=EXCLUDED.printed_at,voided=EXCLUDED.voided,void_reason=EXCLUDED.void_reason,voided_at=EXCLUDED.voided_at,wo_status=EXCLUDED.wo_status,ship_to=EXCLUDED.ship_to,bill_to=EXCLUDED.bill_to`,
              [l.id,l.batchNumber,l.labelNumber,l.size,l.qty,l.isPartial?1:0,l.isOrange?1:0,l.parentLabelId||null,l.customer||null,l.colour||null,l.pcCode||null,l.poNumber||null,l.machineId||null,l.printingMatter||null,l.generated||new Date().toISOString(),l.printed?1:0,l.printedAt||null,l.voided?1:0,l.voidReason||null,l.voidedAt||null,l.voidedBy||null,l.qrData||null,l.woStatus||null,l.shipTo||null,l.billTo||null,l.isExcess?1:0,l.excessNum||null,l.excessTotal||null,l.normalTotal||null]);
          }
        }
        if (scans && scans.length) {
          for (const s of scans) {
            await client.query(`INSERT INTO tracking_scans (id,label_id,batch_number,dept,type,ts,operator,size,qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO NOTHING`,
              [s.id,s.labelId||s.label_id,s.batchNumber||s.batch_number,s.dept,s.type,s.ts,s.operator||null,s.size||null,s.qty||null]);
          }
        }
        if (stageClosure && stageClosure.length) {
          for (const s of stageClosure) {
            await client.query(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at,closed_by) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO UPDATE SET closed=EXCLUDED.closed,closed_at=EXCLUDED.closed_at`,
              [s.id,s.batchNumber||s.batch_number,s.dept,s.closed?1:0,s.closedAt||s.closed_at,s.closedBy||s.closed_by||null]);
          }
        }
        if (wastage && wastage.length) {
          for (const w of wastage) {
            await client.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING`,
              [w.id,w.batchNumber||w.batch_number,w.dept,w.type,w.qty,w.ts,w.by||null]);
          }
        }
        if (dispatchRecs && dispatchRecs.length) {
          for (const d of dispatchRecs) {
            await client.query(`INSERT INTO tracking_dispatch_records (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (id) DO NOTHING`,
              [d.id,d.batchNumber||d.batch_number,d.customer||null,d.qty,d.boxes,d.vehicleNo||d.vehicle_no||null,d.invoiceNo||d.invoice_no||null,d.remarks||null,d.ts,d.by||null]);
          }
        }
        if (alerts && alerts.length) {
          for (const a of alerts) {
            await client.query(`INSERT INTO tracking_alerts (id,label_id,batch_number,dept,scan_in_ts,hours_stuck,resolved,msg) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO UPDATE SET resolved=EXCLUDED.resolved`,
              [a.id,a.labelId||a.label_id,a.batchNumber||a.batch_number,a.dept,a.scanInTs||a.scan_in_ts,a.hoursStuck||a.hours_stuck||null,a.resolved?1:0,a.msg||null]);
          }
        }
        await client.query('COMMIT');
      } catch(e) { await client.query('ROLLBACK'); throw e; }
      finally { client.release(); }
    } else {
      const saveAll = db.transaction(() => {
        if (labels?.length) { const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_labels (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,wo_status,ship_to,bill_to,is_excess,excess_num,excess_total,normal_total) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`); labels.forEach(l => stmt.run(l.id,l.batchNumber,l.labelNumber,l.size,l.qty,l.isPartial?1:0,l.isOrange?1:0,l.parentLabelId||null,l.customer||null,l.colour||null,l.pcCode||null,l.poNumber||null,l.machineId||null,l.printingMatter||null,l.generated||new Date().toISOString(),l.printed?1:0,l.printedAt||null,l.voided?1:0,l.voidReason||null,l.voidedAt||null,l.voidedBy||null,l.qrData||null,l.woStatus||null,l.shipTo||null,l.billTo||null,l.isExcess?1:0,l.excessNum||null,l.excessTotal||null,l.normalTotal||null)); }
        if (scans?.length) { const stmt = db.prepare(`INSERT OR IGNORE INTO tracking_scans (id,label_id,batch_number,dept,type,ts,operator,size,qty) VALUES (?,?,?,?,?,?,?,?,?)`); scans.forEach(s => stmt.run(s.id,s.labelId||s.label_id,s.batchNumber||s.batch_number,s.dept,s.type,s.ts,s.operator||null,s.size||null,s.qty||null)); }
        if (wastage?.length) { const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES (?,?,?,?,?,?,?)`); wastage.forEach(w => stmt.run(w.id,w.batchNumber||w.batch_number,w.dept,w.type,w.qty,w.ts,w.by||null)); }
      });
      saveAll();
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});
// GET /api/tracking/batch-summary/:batchNumber
app.get('/api/tracking/batch-summary/:batchNumber', async (req, res) => {
  try {
    const { batchNumber } = req.params;
    let labels, scans, wastage, dispatch, alerts;
    if (pgPool) {
      [labels, scans, wastage, dispatch, alerts] = await Promise.all([
        pgPool.query('SELECT * FROM tracking_labels WHERE batch_number=$1', [batchNumber]).then(r=>r.rows),
        pgPool.query('SELECT * FROM tracking_scans WHERE batch_number=$1 ORDER BY ts', [batchNumber]).then(r=>r.rows),
        pgPool.query('SELECT * FROM tracking_wastage WHERE batch_number=$1', [batchNumber]).then(r=>r.rows),
        pgPool.query('SELECT * FROM tracking_dispatch_records WHERE batch_number=$1', [batchNumber]).then(r=>r.rows),
        pgPool.query('SELECT * FROM tracking_alerts WHERE batch_number=$1 AND resolved=0', [batchNumber]).then(r=>r.rows),
      ]);
    } else {
      labels   = db.prepare('SELECT * FROM tracking_labels WHERE batch_number = ?').all(batchNumber);
      scans    = db.prepare('SELECT * FROM tracking_scans WHERE batch_number=? ORDER BY ts').all(batchNumber);
      wastage  = db.prepare('SELECT * FROM tracking_wastage WHERE batch_number = ?').all(batchNumber);
      dispatch = db.prepare('SELECT * FROM tracking_dispatch_records WHERE batch_number = ?').all(batchNumber);
      alerts   = db.prepare('SELECT * FROM tracking_alerts WHERE batch_number = ? AND resolved = 0').all(batchNumber);
    }
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

// GET /api/tracking/alerts — boxes stuck 48h+ grouped by batch+dept
app.get('/api/tracking/alerts', async (req, res) => {
  try {
    const ALERT_HOURS = 48;
    const ALERT_START = '2026-04-27T00:00:00';
    let alerts = [];
    if (pgPool) {
      const r = await pgPool.query(`
        SELECT s.batch_number as "batchNumber", s.dept,
          COUNT(DISTINCT s.label_id) as "stuckBoxes",
          MIN(s.ts) as "scanInTs",
          EXTRACT(EPOCH FROM (NOW() - MIN(s.ts)::timestamptz))/3600 as "hoursStuck",
          MAX(s.size) as "size"
        FROM tracking_scans s
        WHERE s.type = 'in' AND s.ts >= $2
          AND EXTRACT(EPOCH FROM (NOW() - s.ts::timestamptz))/3600 >= $1
          AND NOT EXISTS (
            SELECT 1 FROM tracking_scans o
            WHERE o.label_id = s.label_id AND o.dept = s.dept
              AND o.type = 'out' AND o.ts > s.ts
          )
        GROUP BY s.batch_number, s.dept
        HAVING COUNT(DISTINCT s.label_id) > 0
        ORDER BY MIN(s.ts) ASC LIMIT 100
      `, [ALERT_HOURS, ALERT_START]);
      alerts = r.rows.map(a => ({
        id: a.batchNumber+'_'+a.dept, batchNumber: a.batchNumber,
        stuckBoxes: parseInt(a.stuckBoxes), dept: a.dept,
        scanInTs: a.scanInTs, hoursStuck: parseFloat(a.hoursStuck).toFixed(1),
        size: a.size, resolved: 0
      }));
    }
    res.json({ ok: true, alerts, count: alerts.length });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/tracking/alerts/detail — individual box numbers for a batch+dept
app.get('/api/tracking/alerts/detail', async (req, res) => {
  try {
    const { batchNumber, dept } = req.query;
    if (!batchNumber || !dept) return res.status(400).json({ ok:false, error:'batchNumber and dept required' });
    const ALERT_START = '2026-04-27T00:00:00';
    let boxes = [];
    if (pgPool) {
      const r = await pgPool.query(`
        SELECT s.label_id as "labelId", ABS(l.label_number) as "boxNo",
          s.ts as "scanInTs",
          EXTRACT(EPOCH FROM (NOW() - s.ts::timestamptz))/3600 as "hoursStuck"
        FROM tracking_scans s
        LEFT JOIN tracking_labels l ON l.id = s.label_id
        WHERE s.type='in' AND s.batch_number=$1 AND s.dept=$2 AND s.ts>=$4
          AND EXTRACT(EPOCH FROM (NOW() - s.ts::timestamptz))/3600 >= $3
          AND NOT EXISTS (
            SELECT 1 FROM tracking_scans o
            WHERE o.label_id=s.label_id AND o.dept=s.dept
              AND o.type='out' AND o.ts>s.ts
          )
        ORDER BY l.label_number ASC
      `, [batchNumber, dept, 48, ALERT_START]);
      boxes = r.rows.map(b => ({
        labelId: b.labelId,
        boxNo: b.boxNo != null ? b.boxNo : '?',
        hoursStuck: parseFloat(b.hoursStuck).toFixed(1),
        scanInTs: b.scanInTs
      }));
    }
    res.json({ ok: true, batchNumber, dept, boxes });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/tracking/wip-summary — scan counts + stage closures for Planning

// PUT /api/tracking/dispatch-record/:id — edit vehicle/invoice/date on existing record
app.put('/api/tracking/dispatch-record/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { vehicleNo, invoiceNo, customer, ts } = req.body;
    if (pgPool) {
      await pgPool.query(
        `UPDATE tracking_dispatch_records SET vehicle_no=$1, invoice_no=$2, customer=$3, ts=$4 WHERE id=$5`,
        [vehicleNo||null, invoiceNo||null, customer||null, ts||null, id]
      );
    } else {
      db.prepare(`UPDATE tracking_dispatch_records SET vehicle_no=?, invoice_no=?, customer=?, ts=? WHERE id=?`)
        .run(vehicleNo||null, invoiceNo||null, customer||null, ts||null, id);
    }
    // v37I bugfix: recompute actuals so planning sees the latest vehicle/invoice metadata.
    // qty isn't changed by this endpoint so the SUM stays the same, but the metadata refresh matters.
    let batchNumber = null;
    if (pgPool) {
      const r = await pgPool.query(`SELECT batch_number FROM tracking_dispatch_records WHERE id=$1`, [id]);
      batchNumber = r.rows[0]?.batch_number;
    } else {
      const r = db.prepare(`SELECT batch_number FROM tracking_dispatch_records WHERE id=?`).get(id);
      batchNumber = r?.batch_number;
    }
    if (batchNumber) await _recomputeDispatchActuals(batchNumber, vehicleNo, invoiceNo);
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════
// v37I PART A: DISPATCH RECONCILIATION ALERTS
// ═══════════════════════════════════════════════════════════════════
// Two flow types tracked:
//   Flow A: packing-out scan without matching dispatch-in scan within threshold
//   Flow B: dispatch-out scan without manual dispatch record covering it
//
// Default thresholds: 7 days non-export, 15 days export (configurable in system_settings)
// Acknowledged alerts auto-expire after 4 hours; resurface if still unresolved.
// Background job runs every 60 seconds to detect new triggers idempotently.

// v37I.1: Acknowledged alerts auto-expire after 24 hours (was 4h in v37I — now day-scale
// to match the day-scale aging thresholds).
const DRA_ACK_EXPIRY_MS = 24 * 60 * 60 * 1000; // 24 hours

// v37I.1: Threshold helpers — two day-scale thresholds replacing the single minute-scale one.
// Non-export default 7 days (truck cycle), export default 15 days (production cycle for large
// container orders).
async function _getFgAgingDaysNonExport() {
  try {
    if (pgPool) {
      const r = await pgPool.query(`SELECT value FROM system_settings WHERE key='fgAgingDaysNonExport' LIMIT 1`);
      if (r.rows[0]?.value) { const n = parseInt(r.rows[0].value); if (n >= 3 && n <= 30) return n; }
    } else {
      const r = db.prepare(`SELECT value FROM system_settings WHERE key='fgAgingDaysNonExport' LIMIT 1`).get();
      if (r?.value) { const n = parseInt(r.value); if (n >= 3 && n <= 30) return n; }
    }
  } catch(e) {}
  return 7;
}
async function _getFgAgingDaysExport() {
  try {
    if (pgPool) {
      const r = await pgPool.query(`SELECT value FROM system_settings WHERE key='fgAgingDaysExport' LIMIT 1`);
      if (r.rows[0]?.value) { const n = parseInt(r.rows[0].value); if (n >= 7 && n <= 60) return n; }
    } else {
      const r = db.prepare(`SELECT value FROM system_settings WHERE key='fgAgingDaysExport' LIMIT 1`).get();
      if (r?.value) { const n = parseInt(r.value); if (n >= 7 && n <= 60) return n; }
    }
  } catch(e) {}
  return 15;
}

// v37I.1: Detect which batches have any order routed to an export zone.
// Conservative rule: if ANY production_order for a batch is in an EXPORT zone, the entire
// batch's labels use the 15-day threshold. Returns Set<batch_number_uppercase>.
const EXPORT_ZONE_KEYWORDS = ['EXPORT','BANGLADESH','NEPAL','MUMBAI'];
async function _getExportBatchSet() {
  const exportBatches = new Set();
  try {
    if (pgPool) {
      const r = await pgPool.query(`SELECT DISTINCT UPPER(batch_number) AS bn FROM production_orders WHERE batch_number IS NOT NULL AND data_json IS NOT NULL`);
      for (const row of r.rows) {
        // Look up zone via data_json (orders store zone there)
        try {
          const z = await pgPool.query(`SELECT data_json FROM production_orders WHERE UPPER(batch_number)=$1 LIMIT 50`, [row.bn]);
          for (const o of z.rows) {
            const data = typeof o.data_json === 'string' ? JSON.parse(o.data_json) : o.data_json;
            const zone = (data?.zone||'').toUpperCase();
            if (EXPORT_ZONE_KEYWORDS.some(ez => zone.includes(ez))) {
              exportBatches.add(row.bn); break;
            }
          }
        } catch(e) {}
      }
    } else {
      const rows = db.prepare(`SELECT batch_number, data_json FROM production_orders WHERE batch_number IS NOT NULL AND data_json IS NOT NULL`).all();
      for (const row of rows) {
        try {
          const data = JSON.parse(row.data_json);
          const zone = (data?.zone||'').toUpperCase();
          if (EXPORT_ZONE_KEYWORDS.some(ez => zone.includes(ez))) {
            exportBatches.add((row.batch_number||'').toUpperCase());
          }
        } catch(e) {}
      }
    }
  } catch(e) { console.warn('[dra] export batch detection:', e?.message); }
  return exportBatches;
}

// v37I: Generic system_settings GET/POST — key/value config store
app.get('/api/settings/:key', async (req, res) => {
  try {
    const key = req.params.key;
    let row;
    if (pgPool) {
      const r = await pgPool.query(`SELECT value FROM system_settings WHERE key=$1 LIMIT 1`, [key]);
      row = r.rows[0];
    } else {
      row = db.prepare(`SELECT value FROM system_settings WHERE key=? LIMIT 1`).get(key);
    }
    res.json({ ok: true, value: row?.value || null });
  } catch(err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/settings/:key', async (req, res) => {
  try {
    const key = req.params.key;
    const value = req.body?.value;
    if (typeof value !== 'string' && typeof value !== 'number') {
      return res.status(400).json({ ok: false, error: 'value must be string or number' });
    }
    const valStr = String(value).slice(0, 1000);
    const ts = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(`
        INSERT INTO system_settings (key, value, updated_at, updated_by) VALUES ($1, $2, $3, $4)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by
      `, [key, valStr, ts, req.body?.updated_by || 'admin']);
    } else {
      db.prepare(`
        INSERT INTO system_settings (key, value, updated_at, updated_by) VALUES (?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at, updated_by = excluded.updated_by
      `).run(key, valStr, ts, req.body?.updated_by || 'admin');
    }
    res.json({ ok: true, value: valStr });
  } catch(err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/tracking/reconcile-alerts — active alerts for Report J
// Filters out resolved AND not-yet-expired acknowledged ones.
app.get('/api/tracking/reconcile-alerts', async (req, res) => {
  try {
    const now = new Date().toISOString();
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`
        SELECT * FROM dispatch_reconcile_alerts
        WHERE resolved_at IS NULL
          AND (acknowledged_at IS NULL OR ack_expires_at IS NULL OR ack_expires_at < $1)
        ORDER BY triggered_at ASC`, [now]);
      rows = r.rows;
    } else {
      rows = db.prepare(`
        SELECT * FROM dispatch_reconcile_alerts
        WHERE resolved_at IS NULL
          AND (acknowledged_at IS NULL OR ack_expires_at IS NULL OR ack_expires_at < ?)
        ORDER BY triggered_at ASC`).all(now);
    }
    res.json({ ok: true, alerts: rows });
  } catch(err) {
    console.error('[reconcile-alerts]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/tracking/reconcile-alerts/ack — acknowledge an alert with reason
// Body: { id, reason, ack_by? }
// Sets ack_expires_at = now + 4h. Alert resurfaces after expiry if still unresolved.
app.post('/api/tracking/reconcile-alerts/ack', async (req, res) => {
  try {
    const { id, reason, ack_by } = req.body || {};
    if (!id || !reason || !reason.trim()) {
      return res.status(400).json({ ok: false, error: 'id and reason required' });
    }
    const now = new Date();
    const ackAt = now.toISOString();
    const expiresAt = new Date(now.getTime() + DRA_ACK_EXPIRY_MS).toISOString();
    const ackByVal = (ack_by || 'unknown').toString().slice(0, 100);
    if (pgPool) {
      await pgPool.query(`
        UPDATE dispatch_reconcile_alerts
        SET acknowledged_at=$1, acknowledged_by=$2, ack_reason=$3, ack_expires_at=$4
        WHERE id=$5`, [ackAt, ackByVal, reason.toString().slice(0,500), expiresAt, id]);
    } else {
      db.prepare(`
        UPDATE dispatch_reconcile_alerts
        SET acknowledged_at=?, acknowledged_by=?, ack_reason=?, ack_expires_at=?
        WHERE id=?`).run(ackAt, ackByVal, reason.toString().slice(0,500), expiresAt, id);
    }
    res.json({ ok: true });
  } catch(err) {
    console.error('[reconcile-alerts/ack]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// v37I.1: Background scan rewrite — Flow A is now FG STOCK AGING (pack-in without
// dispatch-in past 7d non-export / 15d export). Flow B unchanged.
// Frequency reduced from 60s to 5 min since thresholds are day-scale.
async function _draScanAndInsert() {
  try {
    const daysNonExport = await _getFgAgingDaysNonExport();
    const daysExport = await _getFgAgingDaysExport();
    const cutoffNonExportIso = new Date(Date.now() - daysNonExport * 24 * 60 * 60 * 1000).toISOString();
    const cutoffExportIso    = new Date(Date.now() - daysExport    * 24 * 60 * 60 * 1000).toISOString();
    const now = new Date().toISOString();
    const uid = () => 'dra_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2,10);
    const exportBatches = await _getExportBatchSet();

    // ── Flow A: pack-IN scans older than per-batch threshold with no dispatch-in for same label ──
    // v40 Phase 18.14: Also EXCLUDE labels whose batch has Phase 18 truck dispatch records.
    // Those labels are physically shipped — they just don't have per-box dispatch-IN scans.
    let candidatesA = [];
    if (pgPool) {
      const r = await pgPool.query(`
        SELECT p.label_id, p.batch_number, p.ts
        FROM tracking_scans p
        WHERE p.dept='packing' AND p.type='in'
          AND p.ts <= $1
          AND NOT EXISTS (
            SELECT 1 FROM tracking_scans di
            WHERE di.label_id = p.label_id AND di.dept='dispatch' AND di.type='in'
          )
          AND NOT EXISTS (
            SELECT 1 FROM tracking_dispatch_records dr
            WHERE dr.batch_number = p.batch_number
          )
      `, [cutoffExportIso]); // Use export cutoff to get widest set; filter per-batch below
      candidatesA = r.rows;
    } else {
      candidatesA = db.prepare(`
        SELECT p.label_id, p.batch_number, p.ts
        FROM tracking_scans p
        WHERE p.dept='packing' AND p.type='in'
          AND p.ts <= ?
          AND NOT EXISTS (
            SELECT 1 FROM tracking_scans di
            WHERE di.label_id = p.label_id AND di.dept='dispatch' AND di.type='in'
          )
          AND NOT EXISTS (
            SELECT 1 FROM tracking_dispatch_records dr
            WHERE dr.batch_number = p.batch_number
          )
      `).all(cutoffExportIso);
    }
    // Filter per-batch using per-batch threshold
    candidatesA = candidatesA.filter(c => {
      if (!c.label_id || !c.ts) return false;
      const isExport = exportBatches.has((c.batch_number||'').toUpperCase());
      const cutoff = isExport ? cutoffExportIso : cutoffNonExportIso;
      return c.ts <= cutoff;
    });

    // Auto-resolve: any Flow A active alert whose label_id now has dispatch-in
    // v40 Phase 18.14: Also auto-resolve when the batch received Phase 18 truck dispatch.
    if (pgPool) {
      await pgPool.query(`
        UPDATE dispatch_reconcile_alerts SET resolved_at=$1
        WHERE alert_type='A' AND resolved_at IS NULL
          AND (
            label_id IN (SELECT label_id FROM tracking_scans WHERE dept='dispatch' AND type='in')
            OR batch_number IN (SELECT batch_number FROM tracking_dispatch_records)
          )
      `, [now]);
    } else {
      db.prepare(`
        UPDATE dispatch_reconcile_alerts SET resolved_at=?
        WHERE alert_type='A' AND resolved_at IS NULL
          AND (
            label_id IN (SELECT label_id FROM tracking_scans WHERE dept='dispatch' AND type='in')
            OR batch_number IN (SELECT batch_number FROM tracking_dispatch_records)
          )
      `).run(now);
    }
    // Insert new candidates (idempotent: only if no active alert exists for same label_id)
    for (const c of candidatesA) {
      let existing;
      if (pgPool) {
        const r = await pgPool.query(
          `SELECT id FROM dispatch_reconcile_alerts WHERE alert_type='A' AND label_id=$1 AND resolved_at IS NULL LIMIT 1`,
          [c.label_id]);
        existing = r.rows[0];
      } else {
        existing = db.prepare(
          `SELECT id FROM dispatch_reconcile_alerts WHERE alert_type='A' AND label_id=? AND resolved_at IS NULL LIMIT 1`)
          .get(c.label_id);
      }
      if (existing) continue;
      const newId = uid();
      if (pgPool) {
        await pgPool.query(
          `INSERT INTO dispatch_reconcile_alerts (id,batch_number,label_id,alert_type,triggered_at) VALUES ($1,$2,$3,'A',$4)`,
          [newId, c.batch_number, c.label_id, c.ts]);
      } else {
        db.prepare(
          `INSERT INTO dispatch_reconcile_alerts (id,batch_number,label_id,alert_type,triggered_at) VALUES (?,?,?,'A',?)`)
          .run(newId, c.batch_number, c.label_id, c.ts);
      }
    }

    // ── Flow B: dispatch-out scans where batch has uncovered box count > 0 past threshold ──
    // Uses non-export threshold (60 min equivalent isn't relevant anymore — using 1 day
    // since manual records should be created within the working day at minimum).
    // Per spec: keep Flow B as-is — 60 minutes still makes sense for paperwork-after-scanout.
    const flowBCutoffIso = new Date(Date.now() - 60 * 60 * 1000).toISOString(); // 60 min (preserved)
    let candidatesB = [];
    if (pgPool) {
      const r = await pgPool.query(`
        SELECT s.batch_number,
               MIN(s.ts) AS earliest_ts,
               COUNT(*) AS scan_out_boxes,
               COALESCE((SELECT SUM(boxes) FROM tracking_dispatch_records dr WHERE dr.batch_number = s.batch_number), 0) AS recorded_boxes
        FROM tracking_scans s
        WHERE s.dept='dispatch' AND s.type='out'
        GROUP BY s.batch_number
        HAVING MIN(s.ts) <= $1
           AND COUNT(*) > COALESCE((SELECT SUM(boxes) FROM tracking_dispatch_records dr WHERE dr.batch_number = s.batch_number), 0)
      `, [flowBCutoffIso]);
      candidatesB = r.rows;
    } else {
      candidatesB = db.prepare(`
        SELECT s.batch_number,
               MIN(s.ts) AS earliest_ts,
               COUNT(*) AS scan_out_boxes,
               COALESCE((SELECT SUM(boxes) FROM tracking_dispatch_records dr WHERE dr.batch_number = s.batch_number), 0) AS recorded_boxes
        FROM tracking_scans s
        WHERE s.dept='dispatch' AND s.type='out'
        GROUP BY s.batch_number
        HAVING MIN(s.ts) <= ?
           AND COUNT(*) > COALESCE((SELECT SUM(boxes) FROM tracking_dispatch_records dr WHERE dr.batch_number = s.batch_number), 0)
      `).all(flowBCutoffIso);
    }
    // Auto-resolve Flow B alerts where recorded >= scan-out
    if (pgPool) {
      await pgPool.query(`
        UPDATE dispatch_reconcile_alerts SET resolved_at=$1
        WHERE alert_type='B' AND resolved_at IS NULL
          AND batch_number IN (
            SELECT s.batch_number FROM tracking_scans s
            WHERE s.dept='dispatch' AND s.type='out'
            GROUP BY s.batch_number
            HAVING COUNT(*) <= COALESCE((SELECT SUM(boxes) FROM tracking_dispatch_records dr WHERE dr.batch_number = s.batch_number), 0)
          )
      `, [now]);
    } else {
      db.prepare(`
        UPDATE dispatch_reconcile_alerts SET resolved_at=?
        WHERE alert_type='B' AND resolved_at IS NULL
          AND batch_number IN (
            SELECT s.batch_number FROM tracking_scans s
            WHERE s.dept='dispatch' AND s.type='out'
            GROUP BY s.batch_number
            HAVING COUNT(*) <= COALESCE((SELECT SUM(boxes) FROM tracking_dispatch_records dr WHERE dr.batch_number = s.batch_number), 0)
          )
      `).run(now);
    }
    // Insert Flow B candidates
    for (const c of candidatesB) {
      if (!c.batch_number) continue;
      let existing;
      if (pgPool) {
        const r = await pgPool.query(
          `SELECT id FROM dispatch_reconcile_alerts WHERE alert_type='B' AND batch_number=$1 AND resolved_at IS NULL LIMIT 1`,
          [c.batch_number]);
        existing = r.rows[0];
      } else {
        existing = db.prepare(
          `SELECT id FROM dispatch_reconcile_alerts WHERE alert_type='B' AND batch_number=? AND resolved_at IS NULL LIMIT 1`)
          .get(c.batch_number);
      }
      if (existing) continue;
      const newId = uid();
      if (pgPool) {
        await pgPool.query(
          `INSERT INTO dispatch_reconcile_alerts (id,batch_number,label_id,alert_type,triggered_at) VALUES ($1,$2,NULL,'B',$3)`,
          [newId, c.batch_number, c.earliest_ts]);
      } else {
        db.prepare(
          `INSERT INTO dispatch_reconcile_alerts (id,batch_number,label_id,alert_type,triggered_at) VALUES (?,?,NULL,'B',?)`)
          .run(newId, c.batch_number, c.earliest_ts);
      }
    }
  } catch(e) {
    console.warn('[dra-scan] failed:', e?.message);
  }
}

// v37I.1: Reduced from 60s to 5 minutes — Flow A thresholds are now day-scale (7d/15d),
// so polling every 60 seconds was overkill. Flow B threshold (60 min) is still well within
// the 5-min scan window.
if (typeof process !== 'undefined' && !process.env.SUNLOC_DISABLE_BG_JOBS) {
  setInterval(_draScanAndInsert, 5 * 60 * 1000);
  setTimeout(_draScanAndInsert, 15 * 1000); // initial run after startup
}

// GET /api/tracking/scan-summary — ALL scan counts aggregated by batch+dept+type (no LIMIT)
// This is the correct data source for all reports — replaces raw scan fetching
// v37I (restoring v37G): each query is individually guarded so one bad table never takes
// down the whole endpoint. Without this, a schema gap on any one table would 500 all reports.
app.get('/api/tracking/scan-summary', async (req, res) => {
  try {
    let scanRows, wastageRows, dispatchRows;
    if (pgPool) {
      const safeQuery = async (sql) => {
        try { return (await pgPool.query(sql)).rows; }
        catch(e) { console.warn('[scan-summary] query failed:', e.message); return []; }
      };
      [scanRows, wastageRows, dispatchRows] = await Promise.all([
        safeQuery('SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans GROUP BY batch_number, dept, type'),
        safeQuery('SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type'),
        safeQuery('SELECT batch_number, SUM(qty) as total_qty FROM tracking_dispatch_records GROUP BY batch_number')
      ]);
    } else {
      scanRows     = db.prepare('SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans GROUP BY batch_number, dept, type').all();
      wastageRows  = db.prepare('SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type').all();
      try { dispatchRows = db.prepare('SELECT batch_number, SUM(qty) as total_qty FROM tracking_dispatch_records GROUP BY batch_number').all(); }
      catch(e) { dispatchRows = []; }
    }

    const summary = {};
    const ensure = (bn, dept) => {
      if (!summary[bn]) summary[bn] = {};
      if (!summary[bn][dept]) summary[bn][dept] = { in:0, out:0, inQty:0, outQty:0 };
    };
    scanRows.forEach(r => {
      const bn = r.batch_number; if (!bn) return;
      ensure(bn, r.dept);
      if (r.type === 'in')  { summary[bn][r.dept].in  += parseInt(r.cnt||0); summary[bn][r.dept].inQty  += parseFloat(r.total_qty||0); }
      if (r.type === 'out') { summary[bn][r.dept].out += parseInt(r.cnt||0); summary[bn][r.dept].outQty += parseFloat(r.total_qty||0); }
    });
    const wastage = {};
    wastageRows.forEach(r => {
      const bn = r.batch_number; if (!bn) return;
      if (!wastage[bn]) wastage[bn] = {};
      if (!wastage[bn][r.dept]) wastage[bn][r.dept] = { salvage:0, remelt:0 };
      if (r.type === 'salvage') wastage[bn][r.dept].salvage += parseFloat(r.total_qty||0);
      if (r.type === 'remelt')  wastage[bn][r.dept].remelt  += parseFloat(r.total_qty||0);
    });
    const dispatched = {};
    dispatchRows.forEach(r => { if (r.batch_number) dispatched[r.batch_number] = parseFloat(r.total_qty||0); });

    res.json({ ok: true, summary, wastage, dispatched });
  } catch(err) {
    console.error('[scan-summary]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/tracking/wip-summary', async (req, res) => {
  try {
    let summary, closures;
    if (pgPool) {
      const r1 = await pgPool.query('SELECT batch_number, dept, type, COUNT(*) as cnt FROM tracking_scans GROUP BY batch_number, dept, type');
      summary = r1.rows;
      try {
        const r2 = await pgPool.query("SELECT batch_number, dept, closed, closed_at FROM tracking_stage_closure WHERE closed = 1 OR closed::text = '1'");
        closures = r2.rows;
      } catch(ce) {
        try {
          const r2 = await pgPool.query('SELECT batch_number, dept, closed, closed_at FROM tracking_stage_closure WHERE closed IS NOT NULL');
          closures = r2.rows.filter(r => r.closed == 1 || r.closed === true);
        } catch(ce2) { closures = []; }
      }
    } else {
      summary = db.prepare('SELECT batch_number, dept, type, COUNT(*) as cnt FROM tracking_scans GROUP BY batch_number, dept, type').all();
      closures = db.prepare("SELECT batch_number, dept, closed, closed_at FROM tracking_stage_closure WHERE closed = 1").all();
    }
    res.json({ ok: true, scanSummary: summary, closures });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});


// ── Labels lookup by batchNumber (scanning fallback) ──
// ── Save new labels to PostgreSQL directly ──────────────────
app.post('/api/tracking/labels', async (req, res) => {
  try {
    const { labels } = req.body;
    if (!labels || !labels.length) return res.status(400).json({ ok: false, error: 'No labels' });
    if (pgPool) {
      for (const l of labels) {
        await pgPool.query(`
          INSERT INTO tracking_labels
            (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,
             customer,colour,pc_code,po_number,machine_id,printing_matter,generated,
             printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,
             is_excess,excess_num,excess_total,normal_total)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
          ON CONFLICT (id) DO UPDATE SET
            batch_number=EXCLUDED.batch_number, label_number=EXCLUDED.label_number,
            qty=EXCLUDED.qty, is_partial=EXCLUDED.is_partial,
            printed=EXCLUDED.printed, printed_at=EXCLUDED.printed_at,
            voided=EXCLUDED.voided, void_reason=EXCLUDED.void_reason,
            voided_at=EXCLUDED.voided_at, voided_by=EXCLUDED.voided_by,
            qr_data=EXCLUDED.qr_data, pc_code=EXCLUDED.pc_code,
            is_excess=EXCLUDED.is_excess, excess_num=EXCLUDED.excess_num,
            excess_total=EXCLUDED.excess_total, normal_total=EXCLUDED.normal_total`,
          [l.id, l.batchNumber||l.batch_number,
           // labelNumber may be "OL-15" (orange) or a number — always store as integer
           (()=>{ const n=l.labelNumber||l.label_number; if(n==null) return null; const s=String(n).replace(/^OL-/i,''); return parseInt(s)||null; })(),
           l.size, l.qty, l.isPartial?1:0, l.isOrange?1:0, l.parentLabelId||null,
           l.customer||null, l.colour||null, l.pcCode||null, l.poNumber||null,
           l.machineId||null, l.printingMatter||l.printMatter||null,
           l.generated||new Date().toISOString(),
           l.printed?1:0, l.printedAt||null, l.voided?1:0, l.voidReason||null,
           l.voidedAt||null, l.voidedBy||null, l.qrData||null,
           l.isExcess?1:0, l.excessNum||null, l.excessTotal||null, l.normalTotal||null]
        );
      }
    } else {
      const stmt = db.prepare(`INSERT OR IGNORE INTO tracking_labels
        (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,
         customer,colour,pc_code,po_number,machine_id,printing_matter,generated,
         printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,
         is_excess,excess_num,excess_total,normal_total)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
      const parseLabelNum = n => { if(n==null) return null; const s=String(n).replace(/^OL-/i,''); return parseInt(s)||null; };
      labels.forEach(l => stmt.run(
        l.id, l.batchNumber||l.batch_number, parseLabelNum(l.labelNumber||l.label_number),
        l.size, l.qty, l.isPartial?1:0, l.isOrange?1:0, l.parentLabelId||null,
        l.customer||null, l.colour||null, l.pcCode||null, l.poNumber||null,
        l.machineId||null, l.printingMatter||l.printMatter||null,
        l.generated||new Date().toISOString(),
        l.printed?1:0, l.printedAt||null, l.voided?1:0, l.voidReason||null,
        l.voidedAt||null, l.voidedBy||null, l.qrData||null,
        l.isExcess?1:0, l.excessNum||null, l.excessTotal||null, l.normalTotal||null
      ));
    }
    res.json({ ok: true, saved: labels.length });
  } catch (err) {
    console.error('[LABEL ERROR]', err.message, '| first label:', JSON.stringify(req.body?.labels?.[0]||{}).substring(0,300));
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/tracking/labels', async (req, res) => {
  try {
    const { batchNumber } = req.query;
    if(!batchNumber) return res.status(400).json({ok:false,error:'batchNumber required'});
    if (pgPool) {
      const r = await pgPool.query(
        'SELECT * FROM tracking_labels WHERE batch_number = $1 AND voided = 0', [batchNumber]
      );
      res.json({ok:true, labels: r.rows});
    } else {
      const labels = db.prepare(
        'SELECT * FROM tracking_labels WHERE batch_number = ? AND voided = 0'
      ).all(batchNumber);
      res.json({ok:true, labels});
    }
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── All labels fast endpoint ──
app.get('/api/tracking/labels-all', async (req, res) => {
  try {
    const m=r=>({id:r.id,batchNumber:r.batch_number,labelNumber:r.label_number,size:r.size,qty:r.qty,isPartial:!!r.is_partial,isOrange:!!r.is_orange,parentLabelId:r.parent_label_id||null,customer:r.customer||'',colour:r.colour||'',pcCode:r.pc_code||'',poNumber:r.po_number||'',machineId:r.machine_id||'',printingMatter:r.printing_matter||'',generated:r.generated,printed:!!r.printed,printedAt:r.printed_at||null,voided:!!r.voided,voidReason:r.void_reason||'',voidedAt:r.voided_at||null,voidedBy:r.voided_by||null,qrData:r.qr_data||'',woStatus:r.wo_status||null,shipTo:r.ship_to||'',billTo:r.bill_to||'',isExcess:!!r.is_excess,excessNum:r.excess_num||null,excessTotal:r.excess_total||null,normalTotal:r.normal_total||null});
    if(pgPool){const r=await pgPool.query('SELECT * FROM tracking_labels ORDER BY generated DESC');res.json({ok:true,labels:r.rows.map(m)});}
    else{const labels=db.prepare('SELECT * FROM tracking_labels ORDER BY generated DESC').all();res.json({ok:true,labels:labels.map(m)});}
  }catch(err){res.status(500).json({ok:false,error:err.message});}
});
// ── All scans endpoint (formerly "scans-recent", LIMIT removed in v40 P18.14) ──
// Returns ALL scans by default. Optional ?since=YYYY-MM-DD to limit to recent.
// Phase 18.14 — data consistency: client needs every scan to compute per-box stage
// correctly in Batch Tracker. Previously LIMIT 2000 dropped older batches' label-scan
// linkage, causing Batch Tracker to show all-empty rows for any batch whose scans
// were older than the last 2000 scans system-wide.
app.get('/api/tracking/scans-recent', async (req, res) => {
  try {
    const since = req.query.since || null;   // optional: 'YYYY-MM-DD'
    const mapScan = r => ({
      id: r.id,
      labelId: r.label_id,
      batchNumber: r.batch_number,
      dept: r.dept,
      type: r.type,
      ts: r.ts,
      operator: r.operator || null,
      size: r.size || null,
      qty: r.qty || null,
      labelNumber: r.label_number || null
    });
    const whereClause = since ? `WHERE ts >= '${since.replace(/'/g,'')}'` : '';
    if (pgPool) {
      // Try with label_number column first (after migration v10)
      let rows;
      try {
        const r = await pgPool.query(
          `SELECT * FROM tracking_scans ${whereClause} ORDER BY ts DESC`
        );
        rows = r.rows;
      } catch(e) {
        // Fallback if column issues — select without label_number
        const r = await pgPool.query(
          `SELECT id,label_id,batch_number,dept,type,ts,operator,size,qty FROM tracking_scans ${whereClause} ORDER BY ts DESC`
        );
        rows = r.rows;
      }
      res.json({ ok: true, scans: rows.map(mapScan), count: rows.length });
    } else {
      let scans;
      try {
        scans = db.prepare(`SELECT * FROM tracking_scans ${whereClause} ORDER BY ts DESC`).all();
      } catch(e) {
        scans = db.prepare(`SELECT id,label_id,batch_number,dept,type,ts,operator,size,qty FROM tracking_scans ${whereClause} ORDER BY ts DESC`).all();
      }
      res.json({ ok: true, scans: scans.map(mapScan), count: scans.length });
    }
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── v40 P18.14: Box-stages endpoint (data-consistency upgrade) ──
// Returns authoritative current stage per label_id, computed server-side from
// the full DB. Eliminates client-side guesswork that previously broke when
// (a) tracking_scans was paginated, or (b) Phase 18 truck dispatch wrote to
// tracking_dispatch_records without per-box scan rows.
//
// Stage values:
//   'production'  — no scans yet, still in production
//   'aim'         — at AIM Inspection (in or partway through)
//   'printing'    — at Printing (printed batches only)
//   'pi'          — at Print Inspection
//   'packing'     — at Packing
//   'at_dispatch' — packed-OUT, sitting at dispatch dock awaiting truck
//   'dispatched'  — physically shipped (either per-box dispatch scan, OR Phase 18 truck dispatch)
//
// Flow rules:
//   - Box has zero scans → 'production'
//   - Box's last scan is type='in' → currently at that dept ('aim', 'printing', 'pi', 'packing')
//   - Box's last scan is type='out' → moved to next dept; packing→'at_dispatch'; dispatch→'dispatched'
//   - If batch has Phase 18 dispatches (tracking_dispatch_records rows), the FIFO-earliest
//     'at_dispatch' boxes are promoted to 'dispatched' up to the dispatched-box count.
app.get('/api/tracking/box-stages', async (req, res) => {
  try {
    // 1. Load all non-voided labels
    let labels;
    if (pgPool) {
      const r = await pgPool.query(`SELECT id, batch_number, label_number, COALESCE(voided, 0) AS voided FROM tracking_labels WHERE COALESCE(voided, 0) = 0`);
      labels = r.rows;
    } else {
      labels = db.prepare(`SELECT id, batch_number, label_number, COALESCE(voided, 0) AS voided FROM tracking_labels WHERE COALESCE(voided, 0) = 0`).all();
    }
    // 2. Load all scans (FULL — no limit) and group by label_id
    let scans;
    if (pgPool) {
      const r = await pgPool.query(`SELECT label_id, batch_number, dept, type, ts FROM tracking_scans ORDER BY ts ASC`);
      scans = r.rows;
    } else {
      scans = db.prepare(`SELECT label_id, batch_number, dept, type, ts FROM tracking_scans ORDER BY ts ASC`).all();
    }
    // 3. Load all dispatch records to capture Phase 18 dispatches
    let dispatchRecs;
    if (pgPool) {
      const r = await pgPool.query(`SELECT batch_number, boxes, ts FROM tracking_dispatch_records ORDER BY ts ASC`);
      dispatchRecs = r.rows;
    } else {
      dispatchRecs = db.prepare(`SELECT batch_number, boxes, ts FROM tracking_dispatch_records ORDER BY ts ASC`).all();
    }
    // 4. Build batch flow map — derive isPrinted from planning state (production_orders).
    // tracking_labels has no is_printed column; the flow info lives in planning's orders.
    const isPrintedByBatch = {};
    try {
      const planState = await getPlanningStateAsync();
      const orders = planState.orders || [];
      for (const ord of orders) {
        if (ord.batchNumber) isPrintedByBatch[ord.batchNumber] = !!ord.isPrinted;
      }
    } catch (e) {
      console.warn('[v40 P18.14 box-stages] planning state load failed:', e.message);
    }
    // Note: 'at_dispatch' and 'dispatched' are pseudo-stages beyond the scan flow.
    // The flow array stops at 'dispatch' (the literal dept used in scans).
    const flowFor = (batchNo) => isPrintedByBatch[batchNo]
      ? ['production', 'aim', 'printing', 'pi', 'packing', 'dispatch']
      : ['production', 'aim', 'packing', 'dispatch'];

    // 5. Build per-label scan history map
    const scansByLabel = {};
    for (const s of scans) {
      if (!s.label_id) continue;
      if (!scansByLabel[s.label_id]) scansByLabel[s.label_id] = [];
      scansByLabel[s.label_id].push(s);
    }
    // 6. Build per-batch dispatched-box count (sum of all dispatch records for that batch)
    const dispatchedByBatch = {};
    for (const dr of dispatchRecs) {
      if (!dr.batch_number) continue;
      dispatchedByBatch[dr.batch_number] = (dispatchedByBatch[dr.batch_number] || 0) + (parseInt(dr.boxes) || 0);
    }
    // 7. Determine each label's current stage based on per-box scans first
    const stages = {};
    const labelsByBatch = {};
    for (const l of labels) {
      if (!labelsByBatch[l.batch_number]) labelsByBatch[l.batch_number] = [];
      labelsByBatch[l.batch_number].push(l);
      const sc = scansByLabel[l.id] || [];
      if (sc.length === 0) {
        stages[l.id] = 'production';
      } else {
        const last = sc[sc.length - 1];
        if (last.type === 'in') {
          stages[l.id] = last.dept;  // 'aim', 'printing', 'pi', 'packing'
        } else {
          // type === 'out'
          if (last.dept === 'packing') {
            stages[l.id] = 'at_dispatch';   // packed-OUT, awaiting truck
          } else if (last.dept === 'dispatch') {
            stages[l.id] = 'dispatched';    // per-box dispatch scan
          } else {
            // aim-out, printing-out, pi-out → next dept in flow
            const fl = flowFor(l.batch_number);
            const idx = fl.indexOf(last.dept);
            stages[l.id] = (idx >= 0 && idx < fl.length - 1) ? fl[idx + 1] : 'complete';
          }
        }
      }
    }
    // 8. Overlay Phase 18 dispatched-box info — promote FIFO-earliest 'at_dispatch' boxes
    // to 'dispatched' status when the batch has Phase 18 dispatch records.
    for (const [batchNo, dispatchedCount] of Object.entries(dispatchedByBatch)) {
      if (!dispatchedCount) continue;
      const batchLabels = labelsByBatch[batchNo] || [];
      // Count how many are already at 'dispatched' (from per-box scans)
      const alreadyDispatched = batchLabels.filter(l => stages[l.id] === 'dispatched').length;
      if (alreadyDispatched >= dispatchedCount) continue;
      // Need to promote (dispatchedCount - alreadyDispatched) more boxes from 'at_dispatch' → 'dispatched'.
      // Order: FIFO by packing-out ts (first packed = first shipped).
      const candidates = batchLabels.filter(l => stages[l.id] === 'at_dispatch').map(l => {
        const sc = scansByLabel[l.id] || [];
        const packOut = sc.find(x => x.dept === 'packing' && x.type === 'out');
        return { label: l, sortTs: packOut?.ts || '9999' };
      });
      candidates.sort((a, b) => (a.sortTs || '').localeCompare(b.sortTs || ''));
      const needed = dispatchedCount - alreadyDispatched;
      for (let i = 0; i < Math.min(needed, candidates.length); i++) {
        stages[candidates[i].label.id] = 'dispatched';
      }
    }
    // 9. Build per-batch box counts (server-side authoritative aggregate)
    const boxCounts = {};
    for (const s of scans) {
      if (!s.batch_number || !s.dept || !s.type) continue;
      if (!boxCounts[s.batch_number]) boxCounts[s.batch_number] = {};
      const key = `${s.dept}_${s.type}`;
      boxCounts[s.batch_number][key] = (boxCounts[s.batch_number][key] || 0) + 1;
    }
    res.json({
      ok: true,
      stages,
      boxCounts,
      dispatchedByBatch,
      labelCount: labels.length,
      scanCount: scans.length,
      dispatchRecCount: dispatchRecs.length,
    });
  } catch (err) {
    console.error('[v40 P18.14 box-stages] error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── Wastage fast endpoint ──
app.get('/api/tracking/wastage', async (req, res) => {
  try {
    const m=r=>({...r,batchNumber:r.batch_number});
    if(pgPool){const r=await pgPool.query('SELECT * FROM tracking_wastage ORDER BY ts DESC');res.json({ok:true,wastage:r.rows.map(m)});}
    else{const wastage=db.prepare('SELECT * FROM tracking_wastage ORDER BY ts DESC').all();res.json({ok:true,wastage:wastage.map(m)});}
  }catch(err){res.status(500).json({ok:false,error:err.message});}
});
// ── Individual scan save (called after each scan in/out) ──
app.post('/api/tracking/scan', async (req, res) => {
  try {
    const { scan, adminOverride } = req.body;
    if(!scan || !scan.id) return res.status(400).json({ok:false,error:'Missing scan'});
    const labelId = scan.labelId||scan.label_id;
    const batchNumber = scan.batchNumber||scan.batch_number;
    // HARD BLOCK: Unprinted batches can never be scanned at Printing or PI
    // Check planning state to get isPrinted for this batch
    let isPrintedBatch = true;  // default assumption (printed flow)
    if (scan.dept === 'printing' || scan.dept === 'pi') {
      const planState = await getPlanningStateAsync();
      const order = (planState.orders||[]).find(o =>
        o.batchNumber === batchNumber || o.id === batchNumber
      );
      if (order && order.isPrinted === false) {
        return res.json({ok:false, blocked:true,
          error:`Batch ${batchNumber} is UNPRINTED — scanning at ${scan.dept} is not allowed. Unprinted batches go AIM → Packing directly.`
        });
      }
      if (order) isPrintedBatch = !!order.isPrinted;
    } else {
      // For other depts, still determine flow type so packing previous-stage logic is correct.
      try {
        const planState = await getPlanningStateAsync();
        const order = (planState.orders||[]).find(o =>
          o.batchNumber === batchNumber || o.id === batchNumber
        );
        if (order) isPrintedBatch = !!order.isPrinted;
      } catch (e) { /* assume printed flow if planning state unavailable */ }
    }

    // v40 Phase 18.14b: PER-LABEL UPSTREAM PROGRESSION CHECK
    // Prevents inconsistent counts (e.g. packing.in > aim.out) at the data layer.
    // For a scan-IN at dept X, the same label MUST have a scan-OUT at the previous scannable stage.
    // Flow: unprinted = production → aim → packing → dispatch
    //       printed   = production → aim → printing → pi → packing → dispatch
    // AIM IN is the entry point — no upstream requirement.
    // Type=OUT is checked downstream (must have matching IN at same dept).
    // Admin override allowed but logged for audit.
    if (scan.type === 'in' && scan.dept !== 'aim' && scan.dept !== 'production') {
      // Determine previous scannable stage
      let prevDept = null;
      if (scan.dept === 'printing') prevDept = 'aim';
      else if (scan.dept === 'pi')   prevDept = 'printing';
      else if (scan.dept === 'packing') prevDept = isPrintedBatch ? 'pi' : 'aim';
      else if (scan.dept === 'dispatch') prevDept = 'packing';
      // Special case: if packing-in on a printed batch and prev is PI, that's fine.
      // Special case: if packing-in on UNPRINTED batch, prev is AIM, but boxes can be sent
      // straight from AIM to packing if 'manual' inspection bypass was done. For data integrity,
      // we still require AIM-out scan.
      if (prevDept) {
        let prevOutScan;
        if (pgPool) {
          const r = await pgPool.query(
            `SELECT id FROM tracking_scans WHERE label_id=$1 AND dept=$2 AND type='out' AND batch_number=$3 LIMIT 1`,
            [labelId, prevDept, batchNumber]
          );
          prevOutScan = r.rows[0];
        } else {
          prevOutScan = db.prepare(
            `SELECT id FROM tracking_scans WHERE label_id=? AND dept=? AND type='out' AND batch_number=? LIMIT 1`
          ).get(labelId, prevDept, batchNumber);
        }
        if (!prevOutScan) {
          if (adminOverride) {
            console.warn(`[v40 P18.14b SCAN OVERRIDE] Admin override: label ${labelId} scanned IN at ${scan.dept} without ${prevDept} OUT scan. batch=${batchNumber} operator=${scan.operator||'?'} ts=${scan.ts}`);
            // Allowed through with audit log
          } else {
            return res.json({
              ok: false,
              blocked: true,
              error: `Box not yet scanned OUT of ${prevDept.toUpperCase()}. A box must complete ${prevDept.toUpperCase()} before it can enter ${scan.dept.toUpperCase()}. (Admin can override if data correction is needed.)`,
              suggestion: `Verify the upstream scan in ${prevDept.toUpperCase()}, or request admin to override if the box was physically moved without proper scanning.`,
              upstream_dept: prevDept
            });
          }
        }
      }
    }

    if (pgPool) {
      // Server-side duplicate check: one IN and one OUT max per label per dept per batch
      // Scoped to batch_number so same label in a new batch is never blocked
      const existing = await pgPool.query(
        `SELECT type FROM tracking_scans WHERE label_id=$1 AND dept=$2 AND batch_number=$3`,
        [labelId, scan.dept, batchNumber]
      );
      const doneTypes = existing.rows.map(r=>r.type);
      if(doneTypes.includes(scan.type)){
        return res.json({ok:false, duplicate:true, error:'Already scanned '+scan.type.toUpperCase()+' at '+scan.dept});
      }
      // v40 P18.14b: For type=OUT, require matching IN exists at same dept
      if (scan.type === 'out' && !doneTypes.includes('in')) {
        return res.json({
          ok: false,
          blocked: true,
          error: `Box not yet scanned IN at ${scan.dept.toUpperCase()}. Can't scan OUT before IN.`
        });
      }
      await pgPool.query(
        `INSERT INTO tracking_scans (id,label_id,batch_number,label_number,dept,type,ts,operator,size,qty)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (id) DO NOTHING`,
        [scan.id, labelId, batchNumber, scan.labelNumber||null, scan.dept, scan.type, scan.ts,
         scan.operator||null, scan.size||null, scan.qty||null]
      );
    } else {
      // SQLite path: same duplicate + IN-before-OUT check
      const existing = db.prepare(`SELECT type FROM tracking_scans WHERE label_id=? AND dept=? AND batch_number=?`)
        .all(labelId, scan.dept, batchNumber);
      const doneTypes = existing.map(r=>r.type);
      if (doneTypes.includes(scan.type)) {
        return res.json({ok:false, duplicate:true, error:'Already scanned '+scan.type.toUpperCase()+' at '+scan.dept});
      }
      if (scan.type === 'out' && !doneTypes.includes('in')) {
        return res.json({
          ok: false, blocked: true,
          error: `Box not yet scanned IN at ${scan.dept.toUpperCase()}. Can't scan OUT before IN.`
        });
      }
      db.prepare(`INSERT OR IGNORE INTO tracking_scans
        (id,label_id,batch_number,label_number,dept,type,ts,operator,size,qty)
        VALUES (?,?,?,?,?,?,?,?,?,?)`).run(
        scan.id, labelId, batchNumber, scan.labelNumber||null, scan.dept, scan.type, scan.ts,
        scan.operator||null, scan.size||null, scan.qty||null
      );
    }
    // v40 P18.14d: Legacy dispatch.out scans count toward total dispatched.
    // Recompute actuals so Planning's "Dispatched %" stays in sync.
    if (scan.dept === 'dispatch' && scan.type === 'out' && batchNumber) {
      try {
        if (typeof _recomputeDispatchActuals === 'function') {
          await _recomputeDispatchActuals(batchNumber, null, null);
        }
      } catch (e) {
        console.warn('[v40 P18.14d] recompute after legacy dispatch scan failed:', e.message);
      }
    }
    res.json({ok:true});
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── A-Grade summary per batch — for Planning live update ──────
app.get('/api/tracking/agrade-summary', async (req, res) => {
  try {
    // Pack sizes for fallback calculation
    const PACK_SIZES = {'0':1.5,'00':1.5,'000':1.5,'1':1.25,'2':1.0,'3':0.75,'4':0.5,'5':0.333};

    // Get batch sizes from planning state for fallback
    const planState = getPlanningState();
    const batchSizeMap = {};
    (planState.orders||[]).forEach(o => { if(o.batchNumber) batchSizeMap[o.batchNumber.toUpperCase()] = String(o.size||'2'); });

    // Scan counts per batch per dept per type
    let scans, wastage, prodActuals;
    if (pgPool) {
      const [r1, r2, r3] = await Promise.all([
        pgPool.query('SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans GROUP BY batch_number, dept, type'),
        pgPool.query('SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type'),
        pgPool.query('SELECT batch_number, SUM(qty_lakhs) as gross_prod FROM production_actuals GROUP BY batch_number'),
      ]);
      scans = r1.rows; wastage = r2.rows; prodActuals = r3.rows;
    } else {
      scans = db.prepare('SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans GROUP BY batch_number, dept, type').all();
      wastage = db.prepare('SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type').all();
      prodActuals = db.prepare('SELECT batch_number, SUM(qty_lakhs) as gross_prod FROM production_actuals GROUP BY batch_number').all();
    }
    const grossProdMap = {};
    prodActuals.forEach(r => { if(r.batch_number) grossProdMap[r.batch_number.toUpperCase()] = parseFloat(r.gross_prod||0); });

    // Build per-batch summary
    const batches = {};
    scans.forEach(s => {
      const bn = (s.batch_number||'').toUpperCase(); // normalize to uppercase
      if (!batches[bn]) batches[bn] = {};
      if (!batches[bn][s.dept]) batches[bn][s.dept] = {in:0,out:0,inQty:0,outQty:0};
      batches[bn][s.dept][s.type] = parseInt(s.cnt||0, 10);
      // Use SUM(qty) if available, else fallback to COUNT * packSize
      const sumQty = parseFloat(s.total_qty||0);
      const ps = PACK_SIZES[batchSizeMap[bn]||batchSizeMap[s.batch_number]||'2'] || 1.0;
      const effectiveQty = sumQty > 0 ? sumQty : parseInt(s.cnt||0,10) * ps;
      batches[bn][s.dept][s.type+'Qty'] = effectiveQty;
    });

    wastage.forEach(w => {
      const bn = (w.batch_number||'').toUpperCase(); // normalize to uppercase
      if (!batches[bn]) batches[bn] = {};
      if (!batches[bn][w.dept]) batches[bn][w.dept] = {in:0,out:0,inQty:0,outQty:0};
      if (!batches[bn][w.dept].wastage) batches[bn][w.dept].wastage = {};
      batches[bn][w.dept].wastage[w.type] = parseFloat(w.total_qty||0);
    });

    // Calculate A-grade per batch per stage
    const result = {};
    Object.entries(batches).forEach(([batchNo, depts]) => {
      const aim = depts['aim'] || {};
      const print = depts['printing'] || {};
      const pi = depts['pi'] || {};
      const pack = depts['packing'] || {};
      const dispatch = depts['dispatch'] || {};

      const aimWaste = (aim.wastage?.salvage||0) + (aim.wastage?.remelt||0);
      const printWaste = (print.wastage?.salvage||0) + (print.wastage?.remelt||0);
      const piWaste = (pi.wastage?.salvage||0) + (pi.wastage?.remelt||0);

      const aimOut = aim.outQty || 0;
      const aimInspected = aimOut + aimWaste;
      const printOut = print.outQty || 0;
      const printInspected = printOut + printWaste;
      const piOut = pi.outQty || 0;
      const piInspected = piOut + piWaste;

      const grossProd = grossProdMap[batchNo.toUpperCase()] || 0;
      const packInQty  = pack.inQty || 0;
      const packOutQty = pack.outQty || 0;
      const dispatchInQty = dispatch.inQty || 0;
      // v37E WIP-fix: material at packing is FG, not WIP (uses packIn)
      const totalWastageForWIP = aimWaste + printWaste + piWaste;
      const wipLakhs = Math.max(0, grossProd - totalWastageForWIP - packInQty);
      // v37I.1: Pack-Out stage removed. FG = boxes pack-in'd but not yet received by dispatch.
      // Old: packing.in - packing.out (boxes inside packing dept, packed but not yet shipped).
      // New: packing.in - dispatch.in (boxes packed and pending dispatch receipt — same concept,
      // since pack-out no longer exists as an intermediate stage).
      const fgAwaitingDispatch = Math.max(0, packInQty - dispatchInQty);

      result[batchNo.toUpperCase()] = { // normalize to uppercase for consistent lookup
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
        // v37I.1: packing.outQty/out preserved for legacy/historical reads but no longer
        // authoritative; consumers should use packing.in/.inQty for FG count.
        packing: { inQty: packInQty, outQty: packOutQty, in: pack.in||0, out: pack.out||0 },
        grossProd,
        wipLakhs,          // Lakhs still in production (not yet at packing)
        fgAwaitingDispatch // Lakhs packed at packing dept, awaiting dispatch receipt
      };
    });

    res.json({ ok: true, batches: result });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Dispatch record from Tracking app ──────────────────────────
// v37I bugfix: Recompute tracking_dispatch_actuals.dispatched_qty for one batch as SUM(qty)
// from tracking_dispatch_records. Called from: dispatch-record POST (new manual record),
// dispatch-record PUT (edit/correction), dispatch-update (legacy entry point), and startup
// backfill. Single source of truth, prevents the v37I-pre-bugfix overwrite drift.
async function _recomputeDispatchActuals(batchNumber, vehicleNo, invoiceNo) {
  if (!batchNumber) return 0;
  let totalQty = 0;
  // v40 P18.14d: dispatched_qty = sum of Phase 18 records + sum of legacy dispatch.out scan qty.
  // Both flows can co-exist on a straddle batch (started under v37, finished under v40 truck flow);
  // each represents distinct physical shipments. Planning consumes this value so it must match
  // Tracking's combined-source helpers.
  if (pgPool) {
    const r1 = await pgPool.query(
      `SELECT COALESCE(SUM(qty),0) AS total FROM tracking_dispatch_records WHERE batch_number=$1`,
      [batchNumber]
    );
    const r2 = await pgPool.query(
      `SELECT COALESCE(SUM(qty),0) AS total FROM tracking_scans WHERE batch_number=$1 AND dept='dispatch' AND type='out'`,
      [batchNumber]
    );
    const phase18Qty = parseFloat(r1.rows[0]?.total || 0);
    const legacyQty = parseFloat(r2.rows[0]?.total || 0);
    totalQty = phase18Qty + legacyQty;
    await pgPool.query(`
      INSERT INTO tracking_dispatch_actuals (batch_number,dispatched_qty,vehicle_no,invoice_no,updated_at)
      VALUES ($1,$2,$3,$4,NOW())
      ON CONFLICT(batch_number) DO UPDATE SET
        dispatched_qty=EXCLUDED.dispatched_qty,
        vehicle_no=COALESCE(EXCLUDED.vehicle_no, tracking_dispatch_actuals.vehicle_no),
        invoice_no=COALESCE(EXCLUDED.invoice_no, tracking_dispatch_actuals.invoice_no),
        updated_at=NOW()
    `, [batchNumber, totalQty, vehicleNo||null, invoiceNo||null]);
  } else {
    const r1 = db.prepare(`SELECT COALESCE(SUM(qty),0) AS total FROM tracking_dispatch_records WHERE batch_number=?`).get(batchNumber);
    const r2 = db.prepare(`SELECT COALESCE(SUM(qty),0) AS total FROM tracking_scans WHERE batch_number=? AND dept='dispatch' AND type='out'`).get(batchNumber);
    const phase18Qty = parseFloat(r1?.total || 0);
    const legacyQty = parseFloat(r2?.total || 0);
    totalQty = phase18Qty + legacyQty;
    db.prepare(`
      INSERT INTO tracking_dispatch_actuals (batch_number,dispatched_qty,vehicle_no,invoice_no,updated_at)
      VALUES (?,?,?,?,datetime('now'))
      ON CONFLICT(batch_number) DO UPDATE SET
        dispatched_qty=excluded.dispatched_qty,
        vehicle_no=COALESCE(excluded.vehicle_no, tracking_dispatch_actuals.vehicle_no),
        invoice_no=COALESCE(excluded.invoice_no, tracking_dispatch_actuals.invoice_no),
        updated_at=excluded.updated_at
    `).run(batchNumber, totalQty, vehicleNo||null, invoiceNo||null);
  }
  return totalQty;
}

app.post('/api/tracking/dispatch-record', async (req, res) => {
  try {
    const { record } = req.body;
    if(!record || !record.id) return res.status(400).json({ok:false,error:'Missing record'});
    const batchNumber = record.batchNumber||record.batch_number;
    const vehicleNo = record.vehicleNo||record.vehicle_no||null;
    const invoiceNo = record.invoiceNo||record.invoice_no||null;
    if (pgPool) {
      await pgPool.query(`INSERT INTO tracking_dispatch_records (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,"by") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id) DO NOTHING`,
        [record.id, batchNumber, record.customer||null, record.qty, record.boxes, vehicleNo, invoiceNo, record.remarks||null, record.ts, record.by||null]);
    } else {
      db.prepare(`INSERT OR IGNORE INTO tracking_dispatch_records (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,by) VALUES (?,?,?,?,?,?,?,?,?,?)`).run(record.id, batchNumber, record.customer||null, record.qty, record.boxes, vehicleNo, invoiceNo, record.remarks||null, record.ts, record.by||null);
    }
    // v37I bugfix: auto-recompute dispatched_qty so planning sees the new total even if client
    // forgets / fails to also call dispatch-update. Single source of truth principle.
    const totalQty = await _recomputeDispatchActuals(batchNumber, vehicleNo, invoiceNo);
    res.json({ok:true, totalQty});
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── Dispatch actual update — syncs Tracking qty back to Planning ──
// v37I bugfix: dispatched_qty is now recomputed as SUM(qty) FROM tracking_dispatch_records
// for the batch, NOT overwritten with the incoming per-record qty. Previously each call
// overwrote the previous total — a batch with two 5L records would show only the most
// recent one. New behaviour: dispatched_qty = sum of ALL manual records for that batch.
// vehicleNo/invoiceNo are still the latest values (last record wins) since they describe
// the most recent dispatch event.
app.post('/api/tracking/dispatch-update', async (req, res) => {
  try {
    const { batchNumber, vehicleNo, invoiceNo } = req.body;
    if(!batchNumber) return res.status(400).json({ok:false,error:'Missing batchNumber'});
    const totalQty = await _recomputeDispatchActuals(batchNumber, vehicleNo, invoiceNo);
    res.json({ok:true, totalQty});
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── Get dispatch actuals for Planning app ──
app.get('/api/tracking/dispatch-actuals', async (req, res) => {
  // pgPool used below
  try {
    const rows = pgPool ? (await pgPool.query('SELECT * FROM tracking_dispatch_actuals')).rows : db.prepare('SELECT * FROM tracking_dispatch_actuals').all();
    res.json({ok:true, actuals: rows});
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── Admin Backfill — manual entry of historical scan data ──
app.post('/api/tracking/backfill', async (req, res) => {
  try {
    const { scans } = req.body;
    if (!scans || !Array.isArray(scans)) return res.status(400).json({ ok: false, error: 'scans array required' });
    let count = 0;
    for (const scan of scans) {
      if (!scan.id) continue;
      if (pgPool) {
        await pgPool.query(
          `INSERT INTO tracking_scans (id,label_id,batch_number,label_number,dept,type,ts,operator,size,qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id) DO NOTHING`,
          [scan.id, scan.labelId||scan.label_id||null, scan.batchNumber||scan.batch_number||null, scan.labelNumber||null, scan.dept, scan.type, scan.ts, scan.operator||null, scan.size||null, scan.qty||null]
        );
      } else {
        db.prepare(`INSERT OR IGNORE INTO tracking_scans (id,label_id,batch_number,label_number,dept,type,ts,operator,size,qty) VALUES (?,?,?,?,?,?,?,?,?,?)`).run(scan.id, scan.labelId||scan.label_id||null, scan.batchNumber||scan.batch_number||null, scan.labelNumber||null, scan.dept, scan.type, scan.ts, scan.operator||null, scan.size||null, scan.qty||null);
      }
      count++;
    }
    res.json({ ok: true, imported: count });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
app.post('/api/tracking/backfill-wastage', async (req, res) => {
  try {
    const { wastage } = req.body;
    if (!wastage || !Array.isArray(wastage)) return res.status(400).json({ ok: false, error: 'wastage array required' });
    let count = 0;
    for (const w of wastage) {
      if (!w.id) continue;
      if (pgPool) {
        await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`,
          [w.id, w.batchNumber||w.batch_number||null, w.dept, w.type||null, w.qty||null, w.ts||new Date().toISOString()]);
      } else {
        db.prepare(`INSERT OR IGNORE INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES (?,?,?,?,?,?)`).run(w.id, w.batchNumber||w.batch_number||null, w.dept, w.type||null, w.qty||null, w.ts||new Date().toISOString());
      }
      count++;
    }
    res.json({ ok: true, imported: count });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
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

// ── Label void — mark label voided in DB ──────────────────────
app.post('/api/tracking/label-void', async (req, res) => {
  try {
    const { labelId, reason, voidedBy } = req.body;
    if (!labelId) return res.status(400).json({ ok: false, error: 'labelId required' });
    const ts = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(`UPDATE tracking_labels SET voided=1, void_reason=$1, voided_at=$2, voided_by=$3 WHERE id=$4`, [reason||'', ts, voidedBy||'', labelId]);
    } else {
      db.prepare(`UPDATE tracking_labels SET voided=1, void_reason=?, voided_at=?, voided_by=? WHERE id=?`).run(reason||'', ts, voidedBy||'', labelId);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST update label qty (edit)
// ── /api/tracking/scans — deduplicated scan stream ─
// v40 P18.14: LIMITs removed for data consistency. Dedupe keeps only the
// earliest scan per (label_id, dept, type, minute) bucket.
app.get('/api/tracking/scans', async (req, res) => {
  try {
    const dedupeSQL = `
      SELECT DISTINCT ON (label_id, dept, type, date_trunc('minute', ts::timestamp))
        id, label_id, batch_number, dept, type, ts, operator, size, qty
      FROM tracking_scans
      ORDER BY label_id, dept, type, date_trunc('minute', ts::timestamp), ts ASC
    `;
    if (pgPool) {
      const r = await pgPool.query(dedupeSQL);
      res.json({ ok: true, scans: r.rows });
    } else {
      const scans = db.prepare('SELECT * FROM tracking_scans ORDER BY ts DESC').all();
      res.json({ ok: true, scans });
    }
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/api/tracking/label-update', async (req, res) => {
  try {
    const { labelId, qty, printed, printedAt } = req.body;
    if (!labelId) return res.status(400).json({ ok: false, error: 'labelId required' });
    if (printed !== undefined && qty === undefined) {
      // Update printed status only
      const pVal = printed ? 1 : 0;
      const pAt  = printedAt || new Date().toISOString();
      if (pgPool) {
        await pgPool.query('UPDATE tracking_labels SET printed = $1, printed_at = $2 WHERE id = $3', [pVal, pAt, labelId]);
      } else {
        db.prepare('UPDATE tracking_labels SET printed = ?, printed_at = ? WHERE id = ?').run(pVal, pAt, labelId);
      }
    } else if (qty) {
      // Update qty and mark for reprint
      if (pgPool) {
        await pgPool.query('UPDATE tracking_labels SET qty = $1, printed = 0 WHERE id = $2', [qty, labelId]);
      } else {
        db.prepare('UPDATE tracking_labels SET qty = ?, printed = 0 WHERE id = ?').run(qty, labelId);
      }
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Clean duplicate scans from DB ─────────────────────────────
app.post('/api/tracking/cleanup-scans', async (req, res) => {
  try {
    if (!pgPool) return res.json({ ok: false, error: 'PostgreSQL only' });
    // Delete duplicate scans — keep only the earliest per (label_id, dept, type, minute)
    const result = await pgPool.query(`
      DELETE FROM tracking_scans
      WHERE id NOT IN (
        SELECT DISTINCT ON (label_id, dept, type, date_trunc('minute', ts::timestamp))
          id
        FROM tracking_scans
        ORDER BY label_id, dept, type, date_trunc('minute', ts::timestamp), ts ASC
      )
    `);
    res.json({ ok: true, deleted: result.rowCount });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Clean duplicate scans — keep only first scan per label+dept+type ─
app.post('/api/admin/clean-duplicate-scans', async (req, res) => {
  try {
    if (!pgPool) return res.status(400).json({ ok: false, error: 'PostgreSQL only' });
    // Delete duplicates: keep the earliest scan per label_id+dept+type combination
    const result = await pgPool.query(`
      DELETE FROM tracking_scans
      WHERE id NOT IN (
        SELECT DISTINCT ON (label_id, dept, type) id
        FROM tracking_scans
        ORDER BY label_id, dept, type, ts ASC
      )
    `);
    res.json({ ok: true, deleted: result.rowCount });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Stage status — record which departments are closed per batch ─
app.post('/api/tracking/stage-status', async (req, res) => {
  try {
    const { batchNumber, statusMap } = req.body;
    if (!batchNumber || !statusMap) return res.status(400).json({ ok: false, error: 'batchNumber and statusMap required' });
    const ts = new Date().toISOString();
    for (const [dept, status] of Object.entries(statusMap)) {
      const closed = status === 'closed' ? 1 : 0;
      if (pgPool) {
        await pgPool.query(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(batch_number,dept) DO UPDATE SET closed=EXCLUDED.closed, closed_at=EXCLUDED.closed_at`, [`${batchNumber}-${dept}`, batchNumber, dept, closed, ts]);
      } else {
        db.prepare(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at) VALUES (?,?,?,?,?) ON CONFLICT(batch_number,dept) DO UPDATE SET closed=excluded.closed, closed_at=excluded.closed_at`).run(`${batchNumber}-${dept}`, batchNumber, dept, closed, ts);
      }
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Stage Close — mark a dept stage as closed for a batch ──────
app.post('/api/tracking/stage-close', async (req, res) => {
  try {
    const { batchNumber, dept, closedBy } = req.body;
    if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'Missing batchNumber or dept' });
    const id = `${batchNumber}-${dept}`;
    const ts = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO tracking_stage_closure (id, batch_number, dept, closed, closed_at, closed_by)
         VALUES ($1,$2,$3,1,$4,$5)
         ON CONFLICT(batch_number, dept) DO UPDATE SET closed=1, closed_at=EXCLUDED.closed_at, closed_by=EXCLUDED.closed_by`,
        [id, batchNumber, dept, ts, closedBy||null]
      );
    } else {
      db.prepare(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at,closed_by)
        VALUES (?,?,?,1,?,?) ON CONFLICT(batch_number,dept) DO UPDATE SET closed=1,closed_at=excluded.closed_at,closed_by=excluded.closed_by`)
        .run(id, batchNumber, dept, ts, closedBy||null);
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Wastage — save salvage/remelt records ─────────────────────
app.post('/api/tracking/wastage', async (req, res) => {
  try {
    const { batchNumber, dept, salvage, remelt } = req.body;
    if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'batchNumber and dept required' });
    const ts = new Date().toISOString();
    const genId = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
    if (pgPool) {
      if (parseFloat(salvage) > 0) await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`, [genId(),batchNumber,dept,'salvage',parseFloat(salvage),ts]);
      if (parseFloat(remelt)  > 0) await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`, [genId(),batchNumber,dept,'remelt',parseFloat(remelt),ts]);
    } else {
      const insert = db.prepare(`INSERT OR IGNORE INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES (?,?,?,?,?,?)`);
      if (parseFloat(salvage) > 0) insert.run(genId(),batchNumber,dept,'salvage',parseFloat(salvage),ts);
      if (parseFloat(remelt)  > 0) insert.run(genId(),batchNumber,dept,'remelt',parseFloat(remelt),ts);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Wastage edit — admin/planning correction ──────────────────
app.post('/api/tracking/wastage-edit', async (req, res) => {
  try {
    const { id, qty, editedBy } = req.body;
    if (!id || qty === undefined) return res.status(400).json({ ok: false, error: 'id and qty required' });
    if (pgPool) {
      const r = await pgPool.query(`UPDATE tracking_wastage SET qty=$1, "by"=COALESCE("by",'') || ' [edited by ' || $2 || ']' WHERE id=$3`, [parseFloat(qty), editedBy||'admin', id]);
      if (r.rowCount === 0) return res.status(404).json({ ok: false, error: 'Wastage entry not found' });
    } else {
      const result = db.prepare(`UPDATE tracking_wastage SET qty=?, by=COALESCE(by,'')||' [edited by '||?||']' WHERE id=?`).run(parseFloat(qty), editedBy||'admin', id);
      if (result.changes === 0) return res.status(404).json({ ok: false, error: 'Wastage entry not found' });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Wastage delete — admin/planning correction ─────────────────
app.post('/api/tracking/wastage-delete', async (req, res) => {
  try {
    const { id, deletedBy } = req.body;
    if (!id) return res.status(400).json({ ok: false, error: 'id required' });
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM tracking_wastage WHERE id=$1', [id]);
      if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'Not found' });
      const entry = r.rows[0];
      await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,'admin','tracking','WASTAGE_DELETE',$2)`, [deletedBy||'admin', JSON.stringify({id,batch_number:entry.batch_number,dept:entry.dept,type:entry.type,qty:entry.qty})]);
      await pgPool.query('DELETE FROM tracking_wastage WHERE id=$1', [id]);
    } else {
      const entry = db.prepare('SELECT * FROM tracking_wastage WHERE id=?').get(id);
      if (!entry) return res.status(404).json({ ok: false, error: 'Not found' });
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,'admin','tracking','WASTAGE_DELETE',?)`).run(deletedBy||'admin', JSON.stringify({id,batch_number:entry.batch_number,dept:entry.dept,type:entry.type,qty:entry.qty}));
      db.prepare('DELETE FROM tracking_wastage WHERE id=?').run(id);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Reprint log — audit trail for damaged label replacements ──
app.post('/api/tracking/reprint-log', async (req, res) => {
  try {
    const { log } = req.body;
    if (!log) return res.status(400).json({ ok: false, error: 'log required' });
    if (pgPool) {
      await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,$2,$3,$4,$5)`,
        [log.requestedBy||'tracking', 'tracking', 'tracking', 'LABEL_REPRINT', JSON.stringify(log)]);
    } else {
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,?,?,?,?)`).run(log.requestedBy||'tracking', 'tracking', 'tracking', 'LABEL_REPRINT', JSON.stringify(log));
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── DPR Settings — GET all settings ──────────────────────────
app.get('/api/dpr/settings', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT key, value_json FROM dpr_settings');
      rows = r.rows;
    } else {
      rows = db.prepare('SELECT key, value_json FROM dpr_settings').all();
    }
    const settings = {};
    rows.forEach(r => {
      try { settings[r.key] = JSON.parse(r.value_json); } catch { settings[r.key] = r.value_json; }
    });
    res.json({ ok: true, settings });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── DPR Settings — POST save/update one or more settings ─────
app.post('/api/dpr/settings', async (req, res) => {
  try {
    const { settings } = req.body;
    if (!settings || typeof settings !== 'object') return res.status(400).json({ ok: false, error: 'settings object required' });
    if (pgPool) {
      for (const [key, value] of Object.entries(settings)) {
        await pgPool.query(
          `INSERT INTO dpr_settings (key, value_json, updated_at)
           VALUES ($1, $2, NOW())
           ON CONFLICT(key) DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = NOW()`,
          [key, JSON.stringify(value)]
        );
      }
    } else {
      const upsert = db.prepare(`
        INSERT INTO dpr_settings (key, value_json, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
      `);
      for (const [key, value] of Object.entries(settings)) {
        upsert.run(key, JSON.stringify(value));
      }
    }
    res.json({ ok: true, saved: Object.keys(settings).length });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Month Archives — dedicated PostgreSQL table ────────────────
// POST /api/archives/save — idempotent: INSERT or UPDATE snapshot for a month
app.post('/api/archives/save', async (req, res) => {
  try {
    const { month, snapshot, archivedBy, isAuto } = req.body;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) return res.status(400).json({ ok: false, error: 'Invalid month format. Expected YYYY-MM' });
    await pgPool.query(
      `INSERT INTO month_archives (month, archived_at, archived_by, snapshot_json, is_auto)
       VALUES ($1, NOW(), $2, $3, $4)
       ON CONFLICT (month) DO UPDATE
         SET archived_at  = EXCLUDED.archived_at,
             archived_by  = EXCLUDED.archived_by,
             snapshot_json = EXCLUDED.snapshot_json,
             is_auto       = EXCLUDED.is_auto`,
      [month, archivedBy || 'system', JSON.stringify(snapshot || {}), isAuto !== false]
    );
    res.json({ ok: true, month });
  } catch (err) {
    console.error('[Archives] save error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/archives/list — list all archived months (no snapshot — metadata only)
app.get('/api/archives/list', async (req, res) => {
  try {
    const result = await pgPool.query(
      `SELECT month, archived_at, archived_by, is_auto FROM month_archives ORDER BY month DESC`
    );
    res.json({ ok: true, archives: result.rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/archives/:month — fetch full snapshot for a specific month
app.get('/api/archives/:month', async (req, res) => {
  try {
    const { month } = req.params;
    const result = await pgPool.query(
      `SELECT month, archived_at, archived_by, is_auto, snapshot_json FROM month_archives WHERE month = $1`,
      [month]
    );
    if (result.rows.length === 0) return res.json({ ok: false, error: 'Not found' });
    const row = result.rows[0];
    res.json({ ok: true, month: row.month, archivedAt: row.archived_at, archivedBy: row.archived_by, isAuto: row.is_auto, snapshot: row.snapshot_json });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/archives/check/:month — check archive status + edit window
app.get('/api/archives/check/:month', async (req, res) => {
  try {
    const { month } = req.params;
    const now = new Date();
    const currentYM = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    if (month === currentYM) return res.json({ ok: true, status: 'current', editable: true });
    if (month > currentYM) return res.json({ ok: true, status: 'future', editable: true });
    const result = await pgPool.query(
      `SELECT archived_at FROM month_archives WHERE month = $1`, [month]
    );
    if (result.rows.length === 0) return res.json({ ok: true, status: 'unarchived', editable: true });
    const archivedAt = new Date(result.rows[0].archived_at);
    const daysSince = (now - archivedAt) / 86400000;
    const inGrace = daysSince <= 7;
    res.json({ ok: true, status: inGrace ? 'grace' : 'locked', editable: inGrace, daysSince: Math.floor(daysSince), archivedAt: result.rows[0].archived_at });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── Catch-all: serve index.html for unknown routes (SPA fallback) ──
// MUST be last — after all /api/* routes so they are not intercepted
app.get('*', (req, res) => {
  const idx = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(idx)) res.sendFile(idx);
  else res.json({ ok: false, error: 'No frontend found. Place Planning App and DPR App in /public folder.' });
});


// ══════════════════════════════════════════════════════════════════════
// PLANNING KEY-VALUE STORE — permanent storage for all planning data
// Covers: dispatch plans, pack sizes, print machine master, settings,
//         zone map, daily printing logs — everything that was only in
//         planning_state JSON blob before
// ══════════════════════════════════════════════════════════════════════

// Helper: save a key to planning_kv table
async function kvSet(key, value) {
  const json = JSON.stringify(value);
  if (pgPool) {
    await pgPool.query(
      `INSERT INTO planning_kv (key, value_json, updated_at)
       VALUES ($1, $2, NOW()::TEXT)
       ON CONFLICT(key) DO UPDATE SET value_json=$2, updated_at=NOW()::TEXT`,
      [key, json]
    );
  } else {
    db.prepare(
      `INSERT OR REPLACE INTO planning_kv (key, value_json, updated_at)
       VALUES (?, ?, datetime('now'))`
    ).run(key, json);
  }
}

// Helper: get a key from planning_kv table
async function kvGet(key) {
  try {
    if (pgPool) {
      const r = await pgPool.query(`SELECT value_json FROM planning_kv WHERE key=$1`, [key]);
      if (r.rows[0]) return JSON.parse(r.rows[0].value_json);
    } else {
      const row = db.prepare(`SELECT value_json FROM planning_kv WHERE key=?`).get(key);
      if (row) return JSON.parse(row.value_json);
    }
  } catch(e) {}
  return null;
}

// GET /api/planning/kv/:key — get a planning data value
app.get('/api/planning/kv/:key', async (req, res) => {
  try {
    const value = await kvGet(req.params.key);
    res.json({ ok: true, key: req.params.key, value });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/planning/kv/:key — save a planning data value permanently
app.post('/api/planning/kv/:key', async (req, res) => {
  try {
    const { value } = req.body;
    if (value === undefined) return res.status(400).json({ ok: false, error: 'value required' });
    await kvSet(req.params.key, value);
    res.json({ ok: true, key: req.params.key, savedAt: new Date().toISOString() });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/planning/kv-bulk — save multiple keys at once
app.post('/api/planning/kv-bulk', async (req, res) => {
  try {
    const { data } = req.body; // { key1: value1, key2: value2, ... }
    if (!data || typeof data !== 'object') return res.status(400).json({ ok: false, error: 'data object required' });
    for (const [key, value] of Object.entries(data)) {
      await kvSet(key, value);
    }
    res.json({ ok: true, count: Object.keys(data).length, savedAt: new Date().toISOString() });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/planning/all-kv — get ALL planning kv data in one request (for page load)
app.get('/api/planning/all-kv', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT key, value_json FROM planning_kv`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT key, value_json FROM planning_kv`).all();
    }
    const result = {};
    rows.forEach(row => {
      try { result[row.key] = JSON.parse(row.value_json); } catch(e) {}
    });
    res.json({ ok: true, data: result });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Start server ──────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[Sunloc] Server running on port ${PORT}`);
  console.log(`[Sunloc] DB: ${DB_PATH}`);
  // Ensure PostgreSQL tables exist (handles cases where PgDatabase migrations didn't create them)
  ensurePostgresTables().then(()=>{
    warmPlanningCache();
    warmActualsCache();
    // v37I bugfix: one-time backfill — recompute dispatched_qty for ALL batches that have
    // manual records. Fixes data from before the SUM-based recompute was introduced where
    // multiple records overwrote each other and only the last per-record qty was saved.
    // Runs once per server boot, idempotent (running again is a no-op since values would match).
    _backfillDispatchActuals().catch(e => console.warn('[v37I backfill] failed:', e?.message));
    // v39: Start SAP background pollers (after DB is fully ready).
    _startSapPollers();
  });
});

// ─── v39 Phase 5: Background SAP pollers ───────────────────────
// Two setInterval jobs: one pulls open Sales Orders (indents), one pulls
// recent invoices. Both call the same helpers used by the manual refresh
// endpoints, so logic is single-sourced.
//
// Each poller is:
//   - guarded by an _inflight boolean so overlapping ticks are skipped
//   - wrapped in try/catch — never crashes the process
//   - backs off (skips next 2 ticks) after a failure
//   - reads its interval from sap_config (default 5 min for both)
//   - logs all poll outcomes to console with timestamps
//
// Pollers safely no-op if SAP is not configured (the underlying call
// returns { degraded: true } and we log + back off).
let _indentPollRunning = false;
let _indentPollBackoff = 0;     // ticks to skip after a failure
let _invoicePollRunning = false;
let _invoicePollBackoff = 0;
let _indentPollTimer = null;
let _invoicePollTimer = null;

async function _startSapPollers() {
  // Read intervals from config (or defaults)
  let indentMin = 5, invoiceMin = 5;
  try {
    const cfg = await sap.getConfig({ forceRefresh: true });
    if (cfg) {
      indentMin = cfg.indent_poll_interval_minutes || 5;
      invoiceMin = cfg.invoice_poll_interval_minutes || 5;
    }
  } catch {}
  // Indent poller
  const indentMs = Math.max(1, indentMin) * 60_000;
  _indentPollTimer = setInterval(async () => {
    if (_indentPollRunning) {
      console.log(`[SAP-Indent] Tick skipped — previous still running`);
      return;
    }
    if (_indentPollBackoff > 0) {
      _indentPollBackoff--;
      console.log(`[SAP-Indent] Tick skipped — backing off (${_indentPollBackoff} more skips)`);
      return;
    }
    _indentPollRunning = true;
    try {
      const r = await _doRefreshSapIndents();
      if (r.ok) {
        console.log(`[SAP-Indent] ${new Date().toISOString()} fetched=${r.fetched} upserted=${r.upserted}`);
      } else {
        console.warn(`[SAP-Indent] ${new Date().toISOString()} FAIL: ${r.error}${r.degraded ? ' (degraded)' : ''}`);
        _indentPollBackoff = 2; // skip next 2 ticks
      }
    } catch (e) {
      console.warn(`[SAP-Indent] Unexpected error: ${e.message}`);
      _indentPollBackoff = 2;
    } finally {
      _indentPollRunning = false;
    }
  }, indentMs);

  // Invoice poller
  const invoiceMs = Math.max(1, invoiceMin) * 60_000;
  _invoicePollTimer = setInterval(async () => {
    if (_invoicePollRunning) {
      console.log(`[SAP-Invoice] Tick skipped — previous still running`);
      return;
    }
    if (_invoicePollBackoff > 0) {
      _invoicePollBackoff--;
      console.log(`[SAP-Invoice] Tick skipped — backing off (${_invoicePollBackoff} more skips)`);
      return;
    }
    _invoicePollRunning = true;
    try {
      const r = await _doRefreshSapInvoices();
      if (r.ok) {
        console.log(`[SAP-Invoice] ${new Date().toISOString()} fetched=${r.fetched} upserted=${r.upserted}`);
      } else {
        console.warn(`[SAP-Invoice] ${new Date().toISOString()} FAIL: ${r.error}${r.degraded ? ' (degraded)' : ''}`);
        _invoicePollBackoff = 2;
      }
    } catch (e) {
      console.warn(`[SAP-Invoice] Unexpected error: ${e.message}`);
      _invoicePollBackoff = 2;
    } finally {
      _invoicePollRunning = false;
    }
  }, invoiceMs);

  console.log(`[SAP] Pollers started — indent every ${indentMin}min, invoice every ${invoiceMin}min`);
}

// v37I bugfix: backfill dispatch_actuals from records on startup
// v40 P18.14d: backfill now covers BOTH (a) batches with Phase 18 records and (b) batches with
// only legacy dispatch.out scans. _recomputeDispatchActuals now sums both sources, so any batch
// that has dispatched_qty > 0 in either flow gets the unified actuals row Planning consumes.
async function _backfillDispatchActuals() {
  try {
    let batches;
    if (pgPool) {
      // Union: batches with Phase 18 records ∪ batches with legacy dispatch.out scans
      const r = await pgPool.query(`
        SELECT batch_number FROM (
          SELECT DISTINCT batch_number FROM tracking_dispatch_records WHERE batch_number IS NOT NULL
          UNION
          SELECT DISTINCT batch_number FROM tracking_scans WHERE batch_number IS NOT NULL AND dept='dispatch' AND type='out'
        ) AS u
      `);
      batches = r.rows.map(x => x.batch_number);
    } else {
      batches = db.prepare(`
        SELECT batch_number FROM (
          SELECT DISTINCT batch_number FROM tracking_dispatch_records WHERE batch_number IS NOT NULL
          UNION
          SELECT DISTINCT batch_number FROM tracking_scans WHERE batch_number IS NOT NULL AND dept='dispatch' AND type='out'
        )
      `).all().map(x => x.batch_number);
    }
    if (!batches.length) return;
    let updated = 0;
    for (const b of batches) {
      try {
        // Look up latest vehicle/invoice from the most recent Phase 18 record if any (preserves metadata).
        // Legacy-only batches will get NULL vehicle/invoice — that's expected.
        let latest;
        if (pgPool) {
          const r = await pgPool.query(`SELECT vehicle_no, invoice_no FROM tracking_dispatch_records WHERE batch_number=$1 ORDER BY ts DESC LIMIT 1`, [b]);
          latest = r.rows[0];
        } else {
          latest = db.prepare(`SELECT vehicle_no, invoice_no FROM tracking_dispatch_records WHERE batch_number=? ORDER BY ts DESC LIMIT 1`).get(b);
        }
        await _recomputeDispatchActuals(b, latest?.vehicle_no || null, latest?.invoice_no || null);
        updated++;
      } catch(e) { console.warn(`[v37I backfill] batch ${b}:`, e?.message); }
    }
    console.log(`[v37I/v40 P18.14d backfill] Recomputed dispatch_actuals for ${updated} batch(es) — includes legacy-only and straddle batches.`);
  } catch(e) { console.warn('[v37I backfill] outer failure:', e?.message); }
}
