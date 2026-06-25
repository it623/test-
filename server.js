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

// ── v44Y: PC Master fallback resolver ──────────────────────────────────────────
// Invoice enrichment derives size/colour from an ItemCode. The server `pc_codes`
// table (admin-saved + hand-edited + future PC-Master additions) is the authoritative
// source and is queried FIRST (it wins). This map is the comprehensive shipped master
// (public/pc-codes-data.js → PC_CODES_RAW), used only as a FALLBACK so a code that was
// never saved server-side still resolves its size/colour. Loaded once and cached; a new
// deploy of pc-codes-data.js refreshes it automatically on next startup.
let _pcMasterMap = null;
function _getPcMasterMap() {
  if (_pcMasterMap) return _pcMasterMap;
  _pcMasterMap = new Map();
  try {
    const txt = fs.readFileSync(path.join(__dirname, 'public', 'pc-codes-data.js'), 'utf8');
    const start = txt.indexOf('{');
    const end = txt.lastIndexOf('}');
    if (start >= 0 && end > start) {
      const raw = JSON.parse(txt.slice(start, end + 1));
      for (const sz of Object.keys(raw)) {
        const list = raw[sz] || [];
        for (const e of list) {
          if (!e || !e.c) continue;
          const code = String(e.c).trim();
          if (code && !_pcMasterMap.has(code)) _pcMasterMap.set(code, { size: sz, colour: e.n || '' });
        }
      }
    }
    console.log('[PC master] loaded ' + _pcMasterMap.size + ' codes from pc-codes-data.js (enrichment fallback)');
  } catch (e) {
    console.warn('[PC master] could not load pc-codes-data.js fallback:', e.message);
  }
  return _pcMasterMap;
}
function _pcMasterLookup(code) {
  if (!code) return null;
  return _getPcMasterMap().get(String(code).trim()) || null;
}
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
    pgPool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 10, keepAlive: true, idleTimeoutMillis: 30000, connectionTimeoutMillis: 10000 });
    // v44F: a cloud Postgres dropping an IDLE pooled connection emits a pool 'error'. With NO listener,
    // node-postgres rethrows it as an uncaught exception and the ENTIRE process crashes — which silently
    // stops all scan saves (clients then queue scans locally and retry every 10s). Handle it so a routine
    // idle drop never crashes the server. keepAlive + idle/connect timeouts reduce idle-drop churn and
    // stop a single slow/stuck connection from blocking saves. (max 5 -> 10 for multi-device sync load.)
    pgPool.on('error', (err) => { console.error('[DB] idle pg client error (handled — NOT crashing):', err && err.message); });
    console.log('[DB] Direct pg pool ready (max=10, keepAlive, idle/connect timeouts, error-handled)');
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
  {
    // v40 Phase 18.15: WO Multi-Customer Split
    // A WO order (e.g. 50-box batch 26ZC100) can be split into 1..N child customer orders.
    // Each child gets its own batch number (parent + suffix), customer, qty, and label range.
    // Planner proposes; Admin approves; on approval, all child orders + label/scan rebatch happen atomically.
    version: 22,
    name: 'wo_split_requests',
    sql: `CREATE TABLE IF NOT EXISTS wo_split_requests (
      id TEXT PRIMARY KEY,
      source_order_id TEXT NOT NULL,
      source_batch_number TEXT NOT NULL,
      proposed_by TEXT NOT NULL,
      proposed_at TEXT NOT NULL DEFAULT (datetime('now')),
      approved_by TEXT,
      approved_at TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      total_boxes_split INTEGER NOT NULL DEFAULT 0,
      residual_boxes INTEGER NOT NULL DEFAULT 0,
      rejection_reason TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_wo_split_status ON wo_split_requests(status);
    CREATE INDEX IF NOT EXISTS idx_wo_split_source ON wo_split_requests(source_order_id);

    CREATE TABLE IF NOT EXISTS wo_split_lines (
      id TEXT PRIMARY KEY,
      split_request_id TEXT NOT NULL,
      line_index INTEGER NOT NULL,
      customer TEXT NOT NULL,
      bill_to TEXT,
      po_number TEXT,
      zone TEXT,
      boxes INTEGER NOT NULL,
      qty_lakhs REAL NOT NULL,
      box_start INTEGER NOT NULL,
      box_end INTEGER NOT NULL,
      child_batch_suffix TEXT NOT NULL,
      child_batch_number TEXT NOT NULL,
      child_order_id TEXT,
      sap_doc_entry INTEGER,
      sap_doc_num TEXT,
      FOREIGN KEY (split_request_id) REFERENCES wo_split_requests(id)
    );
    CREATE INDEX IF NOT EXISTS idx_wo_split_lines_req ON wo_split_lines(split_request_id);
    CREATE INDEX IF NOT EXISTS idx_wo_split_lines_child ON wo_split_lines(child_batch_number);`
  },
  {
    // v40 Phase 18.16: Admin Users page + Tracking auth hardening
    // Adds is_active to app_users so admin can disable accounts without deleting them
    // (preserves audit trail). Default = 1 so existing users stay enabled across upgrade.
    version: 23,
    name: 'app_users_is_active',
    sql: `ALTER TABLE app_users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;`
  },
  {
    // v40 Phase 18.17: Data Integrity Dashboard
    // Findings are deduped by finding_key (stable hash of check_type + entity + day-window).
    // Same finding re-detected on a later scan only updates last_seen + raw_data_json.
    // ack_until = NULL means not acknowledged; if set and in future, finding is hidden.
    version: 24,
    name: 'integrity_findings',
    sql: `CREATE TABLE IF NOT EXISTS integrity_findings (
      id TEXT PRIMARY KEY,
      finding_key TEXT NOT NULL UNIQUE,
      check_type TEXT NOT NULL,
      severity TEXT NOT NULL CHECK(severity IN ('critical','warning','info')),
      batch_number TEXT,
      order_id TEXT,
      machine_id TEXT,
      day TEXT,
      description TEXT NOT NULL,
      suggested_app TEXT,
      suggested_page TEXT,
      suggested_role TEXT,
      suggested_action TEXT,
      raw_data_json TEXT,
      first_seen TEXT NOT NULL DEFAULT (datetime('now')),
      last_seen TEXT NOT NULL DEFAULT (datetime('now')),
      ack_by TEXT,
      ack_at TEXT,
      ack_reason TEXT,
      ack_until TEXT,
      resolved INTEGER DEFAULT 0,
      resolved_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_integrity_severity ON integrity_findings(severity);
    CREATE INDEX IF NOT EXISTS idx_integrity_check_type ON integrity_findings(check_type);
    CREATE INDEX IF NOT EXISTS idx_integrity_batch ON integrity_findings(batch_number);
    CREATE INDEX IF NOT EXISTS idx_integrity_resolved ON integrity_findings(resolved);

    CREATE TABLE IF NOT EXISTS integrity_mutes (
      check_type TEXT PRIMARY KEY,
      muted_by TEXT NOT NULL,
      muted_at TEXT NOT NULL DEFAULT (datetime('now')),
      reason TEXT
    );`
  },
  {
    // v40 Phase 18.17: Integrity tasks — admin assigns findings to operators/roles
    // for action. assigned_to is either a username OR 'role:xxx' for role-based fanout.
    // status: pending → seen (operator opened it) → resolved (next scan no longer
    // detects the underlying finding) or → dismissed (admin withdrew the task).
    version: 25,
    name: 'integrity_tasks',
    sql: `CREATE TABLE IF NOT EXISTS integrity_tasks (
      id TEXT PRIMARY KEY,
      finding_id TEXT,
      assigned_to TEXT NOT NULL,
      assigned_by TEXT NOT NULL,
      assigned_at TEXT NOT NULL DEFAULT (datetime('now')),
      app TEXT,
      note TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      seen_at TEXT,
      seen_by TEXT,
      resolved_at TEXT,
      dismissed_at TEXT,
      dismissed_by TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_int_tasks_assignee ON integrity_tasks(assigned_to);
    CREATE INDEX IF NOT EXISTS idx_int_tasks_status ON integrity_tasks(status);
    CREATE INDEX IF NOT EXISTS idx_int_tasks_finding ON integrity_tasks(finding_id);`
  },
  {
    // v41 P19.1 Fix 3A: clean up duplicate print_orders rows.
    // Bug: same batch_number + pc_code could exist as both an assigned row (machine_id set)
    // AND an unassigned ghost (machine_id NULL/empty), causing UI to show the order
    // in both the OPM table AND the "Unassigned Print Orders" list.
    // Fix: delete the unassigned ghosts where a matching assigned row exists. Idempotent.
    // Non-destructive — only removes the duplicate UNASSIGNED row, never the assigned one.
    version: 26,
    name: 'cleanup_duplicate_print_orders',
    sql: `DELETE FROM print_orders
          WHERE (machine_id IS NULL OR machine_id = '' OR machine_id = 'null')
            AND EXISTS (
              SELECT 1 FROM print_orders p2
              WHERE p2.batch_number = print_orders.batch_number
                AND COALESCE(p2.pc_code,'') = COALESCE(print_orders.pc_code,'')
                AND p2.machine_id IS NOT NULL
                AND p2.machine_id != ''
                AND p2.machine_id != 'null'
            );`
  },
  {
    // v41 P19.2 Fix 6G: dismissed SAP indents — lets admin hide unplanned indent lines
    // that will NOT be planned in Sunloc (legacy, cancelled in SAP but not yet closed, etc.).
    // Composite key (sap_doc_entry, line_num) so multiple lines of one Sales Order can be
    // dismissed independently. If SAP reopens the line later, user can un-dismiss.
    version: 27,
    name: 'dismissed_sap_indents',
    sql: `CREATE TABLE IF NOT EXISTS dismissed_sap_indents (
      sap_doc_entry INTEGER NOT NULL,
      line_num INTEGER NOT NULL,
      sap_doc_num TEXT,
      card_code TEXT,
      card_name TEXT,
      item_code TEXT,
      dismissed_by TEXT NOT NULL,
      dismissed_at TEXT NOT NULL DEFAULT (datetime('now')),
      reason TEXT,
      PRIMARY KEY (sap_doc_entry, line_num)
    );
    CREATE INDEX IF NOT EXISTS idx_dsi_doc_entry ON dismissed_sap_indents(sap_doc_entry);`
  },
  {
    // v41 P19.3: Sales Order cumulative consumption ledger.
    // Tracks dispatched qty + value per SAP Sales Order across multiple A/R Invoices.
    // Original qty/value pulled from the SAP indent at first registration.
    // Updated each time a Sunloc-linked invoice is reconciled. Used for:
    //   (a) Showing dispatch managers the remaining headroom for an SO
    //   (b) Enforcing the 15% over-dispatch tolerance with admin override
    //   (c) Flagging fully-exhausted SOs so users know not to plan more against them
    version: 28,
    name: 'sales_order_consumption',
    sql: `CREATE TABLE IF NOT EXISTS sales_order_consumption (
      sap_doc_entry INTEGER PRIMARY KEY,
      sap_doc_num TEXT,
      card_code TEXT,
      card_name TEXT,
      original_qty_lakhs REAL NOT NULL DEFAULT 0,
      original_value_inr REAL NOT NULL DEFAULT 0,
      dispatched_qty_lakhs REAL NOT NULL DEFAULT 0,
      dispatched_value_inr REAL NOT NULL DEFAULT 0,
      invoice_count INTEGER NOT NULL DEFAULT 0,
      last_invoice_at TEXT,
      first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_soc_doc_num ON sales_order_consumption(sap_doc_num);
    CREATE INDEX IF NOT EXISTS idx_soc_card_code ON sales_order_consumption(card_code);`
  },
  {
    // v41 P19.3: Invoice flow rework — Sunloc no longer pushes Deliveries to SAP.
    // Instead, "Generate Invoice" creates a pending_reconciliation row in invoice_requests.
    // The SAP user creates the invoice in SAP manually (their existing workflow).
    // Sunloc's 5-min poller pulls the invoice; reconciliation matches by Sales Order ref.
    //
    // New columns on invoice_requests:
    //   - reconciled_at, reconciled_with_invoice_id — set when SAP invoice match is found
    //   - is_overdispatch_approved — admin approval for going beyond 115% SO tolerance
    //
    // New columns on invoices_received:
    //   - is_legacy_closed — for invoices that don't match any planned SO (legacy/return)
    //
    // Status flow (new):
    //   invoice_request: pending_reconciliation → reconciled → ready_to_scan_out → dispatched
    //   invoices_received: pending → reconciled (or legacy_closed) → scanned → dispatched
    version: 29,
    name: 'invoice_request_reconciliation_fields',
    sql: `ALTER TABLE invoice_requests ADD COLUMN reconciled_at TEXT;
          ALTER TABLE invoice_requests ADD COLUMN reconciled_with_invoice_id TEXT;
          ALTER TABLE invoice_requests ADD COLUMN is_overdispatch_approved INTEGER NOT NULL DEFAULT 0;
          ALTER TABLE invoice_requests ADD COLUMN overdispatch_approved_by TEXT;
          ALTER TABLE invoice_requests ADD COLUMN overdispatch_approved_at TEXT;
          ALTER TABLE invoices_received ADD COLUMN is_legacy_closed INTEGER NOT NULL DEFAULT 0;
          ALTER TABLE invoices_received ADD COLUMN legacy_closed_by TEXT;
          ALTER TABLE invoices_received ADD COLUMN legacy_closed_at TEXT;
          ALTER TABLE invoices_received ADD COLUMN legacy_close_reason TEXT;`
  },
  {
    // v41 P19.3 hardening: soc_applied flag prevents SOC ledger double-counting.
    // The SAP invoice poller runs every 5 minutes and re-processes the same invoices each cycle.
    // Without this flag, every poll would add lineQty to dispatched_qty_lakhs again, inflating
    // it by 12× per hour. With this flag, ledger update happens once per invoice.
    // If an invoice is amended in SAP (qty changes), the ledger row's delta logic in code
    // handles the diff explicitly — flag flips back to 0 in that explicit path only.
    version: 30,
    name: 'invoices_received_soc_applied_flag',
    sql: `ALTER TABLE invoices_received ADD COLUMN soc_applied INTEGER NOT NULL DEFAULT 0;
          CREATE INDEX IF NOT EXISTS idx_inv_recv_soc_applied ON invoices_received(soc_applied);`
  },
  {
    // v41 PERF FIX (tracking slowness root cause): tracking_scans has grown to ~29.5k rows.
    // The scans-recent endpoint runs `ORDER BY ts DESC` (and the initial load filters
    // `WHERE ts >= ...`), but SQLite had NO index on `ts` — only idx_scans_batch(batch_number,dept).
    // Every scan query therefore did a full-table scan + sort of all 29.5k rows, exceeding the
    // client fetch timeout and ABORTING ("refreshScans error / STEP 3 scans failed: aborted"),
    // which in turn starved the labels load (label count showed 0). A plain ts index turns the
    // ORDER BY ts DESC / WHERE ts >= into an index range scan. (The PG path already had
    // idx_scans_dept_ts; this brings SQLite to parity. CREATE INDEX IF NOT EXISTS is a no-op on PG.)
    version: 31,
    name: 'tracking_scans_ts_index',
    sql: `CREATE INDEX IF NOT EXISTS idx_scans_ts ON tracking_scans(ts);`
  },
  {
    // v41b LABEL-LOAD FIX: tracking_labels has grown to ~10.2k rows. The labels-all endpoint
    // runs `SELECT * FROM tracking_labels ORDER BY generated DESC` on every page load. There was
    // an index on batch_number but NONE on `generated`, so the ORDER BY did a full-table sort of
    // all 10.2k rows every load. Combined with the heavy SELECT * (pulling the big qr_data string
    // per row), the fetch was timing out under the shared pool, leaving the client's label cache
    // empty → "Labels Generated: 0", empty label table, empty print queue, no orange labels.
    // This index makes the ORDER BY generated DESC an index scan. (qr_data is also dropped from the
    // bulk response in code — fetched on demand when actually printing a label.)
    version: 32,
    name: 'tracking_labels_generated_index',
    sql: `CREATE INDEX IF NOT EXISTS idx_labels_generated ON tracking_labels(generated);`
  },
  {
    // v41l: batch REOPEN support. A Production Manager may reopen an inadvertently-closed batch
    // ONCE, same calendar day (IST) only. This log records every reopen so the once-per-batch limit
    // survives the deletion of the dpr_batch_closed row (which reopen removes) and any page refresh.
    // A row here for an order_id means "this batch has already used its one reopen" → no second one.
    // Separate table (not ALTER on dpr_batch_closed) because SQLite lacks ADD COLUMN IF NOT EXISTS,
    // so a single cross-dialect ALTER block isn't safe; a fresh CREATE TABLE IF NOT EXISTS is.
    version: 33,
    name: 'dpr_batch_reopen_log',
    sql: `CREATE TABLE IF NOT EXISTS dpr_batch_reopen_log (
        order_id TEXT PRIMARY KEY,
        batch_number TEXT,
        closed_at TEXT,
        reopened_at TEXT NOT NULL DEFAULT (datetime('now')),
        reopened_by TEXT
      );`
  },
  {
    // v41ZB: per-entry remark/note on salvage & remelt wastage. Added as a new end-of-array
    // migration (the live table was created by v2 without this column; later CREATE TABLE
    // IF NOT EXISTS blocks are no-ops on the existing table). schema_migrations tracking
    // ensures this ALTER runs exactly once. Pattern mirrors existing ADD COLUMN migrations.
    version: 34,
    name: 'tracking_wastage_note',
    sql: `ALTER TABLE tracking_wastage ADD COLUMN note TEXT;`
  },
  {
    // v41ZI Item 6: batch-level DPR gross override. A single corrected gross per batch, set by
    // Production Manager / Admin from the DPR "Closed Batches" report when an incorrect DPR gross
    // was entered. When present this value supersedes the SUM(production_actuals) for that batch
    // everywhere gross is consumed (Planning order.actualProd, Tracking Reports D & E). No time
    // gate (per Ishan: discard 24h, keep existing reopen gates untouched). Every change audited.
    version: 35,
    name: 'batch_gross_override',
    sql: `CREATE TABLE IF NOT EXISTS batch_gross_override (
        batch_number TEXT PRIMARY KEY,
        gross_lakhs REAL NOT NULL,
        reason TEXT,
        updated_by TEXT,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      );`
  },
  {
    // v41ZZ: retired (legacy "as-is-where-is") batches. A retire is a STATUS-ONLY close —
    // it records this marker + sets Planning status=closed + DPR dpr_batch_closed, and the
    // batch's WIP is EXCLUDED (treated 0) everywhere. NO production/scan data is touched, so
    // A-Grade / gross / average are unchanged. prev_* columns make it fully reversible (un-retire).
    version: 36,
    name: 'retired_batches',
    sql: `CREATE TABLE IF NOT EXISTS retired_batches (
        batch_number TEXT PRIMARY KEY,
        order_id TEXT,
        retired_at TEXT NOT NULL DEFAULT (datetime('now')),
        retired_by TEXT,
        reason TEXT,
        prod_month TEXT,
        residual_wip REAL DEFAULT 0,
        prev_order_status TEXT,
        prev_dpr_closed INTEGER DEFAULT 0
      );`
  },
  {
    // v43A #4: index parent_label_id so the PI scan-out orange gate (lookup of a box's orange child
    // label) is an index hit, not a seq scan of tracking_labels on every PI scan-out.
    version: 37,
    name: 'idx_labels_parent',
    sql: `CREATE INDEX IF NOT EXISTS idx_labels_parent ON tracking_labels(parent_label_id);`
  },
  {
    // v44 #2(ii): short-qty close. Lets the printing manager close the PRINTING stage with fewer boxes
    // out than in (no formal wastage rows) when AIM delivered fewer or printing wastage ran higher.
    // The reason + shortfall are persisted on the closure and carried downstream (packing/dispatch).
    version: 38,
    name: 'stage_closure_short',
    sql: `ALTER TABLE tracking_stage_closure ADD COLUMN short_close INTEGER DEFAULT 0;
          ALTER TABLE tracking_stage_closure ADD COLUMN short_reason TEXT;
          ALTER TABLE tracking_stage_closure ADD COLUMN short_boxes INTEGER DEFAULT 0;`
  },
  {
    // v44C #6 (Addition 3): full before/after snapshot for every re-customer action (audit-grade log,
    // queryable). One row per action; covers full / split / printed-conversion variants.
    version: 39,
    name: 'recustomer_log',
    sql: `CREATE TABLE IF NOT EXISTS recustomer_log (
        id TEXT PRIMARY KEY,
        batch_number TEXT,
        child_batch_number TEXT,
        action_type TEXT,
        from_customer TEXT,
        to_customer TEXT,
        from_po TEXT,
        to_po TEXT,
        card_code TEXT,
        ship_to TEXT,
        bill_to TEXT,
        split_boxes INTEGER DEFAULT 0,
        total_boxes INTEGER DEFAULT 0,
        converted_to_printed INTEGER DEFAULT 0,
        labels_affected INTEGER DEFAULT 0,
        before_json TEXT,
        after_json TEXT,
        reason TEXT,
        by_user TEXT,
        ts TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_recustomer_batch ON recustomer_log(batch_number);`
  },
  {
    // v44C #6 (Addition 2/D5): scan-reversal ledger. When an unprinted batch is re-customered INTO a
    // printed flow, its packing-in scans are NOT deleted — instead a reversal row is posted here (the
    // "debit"), preserving full history. Reversed scans are excluded from the scan-summary counts and
    // from the box-identity dedup, so the box shows pending-printing and can be re-packed after PI.
    version: 40,
    name: 'tracking_scan_reversals',
    sql: `CREATE TABLE IF NOT EXISTS tracking_scan_reversals (
        id TEXT PRIMARY KEY,
        reversed_scan_id TEXT NOT NULL,
        batch_number TEXT,
        label_id TEXT,
        dept TEXT,
        type TEXT,
        reason TEXT,
        by_user TEXT,
        ts TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_scan_rev_scan ON tracking_scan_reversals(reversed_scan_id);
      CREATE INDEX IF NOT EXISTS idx_scan_rev_label ON tracking_scan_reversals(label_id);`
  },
  {
    // v44E Issue#1: admin WIP reconciliation OVERRIDE. Authoritative per-batch values typed by the
    // admin (Gross / A-Grade / Packing / WIP / Wastage, all in Lakhs). When present, every report
    // (A–G) + the A-Grade calc read these IN PLACE OF the scan-derived figures for that batch.
    // Fully reversible (clear). No scan/DPR/wastage rows are written — the override sits at the
    // consumption layer, so the frozen formulas themselves are untouched.
    version: 41,
    name: 'batch_reconcile_override',
    sql: `CREATE TABLE IF NOT EXISTS batch_reconcile_override (
        batch_number TEXT PRIMARY KEY,
        gross REAL,
        a_grade REAL,
        packing REAL,
        wip REAL,
        wastage REAL,
        reason TEXT,
        by_user TEXT,
        ts TEXT NOT NULL DEFAULT (datetime('now'))
      );`
  },
  {
    // v44Q: invoice_scan_sessions relocated from duplicate version 22 to 42 to restore the
    // append-only strictly-ascending migration discipline. The table is also created by the PG
    // bootstrap (CREATE TABLE IF NOT EXISTS) below, so re-applying this migration is a safe no-op.
    version: 42,
    name: 'invoice_scan_sessions',
    sql: `CREATE TABLE IF NOT EXISTS invoice_scan_sessions (
      invoice_id TEXT PRIMARY KEY,
      scanned_json TEXT NOT NULL DEFAULT '[]',
      saved_at TEXT DEFAULT (datetime('now'))
    );`
  },
  {
    // v44R Phase 2: dispatched-box aggregation. tracking_dispatch_actuals already stores
    // dispatched_qty; add dispatched_boxes so the truck binner can compute remaining boxes
    // (planned - dispatched) per lot. Populated by _recomputeDispatchActuals (SUM of record boxes
    // + COUNT of legacy dispatch-out scans). SQLite path; PG bootstrap adds the column too.
    version: 43,
    name: 'dispatch_actuals_boxes',
    sql: `ALTER TABLE tracking_dispatch_actuals ADD COLUMN dispatched_boxes REAL NOT NULL DEFAULT 0;`
  },
  {
    // v44R Phase 2/3: stable truck identity via lock-on-activation. A truck is ephemeral (recomputed
    // each render) UNTIL it is acted on — a truck-scan-session starts, or a partial dispatch/regularise
    // hits one of its lots. At that point the client locks it: its number + manifest freeze here, and
    // future re-bins lay remaining boxes AROUND locked trucks. manifest_json = [{planId,batchNumber,
    // allocatedBoxes,allocatedQty}]. status: 'active' (locked, in progress) | 'finalized' (dispatched,
    // any short remainder already rolled forward via the remaining-boxes re-bin).
    version: 44,
    name: 'dispatch_truck_locks',
    sql: `CREATE TABLE IF NOT EXISTS dispatch_truck_locks (
      truck_id TEXT PRIMARY KEY,
      zone TEXT NOT NULL,
      truck_number INTEGER,
      manifest_json TEXT NOT NULL DEFAULT '[]',
      status TEXT NOT NULL DEFAULT 'active',
      vehicle_no TEXT,
      lr_no TEXT,
      locked_by TEXT,
      locked_at TEXT,
      finalized_by TEXT,
      finalized_at TEXT,
      remarks TEXT
    );`
  },
  {
    // v44ZC (v44AD): SO-number reconciliation. SAP invoices created via the standard
    // SO -> Delivery -> Invoice chain reference the Delivery (BaseType 15), not the SO, so the
    // poller's SO-DocEntry/batch matches miss and the request never reconciles (dead scan-out).
    // The reliable cross-document key is the SO NUMBER: it appears in the invoice's Comments
    // ("Based On Sales Orders 237") and is known on the request at creation. Storing it explicitly
    // on both tables lets us match number-to-number with no dependence on the indent cache (which
    // prunes completed SOs, so the old DocNum->DocEntry bridge fails at reconcile time).
    //   invoice_requests.so_doc_num     = the SO number this request is for
    //   invoices_received.base_so_doc_num = SO number parsed from the invoice's Comments
    version: 45,
    name: 'so_number_reconciliation_columns',
    sql: `ALTER TABLE invoice_requests ADD COLUMN so_doc_num TEXT;
          ALTER TABLE invoices_received ADD COLUMN base_so_doc_num TEXT;`
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
  // v40 P18.16: 7 dept-specific tracking users matching the existing client-side DEPT_PINS,
  // so on first deploy operators don't experience disruption (1B path). The defaults below
  // are weak by design — admin should rotate them via the new Admin Users page asap.
  // Role names follow the same convention as planning roles.
  { username: 'Track_Planning',    pin: '1111', role: 'tracking_planning', app: 'tracking' },
  { username: 'Track_Labels',      pin: '2222', role: 'tracking_labels',   app: 'tracking' },
  { username: 'Track_AIM',         pin: '3333', role: 'tracking_aim',      app: 'tracking' },
  { username: 'Track_Printing',    pin: '4444', role: 'tracking_printing', app: 'tracking' },
  { username: 'Track_PI',          pin: '5555', role: 'tracking_pi',       app: 'tracking' },
  { username: 'Track_Packing',     pin: '6666', role: 'tracking_packing',  app: 'tracking' },
  { username: 'Track_Dispatch',    pin: '7777', role: 'tracking_dispatch', app: 'tracking' },
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
// v41ZN: throttle the background production_orders merge. The blob (planning_state) is saved on every
// POST regardless; production_orders is only the recovery/reconciliation copy, so it tolerates a short
// lag. Skipping redundant merges keeps the 5-connection pool from being churned every ~13s (the cause
// of intermittent timeouts/offline alongside the now-removed gross JOIN).
let _lastBgMerge = 0;
const BG_MERGE_DEBOUNCE_MS = parseInt(process.env.BG_MERGE_DEBOUNCE_MS, 10) || 30000;

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
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_labels_parent ON tracking_labels(parent_label_id)`).catch(()=>{}); // v43A #4: orange-gate lookup
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
    try { await pgPool.query(`ALTER TABLE tracking_stage_closure ADD COLUMN IF NOT EXISTS short_close INTEGER DEFAULT 0`); } catch(e){}
    try { await pgPool.query(`ALTER TABLE tracking_stage_closure ADD COLUMN IF NOT EXISTS short_reason TEXT`); } catch(e){}
    try { await pgPool.query(`ALTER TABLE tracking_stage_closure ADD COLUMN IF NOT EXISTS short_boxes INTEGER DEFAULT 0`); } catch(e){} // v44 #2(ii)
    await pgPool.query(`CREATE TABLE IF NOT EXISTS recustomer_log (id TEXT PRIMARY KEY, batch_number TEXT, child_batch_number TEXT, action_type TEXT, from_customer TEXT, to_customer TEXT, from_po TEXT, to_po TEXT, card_code TEXT, ship_to TEXT, bill_to TEXT, split_boxes INTEGER DEFAULT 0, total_boxes INTEGER DEFAULT 0, converted_to_printed INTEGER DEFAULT 0, labels_affected INTEGER DEFAULT 0, before_json TEXT, after_json TEXT, reason TEXT, by_user TEXT, ts TEXT)`).catch(()=>{}); // v44C #6
    await pgPool.query(`CREATE TABLE IF NOT EXISTS tracking_scan_reversals (id TEXT PRIMARY KEY, reversed_scan_id TEXT NOT NULL, batch_number TEXT, label_id TEXT, dept TEXT, type TEXT, reason TEXT, by_user TEXT, ts TEXT)`).catch(()=>{}); // v44C #6
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_scan_rev_scan ON tracking_scan_reversals(reversed_scan_id)`).catch(()=>{});
    await pgPool.query(`CREATE TABLE IF NOT EXISTS batch_reconcile_override (batch_number TEXT PRIMARY KEY, gross REAL, a_grade REAL, packing REAL, wip REAL, wastage REAL, reason TEXT, by_user TEXT, ts TEXT)`).catch(()=>{}); // v44E Issue#1
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
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);
    // v40 P18.16: ALTER for existing PG deployments — column may be absent on older instances
    try { await pgPool.query(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS is_active INTEGER NOT NULL DEFAULT 1`); } catch(e) { /* tolerate */ }

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

    // v41ZD: month_archives — previously created ONLY in the SQLite migrations array, so on
    // Railway (Postgres) the table never existed. That made /api/archives/save and
    // /api/archives/list throw, which (a) broke the Archives page ("Loading archives…" hang) and
    // (b) silently failed the on-load auto-archive, leaving activeMonth stuck on the prior month.
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS month_archives (
        id SERIAL PRIMARY KEY,
        month TEXT NOT NULL UNIQUE,
        archived_at TIMESTAMPTZ DEFAULT NOW(),
        archived_by TEXT,
        snapshot_json JSONB,
        is_auto BOOLEAN DEFAULT TRUE
      )
    `);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_month_archives_month ON month_archives(month)`).catch(()=>{});

    // v41ZI Item 6: batch-level DPR gross override (native-PG idempotent create).
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS batch_gross_override (
        batch_number TEXT PRIMARY KEY,
        gross_lakhs DOUBLE PRECISION NOT NULL,
        reason TEXT,
        updated_by TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `).catch(e=>console.warn('[v41ZI PG] batch_gross_override:', e.message));

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
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_labels_parent ON tracking_labels(parent_label_id)`).catch(()=>{}); // v43A #4: orange-gate lookup
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
    try { await pgPool.query(`ALTER TABLE tracking_stage_closure ADD COLUMN IF NOT EXISTS short_close INTEGER DEFAULT 0`); } catch(e){}
    try { await pgPool.query(`ALTER TABLE tracking_stage_closure ADD COLUMN IF NOT EXISTS short_reason TEXT`); } catch(e){}
    try { await pgPool.query(`ALTER TABLE tracking_stage_closure ADD COLUMN IF NOT EXISTS short_boxes INTEGER DEFAULT 0`); } catch(e){} // v44 #2(ii)
    await pgPool.query(`CREATE TABLE IF NOT EXISTS recustomer_log (id TEXT PRIMARY KEY, batch_number TEXT, child_batch_number TEXT, action_type TEXT, from_customer TEXT, to_customer TEXT, from_po TEXT, to_po TEXT, card_code TEXT, ship_to TEXT, bill_to TEXT, split_boxes INTEGER DEFAULT 0, total_boxes INTEGER DEFAULT 0, converted_to_printed INTEGER DEFAULT 0, labels_affected INTEGER DEFAULT 0, before_json TEXT, after_json TEXT, reason TEXT, by_user TEXT, ts TEXT)`).catch(()=>{}); // v44C #6
    await pgPool.query(`CREATE TABLE IF NOT EXISTS tracking_scan_reversals (id TEXT PRIMARY KEY, reversed_scan_id TEXT NOT NULL, batch_number TEXT, label_id TEXT, dept TEXT, type TEXT, reason TEXT, by_user TEXT, ts TEXT)`).catch(()=>{}); // v44C #6
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_scan_rev_scan ON tracking_scan_reversals(reversed_scan_id)`).catch(()=>{});
    await pgPool.query(`CREATE TABLE IF NOT EXISTS batch_reconcile_override (batch_number TEXT PRIMARY KEY, gross REAL, a_grade REAL, packing REAL, wip REAL, wastage REAL, reason TEXT, by_user TEXT, ts TEXT)`).catch(()=>{}); // v44E Issue#1
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
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);
    // v40 P18.16: ALTER for existing PG deployments — column may be absent on older instances
    try { await pgPool.query(`ALTER TABLE app_users ADD COLUMN IF NOT EXISTS is_active INTEGER NOT NULL DEFAULT 1`); } catch(e) { /* tolerate */ }

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
        is_deemed_scan_out INTEGER NOT NULL DEFAULT 0,
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
        success INTEGER NOT NULL DEFAULT 0,
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

    // v44Q: invoice_scan_sessions — persist single-invoice scan progress
    try {
      await pgPool.query(`CREATE TABLE IF NOT EXISTS invoice_scan_sessions (
        invoice_id TEXT PRIMARY KEY,
        scanned_json TEXT NOT NULL DEFAULT '[]',
        saved_at TEXT DEFAULT NOW()::TEXT
      )`);
    } catch (e) { console.warn('[v44Q PG] invoice_scan_sessions:', e.message); }

    // v44R Phase 2: dispatched_boxes on tracking_dispatch_actuals (idempotent)
    try {
      await pgPool.query(`ALTER TABLE tracking_dispatch_actuals ADD COLUMN IF NOT EXISTS dispatched_boxes REAL NOT NULL DEFAULT 0`);
    } catch (e) { console.warn('[v44R PG] dispatch_actuals dispatched_boxes:', e.message); }

    // v44R Phase 2/3: dispatch_truck_locks — stable truck identity (lock-on-activation)
    try {
      await pgPool.query(`CREATE TABLE IF NOT EXISTS dispatch_truck_locks (
        truck_id TEXT PRIMARY KEY,
        zone TEXT NOT NULL,
        truck_number INTEGER,
        manifest_json TEXT NOT NULL DEFAULT '[]',
        status TEXT NOT NULL DEFAULT 'active',
        vehicle_no TEXT,
        lr_no TEXT,
        locked_by TEXT,
        locked_at TEXT,
        finalized_by TEXT,
        finalized_at TEXT,
        remarks TEXT
      )`);
    } catch (e) { console.warn('[v44R PG] dispatch_truck_locks:', e.message); }

    // ─── v40 Phase 18.15: WO Multi-Customer Split tables (PG) ────
    try {
      await pgPool.query(`
        CREATE TABLE IF NOT EXISTS wo_split_requests (
          id TEXT PRIMARY KEY,
          source_order_id TEXT NOT NULL,
          source_batch_number TEXT NOT NULL,
          proposed_by TEXT NOT NULL,
          proposed_at TEXT NOT NULL DEFAULT NOW()::TEXT,
          approved_by TEXT,
          approved_at TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          total_boxes_split INTEGER NOT NULL DEFAULT 0,
          residual_boxes INTEGER NOT NULL DEFAULT 0,
          rejection_reason TEXT
        );
      `);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_wo_split_status ON wo_split_requests(status)`);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_wo_split_source ON wo_split_requests(source_order_id)`);
      await pgPool.query(`
        CREATE TABLE IF NOT EXISTS wo_split_lines (
          id TEXT PRIMARY KEY,
          split_request_id TEXT NOT NULL REFERENCES wo_split_requests(id),
          line_index INTEGER NOT NULL,
          customer TEXT NOT NULL,
          bill_to TEXT,
          po_number TEXT,
          zone TEXT,
          boxes INTEGER NOT NULL,
          qty_lakhs REAL NOT NULL,
          box_start INTEGER NOT NULL,
          box_end INTEGER NOT NULL,
          child_batch_suffix TEXT NOT NULL,
          child_batch_number TEXT NOT NULL,
          child_order_id TEXT,
          sap_doc_entry INTEGER,
          sap_doc_num TEXT
        );
      `);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_wo_split_lines_req ON wo_split_lines(split_request_id)`);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_wo_split_lines_child ON wo_split_lines(child_batch_number)`);
    } catch (e) { console.warn('[v40 P18.15 PG] wo_split tables:', e.message); }
    // ─── end v40 Phase 18.15 tables ────────────────────────────────

    // ─── v40 Phase 18.17: Data Integrity Dashboard tables (PG) ───
    try {
      await pgPool.query(`
        CREATE TABLE IF NOT EXISTS integrity_findings (
          id TEXT PRIMARY KEY,
          finding_key TEXT NOT NULL UNIQUE,
          check_type TEXT NOT NULL,
          severity TEXT NOT NULL,
          batch_number TEXT,
          order_id TEXT,
          machine_id TEXT,
          day TEXT,
          description TEXT NOT NULL,
          suggested_app TEXT,
          suggested_page TEXT,
          suggested_role TEXT,
          suggested_action TEXT,
          raw_data_json TEXT,
          first_seen TEXT NOT NULL DEFAULT NOW()::TEXT,
          last_seen TEXT NOT NULL DEFAULT NOW()::TEXT,
          ack_by TEXT,
          ack_at TEXT,
          ack_reason TEXT,
          ack_until TEXT,
          resolved INTEGER DEFAULT 0,
          resolved_at TEXT
        );
      `);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_integrity_severity ON integrity_findings(severity)`);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_integrity_check_type ON integrity_findings(check_type)`);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_integrity_batch ON integrity_findings(batch_number)`);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_integrity_resolved ON integrity_findings(resolved)`);
      await pgPool.query(`
        CREATE TABLE IF NOT EXISTS integrity_mutes (
          check_type TEXT PRIMARY KEY,
          muted_by TEXT NOT NULL,
          muted_at TEXT NOT NULL DEFAULT NOW()::TEXT,
          reason TEXT
        );
      `);
      await pgPool.query(`
        CREATE TABLE IF NOT EXISTS integrity_tasks (
          id TEXT PRIMARY KEY,
          finding_id TEXT,
          assigned_to TEXT NOT NULL,
          assigned_by TEXT NOT NULL,
          assigned_at TEXT NOT NULL DEFAULT NOW()::TEXT,
          app TEXT,
          note TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          seen_at TEXT,
          seen_by TEXT,
          resolved_at TEXT,
          dismissed_at TEXT,
          dismissed_by TEXT
        );
      `);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_int_tasks_assignee ON integrity_tasks(assigned_to)`);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_int_tasks_status ON integrity_tasks(status)`);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_int_tasks_finding ON integrity_tasks(finding_id)`);
    } catch (e) { console.warn('[v40 P18.17 PG] integrity tables:', e.message); }
    // ─── end v40 Phase 18.17 tables ────────────────────────────────

    // ─── v41 P19.1 Fix 3A: dedup print_orders (PG-side) ──────────────
    // Deletes ghost unassigned print order rows when a corresponding assigned row exists
    // for the same batch_number + pc_code. Idempotent.
    try {
      await pgPool.query(`
        DELETE FROM print_orders
        WHERE (machine_id IS NULL OR machine_id = '' OR machine_id = 'null')
          AND EXISTS (
            SELECT 1 FROM print_orders p2
            WHERE p2.batch_number = print_orders.batch_number
              AND COALESCE(p2.pc_code,'') = COALESCE(print_orders.pc_code,'')
              AND p2.machine_id IS NOT NULL
              AND p2.machine_id != ''
              AND p2.machine_id != 'null'
          );
      `);
    } catch (e) { console.warn('[v41 P19.1 PG] print_orders dedup:', e.message); }

    // ─── v41 P19.2 Fix 6G: dismissed_sap_indents (PG) ──────────────
    try {
      await pgPool.query(`
        CREATE TABLE IF NOT EXISTS dismissed_sap_indents (
          sap_doc_entry INTEGER NOT NULL,
          line_num INTEGER NOT NULL,
          sap_doc_num TEXT,
          card_code TEXT,
          card_name TEXT,
          item_code TEXT,
          dismissed_by TEXT NOT NULL,
          dismissed_at TEXT NOT NULL DEFAULT NOW()::TEXT,
          reason TEXT,
          PRIMARY KEY (sap_doc_entry, line_num)
        );
      `);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_dsi_doc_entry ON dismissed_sap_indents(sap_doc_entry)`);
    } catch (e) { console.warn('[v41 P19.2 PG] dismissed_sap_indents:', e.message); }

    // ─── v41 P19.3: sales_order_consumption ledger (PG) ─────────────
    try {
      await pgPool.query(`
        CREATE TABLE IF NOT EXISTS sales_order_consumption (
          sap_doc_entry INTEGER PRIMARY KEY,
          sap_doc_num TEXT,
          card_code TEXT,
          card_name TEXT,
          original_qty_lakhs REAL NOT NULL DEFAULT 0,
          original_value_inr REAL NOT NULL DEFAULT 0,
          dispatched_qty_lakhs REAL NOT NULL DEFAULT 0,
          dispatched_value_inr REAL NOT NULL DEFAULT 0,
          invoice_count INTEGER NOT NULL DEFAULT 0,
          last_invoice_at TEXT,
          first_seen_at TEXT NOT NULL DEFAULT NOW()::TEXT,
          updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
        );
      `);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_soc_doc_num ON sales_order_consumption(sap_doc_num)`);
      await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_soc_card_code ON sales_order_consumption(card_code)`);
    } catch (e) { console.warn('[v41 P19.3 PG] sales_order_consumption:', e.message); }

    // ─── v41 P19.3: invoice_requests + invoices_received reconciliation fields (PG) ─
    try { await pgPool.query(`ALTER TABLE invoice_requests ADD COLUMN IF NOT EXISTS reconciled_at TEXT`); } catch (e) { console.warn('[v41 P19.3 PG] add reconciled_at:', e.message); }
    // v44ZC (v44AD): SO-number reconciliation columns (see migration 45)
    try { await pgPool.query(`ALTER TABLE invoice_requests ADD COLUMN IF NOT EXISTS so_doc_num TEXT`); } catch (e) { console.warn('[v44ZC PG] add invoice_requests.so_doc_num:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoices_received ADD COLUMN IF NOT EXISTS base_so_doc_num TEXT`); } catch (e) { console.warn('[v44ZC PG] add invoices_received.base_so_doc_num:', e.message); }
    // v44ZC (v44AD): one-time backfill so already-stuck requests/invoices (created before these
    // columns) self-heal on deploy. (a) Requests hold the SO number in po_number — copy it into
    // so_doc_num. (b) Invoices: parse the SO number out of each stored invoice's Comments. After
    // this, the v44N retry pass reconciles them on the next poll via the so_doc_num match.
    try {
      const _r1 = await pgPool.query(`UPDATE invoice_requests SET so_doc_num = NULLIF(po_number,'') WHERE so_doc_num IS NULL AND po_number IS NOT NULL AND po_number <> '' AND status='pending_reconciliation'`);
      const _bf = await pgPool.query(`SELECT sap_doc_entry, payload_json FROM invoices_received WHERE (base_so_doc_num IS NULL OR base_so_doc_num='') AND source='direct_sap' AND payload_json IS NOT NULL LIMIT 5000`);
      let _bfN = 0;
      for (const row of _bf.rows) {
        try {
          const pj = typeof row.payload_json === 'string' ? JSON.parse(row.payload_json) : (row.payload_json || {});
          const so = ((pj.Comments || '').match(/Sales Orders?\s+(\d+)/i) || [])[1] || null;
          if (so) { await pgPool.query(`UPDATE invoices_received SET base_so_doc_num=$1 WHERE sap_doc_entry=$2 AND (base_so_doc_num IS NULL OR base_so_doc_num='')`, [so, row.sap_doc_entry]); _bfN++; }
        } catch {}
      }
      console.log(`[v44ZC] SO-number backfill: requests.so_doc_num rows=${_r1.rowCount}, invoices.base_so_doc_num set=${_bfN}`);
    } catch (e) { console.warn('[v44ZC] SO-number backfill error:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoice_requests ADD COLUMN IF NOT EXISTS reconciled_with_invoice_id TEXT`); } catch (e) { console.warn('[v41 P19.3 PG] add reconciled_with_invoice_id:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoice_requests ADD COLUMN IF NOT EXISTS is_overdispatch_approved INTEGER NOT NULL DEFAULT 0`); } catch (e) { console.warn('[v41 P19.3 PG] add is_overdispatch_approved:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoice_requests ADD COLUMN IF NOT EXISTS overdispatch_approved_by TEXT`); } catch (e) { console.warn('[v41 P19.3 PG] add overdispatch_approved_by:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoice_requests ADD COLUMN IF NOT EXISTS overdispatch_approved_at TEXT`); } catch (e) { console.warn('[v41 P19.3 PG] add overdispatch_approved_at:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoices_received ADD COLUMN IF NOT EXISTS is_legacy_closed INTEGER NOT NULL DEFAULT 0`); } catch (e) { console.warn('[v41 P19.3 PG] add is_legacy_closed:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoices_received ADD COLUMN IF NOT EXISTS legacy_closed_by TEXT`); } catch (e) { console.warn('[v41 P19.3 PG] add legacy_closed_by:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoices_received ADD COLUMN IF NOT EXISTS legacy_closed_at TEXT`); } catch (e) { console.warn('[v41 P19.3 PG] add legacy_closed_at:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoices_received ADD COLUMN IF NOT EXISTS legacy_close_reason TEXT`); } catch (e) { console.warn('[v41 P19.3 PG] add legacy_close_reason:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoices_received ADD COLUMN IF NOT EXISTS soc_applied INTEGER NOT NULL DEFAULT 0`); } catch (e) { console.warn('[v41 P19.3 PG] add soc_applied:', e.message); }
    try { await pgPool.query(`ALTER TABLE invoices_received ADD COLUMN IF NOT EXISTS total_qty_lakhs DOUBLE PRECISION`); } catch (e) { console.warn('[v44O #3 PG] add total_qty_lakhs:', e.message); }
    try { await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_inv_recv_soc_applied ON invoices_received(soc_applied)`); } catch (e) { console.warn('[v41 P19.3 PG] idx_inv_recv_soc_applied:', e.message); }

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
// v41ZI Item 4: pure batch-keyed DPR gross — SUM(qty_lakhs) GROUP BY batch_number ONLY (no order_id
// in the GROUP BY). The legacy _actualsCache groups by (order_id,batch_number) and then writes one
// dict entry per group, so a batch whose production_actuals rows carry differing/NULL order_id values
// (common after a rebatch, or when DPR sent only batchNumber) splits into several groups and the
// batch-keyed total ends up holding only the LAST group's partial sum — surfacing as blank/under-
// counted Gross Prod in Reports D & E. This map is the single source of truth for per-batch gross.
let _grossByBatch = null;
// v41ZI Item 6: per-batch admin/PM override of the DPR gross. When present it supersedes _grossByBatch.
let _grossOverride = {};
// v41ZY: one-time idempotent backfill — fill batch_number on existing production_actuals rows saved
// as NULL before the explicit-batch fix, from their order's batch in production_orders. This
// retroactively repairs batches whose cumulative DPR gross collapsed after close (NULL-batch rows
// were dropped once the order left active planning state). Cheap no-op once no NULL rows remain.
let _poBatchBackfillDone = false;
async function backfillProductionActualsBatch() {
  if (pgPool) {
    await pgPool.query(`
      UPDATE production_actuals pa
         SET batch_number = po.batch_number
        FROM production_orders po
       WHERE pa.order_id = po.id
         AND (pa.batch_number IS NULL OR pa.batch_number = '')
         AND po.batch_number IS NOT NULL AND po.batch_number <> ''`);
  } else {
    db.exec(`
      UPDATE production_actuals
         SET batch_number = (SELECT po.batch_number FROM production_orders po WHERE po.id = production_actuals.order_id)
       WHERE (batch_number IS NULL OR batch_number = '')
         AND order_id IN (SELECT id FROM production_orders WHERE batch_number IS NOT NULL AND batch_number <> '')`);
  }
}
async function warmActualsCache() {
  // v41ZY: ensure existing NULL-batch rows are repaired before the per-batch sum is computed. Runs
  // once (retried until it succeeds), then never again — so the very next warm reflects the fix.
  if (!_poBatchBackfillDone) {
    try { await backfillProductionActualsBatch(); _poBatchBackfillDone = true; console.log('[DPR] production_actuals batch backfill complete'); }
    catch(e) { console.warn('[DPR] batch backfill failed (will retry next warm):', e.message); }
  }
  // Throttle to 60s — prevents DB hammering from every device's 30s auto-sync
  if (Date.now() - _actualsCacheTime < 60000 && _actualsCache) return;
  _actualsCacheTime = Date.now();
  if (pgPool) {
    try {
      const r = await pgPool.query('SELECT order_id, batch_number, SUM(qty_lakhs) as total FROM production_actuals GROUP BY order_id, batch_number');
      _actualsCache = {};
      for (const row of r.rows) {
        if (row.order_id) _actualsCache[row.order_id] = parseFloat(row.total) || 0;
        if (row.batch_number) _actualsCache[row.batch_number] = parseFloat(row.total) || 0;
      }
      // v41ZL #4 / v41ZN: authoritative per-batch DPR gross, attributing every production_actuals
      // group to its EFFECTIVE batch — the row's own batch_number when present, otherwise the batch
      // of the order it was logged against. v41ZL did this with a SQL LEFT JOIN on production_orders
      // + GROUP BY a COALESCE expression; but production_orders is write-churned by the planning/state
      // background merge, so that join+expression-group contended on the DB and timed out closed-batches
      // and made the apps go offline (v41ZM). We now do the same attribution IN MEMORY from the rows
      // already fetched above plus the warmed order cache — no extra query, no join on the hot table.
      const _orderBatch = {};
      try {
        const _co = (_planningStateCache && _planningStateCache.orders) || [];
        for (const o of _co) { if (o && o.id && o.batchNumber) _orderBatch[o.id] = o.batchNumber; }
      } catch(_) {}
      _grossByBatch = {};
      for (const row of r.rows) {
        const batch = (row.batch_number && String(row.batch_number).trim()) ? row.batch_number : _orderBatch[row.order_id];
        if (!batch) continue;
        _grossByBatch[batch] = (_grossByBatch[batch] || 0) + (parseFloat(row.total) || 0);
      }
      console.log('[DB] Actuals cache warmed:', r.rows.length, 'entries;', Object.keys(_grossByBatch).length, 'batches');
    } catch(e) { console.error('[DB] Actuals cache error:', e.message); }
  }
  // v41ZI Item 6: always refresh the override map (both DB modes) so effectiveGross() applies
  // overrides even in the SQLite fallback path (where the PG aggregation above is skipped).
  await loadGrossOverrides();
}

// v41ZI Item 6: load all batch gross overrides into _grossOverride. Works in both DB modes
// (single query). Called from warmActualsCache and re-called immediately after any override write.
async function loadGrossOverrides() {
  try {
    let rows;
    if (pgPool) rows = (await pgPool.query('SELECT batch_number, gross_lakhs FROM batch_gross_override')).rows;
    else rows = db.prepare('SELECT batch_number, gross_lakhs FROM batch_gross_override').all();
    const next = {};
    for (const r of (rows||[])) { if (r.batch_number != null) next[r.batch_number] = parseFloat(r.gross_lakhs) || 0; }
    _grossOverride = next;
  } catch(e) { /* table may not exist yet on very first boot before migrations — safe to ignore */ }
}

// v41ZI Item 4 + 6: the authoritative DPR gross for a batch.
//   override (if set)  →  pure batch-keyed sum (cache)  →  direct query fallback  →  0
// In PG mode the cache is warm before any per-order loop runs (planning/state warms it first), so
// the slow synchronous fallback is only ever hit for one-off single-batch lookups or SQLite dev.
function effectiveGross(batchNumber) {
  if (!batchNumber) return 0;
  if (Object.prototype.hasOwnProperty.call(_grossOverride, batchNumber)) return _grossOverride[batchNumber];
  if (_grossByBatch && Object.prototype.hasOwnProperty.call(_grossByBatch, batchNumber)) return _grossByBatch[batchNumber];
  try {
    const row = db.prepare('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE batch_number = ?').get(batchNumber);
    return (row && row.total) ? (parseFloat(row.total) || 0) : 0;
  } catch(e) { return 0; }
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
  // v41e FIX (issue 4): the client authenticates with the session token (x-session-token), NOT an
  // x-sunloc-role header — so the old header/body role read was always empty and every admin got
  // a false "Admin role required" 403. Resolve the role from the verified session instead, with a
  // legacy fallback to the explicit role header/body for any old caller that still sends it.
  const session = verifyToken(req.headers['x-session-token'] || req.body?.token || req.query?.token);
  let role = (session?.role || '').toString().toLowerCase();
  if (!role) role = (req.headers['x-sunloc-role'] || req.body?._role || '').toString().toLowerCase();
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
    const totalQty = (ind.DocumentLines || []).reduce((sum, l) => sum + (parseFloat(l.Quantity) || 0), 0);
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
  // v41ZS Issue 1: prune the cache of orders no longer open in SAP. fetchOpenSalesOrders returns
  // ONLY bost_Open orders; a cached row whose DocEntry was NOT in this fetch has since been closed/
  // executed in SAP and must be removed, else it lingers forever in the Unplanned Orders list (the
  // upsert above only touches rows that ARE returned). Guards: prune ONLY on a COMPLETE fetch
  // (r.complete — never on a partial/paged-failure set, which would wrongly delete unfetched open
  // orders) and never when the fetch came back empty (avoid wiping the cache on a transient empty
  // response). Anything not in bost_Open is genuinely closed (a SAP SO header stays open while any
  // line is open), so this cannot drop a genuinely-open order.
  if (r.complete && indents.length > 0) {
    const keepEntries = indents.map(i => parseInt(i.DocEntry, 10)).filter(e => Number.isInteger(e));
    if (keepEntries.length > 0) {
      try {
        let pruned;
        if (pgPool) {
          const pr = await pgPool.query(`DELETE FROM sap_indent_cache WHERE sap_doc_entry <> ALL($1::int[])`, [keepEntries]);
          pruned = pr.rowCount;
        } else {
          const placeholders = keepEntries.map(() => '?').join(',');
          const pr = db.prepare(`DELETE FROM sap_indent_cache WHERE sap_doc_entry NOT IN (${placeholders})`).run(...keepEntries);
          pruned = pr.changes;
        }
        if (pruned) console.log('[SAP] indent cache pruned', pruned, 'no-longer-open order(s)');
      } catch (e) { console.warn('[SAP] indent cache prune failed:', e.message); }
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
//
// v41 P19.3: Enhanced reconciliation by Sales Order Number.
// In addition to the legacy batch-UDF match, this poller now matches each invoice's
// DocumentLines[*].BaseEntry (SAP Sales Order reference) against pending_reconciliation
// invoice_requests. Matching is done at the line level — one invoice may reconcile
// multiple invoice_requests (consolidated dispatch case) or one (per-batch case).
// On successful reconcile:
//   1. invoice_requests row → status='reconciled', reconciled_at, reconciled_with_invoice_id
//   2. sales_order_consumption ledger updated (UPSERT, increment dispatched qty + value)
async function _doRefreshSapInvoices() {
  const cfg = await sap.getConfig();
  const lookback = (cfg && cfg.invoice_poll_lookback_days) || 7;
  const r = await sap.fetchRecentInvoices({ lookbackDays: lookback });
  if (!r.ok) return { ok: false, error: r.error, degraded: r.degraded, fetched: 0, upserted: 0, serverBuild: 'v44ZL' };
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
            `SELECT id FROM invoice_requests WHERE batch_number=$1 AND status IN ('pending','sent_to_sap','pending_reconciliation') ORDER BY created_at DESC LIMIT 1`,
            [batchUdf]
          );
          if (m.rows[0]) { invReqId = m.rows[0].id; source = 'sunloc'; }
        } else {
          const m = db.prepare(`SELECT id FROM invoice_requests WHERE batch_number=? AND status IN ('pending','sent_to_sap','pending_reconciliation') ORDER BY created_at DESC LIMIT 1`).get(batchUdf);
          if (m) { invReqId = m.id; source = 'sunloc'; }
        }
      }
    } catch (e) { console.warn('[SAP] invoice match error:', e.message); }
    // v44S Issue 4: the bulk fetchRecentInvoices omits DocumentLines (B1 SL can reject $select on the
    // lines collection), so a DIRECT-SAP invoice (no linked Sunloc request) arrives with no line
    // detail — leaving PC / Size / Colour blank and Qty 0 in Report H / Generated Invoices. Pull the
    // lines once via getInvoice(DocEntry) so the existing ItemCode→PC-master derivation below can fill
    // PC / Size / Colour and the real Qty (Lakhs). Sunloc-linked invoices get these from their
    // invoice_request, so they don't need the call. Bounded: skip if already enriched (pc_code stored
    // in a prior cycle). Guarded: on any failure we fall back to the prior blank behaviour — no
    // regression and no ingestion blocking.
    const _recId = `inv_${inv.DocEntry}`;
    if (!invReqId && (!inv.DocumentLines || !inv.DocumentLines.length)) {
      let _alreadyEnriched = false;
      try {
        if (pgPool) { const e = await pgPool.query(`SELECT pc_code FROM invoices_received WHERE id=$1`, [_recId]); _alreadyEnriched = !!(e.rows[0] && e.rows[0].pc_code); }
        else { const e = db.prepare(`SELECT pc_code FROM invoices_received WHERE id=?`).get(_recId); _alreadyEnriched = !!(e && e.pc_code); }
      } catch (e) { /* treat as not enriched */ }
      if (!_alreadyEnriched) {
        try {
          const full = await sap.getInvoice(inv.DocEntry);
          if (full && full.ok && full.invoice && Array.isArray(full.invoice.DocumentLines)) inv.DocumentLines = full.invoice.DocumentLines;
        } catch (e) { console.warn('[SAP] direct-invoice line enrich failed for DocEntry', inv.DocEntry, '-', e.message); }
      }
    }
    // v41 fix: total_boxes is an INTEGER column. SAP DocumentLines.Quantity can be decimal
    // (e.g. 31.35 Lakhs), which PostgreSQL rejects for an integer column ("invalid input syntax
    // for type integer: 31.35"). Round to nearest whole unit for the box-count column.
    // v44O #3: SAP DocumentLines.Quantity is in LAKHS, not boxes. Deriving the scan-out box count
    // from it gave a wrong (or, when DocumentLines weren't fetched, zero) "Expected boxes", which
    // blocked Dispatch Out. For a Sunloc-linked invoice the authoritative physical box count and
    // Lakhs both come from the invoice_request (applied below).
    // v44S Issue 4: Quantity is Lakhs, so it must populate total_qty_lakhs — NOT total_boxes. We
    // leave totalBoxes at 0 for direct-SAP (SAP carries no Sunloc box count); the Qty column now fills
    // from the real summed Lakhs. Sunloc-linked invoices still get authoritative boxes below.
    let totalBoxes = 0;
    let totalQtyLakhs = (inv.DocumentLines || []).reduce((sum, l) => sum + (parseFloat(l.Quantity) || 0), 0);
    const docTotal = parseFloat(inv.DocTotal) || 0;
    const vatSum = parseFloat(inv.VatSum) || 0;
    const taxable = docTotal - vatSum;
    const recId = `inv_${inv.DocEntry}`;
    const payload = JSON.stringify(inv);

    // v41s Q2: derive pc_code/size/colour for the header row (= first line's values).
    // Per-line details are still available in payload_json for the detail modal and filtering.
    // Priority: (1) linked Sunloc invoice_request (definitive — dispatch manager entered these);
    // (2) PC Code master lookup via first line's ItemCode (covers direct_sap and Sunloc both,
    // since SAP carries the item but not always our Sunloc UDFs).
    let pcCode = '', size = '', colour = '', reqBoxes = null, reqQtyLakhs = null;
    try {
      if (invReqId) {
        // Use the linked request's values — most reliable.
        let reqRow;
        if (pgPool) {
          const rr = await pgPool.query(`SELECT pc_code, size, colour, boxes, qty_lakhs FROM invoice_requests WHERE id=$1`, [invReqId]);
          reqRow = rr.rows[0];
        } else {
          reqRow = db.prepare(`SELECT pc_code, size, colour, boxes, qty_lakhs FROM invoice_requests WHERE id=?`).get(invReqId);
        }
        if (reqRow) {
          pcCode = reqRow.pc_code || '';
          size   = reqRow.size   || '';
          colour = reqRow.colour || '';
          reqBoxes    = reqRow.boxes;
          reqQtyLakhs = reqRow.qty_lakhs;
        }
      }
      // Fallback: PC master lookup on first line's ItemCode (works for direct_sap and as backup).
      if ((!pcCode || !size || !colour)) {
        const firstLine = (inv.DocumentLines || [])[0];
        const itemCode = firstLine ? (firstLine.ItemCode || '') : '';
        if (itemCode) {
          let pcRow;
          if (pgPool) {
            const r = await pgPool.query(`SELECT size, code, colour FROM pc_codes WHERE code=$1 LIMIT 1`, [itemCode]);
            pcRow = r.rows[0];
          } else {
            pcRow = db.prepare(`SELECT size, code, colour FROM pc_codes WHERE code=? LIMIT 1`).get(itemCode);
          }
          if (pcRow) {
            if (!pcCode) pcCode = pcRow.code || itemCode;
            if (!size)   size   = pcRow.size || '';
            if (!colour) colour = pcRow.colour || '';
          }
          // v44Y: still missing size/colour? fall back to the shipped PC master (pc-codes-data.js).
          // The server pc_codes table (admin-saved / hand-edited) is queried above and WINS; this
          // only fills codes that were never saved server-side.
          if (!size || !colour) {
            const pm = _pcMasterLookup(itemCode);
            if (pm) {
              if (!size)   size   = pm.size   || '';
              if (!colour) colour = pm.colour || '';
            }
          }
          // No match anywhere — at least record the ItemCode so the column isn't blank.
          if (!pcCode) pcCode = itemCode;
        }
      }
    } catch (e) { console.warn('[v41s] pc/size/colour derivation failed:', e.message); }

    // v44O #3: for a Sunloc-linked invoice, the invoice_request holds the authoritative physical
    // box count and Lakhs the dispatch manager selected — use them (SAP Quantity is Lakhs, not boxes).
    if (invReqId) {
      if (reqBoxes != null && parseInt(reqBoxes) > 0) totalBoxes = parseInt(reqBoxes);
      if (reqQtyLakhs != null && parseFloat(reqQtyLakhs) > 0) totalQtyLakhs = parseFloat(reqQtyLakhs);
    }

    try {
      if (pgPool) {
        await pgPool.query(`
          INSERT INTO invoices_received (id, sap_doc_entry, sap_doc_num, sap_invoice_no,
            invoice_date, customer, card_code, po_number, batch_number, pc_code, size, colour,
            total_boxes, taxable_amount, igst_amount, total_amount, irn, source, invoice_request_id,
            fetched_at, payload_json, total_qty_lakhs)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NOW()::TEXT,$20,$21)
          ON CONFLICT (sap_doc_entry) DO UPDATE SET
            sap_doc_num=$3, sap_invoice_no=$4, invoice_date=$5, customer=$6,
            card_code=$7, po_number=$8,
            batch_number=COALESCE(NULLIF($9,''), invoices_received.batch_number),
            pc_code=COALESCE(NULLIF(EXCLUDED.pc_code,''), invoices_received.pc_code),
            size=COALESCE(NULLIF(EXCLUDED.size,''), invoices_received.size),
            colour=COALESCE(NULLIF(EXCLUDED.colour,''), invoices_received.colour),
            total_boxes=CASE WHEN $13>0 THEN $13 ELSE invoices_received.total_boxes END,
            taxable_amount=$14, igst_amount=$15, total_amount=$16, irn=$17,
            payload_json=$20, total_qty_lakhs=CASE WHEN $21>0 THEN $21 ELSE invoices_received.total_qty_lakhs END, fetched_at=NOW()::TEXT
        `, [recId, inv.DocEntry, String(inv.DocNum || ''), String(inv.DocNum || ''),
            inv.DocDate || null, inv.CardName || '', inv.CardCode || '', poUdf, batchUdf,
            pcCode, size, colour,
            totalBoxes, taxable, vatSum, docTotal, inv.U_IRN || null, source, invReqId, payload, totalQtyLakhs]);
      } else {
        db.prepare(`
          INSERT INTO invoices_received (id, sap_doc_entry, sap_doc_num, sap_invoice_no,
            invoice_date, customer, card_code, po_number, batch_number, pc_code, size, colour,
            total_boxes, taxable_amount, igst_amount, total_amount, irn, source, invoice_request_id,
            fetched_at, payload_json, total_qty_lakhs)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),?,?)
          ON CONFLICT(sap_doc_entry) DO UPDATE SET
            sap_doc_num=excluded.sap_doc_num, sap_invoice_no=excluded.sap_invoice_no,
            invoice_date=excluded.invoice_date, customer=excluded.customer,
            card_code=excluded.card_code, po_number=excluded.po_number,
            batch_number=COALESCE(NULLIF(excluded.batch_number,''), invoices_received.batch_number),
            pc_code=COALESCE(NULLIF(excluded.pc_code,''), invoices_received.pc_code),
            size=COALESCE(NULLIF(excluded.size,''), invoices_received.size),
            colour=COALESCE(NULLIF(excluded.colour,''), invoices_received.colour),
            total_boxes=CASE WHEN excluded.total_boxes>0 THEN excluded.total_boxes ELSE invoices_received.total_boxes END,
            taxable_amount=excluded.taxable_amount, igst_amount=excluded.igst_amount,
            total_amount=excluded.total_amount, irn=excluded.irn,
            payload_json=excluded.payload_json, total_qty_lakhs=CASE WHEN excluded.total_qty_lakhs>0 THEN excluded.total_qty_lakhs ELSE invoices_received.total_qty_lakhs END, fetched_at=datetime('now')
        `).run(recId, inv.DocEntry, String(inv.DocNum || ''), String(inv.DocNum || ''),
            inv.DocDate || null, inv.CardName || '', inv.CardCode || '', poUdf, batchUdf,
            pcCode, size, colour,
            totalBoxes, taxable, vatSum, docTotal, inv.U_IRN || null, source, invReqId, payload, totalQtyLakhs);
      }
      // v44ZC (v44AD): capture the real SO NUMBER from the invoice's Comments ("Based On Sales
      // Orders 237 ...") and store it separately from the invoice number (sap_doc_num=DocNum). The
      // modal can then show the true SO instead of the invoice number, and reconciliation matches on it.
      try {
        const _baseSoNum = ((inv.Comments || '').match(/Sales Orders?\s+(\d+)/i) || [])[1] || null;
        if (_baseSoNum) {
          if (pgPool) await pgPool.query(`UPDATE invoices_received SET base_so_doc_num=$1 WHERE sap_doc_entry=$2 AND (base_so_doc_num IS NULL OR base_so_doc_num='')`, [_baseSoNum, inv.DocEntry]);
          else        db.prepare(`UPDATE invoices_received SET base_so_doc_num=? WHERE sap_doc_entry=? AND (base_so_doc_num IS NULL OR base_so_doc_num='')`).run(_baseSoNum, inv.DocEntry);
        }
      } catch (e) { console.warn('[v44ZC] set invoice base_so_doc_num:', e.message); }
      if (invReqId) {
        try {
          if (pgPool) {
            await pgPool.query(
              `UPDATE invoice_requests SET status='reconciled', sap_response_doc_num=$1, sap_response_doc_entry=$2, sap_response_irn=$3, reconciled_at=NOW()::TEXT, reconciled_with_invoice_id=$4, updated_at=NOW()::TEXT WHERE id=$5`,
              [String(inv.DocNum || ''), inv.DocEntry, inv.U_IRN || null, recId, invReqId]
            );
          } else {
            db.prepare(
              `UPDATE invoice_requests SET status='reconciled', sap_response_doc_num=?, sap_response_doc_entry=?, sap_response_irn=?, reconciled_at=datetime('now'), reconciled_with_invoice_id=?, updated_at=datetime('now') WHERE id=?`
            ).run(String(inv.DocNum || ''), inv.DocEntry, inv.U_IRN || null, recId, invReqId);
          }
        } catch (e) { console.warn('[SAP] invoice_requests update error:', e.message); }
      }

      // v41 P19.3: Additional SO-based reconciliation pass.
      // For each invoice line, if it references a Sales Order (BaseType=17, BaseEntry=<SO DocEntry>),
      // try to match any remaining pending_reconciliation invoice_requests with matching sap_doc_entry.
      // Also update sales_order_consumption ledger so dispatch managers can see remaining headroom.
      try {
        const lines = inv.DocumentLines || [];
        for (const line of lines) {
          // BaseType 17 = Sales Order in SAP B1
          if (line.BaseType === 17 && line.BaseEntry) {
            const soDocEntry = parseInt(line.BaseEntry, 10);
            const lineQty   = parseFloat(line.Quantity) || 0;
            const linePrice = parseFloat(line.Price) || 0;
            const lineTotal = parseFloat(line.LineTotal) || (lineQty * linePrice);

            // Reconcile any still-pending invoice_requests for this SO (where not already matched by batch UDF)
            try {
              // v41r FIX (race fix #1): when a pending_reconciliation request matches an invoice via
              // SO BaseEntry, the request gets reconciled — but historically the invoices_received row
              // stayed at source='direct_sap', invoice_request_id=null, so it was hidden from the
              // Invoice Queue (which gates on source='sunloc' OR admin-approved). Promote the invoice
              // here too so it appears in the queue immediately. Also copy batch_number from the
              // request if the invoice's batch_number is blank (common when SAP user didn't fill the
              // U_SunlocBatch UDF).
              let pendingReqsFull;
              if (pgPool) {
                const r3 = await pgPool.query(
                  `SELECT id, qty_lakhs, batch_number FROM invoice_requests WHERE sap_doc_entry=$1 AND status='pending_reconciliation' ORDER BY created_at ASC`,
                  [soDocEntry]
                );
                pendingReqsFull = r3.rows;
              } else {
                pendingReqsFull = db.prepare(`SELECT id, qty_lakhs, batch_number FROM invoice_requests WHERE sap_doc_entry=? AND status='pending_reconciliation' ORDER BY created_at ASC`).all(soDocEntry);
              }
              // Mark all matching pending_reconciliation rows as reconciled (first-come-first-served).
              // Note: in practice these should already have been matched via batchUdf, but this is the safety net.
              for (const pr of pendingReqsFull) {
                if (pgPool) {
                  await pgPool.query(
                    `UPDATE invoice_requests SET status='reconciled', sap_response_doc_num=$1, sap_response_doc_entry=$2, reconciled_at=NOW()::TEXT, reconciled_with_invoice_id=$3, updated_at=NOW()::TEXT WHERE id=$4 AND status='pending_reconciliation'`,
                    [String(inv.DocNum || ''), inv.DocEntry, recId, pr.id]
                  );
                  // v41r: promote the invoice row so it surfaces in Invoice Queue
                  await pgPool.query(
                    `UPDATE invoices_received
                       SET source='sunloc',
                           invoice_request_id=$1,
                           batch_number=COALESCE(NULLIF(batch_number,''), $2)
                     WHERE id=$3 AND source='direct_sap'`,
                    [pr.id, pr.batch_number || null, recId]
                  );
                } else {
                  db.prepare(
                    `UPDATE invoice_requests SET status='reconciled', sap_response_doc_num=?, sap_response_doc_entry=?, reconciled_at=datetime('now'), reconciled_with_invoice_id=?, updated_at=datetime('now') WHERE id=? AND status='pending_reconciliation'`
                  ).run(String(inv.DocNum || ''), inv.DocEntry, recId, pr.id);
                  db.prepare(
                    `UPDATE invoices_received
                       SET source='sunloc',
                           invoice_request_id=?,
                           batch_number=COALESCE(NULLIF(batch_number,''), ?)
                     WHERE id=? AND source='direct_sap'`
                  ).run(pr.id, pr.batch_number || null, recId);
                }
                console.log(`[v41r] Reconciled invoice_request ${pr.id} via SO BaseEntry=${soDocEntry} + promoted invoice ${recId} direct_sap→sunloc`);
              }
            } catch (e) { console.warn('[v41 P19.3] SO reconciliation error:', e.message); }

            // Update sales_order_consumption ledger (UPSERT). Adds this invoice's contribution.
            // v41 P19.3 hardening: skip if already applied (avoids double-count across poll cycles).
            try {
              // Check soc_applied flag — if set, this invoice has already contributed to SOC ledger
              let alreadyApplied = false;
              try {
                if (pgPool) {
                  const af = await pgPool.query(`SELECT soc_applied FROM invoices_received WHERE id=$1`, [recId]);
                  alreadyApplied = !!(af.rows[0] && af.rows[0].soc_applied);
                } else {
                  const af = db.prepare(`SELECT soc_applied FROM invoices_received WHERE id=?`).get(recId);
                  alreadyApplied = !!(af && af.soc_applied);
                }
              } catch {}
              if (alreadyApplied) continue; // skip — already counted

              // First fetch original_qty from sap_indent_cache if no ledger row exists yet
              let originalQty = 0, originalValue = 0;
              try {
                if (pgPool) {
                  const ix = await pgPool.query(`SELECT total_qty, payload_json FROM sap_indent_cache WHERE sap_doc_entry=$1`, [soDocEntry]);
                  if (ix.rows[0]) {
                    originalQty = parseFloat(ix.rows[0].total_qty) || 0;
                    try {
                      const indPayload = JSON.parse(ix.rows[0].payload_json || '{}');
                      originalValue = parseFloat(indPayload.DocTotal) || 0;
                    } catch {}
                  }
                } else {
                  const ix = db.prepare(`SELECT total_qty, payload_json FROM sap_indent_cache WHERE sap_doc_entry=?`).get(soDocEntry);
                  if (ix) {
                    originalQty = parseFloat(ix.total_qty) || 0;
                    try {
                      const indPayload = JSON.parse(ix.payload_json || '{}');
                      originalValue = parseFloat(indPayload.DocTotal) || 0;
                    } catch {}
                  }
                }
              } catch {}

              if (pgPool) {
                await pgPool.query(`
                  INSERT INTO sales_order_consumption (sap_doc_entry, sap_doc_num, card_code, card_name,
                    original_qty_lakhs, original_value_inr, dispatched_qty_lakhs, dispatched_value_inr,
                    invoice_count, last_invoice_at, updated_at)
                  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, NOW()::TEXT, NOW()::TEXT)
                  ON CONFLICT (sap_doc_entry) DO UPDATE SET
                    dispatched_qty_lakhs = sales_order_consumption.dispatched_qty_lakhs + EXCLUDED.dispatched_qty_lakhs,
                    dispatched_value_inr = sales_order_consumption.dispatched_value_inr + EXCLUDED.dispatched_value_inr,
                    invoice_count = sales_order_consumption.invoice_count + 1,
                    last_invoice_at = NOW()::TEXT,
                    updated_at = NOW()::TEXT
                `, [soDocEntry, '', inv.CardCode || '', inv.CardName || '',
                    originalQty, originalValue, lineQty, lineTotal]);
              } else {
                db.prepare(`
                  INSERT INTO sales_order_consumption (sap_doc_entry, sap_doc_num, card_code, card_name,
                    original_qty_lakhs, original_value_inr, dispatched_qty_lakhs, dispatched_value_inr,
                    invoice_count, last_invoice_at, updated_at)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
                  ON CONFLICT(sap_doc_entry) DO UPDATE SET
                    dispatched_qty_lakhs = dispatched_qty_lakhs + excluded.dispatched_qty_lakhs,
                    dispatched_value_inr = dispatched_value_inr + excluded.dispatched_value_inr,
                    invoice_count = invoice_count + 1,
                    last_invoice_at = datetime('now'),
                    updated_at = datetime('now')
                `).run(soDocEntry, '', inv.CardCode || '', inv.CardName || '',
                    originalQty, originalValue, lineQty, lineTotal);
              }
              // v41 P19.3 hardening: mark this invoice as having contributed to SOC ledger.
              // Prevents the next poll cycle from incrementing dispatched_qty/value again.
              try {
                if (pgPool) {
                  await pgPool.query(`UPDATE invoices_received SET soc_applied=1 WHERE id=$1`, [recId]);
                } else {
                  db.prepare(`UPDATE invoices_received SET soc_applied=1 WHERE id=?`).run(recId);
                }
              } catch (e) { console.warn('[v41 P19.3] set soc_applied flag error:', e.message); }
            } catch (e) { console.warn('[v41 P19.3] SO consumption ledger update error:', e.message); }
          }
        }
      } catch (e) { console.warn('[v41 P19.3] SO reconciliation pass error:', e.message); }
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

  // v44N RETRY PASS: For invoices already stored in invoices_received (fetched before the
  // DocumentLines fix) that still have no invoice_request_id, try to match them against
  // pending_reconciliation requests via the Comments field ("Based On Sales Orders <SO_DocEntry>").
  // This ensures old unreconciled invoices self-heal without manual DB intervention.
  try {
    if (pgPool) {
      const unmatched = await pgPool.query(
        `SELECT iv.id, iv.sap_doc_entry, iv.sap_invoice_no, iv.total_boxes, iv.total_qty_lakhs, iv.payload_json
         FROM invoices_received iv
         WHERE iv.invoice_request_id IS NULL AND iv.source = 'direct_sap'`
      );
      for (const iv of unmatched.rows) {
        try {
          const payload = typeof iv.payload_json === 'string' ? JSON.parse(iv.payload_json) : (iv.payload_json || {});
          const comments = payload.Comments || '';
          // Extract all SO numbers from Comments e.g. "Based On Sales Orders 245. Based On Deliveries 575."
          const soMatches = comments.match(/Sales Orders?\s+(\d+)/gi) || [];
          for (const soMatch of soMatches) {
            const soNum = parseInt(soMatch.replace(/\D/g, ''), 10);
            if (!soNum) continue;
            // Find the SO DocEntry from sap_indent_cache by DocNum
            const soRow = await pgPool.query(
              `SELECT sap_doc_entry FROM sap_indent_cache WHERE sap_doc_num=$1`,
              [String(soNum)]
            );
            // v44P (#1 hardening): SAP's "Based On Sales Orders <n>" text can carry either the SO
            // DocNum or the SO DocEntry depending on configuration. If the DocNum lookup misses,
            // fall back to treating the number as the DocEntry directly (the request stores the SO
            // DocEntry in sap_doc_entry), so reconciliation can't silently fail on that ambiguity.
            const soDocEntry = soRow.rows[0] ? soRow.rows[0].sap_doc_entry : soNum;
            if (!soDocEntry) continue;
            // Find pending request by SO DocEntry OR — v44ZC (v44AD) — by SO NUMBER directly
            // (so_doc_num), the cache-independent key. The DocEntry path needs the indent cache to
            // resolve the SO number, but the cache prunes completed SOs, so it fails at reconcile
            // time; the so_doc_num match (recorded on the request at creation) reconciles regardless.
            const req = await pgPool.query(
              `SELECT id, batch_number, boxes, qty_lakhs FROM invoice_requests WHERE (sap_doc_entry=$1 OR so_doc_num=$2) AND status='pending_reconciliation' ORDER BY created_at ASC`,
              [soDocEntry, String(soNum)]
            );
            for (const pr of req.rows) {
              const recId = `inv_${iv.sap_doc_entry}`;
              const boxes = (pr.boxes && parseInt(pr.boxes) > 0) ? parseInt(pr.boxes) : (iv.total_boxes || 0);
              const qty   = (pr.qty_lakhs && parseFloat(pr.qty_lakhs) > 0) ? parseFloat(pr.qty_lakhs) : (iv.total_qty_lakhs || 0);
              await pgPool.query(
                `UPDATE invoice_requests SET status='reconciled', sap_response_doc_num=$1, sap_response_doc_entry=$2, reconciled_at=NOW()::TEXT, reconciled_with_invoice_id=$3, updated_at=NOW()::TEXT WHERE id=$4 AND status='pending_reconciliation'`,
                [String(iv.sap_invoice_no || ''), iv.sap_doc_entry, recId, pr.id]
              );
              await pgPool.query(
                `UPDATE invoices_received SET source='sunloc', invoice_request_id=$1, batch_number=COALESCE(NULLIF(batch_number,''),$2), total_boxes=$3, total_qty_lakhs=$4 WHERE sap_doc_entry=$5`,
                [pr.id, pr.batch_number || null, boxes, qty, iv.sap_doc_entry]
              );
              console.log(`[SAP] v44N retry-reconciled: batch=${pr.batch_number} inv=${iv.sap_invoice_no} via Comments SO#${soNum}`);
            }
          }
        } catch (e) { console.warn('[SAP] v44N retry pass error:', e.message); }
      }
    }
  } catch (e) { console.warn('[SAP] v44N retry pass outer error:', e.message); }

  // v44P (#2): LINE-DETAIL ENRICHMENT. The bulk invoice fetch is lean (no DocumentLines), so
  // direct-SAP invoices land with blank PC Code / Size / Colour and zero Qty — invisible in the
  // Generated Invoices + Report H tables and unsearchable by those filters. Here we fetch the
  // FULL invoice once per row that is still missing PC Code (bounded per cycle), derive
  // pc_code/size/colour from the first line's ItemCode via the pc_codes master, and qty (Lakhs)
  // from the line quantities, then store them (and the full payload, so the per-line detail modal
  // and line-count hint work). Values persist via the COALESCE/CASE upsert above, so each invoice
  // is enriched at most once; the backlog drains over a few cycles then steady-state is ~0.
  try {
    if (pgPool) {
      const need = await pgPool.query(
        `SELECT id, sap_doc_entry FROM invoices_received
         WHERE (pc_code IS NULL OR pc_code='')
         ORDER BY fetched_at DESC NULLS LAST LIMIT 40`
      );
      for (const row of need.rows) {
        try {
          const full = await sap.getInvoice(row.sap_doc_entry);
          if (!full.ok || !full.invoice) continue;
          const lines = full.invoice.DocumentLines || [];
          if (!lines.length) continue;
          let pc = '', sz = '', col = '';
          const itemCode = lines[0].ItemCode || '';
          if (itemCode) {
            const pcRow = (await pgPool.query(`SELECT code, size, colour FROM pc_codes WHERE code=$1 LIMIT 1`, [itemCode])).rows[0];
            if (pcRow) { pc = pcRow.code || itemCode; sz = pcRow.size || ''; col = pcRow.colour || ''; }
            // v44Y: fall back to the shipped PC master for size/colour when the server table lacks them.
            if (!sz || !col) {
              const pm = _pcMasterLookup(itemCode);
              if (pm) { if (!sz) sz = pm.size || ''; if (!col) col = pm.colour || ''; }
            }
            if (!pc) pc = itemCode;
          }
          const qtyL = lines.reduce((s, l) => s + (parseFloat(l.Quantity) || 0), 0);
          await pgPool.query(
            `UPDATE invoices_received SET
               pc_code=COALESCE(NULLIF($1,''), pc_code),
               size=COALESCE(NULLIF($2,''), size),
               colour=COALESCE(NULLIF($3,''), colour),
               total_qty_lakhs=CASE WHEN $4::numeric>0 THEN $4 ELSE total_qty_lakhs END
             WHERE id=$5`,
            [pc, sz, col, qtyL, row.id]
          );
          console.log(`[SAP] v44P enriched line-detail for inv DocEntry=${row.sap_doc_entry} (pc=${pc||'?'})`);
        } catch (e) { /* per-row: skip and continue */ }
      }
    }
  } catch (e) { console.warn('[SAP] v44P line-enrich pass error:', e.message); }

  return { ok: true, fetched: invoices.length, upserted, serverBuild: 'v44ZL' };
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
// v41f (issue 1 diagnostic): dump the raw field names + sample values from cached SAP indent
// lines so we can identify exactly which field holds the printing matter. Admin only. Returns,
// per cached indent, the full set of keys present on its first DocumentLine and the U_* (UDF)
// fields with their values — without exposing the entire payload. Use: GET /api/sap/indent-fields
app.get('/api/sap/indent-fields', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    // v41m: optional ?docNum=199 to inspect ONE specific indent (e.g. a missing order). Without it,
    // returns the 20 most-recent cached indents' field shapes (for print-matter/currency discovery).
    const wantDocNum = (req.query.docNum || '').toString().trim();
    let rows;
    if (pgPool) {
      rows = wantDocNum
        ? (await pgPool.query(`SELECT sap_doc_num, sap_doc_entry, card_name, fetched_at, payload_json FROM sap_indent_cache WHERE sap_doc_num=$1`, [wantDocNum])).rows
        : (await pgPool.query(`SELECT sap_doc_num, sap_doc_entry, card_name, fetched_at, payload_json FROM sap_indent_cache ORDER BY fetched_at DESC LIMIT 20`)).rows;
    } else {
      rows = wantDocNum
        ? db.prepare(`SELECT sap_doc_num, sap_doc_entry, card_name, fetched_at, payload_json FROM sap_indent_cache WHERE sap_doc_num=?`).all(wantDocNum)
        : db.prepare(`SELECT sap_doc_num, sap_doc_entry, card_name, fetched_at, payload_json FROM sap_indent_cache ORDER BY fetched_at DESC LIMIT 20`).all();
    }
    if (wantDocNum && rows.length === 0) {
      return res.json({ ok: true, docNum: wantDocNum, inCache: false,
        note: `Indent ${wantDocNum} is NOT in the SAP cache — it was not returned by the SAP fetch (check DocumentStatus is open and it matches the fetch filter). Run a Force Refresh, then retry.` });
    }
    const out = [];
    for (const row of rows) {
      let payload = null;
      try { payload = JSON.parse(row.payload_json); } catch { continue; }
      const docLines = payload?.DocumentLines || [];
      const line = docLines[0];
      const allKeys = line ? Object.keys(line) : [];
      const udfFields = {};
      for (const k of allKeys) { if (k.startsWith('U_')) udfFields[k] = line[k]; }
      out.push({
        docNum: row.sap_doc_num,
        docEntry: row.sap_doc_entry,
        cardName: row.card_name,
        fetchedAt: row.fetched_at,
        docCurrency: payload?.DocCurrency || null,
        documentStatus: payload?.DocumentStatus || payload?.DocStatus || null,
        lineCount: docLines.length,
        // v41m: per-line open/qty so we can see if reconcile drops a line for qty<=0.
        linesQty: docLines.map(l => ({
          itemCode: l.ItemCode, lineStatus: l.LineStatus,
          quantity: l.Quantity, remainingOpenQuantity: l.RemainingOpenQuantity,
          // v41o: expose every UoM-like field so we can confirm the export(THOUSAND)/domestic(LAC) signal
          UoMCode: l.UoMCode, UoMEntry: l.UoMEntry,
          UnitsOfMeasurement: l.UnitsOfMeasurement, MeasureUnit: l.MeasureUnit,
          // v41o: per-line print matter (the real SAP field is U_PRNT_MATTR)
          U_PRNT_MATTR: l.U_PRNT_MATTR, U_ITEM_DES: l.U_ITEM_DES
        })),
        lineKeys: allKeys,
        udfFields,
        itemDescription: line?.ItemDescription || null,
        freeText: line?.FreeText || null,
        text: line?.Text || null,
      });
    }
    res.json({ ok: true, inCache: true, count: out.length, lines: out });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/sap/indents', async (req, res) => {
  try {
    // v41z FIX: Always return ALL indents (processed and unprocessed).
    // The client-side _v39_reconcileSapIndents calculates remaining unplanned qty per line
    // and only shows lines where unplannedQty > 0. Hiding processed indents was causing
    // old open SAP orders (PO 191, 199, 166, 200 etc.) to not appear in Unplanned Orders.
    let rows;
    if (pgPool) {
      const r = await pgPool.query(
        `SELECT * FROM sap_indent_cache ORDER BY doc_due_date ASC NULLS LAST, fetched_at DESC LIMIT 5000`
      );
      rows = r.rows;
    } else {
      rows = db.prepare(
        `SELECT * FROM sap_indent_cache ORDER BY doc_due_date ASC, fetched_at DESC LIMIT 5000`
      ).all();
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
        DocCurrency: payload?.DocCurrency || null, // v41e: authoritative export signal (non-INR = export)
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

// ─────────────────────────────────────────────────────────────────
// v41 P19.2 Fix 6G: Dismiss / un-dismiss unplanned SAP indent lines
// Lets admin hide indent lines that will NOT be planned in Sunloc
// (legacy stock, cancelled-in-SAP-but-not-closed, etc.).
// Composite key: (sap_doc_entry, line_num) so a multi-line SO can be
// partially dismissed.
// ─────────────────────────────────────────────────────────────────

app.post('/api/sap/dismiss-indent-line', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const { sapDocEntry, lineNum, sapDocNum, cardCode, cardName, itemCode, reason } = req.body || {};
    if (sapDocEntry == null || lineNum == null) {
      return res.status(400).json({ ok: false, error: 'sapDocEntry and lineNum required' });
    }
    const session = verifyToken(req.headers['x-session-token'] || req.body.token);
    const username = session?.username || 'admin';
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO dismissed_sap_indents (sap_doc_entry, line_num, sap_doc_num, card_code, card_name, item_code, dismissed_by, reason)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (sap_doc_entry, line_num) DO UPDATE SET
           dismissed_by = EXCLUDED.dismissed_by,
           dismissed_at = NOW()::TEXT,
           reason = EXCLUDED.reason`,
        [parseInt(sapDocEntry,10), parseInt(lineNum,10), sapDocNum || '', cardCode || '', cardName || '', itemCode || '', username, reason || '']
      );
    } else {
      db.prepare(
        `INSERT INTO dismissed_sap_indents (sap_doc_entry, line_num, sap_doc_num, card_code, card_name, item_code, dismissed_by, reason)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (sap_doc_entry, line_num) DO UPDATE SET
           dismissed_by = excluded.dismissed_by,
           dismissed_at = datetime('now'),
           reason = excluded.reason`
      ).run(parseInt(sapDocEntry,10), parseInt(lineNum,10), sapDocNum || '', cardCode || '', cardName || '', itemCode || '', username, reason || '');
    }
    logAudit(username, session?.role || 'admin', session?.app || 'planning', 'SAP_INDENT_DISMISSED',
      `Dismissed SAP indent line ${sapDocNum}/L${lineNum}: ${reason||'(no reason)'}`, req.ip);
    res.json({ ok: true });
  } catch (err) {
    console.error('[v41 P19.2] dismiss-indent-line failed:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/sap/undismiss-indent-line', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  try {
    const { sapDocEntry, lineNum } = req.body || {};
    if (sapDocEntry == null || lineNum == null) {
      return res.status(400).json({ ok: false, error: 'sapDocEntry and lineNum required' });
    }
    const session = verifyToken(req.headers['x-session-token'] || req.body.token);
    if (pgPool) {
      await pgPool.query(`DELETE FROM dismissed_sap_indents WHERE sap_doc_entry=$1 AND line_num=$2`,
        [parseInt(sapDocEntry,10), parseInt(lineNum,10)]);
    } else {
      db.prepare(`DELETE FROM dismissed_sap_indents WHERE sap_doc_entry=? AND line_num=?`)
        .run(parseInt(sapDocEntry,10), parseInt(lineNum,10));
    }
    logAudit(session?.username || 'admin', session?.role || 'admin', session?.app || 'planning',
      'SAP_INDENT_UNDISMISSED', `Un-dismissed SAP indent line entry=${sapDocEntry} L${lineNum}`, req.ip);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/sap/dismissed-indents', async (req, res) => {
  try {
    const session = verifyToken(req.headers['x-session-token'] || req.query.token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    let rows = [];
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM dismissed_sap_indents ORDER BY dismissed_at DESC`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM dismissed_sap_indents ORDER BY dismissed_at DESC`).all();
    }
    res.json({ ok: true, dismissed: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ─── end v41 P19.2 Fix 6G ────────────────────────────────────────


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
    // v41 P19.3: Invoice flow rework — NO SAP push.
    // Sunloc no longer creates Deliveries in SAP. Instead, this endpoint:
    //   1. Checks 115% over-dispatch tolerance against SO consumption ledger
    //   2. Inserts an invoice_requests row at status='pending_reconciliation'
    //   3. Returns to client — SAP user manually creates the invoice in SAP
    //   4. Sunloc poller pulls the resulting invoice → matches by Sales Order ref → reconciles
    //
    // 115% tolerance: hard ceiling. Beyond 115% requires admin override via body.adminOverride.
    // SAP user's manual invoice creation workflow remains unchanged.
    let overdispatchReason = null;
    try {
      let soc;
      if (pgPool) {
        const r = await pgPool.query(`SELECT * FROM sales_order_consumption WHERE sap_doc_entry=$1`, [parseInt(body.sapDocEntry,10)]);
        soc = r.rows[0];
      } else {
        soc = db.prepare(`SELECT * FROM sales_order_consumption WHERE sap_doc_entry=?`).get(parseInt(body.sapDocEntry,10));
      }
      if (soc && soc.original_qty_lakhs > 0) {
        const wouldDispatch = (parseFloat(soc.dispatched_qty_lakhs) || 0) + (parseFloat(body.qtyLakhs) || 0);
        const tolerance = parseFloat(soc.original_qty_lakhs) * 1.15;
        if (wouldDispatch > tolerance) {
          overdispatchReason = `Cumulative dispatch ${wouldDispatch.toFixed(3)}L would exceed 115% tolerance (${tolerance.toFixed(3)}L) of original SO qty ${parseFloat(soc.original_qty_lakhs).toFixed(3)}L.`;
          if (!body.adminOverride) {
            return res.status(409).json({
              ok: false,
              error: 'over_dispatch_blocked',
              message: overdispatchReason + ' Admin override required.',
              soc: {
                originalQty: parseFloat(soc.original_qty_lakhs),
                dispatchedQty: parseFloat(soc.dispatched_qty_lakhs),
                tolerance: tolerance,
                wouldDispatch: wouldDispatch
              }
            });
          }
        }
      }
    } catch (e) {
      console.warn('[v41 P19.3] SOC tolerance check failed:', e.message);
    }

    const id = 'invreq_' + crypto.randomBytes(8).toString('hex');
    const selectedLabelsJson = JSON.stringify(body.selectedLabels || []);

    // v41r FIX #2: SAP-first / Sunloc-late race. If the SAP user already created the invoice for
    // this Sales Order BEFORE the dispatch manager clicked Generate Invoice, the poller has
    // already pulled it as source='direct_sap' (because no matching pending request existed at
    // poll time). Without this block, we'd create a fresh pending_reconciliation row that may
    // never auto-link, and the direct_sap invoice would stay hidden from the Invoice Queue.
    // Look ahead: if a direct_sap invoice exists for this SO (not yet dispatched, not legacy-
    // closed, not already linked), promote it now AND write our request as already reconciled.
    // The dispatch manager's batch immediately appears in the Invoice Queue for Scan Out.
    let preLinkedInvoiceRow = null;
    try {
      const soDocEntry = parseInt(body.sapDocEntry, 10);
      if (soDocEntry) {
        const findSql = `SELECT id, sap_doc_num, sap_doc_entry, batch_number FROM invoices_received
          WHERE source='direct_sap' AND invoice_request_id IS NULL
            AND dispatch_status='pending' AND COALESCE(is_legacy_closed,0)=0
            AND id IN (
              SELECT DISTINCT ir.id FROM invoices_received ir
              WHERE ir.source='direct_sap' AND ir.invoice_request_id IS NULL
            )`;
        // We need to match by Sales Order BaseEntry inside DocumentLines (payload_json).
        // Postgres + SQLite both support JSON extraction; to keep this portable, fetch
        // candidate direct_sap invoices and inspect their payloads in JS.
        let candidates;
        if (pgPool) {
          const r = await pgPool.query(
            `SELECT id, sap_doc_num, sap_doc_entry, batch_number, payload_json
               FROM invoices_received
              WHERE source='direct_sap' AND invoice_request_id IS NULL
                AND dispatch_status='pending' AND COALESCE(is_legacy_closed,0)=0
              ORDER BY fetched_at DESC LIMIT 200`
          );
          candidates = r.rows;
        } else {
          candidates = db.prepare(
            `SELECT id, sap_doc_num, sap_doc_entry, batch_number, payload_json
               FROM invoices_received
              WHERE source='direct_sap' AND invoice_request_id IS NULL
                AND dispatch_status='pending' AND COALESCE(is_legacy_closed,0)=0
              ORDER BY fetched_at DESC LIMIT 200`
          ).all();
        }
        for (const cand of (candidates || [])) {
          try {
            const payload = JSON.parse(cand.payload_json || '{}');
            const lines = payload.DocumentLines || [];
            const matches = lines.some(L => L.BaseType === 17 && parseInt(L.BaseEntry, 10) === soDocEntry);
            if (matches) { preLinkedInvoiceRow = cand; break; }
          } catch {} // skip un-parseable rows
        }
      }
    } catch (e) { console.warn('[v41r pre-link lookback] failed:', e.message); }

    const initialStatus = preLinkedInvoiceRow ? 'reconciled' : 'pending_reconciliation';
    try {
      if (pgPool) {
        await pgPool.query(`
          INSERT INTO invoice_requests (id, batch_number, customer, card_code, po_number,
            sap_doc_entry, size, colour, pc_code, boxes, qty_lakhs, rate_per_lakh,
            selected_labels, selection_mode, truck_number, status, created_by,
            is_overdispatch_approved, overdispatch_approved_by, overdispatch_approved_at,
            sap_response_doc_num, sap_response_doc_entry, reconciled_at, reconciled_with_invoice_id)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
        `, [id, body.batchNumber, body.customer, body.cardCode || '', body.poNumber || '',
            body.sapDocEntry, body.size || '', body.colour || '', body.pcCode || '',
            parseInt(body.boxes) || 0, parseFloat(body.qtyLakhs) || 0, parseFloat(body.ratePerLakh) || 0,
            selectedLabelsJson, body.selectionMode || 'batch', body.truckNumber || null,
            initialStatus, body.createdBy || 'unknown',
            overdispatchReason ? 1 : 0,
            overdispatchReason ? (body.createdBy || 'admin') : null,
            overdispatchReason ? new Date().toISOString() : null,
            preLinkedInvoiceRow ? (preLinkedInvoiceRow.sap_doc_num || '') : null,
            preLinkedInvoiceRow ? preLinkedInvoiceRow.sap_doc_entry : null,
            preLinkedInvoiceRow ? new Date().toISOString() : null,
            preLinkedInvoiceRow ? preLinkedInvoiceRow.id : null]);
      } else {
        db.prepare(`
          INSERT INTO invoice_requests (id, batch_number, customer, card_code, po_number,
            sap_doc_entry, size, colour, pc_code, boxes, qty_lakhs, rate_per_lakh,
            selected_labels, selection_mode, truck_number, status, created_by,
            is_overdispatch_approved, overdispatch_approved_by, overdispatch_approved_at,
            sap_response_doc_num, sap_response_doc_entry, reconciled_at, reconciled_with_invoice_id)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        `).run(id, body.batchNumber, body.customer, body.cardCode || '', body.poNumber || '',
            body.sapDocEntry, body.size || '', body.colour || '', body.pcCode || '',
            parseInt(body.boxes) || 0, parseFloat(body.qtyLakhs) || 0, parseFloat(body.ratePerLakh) || 0,
            selectedLabelsJson, body.selectionMode || 'batch', body.truckNumber || null,
            initialStatus, body.createdBy || 'unknown',
            overdispatchReason ? 1 : 0,
            overdispatchReason ? (body.createdBy || 'admin') : null,
            overdispatchReason ? new Date().toISOString() : null,
            preLinkedInvoiceRow ? (preLinkedInvoiceRow.sap_doc_num || '') : null,
            preLinkedInvoiceRow ? preLinkedInvoiceRow.sap_doc_entry : null,
            preLinkedInvoiceRow ? new Date().toISOString() : null,
            preLinkedInvoiceRow ? preLinkedInvoiceRow.id : null);
      }
      // v44ZC (v44AD): record the SO NUMBER on the request (see request-batch handler).
      try {
        const _soNum = (body.sapDocNum && String(body.sapDocNum).trim()) || (body.poNumber && String(body.poNumber).trim()) || null;
        if (_soNum) {
          if (pgPool) await pgPool.query(`UPDATE invoice_requests SET so_doc_num=$1 WHERE id=$2`, [_soNum, id]);
          else        db.prepare(`UPDATE invoice_requests SET so_doc_num=? WHERE id=?`).run(_soNum, id);
        }
      } catch (e) { console.warn('[v44ZC] set request so_doc_num:', e.message); }
      // v41r: if we pre-linked, promote the invoice row in the same logical operation.
      if (preLinkedInvoiceRow) {
        try {
          if (pgPool) {
            await pgPool.query(
              `UPDATE invoices_received
                  SET source='sunloc', invoice_request_id=$1,
                      batch_number=COALESCE(NULLIF(batch_number,''), $2)
                WHERE id=$3 AND source='direct_sap' AND invoice_request_id IS NULL`,
              [id, body.batchNumber || null, preLinkedInvoiceRow.id]
            );
          } else {
            db.prepare(
              `UPDATE invoices_received
                  SET source='sunloc', invoice_request_id=?,
                      batch_number=COALESCE(NULLIF(batch_number,''), ?)
                WHERE id=? AND source='direct_sap' AND invoice_request_id IS NULL`
            ).run(id, body.batchNumber || null, preLinkedInvoiceRow.id);
          }
          try { logAudit(body.createdBy || 'unknown', 'planning', 'invoice', 'INVOICE_PRE_LINK',
            `Pre-linked existing SAP invoice ${preLinkedInvoiceRow.sap_doc_num} (id ${preLinkedInvoiceRow.id}) to new request ${id} for batch ${body.batchNumber} — SAP-first race resolved at generate time`); } catch {}
          console.log(`[v41r] Pre-linked direct_sap invoice ${preLinkedInvoiceRow.id} to new request ${id} (batch ${body.batchNumber})`);
        } catch (e) { console.warn('[v41r] pre-link invoice promote failed:', e.message); }
      }
    } catch (e) {
      return res.status(500).json({ ok: false, error: 'Failed to write invoice_requests row: ' + e.message });
    }
    return res.json({
      ok: true,
      request_id: id,
      status: initialStatus,
      preLinked: !!preLinkedInvoiceRow,
      preLinkedInvoice: preLinkedInvoiceRow ? {
        id: preLinkedInvoiceRow.id,
        sap_doc_num: preLinkedInvoiceRow.sap_doc_num,
        sap_doc_entry: preLinkedInvoiceRow.sap_doc_entry
      } : null,
      message: preLinkedInvoiceRow
        ? `SAP invoice #${preLinkedInvoiceRow.sap_doc_num} already exists for this Sales Order — linked immediately. Batch now appears in the Invoice Queue for Scan Out.`
        : `Invoice request registered. SAP user should create the corresponding invoice in SAP; Sunloc will reconcile when the invoice is pulled by the next poll cycle (every ~5 min).`,
      overdispatchApproved: !!overdispatchReason
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// v40 Phase 18.1 + v41 P19.3: POST /api/invoice/request-batch
// Consolidated multi-batch invoice-request creation. Accepts array of batches.
// v41 change: no longer pushes Deliveries to SAP — just creates pending_reconciliation
// rows in invoice_requests. SAP user creates the actual invoices manually in SAP;
// Sunloc's 5-min poller reconciles each one when it appears.
// Returns per-batch results so the client can show per-row success/failure in the
// consolidated approval modal.
// Server-side validation re-enforces eligibility gates (defense in depth):
//   - Must have sapDocEntry + sapDocNum
//   - Must have boxes > 0 AND qtyLakhs > 0
//   - Must not already have a pending/in-flight invoice request
//   - Must respect 115% over-dispatch tolerance (admin override available)
// Body: { batches: [{ batchNumber, customer, cardCode, poNumber, sapDocEntry, size, colour, pcCode, boxes, qtyLakhs, truckNumber, itemCode? }], createdBy, remarks, adminOverride? }
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
        // Validate. v44Z: accept a Sales Order NUMBER (sapDocNum) when the internal DocEntry is
        // absent — many orders are linked by SO number only. The DocEntry is resolved below.
        if (!b.batchNumber || !b.customer || (!b.sapDocEntry && !b.sapDocNum)) {
          batchRes.error = 'missing required fields (need batchNumber, customer, and a Sales Order DocEntry or Number)';
          results.push(batchRes); continue;
        }
        if (!(parseInt(b.boxes) > 0) || !(parseFloat(b.qtyLakhs) > 0)) {
          batchRes.error = 'invalid boxes or qty (both must be > 0)';
          results.push(batchRes); continue;
        }
        // v44Z: resolve the SAP Sales Order DocEntry from the SO Number via the indent cache when
        // only the number was supplied (order linked by SO number, not by internal DocEntry). The
        // poller reconciles by DocEntry, so it is required; if the SO isn't cached, say so plainly
        // instead of failing silently.
        if (!b.sapDocEntry && b.sapDocNum) {
          let cached;
          if (pgPool) {
            const r = await pgPool.query(`SELECT sap_doc_entry FROM sap_indent_cache WHERE sap_doc_num=$1 LIMIT 1`, [String(b.sapDocNum).trim()]);
            cached = r.rows[0];
          } else {
            cached = db.prepare(`SELECT sap_doc_entry FROM sap_indent_cache WHERE sap_doc_num=? LIMIT 1`).get(String(b.sapDocNum).trim());
          }
          if (cached && cached.sap_doc_entry != null) {
            b.sapDocEntry = cached.sap_doc_entry;
          } else {
            batchRes.error = 'Sales Order ' + b.sapDocNum + ' not found in SAP indent cache — re-pull indents (SAP button) then retry';
            results.push(batchRes); continue;
          }
        }
        // v44Z (Ishan): the in-flight duplicate guard is REMOVED. Multiple in-flight invoices for one
        // batch are permitted — e.g. four 5L invoices on a 20L batch while three await SAP. The only
        // gate that remains is the 115%-of-SO over-dispatch tolerance checked below.
        // v41 P19.3: Check 115% over-dispatch tolerance for this batch
        let overdispatchReason = null;
        try {
          let soc;
          if (pgPool) {
            const r = await pgPool.query(`SELECT * FROM sales_order_consumption WHERE sap_doc_entry=$1`, [parseInt(b.sapDocEntry,10)]);
            soc = r.rows[0];
          } else {
            soc = db.prepare(`SELECT * FROM sales_order_consumption WHERE sap_doc_entry=?`).get(parseInt(b.sapDocEntry,10));
          }
          if (soc && soc.original_qty_lakhs > 0) {
            const wouldDispatch = (parseFloat(soc.dispatched_qty_lakhs) || 0) + (parseFloat(b.qtyLakhs) || 0);
            const tolerance = parseFloat(soc.original_qty_lakhs) * 1.15;
            if (wouldDispatch > tolerance) {
              overdispatchReason = `${wouldDispatch.toFixed(3)}L would exceed 115% tolerance (${tolerance.toFixed(3)}L) of SO original ${parseFloat(soc.original_qty_lakhs).toFixed(3)}L.`;
              if (!body.adminOverride) {
                batchRes.error = 'over_dispatch_blocked: ' + overdispatchReason + ' Admin override required.';
                results.push(batchRes); continue;
              }
            }
          }
        } catch (e) {
          console.warn('[v41 P19.3 batch] SOC tolerance check failed:', e.message);
        }

        // v41 P19.3: Insert pending_reconciliation row — NO SAP push.
        // SAP user creates the invoice manually in SAP; Sunloc poller reconciles.
        const id = 'invreq_' + crypto.randomBytes(8).toString('hex');
        // v41r FIX #2 (consolidated path): same SAP-first race handling as /api/invoice/request.
        // Look for an existing direct_sap invoice matching this batch's Sales Order; if found,
        // insert the request as already-reconciled and promote the invoice in one step.
        let preLinkedInv = null;
        try {
          const soDocEntry = parseInt(b.sapDocEntry, 10);
          if (soDocEntry) {
            let cands;
            if (pgPool) {
              const r = await pgPool.query(
                `SELECT id, sap_doc_num, sap_doc_entry, batch_number, payload_json
                   FROM invoices_received
                  WHERE source='direct_sap' AND invoice_request_id IS NULL
                    AND dispatch_status='pending' AND COALESCE(is_legacy_closed,0)=0
                  ORDER BY fetched_at DESC LIMIT 200`
              );
              cands = r.rows;
            } else {
              cands = db.prepare(
                `SELECT id, sap_doc_num, sap_doc_entry, batch_number, payload_json
                   FROM invoices_received
                  WHERE source='direct_sap' AND invoice_request_id IS NULL
                    AND dispatch_status='pending' AND COALESCE(is_legacy_closed,0)=0
                  ORDER BY fetched_at DESC LIMIT 200`
              ).all();
            }
            for (const c of (cands || [])) {
              try {
                const p = JSON.parse(c.payload_json || '{}');
                const ll = p.DocumentLines || [];
                if (ll.some(L => L.BaseType === 17 && parseInt(L.BaseEntry, 10) === soDocEntry)) {
                  preLinkedInv = c; break;
                }
              } catch {}
            }
          }
        } catch (e) { console.warn('[v41r batch pre-link] failed:', e.message); }

        const initStatusB = preLinkedInv ? 'reconciled' : 'pending_reconciliation';
        const selectedLabelsJson = JSON.stringify(b.selectedLabels || []);
        if (pgPool) {
          await pgPool.query(`
            INSERT INTO invoice_requests (id, batch_number, customer, card_code, po_number,
              sap_doc_entry, size, colour, pc_code, boxes, qty_lakhs, rate_per_lakh,
              selected_labels, selection_mode, truck_number, status, created_by,
              is_overdispatch_approved, overdispatch_approved_by, overdispatch_approved_at,
              sap_response_doc_num, sap_response_doc_entry, reconciled_at, reconciled_with_invoice_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
          `, [id, b.batchNumber, b.customer, b.cardCode || '', b.poNumber || '',
              b.sapDocEntry, b.size || '', b.colour || '', b.pcCode || '',
              parseInt(b.boxes) || 0, parseFloat(b.qtyLakhs) || 0, parseFloat(b.ratePerLakh) || 0,
              selectedLabelsJson, 'consolidated', b.truckNumber || null,
              initStatusB, body.createdBy || 'unknown',
              overdispatchReason ? 1 : 0,
              overdispatchReason ? (body.createdBy || 'admin') : null,
              overdispatchReason ? new Date().toISOString() : null,
              preLinkedInv ? (preLinkedInv.sap_doc_num || '') : null,
              preLinkedInv ? preLinkedInv.sap_doc_entry : null,
              preLinkedInv ? new Date().toISOString() : null,
              preLinkedInv ? preLinkedInv.id : null]);
        } else {
          db.prepare(`
            INSERT INTO invoice_requests (id, batch_number, customer, card_code, po_number,
              sap_doc_entry, size, colour, pc_code, boxes, qty_lakhs, rate_per_lakh,
              selected_labels, selection_mode, truck_number, status, created_by,
              is_overdispatch_approved, overdispatch_approved_by, overdispatch_approved_at,
              sap_response_doc_num, sap_response_doc_entry, reconciled_at, reconciled_with_invoice_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          `).run(id, b.batchNumber, b.customer, b.cardCode || '', b.poNumber || '',
              b.sapDocEntry, b.size || '', b.colour || '', b.pcCode || '',
              parseInt(b.boxes) || 0, parseFloat(b.qtyLakhs) || 0, parseFloat(b.ratePerLakh) || 0,
              selectedLabelsJson, 'consolidated', b.truckNumber || null,
              initStatusB, body.createdBy || 'unknown',
              overdispatchReason ? 1 : 0,
              overdispatchReason ? (body.createdBy || 'admin') : null,
              overdispatchReason ? new Date().toISOString() : null,
              preLinkedInv ? (preLinkedInv.sap_doc_num || '') : null,
              preLinkedInv ? preLinkedInv.sap_doc_entry : null,
              preLinkedInv ? new Date().toISOString() : null,
              preLinkedInv ? preLinkedInv.id : null);
        }
        // v44ZC (v44AD): record the SO NUMBER on the request so the poller can reconcile the
        // returning invoice number-to-number against its Comments, with no indent-cache dependence.
        try {
          const _soNum = (b.sapDocNum && String(b.sapDocNum).trim()) || (b.poNumber && String(b.poNumber).trim()) || null;
          if (_soNum) {
            if (pgPool) await pgPool.query(`UPDATE invoice_requests SET so_doc_num=$1 WHERE id=$2`, [_soNum, id]);
            else        db.prepare(`UPDATE invoice_requests SET so_doc_num=? WHERE id=?`).run(_soNum, id);
          }
        } catch (e) { console.warn('[v44ZC] set request so_doc_num:', e.message); }
        // v41r: promote the invoice if we pre-linked.
        if (preLinkedInv) {
          try {
            if (pgPool) {
              await pgPool.query(
                `UPDATE invoices_received
                    SET source='sunloc', invoice_request_id=$1,
                        batch_number=COALESCE(NULLIF(batch_number,''), $2)
                  WHERE id=$3 AND source='direct_sap' AND invoice_request_id IS NULL`,
                [id, b.batchNumber || null, preLinkedInv.id]
              );
            } else {
              db.prepare(
                `UPDATE invoices_received
                    SET source='sunloc', invoice_request_id=?,
                        batch_number=COALESCE(NULLIF(batch_number,''), ?)
                  WHERE id=? AND source='direct_sap' AND invoice_request_id IS NULL`
              ).run(id, b.batchNumber || null, preLinkedInv.id);
            }
            try { logAudit(body.createdBy || 'unknown', 'planning', 'invoice', 'INVOICE_PRE_LINK',
              `[consolidated] Pre-linked existing SAP invoice ${preLinkedInv.sap_doc_num} to request ${id} for batch ${b.batchNumber}`); } catch {}
          } catch (e) { console.warn('[v41r batch pre-link promote] failed:', e.message); }
        }
        batchRes.ok = true;
        batchRes.request_id = id;
        batchRes.status = initStatusB;
        batchRes.preLinked = !!preLinkedInv;
        if (preLinkedInv) {
          batchRes.preLinkedInvoice = { id: preLinkedInv.id, sap_doc_num: preLinkedInv.sap_doc_num, sap_doc_entry: preLinkedInv.sap_doc_entry };
          batchRes.note = `Pre-linked existing SAP invoice #${preLinkedInv.sap_doc_num}`;
        }
        if (overdispatchReason) {
          batchRes.overdispatchApproved = true;
          batchRes.note = (batchRes.note ? batchRes.note + ' · ' : '') + overdispatchReason + ' (admin override applied)';
        }
      } catch (e) {
        batchRes.error = 'server error: ' + e.message;
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

// GET /api/invoice/received — list received invoices (with optional filters)
app.get('/api/invoice/received', async (req, res) => {
  try {
    const status = (req.query.status || '').toString();
    const batch = (req.query.batch || '').toString();
    const customer = (req.query.customer || '').toString();
    const fromDate = (req.query.from_date || '').toString();
    const toDate = (req.query.to_date || '').toString();
    // v41s Q2: line-detail filters
    const pcCode = (req.query.pc_code || '').toString().trim();
    const size = (req.query.size || '').toString().trim();
    const colour = (req.query.colour || '').toString().trim();
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
    // v41s Q2: post-filter by pc_code/size/colour. Match ANY line of the invoice
    // (header column OR any DocumentLine in payload_json). Case-insensitive substring match.
    if (pcCode || size || colour) {
      const lc = (s) => String(s || '').toLowerCase();
      const needPc = lc(pcCode), needSz = lc(size), needCol = lc(colour);
      rows = rows.filter(inv => {
        // Header-row match
        const headerOk =
          (!needPc  || lc(inv.pc_code).includes(needPc)) &&
          (!needSz  || lc(inv.size).includes(needSz)) &&
          (!needCol || lc(inv.colour).includes(needCol));
        if (headerOk) return true;
        // Per-line match (payload_json.DocumentLines)
        try {
          const payload = typeof inv.payload_json === 'string' ? JSON.parse(inv.payload_json) : (inv.payload_json || {});
          const lines = payload.DocumentLines || [];
          return lines.some(L => {
            return (!needPc  || lc(L.ItemCode).includes(needPc)) &&
                   (!needSz  || lc(L.U_SIZE_CAPSULE || L.U_SIZE || '').includes(needSz)) &&
                   (!needCol || lc(L.U_ITEM_DES || L.U_COLOUR || '').includes(needCol));
          });
        } catch { return false; }
      });
    }
    // v44ZK Issue 3: compute a reliable needs_realloc flag — true only when a regularised
    // (dispatched + legacy-closed) invoice's linked dispatch RECORD is a concatenated multi-batch
    // row (batch_number contains a space/comma = old single-record regularise that never netted in
    // the truck plan). New per-batch allocations write only single-token records, so once an invoice
    // is re-allocated this flips to false and it drops out of the "Needs re-allocation" filter.
    try {
      const recIds = rows.map(r => r.dispatch_record_id).filter(Boolean);
      const stuckRecIds = new Set();
      if (recIds.length) {
        let recRows;
        if (pgPool) {
          recRows = (await pgPool.query(`SELECT id, batch_number FROM tracking_dispatch_records WHERE id = ANY($1)`, [recIds])).rows;
        } else {
          const ph = recIds.map(() => '?').join(',');
          recRows = db.prepare(`SELECT id, batch_number FROM tracking_dispatch_records WHERE id IN (${ph})`).all(...recIds);
        }
        for (const rr of recRows) if (/[\s,]/.test(String(rr.batch_number || ''))) stuckRecIds.add(rr.id);
      }
      for (const inv of rows) {
        inv.needs_realloc = !!(inv.dispatch_status === 'dispatched'
          && parseInt(inv.is_legacy_closed, 10) === 1
          && inv.dispatch_record_id && stuckRecIds.has(inv.dispatch_record_id));
      }
    } catch (e) { console.warn('[v44ZK needs_realloc]', e.message); }
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
    // v44O #3: ledger qty in Lakhs — prefer the authoritative total_qty_lakhs (from the
    // invoice_request, or SAP Σ Quantity which is itself in Lakhs). The old total_boxes/100 was
    // wrong: a box count is not Lakhs/100 (e.g. 4 boxes is not 0.04 Lakhs).
    const qty = parseFloat(inv.total_qty_lakhs) > 0
      ? parseFloat(inv.total_qty_lakhs)
      : (parseFloat(inv.total_boxes) > 0 ? parseFloat(inv.total_boxes) / 100 : 0);
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

// ── v44ZB (v44AB): batch + customer + qty fallback reconciliation ────────────────────────────
// Root cause (proven via batch 26ZG113): when a SAP invoice arrives based on a DIFFERENT Sales
// Order than the Sunloc request recorded, the poller's strict sap_doc_entry match fails, so the
// invoice is filed source='direct_sap', invoice_request_id=null. The goods then dispatch (deemed /
// regularise), but the original invoice_request is never cleared and sits in pending_reconciliation
// forever. This fallback matches on batch_number + customer + qty_lakhs (NOT boxes — direct_sap rows
// carry total_boxes=0 until separately enriched), within the 115% over-dispatch ceiling. It AUTO-
// reconciles only on a clean single match; multiple candidates or an out-of-band qty are surfaced as
// a proposal (_fallbackProposal) and left in the list — never silently cleared. Reconcile semantics
// mirror the poller exactly (request -> 'reconciled' + refs; invoice promoted direct_sap -> sunloc +
// linked). No sap-client.js / SAP API involvement — purely our own tables.
const _RECON_OVER  = 1.15;   // 115% over-dispatch ceiling (matches dispatch tolerance)
const _RECON_UNDER = 0.99;   // small float-rounding slack on the low side

async function _orphanInvoicesForRequest(reqRow) {
  const batch = reqRow.batch_number || '';
  const cust  = reqRow.customer || '';
  if (!batch.trim() || !cust.trim()) return [];
  if (pgPool) {
    const r = await pgPool.query(
      `SELECT * FROM invoices_received
        WHERE invoice_request_id IS NULL
          AND dispatch_status='dispatched'
          AND LOWER(TRIM(batch_number)) = LOWER(TRIM($1))
          AND LOWER(TRIM(customer))     = LOWER(TRIM($2))
        ORDER BY invoice_date ASC`,
      [batch, cust]
    );
    return r.rows;
  }
  return db.prepare(
    `SELECT * FROM invoices_received
      WHERE invoice_request_id IS NULL
        AND dispatch_status='dispatched'
        AND LOWER(TRIM(batch_number)) = LOWER(TRIM(?))
        AND LOWER(TRIM(customer))     = LOWER(TRIM(?))
      ORDER BY invoice_date ASC`
  ).all(batch, cust);
}

async function _applyFallbackReconcile(reqRow, inv) {
  // Mirror the poller's reconcile: request -> reconciled (+ SAP refs), invoice -> sunloc + linked.
  // Guards (status='pending_reconciliation', invoice_request_id IS NULL) keep it idempotent + race-safe.
  const docNum = String(inv.sap_doc_num || '');
  if (pgPool) {
    await pgPool.query(
      `UPDATE invoice_requests
          SET status='reconciled', sap_response_doc_num=$1, sap_response_doc_entry=$2,
              reconciled_at=NOW()::TEXT, reconciled_with_invoice_id=$3, updated_at=NOW()::TEXT
        WHERE id=$4 AND status='pending_reconciliation'`,
      [docNum, inv.sap_doc_entry || null, inv.id, reqRow.id]
    );
    await pgPool.query(
      `UPDATE invoices_received
          SET source='sunloc', invoice_request_id=$1,
              batch_number=COALESCE(NULLIF(batch_number,''), $2)
        WHERE id=$3 AND invoice_request_id IS NULL`,
      [reqRow.id, reqRow.batch_number || null, inv.id]
    );
  } else {
    db.prepare(
      `UPDATE invoice_requests
          SET status='reconciled', sap_response_doc_num=?, sap_response_doc_entry=?,
              reconciled_at=datetime('now'), reconciled_with_invoice_id=?, updated_at=datetime('now')
        WHERE id=? AND status='pending_reconciliation'`
    ).run(docNum, inv.sap_doc_entry || null, inv.id, reqRow.id);
    db.prepare(
      `UPDATE invoices_received
          SET source='sunloc', invoice_request_id=?,
              batch_number=COALESCE(NULLIF(batch_number,''), ?)
        WHERE id=? AND invoice_request_id IS NULL`
    ).run(reqRow.id, reqRow.batch_number || null, inv.id);
  }
  console.log(`[v44ZB fallback-recon] request ${reqRow.id} (batch ${reqRow.batch_number}) reconciled via invoice ${inv.id} DocNum=${docNum} on batch+customer+qty match`);
}

// Request-side: evaluate one pending request. Returns {reconciled:true} on a clean single auto-match,
// {proposal:[...]} when ambiguous (multiple candidates or qty out of band), or null when no match.
async function _fallbackReconcileRequest(reqRow) {
  if (!reqRow || reqRow.status !== 'pending_reconciliation') return null;
  const cands = await _orphanInvoicesForRequest(reqRow);
  if (!cands.length) return null;
  const reqQty = parseFloat(reqRow.qty_lakhs) || 0;
  if (cands.length === 1 && reqQty > 0) {
    const q = parseFloat(cands[0].total_qty_lakhs) || 0;
    if (q >= reqQty * _RECON_UNDER && q <= reqQty * _RECON_OVER) {
      await _applyFallbackReconcile(reqRow, cands[0]);
      return { reconciled: true, invoiceId: cands[0].id };
    }
  }
  return {
    proposal: cands.map(c => ({
      invoiceId: c.id, docNum: c.sap_doc_num, sapDocEntry: c.sap_doc_entry,
      qty: c.total_qty_lakhs, boxes: c.total_boxes, dispatchStatus: c.dispatch_status
    }))
  };
}

// Invoice-side: a freshly-dispatched, unlinked invoice clears a single matching pending request.
async function _fallbackReconcileInvoice(invId) {
  let inv;
  if (pgPool) inv = (await pgPool.query(`SELECT * FROM invoices_received WHERE id=$1`, [invId])).rows[0];
  else        inv = db.prepare(`SELECT * FROM invoices_received WHERE id=?`).get(invId);
  if (!inv || inv.invoice_request_id || inv.dispatch_status !== 'dispatched') return null;
  if (!inv.batch_number || !inv.customer) return null;
  let reqs;
  if (pgPool) {
    reqs = (await pgPool.query(
      `SELECT * FROM invoice_requests
        WHERE status='pending_reconciliation'
          AND LOWER(TRIM(batch_number)) = LOWER(TRIM($1))
          AND LOWER(TRIM(customer))     = LOWER(TRIM($2))
        ORDER BY created_at ASC`,
      [inv.batch_number, inv.customer]
    )).rows;
  } else {
    reqs = db.prepare(
      `SELECT * FROM invoice_requests
        WHERE status='pending_reconciliation'
          AND LOWER(TRIM(batch_number)) = LOWER(TRIM(?))
          AND LOWER(TRIM(customer))     = LOWER(TRIM(?))
        ORDER BY created_at ASC`
    ).all(inv.batch_number, inv.customer);
  }
  if (reqs.length !== 1) return null;   // only auto-clear an unambiguous single request
  const reqRow = reqs[0];
  const reqQty = parseFloat(reqRow.qty_lakhs) || 0;
  const q = parseFloat(inv.total_qty_lakhs) || 0;
  if (reqQty > 0 && q >= reqQty * _RECON_UNDER && q <= reqQty * _RECON_OVER) {
    await _applyFallbackReconcile(reqRow, inv);
    return { reconciled: true, requestId: reqRow.id };
  }
  return null;
}

// v41ZS — POST /api/invoice/:id/regularise-dispatch
// Admin-only. Regularise a legacy / return / no-SO invoice (whether physically dispatched or not)
// WITHOUT a scan-out. The dispatch officer can't push these through the normal flow because there's
// no Sunloc request to reconcile and (for legacy/return stock) no system-generated labels to scan.
// Admin optionally attaches a batch, then this marks the invoice admin-approved + legacy-closed +
// dispatched so it leaves the queue. No scan counts are logged — there's nothing to reconcile
// against, and counting for the sake of counting adds no value (per spec).
// v44ZJ Issue 2+5: regularise now accepts an explicit per-batch ALLOCATIONS array
// [{batch, qty, boxes}] so a multi-batch invoice writes ONE CLEAN per-batch dispatch
// record each (single batch key — never a concatenated "A\r\nB" string). Clean keys let
// _recomputeDispatchActuals upsert tracking_dispatch_actuals per real batch, which the
// truck binner / lot-status consumer then nets correctly. Back-compat: a bare {batchNumber}
// still works as a single allocation using the invoice totals. Re-allocation is allowed on
// an already-regularised (legacy-closed) invoice: prior regularise records for THIS invoice
// (tagged in remarks) plus its old dispatch_record_id row are deleted and replaced, and the
// affected batches (old ∪ new) are recomputed — this is what clears the historical stuck lots.
// v44ZJ Issue 2+5: pack-size → boxes (Lakhs per box), mirrors tracking.html lakhToBox().
const _V44ZJ_PACK_SIZES = { '00':0.75, '0':1.00, '1':1.25, '2':1.75, '3':2.25, '4':3.00 };
function _v44zj_lakhToBox(lakhs, size) {
  const ps = _V44ZJ_PACK_SIZES[String(size)];
  const q = parseFloat(lakhs) || 0;
  return ps ? Math.ceil(q / ps) : 0;
}

// v44ZJ Issue 2+5: single source of truth for applying a regularisation. Writes one CLEAN
// per-batch dispatch record per allocation (single batch key — never a concatenated string),
// deletes any prior regularise records for this invoice (tagged in remarks, or its old
// dispatch_record_id row), and recomputes actuals for every affected batch (new ∪ prior) so
// the truck binner / lot-status consumer net correctly. Used by both the HTTP handler and the
// historical auto-pair reconcile.
async function _applyRegularisation(inv, clean, who, ts, rsn) {
  const invId = inv.id;
  const joinedBatches = clean.map(c => c.batch).join(', ');
  const likePat = 'Regularised[inv:' + invId + ']%';
  let priorBatches = [];
  try {
    if (pgPool) {
      const pr = await pgPool.query(`SELECT DISTINCT batch_number AS b FROM tracking_dispatch_records WHERE remarks LIKE $1 OR id = $2`, [likePat, inv.dispatch_record_id || '']);
      priorBatches = pr.rows.map(r => r.b).filter(Boolean);
      await pgPool.query(`DELETE FROM tracking_dispatch_records WHERE remarks LIKE $1 OR id = $2`, [likePat, inv.dispatch_record_id || '']);
    } else {
      priorBatches = db.prepare(`SELECT DISTINCT batch_number AS b FROM tracking_dispatch_records WHERE remarks LIKE ? OR id = ?`).all(likePat, inv.dispatch_record_id || '').map(r => r.b).filter(Boolean);
      db.prepare(`DELETE FROM tracking_dispatch_records WHERE remarks LIKE ? OR id = ?`).run(likePat, inv.dispatch_record_id || '');
    }
  } catch (e) { console.warn('[v44ZJ realloc cleanup]', e.message); }

  if (pgPool) {
    await pgPool.query(
      `UPDATE invoices_received SET
         batch_number=$1, is_legacy_closed=1, legacy_closed_by=$2, legacy_closed_at=$3,
         legacy_close_reason=$4, admin_approved_by=$2, admin_approved_at=$3,
         is_deemed_scan_out=1, deemed_reason=$4, deemed_by=$2,
         dispatched_at=$3, dispatched_by=$2, dispatch_status='dispatched'
       WHERE id=$5`,
      [joinedBatches, who, ts, rsn, invId]
    );
  } else {
    db.prepare(
      `UPDATE invoices_received SET
         batch_number=?, is_legacy_closed=1, legacy_closed_by=?, legacy_closed_at=?,
         legacy_close_reason=?, admin_approved_by=?, admin_approved_at=?,
         is_deemed_scan_out=1, deemed_reason=?, deemed_by=?,
         dispatched_at=?, dispatched_by=?, dispatch_status='dispatched'
       WHERE id=?`
    ).run(joinedBatches, who, ts, rsn, who, ts, rsn, who, ts, who, invId);
  }

  let firstRecId = null;
  for (const c of clean) {
    const recId = 'disprec_' + crypto.randomBytes(6).toString('hex');
    if (!firstRecId) firstRecId = recId;
    const recRemarks = 'Regularised[inv:' + invId + ']: ' + rsn;
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO tracking_dispatch_records (id, batch_number, customer, qty, boxes, vehicle_no, invoice_no, remarks, ts, "by")
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [recId, c.batch, inv.customer || '', c.qty, c.boxes, '', inv.sap_doc_num || '', recRemarks, ts, who]
      );
    } else {
      db.prepare(
        `INSERT INTO tracking_dispatch_records (id, batch_number, customer, qty, boxes, vehicle_no, invoice_no, remarks, ts, by)
         VALUES (?,?,?,?,?,?,?,?,?,?)`
      ).run(recId, c.batch, inv.customer || '', c.qty, c.boxes, '', inv.sap_doc_num || '', recRemarks, ts, who);
    }
  }
  if (firstRecId) {
    if (pgPool) await pgPool.query(`UPDATE invoices_received SET dispatch_record_id=$1 WHERE id=$2`, [firstRecId, invId]);
    else        db.prepare(`UPDATE invoices_received SET dispatch_record_id=? WHERE id=?`).run(firstRecId, invId);
  }
  const affected = Array.from(new Set([...clean.map(c => c.batch), ...priorBatches]));
  for (const b of affected) {
    try { if (typeof _recomputeDispatchActuals === 'function') await _recomputeDispatchActuals(b, null, inv.sap_doc_num || null); } catch (e) {}
  }
  return { joinedBatches, affected };
}

// Parse + validate an allocation list into clean single-batch {batch,qty,boxes} rows.
// Returns { clean } or { error }.
function _v44zj_parseAllocations(body, inv) {
  let allocations = Array.isArray(body && body.allocations) ? body.allocations : null;
  if (!allocations) {
    const bn = (body && body.batchNumber && String(body.batchNumber).trim()) ? String(body.batchNumber).trim() : (inv.batch_number || '');
    allocations = bn ? [{ batch: bn, qty: parseFloat(inv.total_qty_lakhs) || 0, boxes: parseInt(inv.total_boxes, 10) || 0 }] : [];
  }
  const clean = [];
  for (const a of (allocations || [])) {
    const b = String(a && (a.batch != null ? a.batch : a.batchNumber) || '').trim();
    if (!b) return { error: 'Each batch allocation requires a batch number.' };
    if (/[\s,]/.test(b)) return { error: 'Each allocation must be a SINGLE batch (got multiple in one field): "' + b + '". Add a separate row per batch.' };
    const q  = Math.max(0, parseFloat(a.qty) || 0);
    const bx = Math.max(0, parseInt(a.boxes, 10) || 0);
    if (q <= 0 && bx <= 0) return { error: 'Allocation for batch ' + b + ' needs a quantity and/or boxes.' };
    clean.push({ batch: b, qty: q, boxes: bx });
  }
  if (!clean.length) return { error: 'At least one batch allocation is required to regularise — it is needed to reconcile the truck plan.' };
  return { clean };
}

app.post('/api/invoice/:id/regularise-dispatch', async (req, res) => {
  try {
    const session = verifyToken(req.headers['x-session-token'] || req.body?.token);
    const isAdmin = (session && session.role === 'admin') || req.headers['x-user-role'] === 'admin' || req.body?.userRole === 'admin';
    if (!isAdmin) return res.status(403).json({ ok: false, error: 'Admin only — regularising a legacy/return dispatch requires admin sign-in.' });
    const invId = req.params.id;
    const { reason } = req.body || {};
    let inv;
    if (pgPool) { inv = (await pgPool.query(`SELECT * FROM invoices_received WHERE id=$1`, [invId])).rows[0]; }
    else        { inv = db.prepare(`SELECT * FROM invoices_received WHERE id=?`).get(invId); }
    if (!inv) return res.status(404).json({ ok: false, error: 'Invoice not found' });
    const isReallocation = inv.dispatch_status === 'dispatched';
    if (isReallocation && !(parseInt(inv.is_legacy_closed, 10) === 1)) {
      return res.status(409).json({ ok: false, error: 'Invoice already dispatched via scan-out — cannot re-allocate a non-legacy dispatch.', already_dispatched: true });
    }
    const parsed = _v44zj_parseAllocations(req.body, inv);
    if (parsed.error) return res.status(400).json({ ok: false, error: parsed.error });
    const who = (session && session.username) || req.body?.userName || 'admin';
    const ts  = new Date().toISOString();
    const rsn = (reason && String(reason).trim()) || 'Legacy / return / no-SO — regularised by admin';
    const r = await _applyRegularisation(inv, parsed.clean, who, ts, rsn);
    try { logAudit(who, 'admin', 'invoice', 'INVOICE_REGULARISE_DISPATCH',
      `Regularised dispatch for invoice ${invId} (SO ${inv.sap_doc_num || '—'}) — ${parsed.clean.length} alloc(s): ${parsed.clean.map(c=>`${c.batch}=${c.qty}L/${c.boxes}b`).join(', ')}: ${rsn}`, req.ip); } catch {}
    try { await _fallbackReconcileInvoice(invId); } catch (e) { console.warn('[v44ZB fallback-recon] regularise sweep:', e.message); }
    res.json({ ok: true, id: invId, batch_number: r.joinedBatches || null, allocations: parsed.clean, reallocated: isReallocation, dispatch_status: 'dispatched', is_legacy_closed: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// v44ZJ Issue 5 (historical): auto-pair reconcile of already-regularised MULTI-BATCH invoices
// whose dispatch landed under a concatenated key. For each candidate we read payload_json
// DocumentLines (per-line ItemCode + Quantity) and resolve each batch token's PC/size from
// print_orders. If every line maps to exactly ONE batch token by PC (a clean bijection), we
// rebuild clean per-batch allocations (qty = line qty, boxes = lakhToBox(qty, batch size)) and
// re-apply via the shared helper — no human input. Ambiguous cases (a PC shared across tokens,
// unmatched line, or count mismatch) are SKIPPED and reported for manual re-allocation.
app.post('/api/invoice/reconcile-regularised-multibatch', async (req, res) => {
  try {
    const session = verifyToken(req.headers['x-session-token'] || req.body?.token);
    const isAdmin = (session && session.role === 'admin') || req.headers['x-user-role'] === 'admin' || req.body?.userRole === 'admin';
    if (!isAdmin) return res.status(403).json({ ok: false, error: 'Admin only.' });
    const dryRun = !!(req.body && req.body.dryRun);
    const who = (session && session.username) || req.body?.userName || 'admin';
    const ts  = new Date().toISOString();
    let rows;
    if (pgPool) rows = (await pgPool.query(`SELECT * FROM invoices_received WHERE is_legacy_closed=1 AND batch_number IS NOT NULL AND batch_number <> ''`)).rows;
    else        rows = db.prepare(`SELECT * FROM invoices_received WHERE is_legacy_closed=1 AND batch_number IS NOT NULL AND batch_number <> ''`).all();
    // v44ZK: restrict to TRULY stuck invoices — those whose linked dispatch record is a concatenated
    // single-batch row (separator in batch_number). Already-split invoices have clean single-token
    // records and are skipped, so this matches the "Needs re-allocation" filter and avoids rewriting
    // invoices that are already correct.
    const _recIds = rows.map(r => r.dispatch_record_id).filter(Boolean);
    const _stuckRecIds = new Set();
    if (_recIds.length) {
      let _rr;
      if (pgPool) _rr = (await pgPool.query(`SELECT id, batch_number FROM tracking_dispatch_records WHERE id = ANY($1)`, [_recIds])).rows;
      else { const ph = _recIds.map(() => '?').join(','); _rr = db.prepare(`SELECT id, batch_number FROM tracking_dispatch_records WHERE id IN (${ph})`).all(..._recIds); }
      for (const r of _rr) if (/[\s,]/.test(String(r.batch_number || ''))) _stuckRecIds.add(r.id);
    }
    rows = rows.filter(inv => inv.dispatch_record_id && _stuckRecIds.has(inv.dispatch_record_id));
    // v44ZK auto-pair improvement: the legacy batch_number field on regularised invoices is a
    // polluted free-text blob (real batch(es) PLUS pasted qty / PC / colour / size / unit tokens),
    // e.g. "26ZE075, RED, TR/CT, '2', 50.75, lac". So:
    //   1. Extract ONLY batch-shaped tokens (NN<letters>NN) — discards the pollution. dedupe.
    //   2. SINGLE real batch  → the whole invoice goes to that one batch. No PC match / no per-line
    //      detail needed (this clears the large majority of the stuck legacy invoices).
    //   3. MULTIPLE real batches → still need per-line detail to split by PC. Resolve each batch's
    //      PC/size from print_orders, falling back to tracking_labels, comparing PCs leading-zero-
    //      and case-insensitively (SAP ItemCode '0043' vs stored '43'). Clean bijection → pair.
    //   4. Anything still ambiguous (multi-batch with no per-line detail, or same-PC duplicates) is
    //      SKIPPED with a reason — correctly left for manual re-allocation.
    const BATCH_RE = /^\d+[A-Z]+\d+$/i;
    const _normPc = (s) => String(s == null ? '' : s).trim().replace(/^0+(?=\d)/, '');
    // batch -> {pc_code, size} resolver: print_orders first, then tracking_labels (both batch-keyed)
    const _batchInfo = async (token) => {
      let r;
      if (pgPool) r = (await pgPool.query(`SELECT pc_code, size FROM print_orders WHERE batch_number=$1 LIMIT 1`, [token])).rows[0];
      else        r = db.prepare(`SELECT pc_code, size FROM print_orders WHERE batch_number=? LIMIT 1`).get(token);
      if (r && (r.pc_code || r.size)) return r;
      if (pgPool) r = (await pgPool.query(`SELECT pc_code, size FROM tracking_labels WHERE batch_number=$1 AND pc_code IS NOT NULL AND pc_code <> '' LIMIT 1`, [token])).rows[0];
      else        r = db.prepare(`SELECT pc_code, size FROM tracking_labels WHERE batch_number=? AND pc_code IS NOT NULL AND pc_code <> '' LIMIT 1`).get(token);
      return r || null;
    };
    const reconciled = [], skipped = [];
    for (const inv of rows) {
      const rawTokens = String(inv.batch_number || '').split(/[\s,]+/).map(t => t.trim()).filter(Boolean);
      // strip pollution: keep only batch-shaped tokens, dedupe (preserve order)
      const realBatches = [...new Set(rawTokens.filter(t => BATCH_RE.test(t)))];
      if (!realBatches.length) { skipped.push({ id: inv.id, so: inv.sap_doc_num, batches: rawTokens, reason: 'no batch-shaped tokens in batch_number' }); continue; }
      // resolve PC/size for every real batch (single source for both branches)
      const tokInfo = {};
      for (const b of realBatches) tokInfo[b] = await _batchInfo(b);

      let clean = null;
      if (realBatches.length === 1) {
        // SINGLE real batch — the entire invoice belongs to it. No PC match / per-line detail needed.
        const b   = realBatches[0];
        const qty = parseFloat(inv.total_qty_lakhs) || 0;
        const sz  = tokInfo[b] ? tokInfo[b].size : null;
        const boxes = (parseInt(inv.total_boxes, 10) || 0) || _v44zj_lakhToBox(qty, sz);
        if (qty <= 0 && boxes <= 0) { skipped.push({ id: inv.id, so: inv.sap_doc_num, batches: realBatches, reason: 'single batch but invoice qty and boxes are both zero' }); continue; }
        clean = [{ batch: b, qty, boxes }];
      } else {
        // MULTIPLE real batches — need per-line detail to split by PC.
        let lines = [];
        try { const p = typeof inv.payload_json === 'string' ? JSON.parse(inv.payload_json) : (inv.payload_json || {}); lines = (p.DocumentLines || []); } catch (e) {}
        if (!lines.length) { skipped.push({ id: inv.id, so: inv.sap_doc_num, batches: realBatches, reason: 'multiple batches but no per-line detail in payload_json to split' }); continue; }
        let ambiguous = null;
        const used = new Set();
        const allocs = [];
        for (const L of lines) {
          const item = _normPc(L.ItemCode);
          const qty  = parseFloat(L.Quantity) || 0;
          const matches = realBatches.filter(t => tokInfo[t] && _normPc(tokInfo[t].pc_code) !== '' && _normPc(tokInfo[t].pc_code) === item && !used.has(t));
          if (matches.length !== 1) { ambiguous = `line ItemCode ${L.ItemCode || '—'} matched ${matches.length} unused batch(es)`; break; }
          const tk = matches[0]; used.add(tk);
          allocs.push({ batch: tk, qty, boxes: _v44zj_lakhToBox(qty, tokInfo[tk] ? tokInfo[tk].size : null) });
        }
        if (ambiguous) { skipped.push({ id: inv.id, so: inv.sap_doc_num, batches: realBatches, reason: ambiguous }); continue; }
        if (used.size !== realBatches.length) { skipped.push({ id: inv.id, so: inv.sap_doc_num, batches: realBatches, reason: `paired ${used.size}/${realBatches.length} batches — count mismatch` }); continue; }
        clean = allocs;
      }

      if (dryRun) { reconciled.push({ id: inv.id, so: inv.sap_doc_num, dryRun: true, allocations: clean }); continue; }
      const rsn = (inv.legacy_close_reason && String(inv.legacy_close_reason).trim()) || 'Auto-pair reconcile (v44ZK) — split combined multi-batch dispatch';
      try {
        const r = await _applyRegularisation(inv, clean, who, ts, rsn);
        try { logAudit(who, 'admin', 'invoice', 'INVOICE_AUTOPAIR_RECONCILE', `Auto-paired invoice ${inv.id} (SO ${inv.sap_doc_num||'—'}): ${clean.map(c=>`${c.batch}=${c.qty}L/${c.boxes}b`).join(', ')}`, req.ip); } catch {}
        reconciled.push({ id: inv.id, so: inv.sap_doc_num, allocations: clean, affected: r.affected });
      } catch (e) { skipped.push({ id: inv.id, so: inv.sap_doc_num, batches: realBatches, reason: 'apply failed: ' + e.message }); }
    }
    res.json({ ok: true, dryRun, reconciledCount: reconciled.length, skippedCount: skipped.length, reconciled, skipped });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
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

// v44Q: Single-invoice scan session persistence
// GET  /api/invoice/scan-session/:invoiceId  — restore saved scans
// PUT  /api/invoice/scan-session/:invoiceId  — save scans
// DELETE /api/invoice/scan-session/:invoiceId — clear after dispatch

app.get('/api/invoice/scan-session/:invoiceId', async (req, res) => {
  const id = req.params.invoiceId;
  try {
    let row;
    if (pgPool) {
      const r = await pgPool.query(`SELECT scanned_json, saved_at FROM invoice_scan_sessions WHERE invoice_id=$1`, [id]);
      row = r.rows[0];
    } else {
      row = db.prepare(`SELECT scanned_json, saved_at FROM invoice_scan_sessions WHERE invoice_id=?`).get(id);
    }
    if (!row) return res.json({ ok: true, scanned: [] });
    const scanned = JSON.parse(row.scanned_json || '[]');
    res.json({ ok: true, scanned, saved_at: row.saved_at });
  } catch (e) { res.json({ ok: false, error: e.message, scanned: [] }); }
});

app.put('/api/invoice/scan-session/:invoiceId', async (req, res) => {
  const id = req.params.invoiceId;
  const { scanned } = req.body || {};
  try {
    const json = JSON.stringify(scanned || []);
    const now = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO invoice_scan_sessions (invoice_id, scanned_json, saved_at) VALUES ($1,$2,$3)
         ON CONFLICT (invoice_id) DO UPDATE SET scanned_json=$2, saved_at=$3`,
        [id, json, now]
      );
    } else {
      db.prepare(`INSERT INTO invoice_scan_sessions (invoice_id, scanned_json, saved_at) VALUES (?,?,?)
                  ON CONFLICT(invoice_id) DO UPDATE SET scanned_json=excluded.scanned_json, saved_at=excluded.saved_at`).run(id, json, now);
    }
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

app.delete('/api/invoice/scan-session/:invoiceId', async (req, res) => {
  const id = req.params.invoiceId;
  try {
    if (pgPool) await pgPool.query(`DELETE FROM invoice_scan_sessions WHERE invoice_id=$1`, [id]);
    else db.prepare(`DELETE FROM invoice_scan_sessions WHERE invoice_id=?`).run(id);
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, error: e.message }); }
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
           AND COALESCE(is_legacy_closed, 0) = 0
         ORDER BY invoice_date DESC, fetched_at DESC
         LIMIT ${limit}`
      );
      rows = r.rows;
    } else {
      rows = db.prepare(
        `SELECT * FROM invoices_received
         WHERE dispatch_status = 'pending'
           AND (source = 'sunloc' OR (source = 'direct_sap' AND admin_approved_at IS NOT NULL))
           AND COALESCE(is_legacy_closed, 0) = 0
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
              is_deemed_scan_out=1,
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
    // v44ZB (v44AB): a deemed-dispatched, unlinked invoice may match a pending request by
    // batch+customer+qty — clear that orphan immediately so it never sits in reconciliation.
    try { await _fallbackReconcileInvoice(invId); } catch (e) { console.warn('[v44ZB fallback-recon] deemed sweep:', e.message); }
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

    // v41x FIX: apply the same conflict-resolution guard as /api/orders/upsert-bulk and
    // POST /api/planning/state. The single-order upsert was unconditionally overwriting
    // status, which is fine for fresh user edits (they have _localEditedAt) but allowed
    // background/legacy code paths without _localEditedAt to silently regress order
    // statuses. Same rule: client wins UNLESS DB.updated_at is >5s newer than the
    // client's _localEditedAt timestamp.
    let finalStatus = ord.status || 'pending';
    let finalDeleted = ord.deleted || false;
    let finalActualProd = ord.actualProd || 0;
    let mergedOrd = ord;
    let preserved = false;
    let existing = null;
    try {
      if (pgPool) {
        const r = await pgPool.query(`SELECT data_json, status, deleted, updated_at FROM production_orders WHERE id=$1`, [ord.id]);
        existing = r.rows[0];
      } else {
        existing = db.prepare(`SELECT data_json, status, deleted, updated_at FROM production_orders WHERE id=?`).get(ord.id);
      }
    } catch(e) {}
    if (existing) {
      let exData = {};
      try { exData = typeof existing.data_json === 'string' ? JSON.parse(existing.data_json) : (existing.data_json || {}); } catch(e) {}
      const clientEdit = parseInt(ord._localEditedAt || 0);
      const dbUpdated  = existing.updated_at ? new Date(existing.updated_at).getTime() : 0;
      if (existing.status && ord.status && existing.status !== ord.status) {
        if (dbUpdated > clientEdit + 5000) {
          finalStatus = existing.status;
          preserved = true;
        }
      }
      if (exData.deleted || existing.deleted) finalDeleted = true;
      finalActualProd = Math.max(ord.actualProd || 0, exData.actualProd || 0);
      const hasManualDate = exData.manualEndDate || exData.manualStartDate;
      mergedOrd = {
        ...ord,
        status: finalStatus,
        deleted: finalDeleted,
        actualProd: finalActualProd,
        startDate:       hasManualDate ? exData.startDate   : ord.startDate,
        endDate:         hasManualDate ? exData.endDate     : ord.endDate,
        manualStartDate: exData.manualStartDate || ord.manualStartDate,
        manualEndDate:   exData.manualEndDate   || ord.manualEndDate,
        // v41z: protect SAP refs and PO number — DB wins if set; stale client cannot blank them
        sapDocEntry: exData.sapDocEntry || ord.sapDocEntry || null,
        sapDocNum:   exData.sapDocNum   || ord.sapDocNum   || '',
        poNumber:    exData.poNumber    || ord.poNumber    || '',
        // v41z2: user-editable fields — client wins when saving; fall back to DB only if null
        qty:      ord.qty      != null ? ord.qty      : (exData.qty      != null ? exData.qty      : null),
        grossQty: ord.grossQty != null ? ord.grossQty : (exData.grossQty != null ? exData.grossQty : null),
        aGrade:   ord.aGrade   != null ? ord.aGrade   : (exData.aGrade   != null ? exData.aGrade   : null),
        packing:  ord.packing  || exData.packing  || null,
        zone:     ord.zone     || exData.zone     || null,
        pcCode:    ord.pcCode    || exData.pcCode    || null,
        startDate: ord.startDate || exData.startDate || null,
        endDate:   ord.endDate   || exData.endDate   || null,
      };
    }
    if (preserved) {
      console.log(`[v41x upsert] Preserved DB status on ${ord.id} (client="${ord.status}" → DB="${finalStatus}", stale write blocked)`);
    }
    const json = JSON.stringify(mergedOrd);
    if (pgPool) {
      await pgPool.query(`
        INSERT INTO production_orders (id, data_json, machine_id, batch_number, status, deleted, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,NOW()::TEXT)
        ON CONFLICT(id) DO UPDATE SET
          data_json=$2, machine_id=$3, batch_number=$4,
          status=$5, deleted=$6, updated_at=NOW()::TEXT
      `, [mergedOrd.id, json, mergedOrd.machineId||null, mergedOrd.batchNumber||null,
          finalStatus, finalDeleted]);
    } else {
      db.prepare(`INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at)
        VALUES (?,?,?,?,?,?,datetime('now'))
        ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json,
        machine_id=excluded.machine_id,batch_number=excluded.batch_number,
        status=excluded.status,deleted=excluded.deleted,updated_at=datetime('now')`)
        .run(mergedOrd.id, json, mergedOrd.machineId||null, mergedOrd.batchNumber||null, finalStatus, finalDeleted?1:0);
    }
    res.json({ ok: true, preserved, savedAt: new Date().toISOString() });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─────────────────────────────────────────────────────────────────
// v40 Phase 18.14g: REBATCH machine — preempt a pending order to an earlier
// slot in the queue, cascade-renumbering the displaced ones.
//
// Use case: machine MC29 has queue 26ZC10 (running), 26ZC11..26ZC15 (all pending).
// Planning manager wants to bring 26ZC15 forward to position 11. This endpoint:
//   1. Validates each renumbered order is strictly pending (no scans/labels/actuals)
//   2. Updates batchNumber serials in cascade
//   3. Updates planning_state JSON, production_orders rows, print_orders rows atomically
//   4. Rejects if any affected order has any tracking activity
//
// Request body: { machineId, renames: [ { orderId, newBatchNumber }, ... ] }
// Server validates the mapping is internally consistent (no duplicates, all on this machine)
// then applies. Returns { ok, count } on success or { ok:false, conflicts:[...] } on rejection.
// ─────────────────────────────────────────────────────────────────
app.post('/api/planning/rebatch-machine', async (req, res) => {
  try {
    const { machineId, renames } = req.body || {};
    if (!machineId) return res.status(400).json({ ok: false, error: 'machineId required' });
    if (!Array.isArray(renames) || renames.length === 0) return res.status(400).json({ ok: false, error: 'renames array required' });

    // Validate input format
    for (const r of renames) {
      if (!r.orderId || !r.newBatchNumber) return res.status(400).json({ ok: false, error: 'Each rename requires orderId + newBatchNumber' });
    }
    const newBatchNumbers = renames.map(r => r.newBatchNumber);
    if (new Set(newBatchNumbers).size !== newBatchNumbers.length) {
      return res.status(400).json({ ok: false, error: 'Duplicate batch numbers in renames' });
    }

    // Fetch current planning state
    const planState = await getPlanningStateAsync();
    if (!planState.orders) return res.status(500).json({ ok: false, error: 'Planning state missing orders' });

    // Build oldByOrderId map (existing order metadata)
    const oldByOrderId = {};
    for (const o of planState.orders) { if (o && o.id) oldByOrderId[o.id] = o; }

    // Validate every renamed order is pending, on this machine, and has no tracking activity
    const conflicts = [];
    for (const r of renames) {
      const o = oldByOrderId[r.orderId];
      if (!o) { conflicts.push({ orderId: r.orderId, reason: 'Order not found' }); continue; }
      if (o.deleted) { conflicts.push({ orderId: r.orderId, batchNumber: o.batchNumber, reason: 'Order is deleted' }); continue; }
      if (o.machineId !== machineId) { conflicts.push({ orderId: r.orderId, batchNumber: o.batchNumber, reason: `On different machine (${o.machineId})` }); continue; }
      if (o.status !== 'pending') { conflicts.push({ orderId: r.orderId, batchNumber: o.batchNumber, reason: `Not pending (status: ${o.status})` }); continue; }
      // Check for any scan/label/actuals activity tied to this batch_number
      if (o.batchNumber) {
        let scanCount = 0, labelCount = 0, actualsCount = 0;
        try {
          if (pgPool) {
            const r1 = await pgPool.query(`SELECT COUNT(*) c FROM tracking_scans WHERE batch_number=$1`, [o.batchNumber]);
            const r2 = await pgPool.query(`SELECT COUNT(*) c FROM tracking_labels WHERE batch_number=$1`, [o.batchNumber]);
            const r3 = await pgPool.query(`SELECT COUNT(*) c FROM production_actuals WHERE batch_number=$1`, [o.batchNumber]);
            scanCount = parseInt(r1.rows[0]?.c || 0);
            labelCount = parseInt(r2.rows[0]?.c || 0);
            actualsCount = parseInt(r3.rows[0]?.c || 0);
          } else {
            scanCount = db.prepare(`SELECT COUNT(*) c FROM tracking_scans WHERE batch_number=?`).get(o.batchNumber)?.c || 0;
            labelCount = db.prepare(`SELECT COUNT(*) c FROM tracking_labels WHERE batch_number=?`).get(o.batchNumber)?.c || 0;
            actualsCount = db.prepare(`SELECT COUNT(*) c FROM production_actuals WHERE batch_number=?`).get(o.batchNumber)?.c || 0;
          }
        } catch(e) { /* tolerate missing tables */ }
        if (scanCount > 0 || labelCount > 0 || actualsCount > 0) {
          conflicts.push({ orderId: r.orderId, batchNumber: o.batchNumber,
            reason: `Cannot rebatch — has ${scanCount} scan(s), ${labelCount} label(s), ${actualsCount} actual(s).` });
        }
      }
    }
    if (conflicts.length > 0) {
      return res.status(409).json({ ok: false, error: 'Rebatch blocked', conflicts });
    }

    // Validate no collision with batch numbers belonging to OTHER orders on same machine (or anywhere)
    // For each new batch number, check no DIFFERENT order on any machine already has it.
    const renameOrderIdSet = new Set(renames.map(r => r.orderId));
    for (const r of renames) {
      const owner = planState.orders.find(o => o.batchNumber === r.newBatchNumber && !o.deleted);
      if (owner && !renameOrderIdSet.has(owner.id)) {
        conflicts.push({ orderId: r.orderId, batchNumber: r.newBatchNumber, reason: `Batch number ${r.newBatchNumber} already used by order ${owner.id} (${owner.customer||'?'}, MC ${owner.machineId||'?'})` });
      }
    }
    if (conflicts.length > 0) {
      return res.status(409).json({ ok: false, error: 'Rebatch blocked by batch number collisions', conflicts });
    }

    // Apply renames. Use a 2-phase approach in JSON: first set all to temporary placeholders
    // (so we don't trip uniqueness when intermediate state collides), then set to final values.
    const renameById = {};
    for (const r of renames) renameById[r.orderId] = r.newBatchNumber;

    for (const r of renames) {
      const o = oldByOrderId[r.orderId];
      if (o) o.batchNumber = `__TMP__${r.orderId}`;
    }
    for (const r of renames) {
      const o = oldByOrderId[r.orderId];
      if (o) o.batchNumber = r.newBatchNumber;
    }
    // Also rename in printOrders (linked production order)
    if (planState.printOrders) {
      for (const po of planState.printOrders) {
        if (po.productionOrderId && renameById[po.productionOrderId]) {
          po.batchNumber = renameById[po.productionOrderId];
        }
      }
    }

    // Persist planning_state JSON
    if (pgPool) {
      await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json, saved_at=NOW()::TEXT`, [JSON.stringify(planState)]);
    } else {
      db.prepare(`INSERT INTO planning_state (id,state_json,saved_at) VALUES (1,?,datetime('now')) ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json, saved_at=datetime('now')`).run(JSON.stringify(planState));
    }
    _planningStateCache = planState; _planningStateCacheTime = Date.now();

    // Update production_orders table rows + print_orders table rows
    for (const r of renames) {
      const o = oldByOrderId[r.orderId];
      const json = JSON.stringify(o);
      if (pgPool) {
        await pgPool.query(`UPDATE production_orders SET batch_number=$1, data_json=$2, updated_at=NOW()::TEXT WHERE id=$3`, [r.newBatchNumber, json, r.orderId]);
        // Update any print_orders rows that link to this production order
        await pgPool.query(`UPDATE print_orders SET batch_number=$1 WHERE production_order_id=$2`, [r.newBatchNumber, r.orderId]);
      } else {
        db.prepare(`UPDATE production_orders SET batch_number=?, data_json=?, updated_at=datetime('now') WHERE id=?`).run(r.newBatchNumber, json, r.orderId);
        db.prepare(`UPDATE print_orders SET batch_number=? WHERE production_order_id=?`).run(r.newBatchNumber, r.orderId);
      }
    }

    res.json({ ok: true, count: renames.length });
  } catch(err) {
    console.error('[v40 P18.14g] rebatch failed:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
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

// v41ZT Issue 4: FULL cascade purge of an OLD customer's footprint when an order is deleted from
// Planning, so a batch reassigned to a NEW customer has no leftover collision. Removes the order,
// its print orders, and its dispatch plans, and VOIDS the OLD customer's labels for that batch.
// Scoping is deliberately precise to spare the reassigned batch's NEW-customer records:
//   • orders/print-orders/dispatch are removed by the exact orderId / production_order_id, AND
//     additionally by (batch_number + EXACT old-customer match) to catch rows with no link;
//   • labels are VOIDED (not hard-deleted — preserves audit trail; active queries filter voided=0)
//     only where (batch_number + EXACT old-customer match). An exact (case-insensitive, trimmed)
//     customer comparison is used — never a substring LIKE — so e.g. old "ALKEM" never matches a
//     new "ALKEM LABS" on the same batch.
app.post('/api/orders/purge-cascade', async (req, res) => {
  try {
    const { orderId, batchNumber, customer, voidedBy } = req.body;
    if (!batchNumber && !orderId) return res.status(400).json({ ok:false, error:'batchNumber or orderId required' });
    const cust = (customer != null) ? String(customer).trim() : '';
    const out = { orders:0, printOrders:0, dispatchPlans:0, dispatchRecords:0, labelsVoided:0 };
    if (pgPool) {
      if (orderId) {
        out.orders       += (await pgPool.query(`DELETE FROM production_orders WHERE id=$1`, [orderId])).rowCount || 0;
        out.printOrders  += (await pgPool.query(`DELETE FROM print_orders WHERE production_order_id=$1`, [orderId])).rowCount || 0;
        out.dispatchPlans+= (await pgPool.query(`UPDATE dispatch_plans SET deleted=true WHERE production_order_id=$1`, [orderId])).rowCount || 0;
      }
      if (batchNumber && cust) {
        out.orders += (await pgPool.query(
          `DELETE FROM production_orders WHERE batch_number=$1 AND LOWER(TRIM(COALESCE(data_json::jsonb->>'customer','')))=LOWER($2)`,
          [batchNumber, cust])).rowCount || 0;
        out.printOrders += (await pgPool.query(
          `DELETE FROM print_orders WHERE batch_number=$1 AND LOWER(TRIM(COALESCE(customer,'')))=LOWER($2)`,
          [batchNumber, cust])).rowCount || 0;
        out.dispatchPlans += (await pgPool.query(
          `UPDATE dispatch_plans SET deleted=true WHERE batch_number=$1 AND LOWER(TRIM(COALESCE(customer,'')))=LOWER($2)`,
          [batchNumber, cust])).rowCount || 0;
        out.labelsVoided += (await pgPool.query(
          `UPDATE tracking_labels SET voided=1, void_reason=$3, voided_at=NOW()::TEXT, voided_by=$4
             WHERE batch_number=$1 AND LOWER(TRIM(COALESCE(customer,'')))=LOWER($2) AND voided=0`,
          [batchNumber, cust, 'Order deleted — batch reassigned (cascade purge)', voidedBy || 'planning'])).rowCount || 0;
        // v41ZV Issue 4: dispatch records also carry (batch_number + customer) — clear the old
        // customer's for this batch (exact match spares the reassigned batch's new-customer records).
        out.dispatchRecords += (await pgPool.query(
          `DELETE FROM tracking_dispatch_records WHERE batch_number=$1 AND LOWER(TRIM(COALESCE(customer,'')))=LOWER($2)`,
          [batchNumber, cust])).rowCount || 0;
      } else if (batchNumber && !cust && !orderId) {
        // no customer scope and no orderId — fall back to batch-wide order delete only (legacy behaviour)
        out.orders += (await pgPool.query(`DELETE FROM production_orders WHERE batch_number=$1`, [batchNumber])).rowCount || 0;
      }
    } else {
      if (orderId) {
        db.prepare(`DELETE FROM production_orders WHERE id=?`).run(orderId);
        db.prepare(`DELETE FROM print_orders WHERE production_order_id=?`).run(orderId);
        try { db.prepare(`UPDATE dispatch_plans SET deleted=1 WHERE production_order_id=?`).run(orderId); } catch(e){}
      }
      if (batchNumber && cust) {
        db.prepare(`DELETE FROM print_orders WHERE batch_number=? AND LOWER(TRIM(COALESCE(customer,'')))=LOWER(?)`).run(batchNumber, cust);
        try { db.prepare(`UPDATE dispatch_plans SET deleted=1 WHERE batch_number=? AND LOWER(TRIM(COALESCE(customer,'')))=LOWER(?)`).run(batchNumber, cust); } catch(e){}
        db.prepare(`UPDATE tracking_labels SET voided=1, void_reason=?, voided_at=datetime('now'), voided_by=?
                      WHERE batch_number=? AND LOWER(TRIM(COALESCE(customer,'')))=LOWER(?) AND voided=0`)
          .run('Order deleted — batch reassigned (cascade purge)', voidedBy || 'planning', batchNumber, cust);
        try { db.prepare(`DELETE FROM tracking_dispatch_records WHERE batch_number=? AND LOWER(TRIM(COALESCE(customer,'')))=LOWER(?)`).run(batchNumber, cust); } catch(e){}
      } else if (batchNumber) {
        db.prepare(`DELETE FROM production_orders WHERE batch_number=?`).run(batchNumber);
      }
    }
    res.json({ ok:true, ...out });
  } catch(err) { console.error('[purge-cascade]', err); res.status(500).json({ ok:false, error: err.message }); }
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
      const r2 = await pgPool.query(`SELECT batch_number, customer, generated FROM tracking_labels WHERE voided = 0 AND customer IS NOT NULL AND customer <> '' ORDER BY generated DESC`);
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

    // v41w FIX: this endpoint is called on every page load (loadState migration push) AND
    // whenever a client wants to sync. Previously it unconditionally overwrote DB.status with
    // the client's payload, which caused a critical regression: if any tab/device had stale
    // localStorage state with order X='running' (from before someone else closed it), the
    // page-load bulk push would resurrect X to 'running' on the server — violating the Max-2
    // hard block by retroactively un-closing orders. Now: per-order conflict resolution
    // identical to the background merge in POST /api/planning/state — client wins UNLESS the
    // DB's updated_at is meaningfully newer (>5s) than the client's _localEditedAt timestamp.
    // v41w PERF: batch SELECT all existing rows in ONE query (was N sequential round-trips
    // at 500+ orders, the previous bulk-push was the slowest path on the server).
    let preservedCount = 0;
    const preservedOrders = [];  // v41y: track preserved orders to return to client
    const mergedList = [];
    const existingMap = {};
    try {
      const ids = orders.map(o => o && o.id).filter(Boolean);
      if (ids.length > 0) {
        if (pgPool) {
          const r = await pgPool.query(
            `SELECT id, data_json, status, deleted, updated_at FROM production_orders WHERE id = ANY($1)`,
            [ids]
          );
          r.rows.forEach(row => { existingMap[row.id] = row; });
        } else {
          const stmt = db.prepare(`SELECT id, data_json, status, deleted, updated_at FROM production_orders WHERE id IN (${ids.map(()=>'?').join(',')})`);
          stmt.all(...ids).forEach(row => { existingMap[row.id] = row; });
        }
      }
    } catch(e) {
      console.warn('[v41w upsert-bulk] Existing-row preload failed (falling back to client status):', e.message);
    }
    // Build DB running count per machine for 2-order limit enforcement
    // Sort oldest-first so the 2 most established running orders are protected
    const dbRunningPerMachine = {};
    const _dbRunningOrderIds = {};
    const _allRunningRows = Object.entries(existingMap)
      .filter(([, row]) => row && row.status === 'running' && !row.deleted)
      .sort((a, b) => {
        const ta = a[1].updated_at ? new Date(a[1].updated_at).getTime() : 0;
        const tb = b[1].updated_at ? new Date(b[1].updated_at).getTime() : 0;
        return ta - tb;
      });
    _allRunningRows.forEach(([rowId, row]) => {
      let machineId = null;
      try { const d = typeof row.data_json === 'string' ? JSON.parse(row.data_json) : row.data_json; machineId = d && d.machineId; } catch(e) {}
      if (machineId) {
        dbRunningPerMachine[machineId] = (dbRunningPerMachine[machineId] || 0) + 1;
        if (!_dbRunningOrderIds[machineId]) _dbRunningOrderIds[machineId] = [];
        _dbRunningOrderIds[machineId].push(rowId);
      }
    });
    // ACTIVE ENFORCEMENT: if DB already has 3+ running on a machine, downgrade the newest ones
    const _forcePendingIds = new Set();
    Object.entries(_dbRunningOrderIds).forEach(([machineId, ids]) => {
      if (ids.length > 2) {
        ids.slice(2).forEach(id => {
          _forcePendingIds.add(id);
          console.log('[v41z upsert-bulk] MC ' + machineId + ' has ' + ids.length + ' running — downgrading ' + id + ' to pending (2-order limit)');
        });
      }
    });
    for (const ord of orders) {
      if (!ord.id) continue;
      let finalStatus = ord.status || 'pending';
      let finalDeleted = ord.deleted || false;
      let finalActualProd = ord.actualProd || 0;
      let mergedOrd = ord;
      const existing = existingMap[ord.id];
      if (existing) {
        let exData = {};
        try { exData = typeof existing.data_json === 'string' ? JSON.parse(existing.data_json) : (existing.data_json || {}); } catch(e) {}
        const clientEdit = parseInt(ord._localEditedAt || 0);
        const dbUpdated  = existing.updated_at ? new Date(existing.updated_at).getTime() : 0;
        // Status conflict resolution
        // CRITICAL: Enforce 2-order limit using DB running counts
        const _alreadyRunningInDB = existing.status === 'running';
        const _machineRunCount = dbRunningPerMachine[ord.machineId] || 0;
        // PERMANENT STATUS PROTECTION: running and closed are user-set and must NEVER
        // be changed automatically by any process — only the user can change them.
        const _dbIsRunningOrClosed = existing.status === 'running' || existing.status === 'closed';
        const _clientIsRunningOrClosed = ord.status === 'running' || ord.status === 'closed';

        // If DB already has running/closed — preserve it always, client cannot overwrite
        if (_dbIsRunningOrClosed && ord.status !== existing.status) {
          finalStatus = existing.status;
          preservedCount++;
          preservedOrders.push({ id: ord.id, batchNumber: ord.batchNumber||null, machineId: ord.machineId||null, clientStatus: ord.status, dbStatus: existing.status });
        // If client is setting running/closed — always accept (user action)
        } else if (_clientIsRunningOrClosed && !_dbIsRunningOrClosed) {
          finalStatus = ord.status;
          if (ord.status === 'running' && !_alreadyRunningInDB && ord.machineId) {
            dbRunningPerMachine[ord.machineId] = (dbRunningPerMachine[ord.machineId] || 0) + 1;
          }
        // 2-order limit: only applies to running, never to closed
        } else if (_forcePendingIds.has(ord.id) && ord.status === 'running' && existing.status !== 'closed') {
          finalStatus = 'pending';
          preservedCount++;
          preservedOrders.push({ id: ord.id, batchNumber: ord.batchNumber||null, machineId: ord.machineId||null, clientStatus: ord.status, dbStatus: 'pending' });
        } else {
        const _wouldExceedLimit = ord.status === 'running' && !_alreadyRunningInDB && _machineRunCount >= 2;
        if (_wouldExceedLimit) {
          finalStatus = 'pending';
          preservedCount++;
        } else if (existing.status && ord.status && existing.status !== ord.status) {
          if (dbUpdated > clientEdit + 5000) {
            finalStatus = existing.status;
            preservedCount++;
            preservedOrders.push({ id: ord.id, batchNumber: ord.batchNumber||null, machineId: ord.machineId||null, clientStatus: ord.status, dbStatus: existing.status });
          }
          // else: client wins
        }
        } // end 2-order limit else
        // Deleted is sticky once true — never resurrect a deleted order
        if ((exData.deleted || existing.deleted) && !ord.deleted) {
          finalDeleted = true;
          preservedOrders.push({
            id: ord.id,
            batchNumber: ord.batchNumber || null,
            machineId: ord.machineId || null,
            clientStatus: ord.status || null,
            dbStatus: ord.status || null,
            deleted: true,
          });
        } else if (exData.deleted || existing.deleted) {
          finalDeleted = true;
        }
        // Take max of actualProd (DPR can write higher value independently)
        finalActualProd = Math.max(ord.actualProd || 0, exData.actualProd || 0);
        // Preserve manual date flags from DB if set
        const hasManualDate = exData.manualEndDate || exData.manualStartDate;
        mergedOrd = {
          ...ord,
          status: finalStatus,
          deleted: finalDeleted,
          actualProd: finalActualProd,
          startDate:       hasManualDate ? exData.startDate   : ord.startDate,
          endDate:         hasManualDate ? exData.endDate     : ord.endDate,
          manualStartDate: exData.manualStartDate || ord.manualStartDate,
          manualEndDate:   exData.manualEndDate   || ord.manualEndDate,
          // v41z: protect SAP refs and PO number — DB wins if set; stale client cannot blank them
          sapDocEntry: exData.sapDocEntry || ord.sapDocEntry || null,
          sapDocNum:   exData.sapDocNum   || ord.sapDocNum   || '',
          poNumber:    exData.poNumber    || ord.poNumber    || '',
          // v41z2: user-editable fields — client wins when saving; DB only as fallback
          qty:      ord.qty      != null ? ord.qty      : (exData.qty      != null ? exData.qty      : null),
          grossQty: ord.grossQty != null ? ord.grossQty : (exData.grossQty != null ? exData.grossQty : null),
          aGrade:   ord.aGrade   != null ? ord.aGrade   : (exData.aGrade   != null ? exData.aGrade   : null),
          packing:  ord.packing  || exData.packing  || null,
          zone:     ord.zone     || exData.zone     || null,
          pcCode:    ord.pcCode    || exData.pcCode    || null,
          startDate: ord.startDate || exData.startDate || null,
          endDate:   ord.endDate   || exData.endDate   || null,
        };
      }
      const json = JSON.stringify(mergedOrd);
      mergedList.push({ row: mergedOrd, json, finalStatus, finalDeleted });
    }
    // v41w PERF: batch the INSERTs into chunked multi-row upserts (same pattern as background
    // merge in POST /api/planning/state). At 500+ orders, sequential INSERTs starved the pool.
    if (pgPool) {
      const CHUNK = 500;
      for (let i = 0; i < mergedList.length; i += CHUNK) {
        const chunk = mergedList.slice(i, i + CHUNK);
        const vals = [];
        const params = [];
        chunk.forEach((m, idx) => {
          const b = idx * 6;
          vals.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},NOW()::TEXT)`);
          params.push(m.row.id, m.json, m.row.machineId||null, m.row.batchNumber||null,
                      m.finalStatus, m.finalDeleted);
        });
        await pgPool.query(`
          INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at)
          VALUES ${vals.join(',')}
          ON CONFLICT(id) DO UPDATE SET data_json=EXCLUDED.data_json,machine_id=EXCLUDED.machine_id,
            batch_number=EXCLUDED.batch_number,status=EXCLUDED.status,deleted=EXCLUDED.deleted,
            updated_at=NOW()::TEXT
        `, params);
      }
    } else {
      // SQLite: better-sqlite3 transactions are synchronous and fast — wrap all inserts
      const stmt = db.prepare(`INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at)
          VALUES (?,?,?,?,?,?,datetime('now'))
          ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json,
          machine_id=excluded.machine_id,batch_number=excluded.batch_number,
          status=excluded.status,deleted=excluded.deleted,updated_at=datetime('now')`);
      const tx = db.transaction((rows) => {
        for (const m of rows) {
          stmt.run(m.row.id, m.json, m.row.machineId||null, m.row.batchNumber||null,
                   m.finalStatus, m.finalDeleted?1:0);
        }
      });
      tx(mergedList);
    }
    if (preservedCount > 0) {
      console.log(`[v41w upsert-bulk] Preserved DB status on ${preservedCount}/${orders.length} orders (stale client write blocked)`);
    }
    res.json({ ok: true, count: orders.length, preservedCount, preservedOrders, savedAt: new Date().toISOString() });
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
    //
    // v41z STRUCTURAL FIX (Max-2 regression root cause): the GET path was treating the blob
    // as authoritative for STATUS, but the close action writes status to BOTH the production_orders
    // table (via /api/orders/upsert) AND the blob (via /api/planning/state). If the blob write
    // fails (network timeout, abort, server pool exhaustion) but the table write succeeds, the
    // blob keeps the stale status forever and every subsequent GET returns the stale value —
    // the user sees their close "revert" on every refresh because the blob never caught up to
    // the table. v41z fixes this by reconciling per-order: if production_orders.updated_at is
    // newer than the blob's last save AND the statuses differ, the DB status wins on read.
    // This is the inverse of the v41w/v41y write-side guard (which protected DB against stale
    // client writes) — symmetric reads now protect the user's view against stale blob entries.
    try {
      let dbOrders = [];
      let dbOrderRows = [];  // v41z: also need updated_at per order
      if (pgPool) {
        const r = await pgPool.query('SELECT data_json, status as db_status, updated_at as db_updated_at FROM production_orders WHERE deleted = false ORDER BY updated_at ASC');
        dbOrderRows = r.rows;
        dbOrders = r.rows.map(r => {
          const o = JSON.parse(r.data_json);
          o._dbStatus = r.db_status;
          o._dbUpdatedAt = r.db_updated_at;
          return o;
        });
      } else {
        const rows = db.prepare('SELECT data_json, status as db_status, updated_at as db_updated_at FROM production_orders WHERE deleted = 0 ORDER BY updated_at ASC').all();
        dbOrderRows = rows;
        dbOrders = rows.map(r => {
          const o = JSON.parse(r.data_json);
          o._dbStatus = r.db_status;
          o._dbUpdatedAt = r.db_updated_at;
          return o;
        });
      }
      // v41z: get blob's saved_at to compare against each order's DB updated_at
      let blobSavedAt = 0;
      try {
        const r2 = pgPool
          ? (await pgPool.query('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1')).rows[0]
          : db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get();
        blobSavedAt = r2?.saved_at ? new Date(r2.saved_at).getTime() : 0;
      } catch(e) {}
      if (dbOrders.length > 0) {
        // Build lookup maps from planning_state orders
        const stateOrderById = new Map((state.orders||[]).map(o => [o.id, o]));
        const stateOrderByBatchMc = new Map();
        (state.orders||[]).forEach(o => {
          if (o.batchNumber && o.machineId) stateOrderByBatchMc.set(`${o.batchNumber}__${o.machineId}`, o);
        });
        // v41z STRUCTURAL FIX: reconcile status/deleted of EXISTING orders. If production_orders
        // has a newer updated_at (>30s past blob's saved_at — generous grace for clock skew and
        // background-merge timing) AND the status differs, prefer the DB status. This catches
        // the "close succeeded in table but blob save failed" stale-blob case.
        let reconciledCount = 0;
        const dbById = new Map();
        dbOrders.forEach(o => dbById.set(o.id, o));
        (state.orders||[]).forEach(o => {
          const dbO = dbById.get(o.id);
          if (!dbO) return;
          // If DB says deleted, propagate (deleted is sticky)
          if (dbO._dbStatus !== undefined && dbO.deleted && !o.deleted) {
            o.deleted = true;
            reconciledCount++;
            return;
          }
          const dbUpd = dbO._dbUpdatedAt ? new Date(dbO._dbUpdatedAt).getTime() : 0;
          // Only override if DB is meaningfully newer than blob's last save (30s grace) AND statuses differ.
          // Without the grace, normal sync timing where DB is written just after blob would always win.
          // CRITICAL: Never auto-revert running or closed — real physical actions
          const blobStatusIsProtected = o.status === 'running' || o.status === 'closed';
          if (dbO._dbStatus && o.status && dbO._dbStatus !== o.status && dbUpd > blobSavedAt + 30000 && !blobStatusIsProtected) {
            o.status = dbO._dbStatus;
            reconciledCount++;
          }
        });
        if (reconciledCount > 0) {
          console.log(`[v41z GET reconcile] Updated ${reconciledCount} blob order(s) with newer DB status (stale-blob recovery)`);
          // v41z: kick off a deferred background blob update to make the fix permanent.
          // Without this, every GET re-does the same reconciliation forever. The setImmediate
          // ensures the response goes out first; the rewrite uses the already-reconciled state.
          if (pgPool) {
            const stateForBlobWrite = state;
            setImmediate(async () => {
              try {
                const json = JSON.stringify(stateForBlobWrite);
                const existing = await pgPool.query('SELECT id FROM planning_state LIMIT 1');
                if (existing.rows[0]) {
                  await pgPool.query('UPDATE planning_state SET state_json = $1, saved_at = NOW() WHERE id = $2', [json, existing.rows[0].id]);
                  // Update cache so subsequent reads see the corrected state
                  _planningStateCache = stateForBlobWrite;
                  _planningStateCacheTime = Date.now();
                  console.log(`[v41z GET reconcile] Persisted ${reconciledCount} status correction(s) back to blob — won't re-reconcile`);
                }
              } catch(e) {
                console.warn('[v41z GET reconcile] Deferred blob write failed:', e.message);
              }
            });
          }
        }
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
          const { _dbStatus, _dbUpdatedAt, ...cleanOrd } = dbOrd;
          // CRITICAL: Enforce max 2 IN PRODUCTION per machine
          if (cleanOrd.status === 'running' && cleanOrd.machineId) {
            const runningOnMachine = state.orders.filter(o => o.machineId === cleanOrd.machineId && o.status === 'running' && !o.deleted).length;
            if (runningOnMachine >= 2) {
              cleanOrd.status = 'pending';
              console.log(`[State] Recovered ${cleanOrd.batchNumber} on ${cleanOrd.machineId} — downgraded to pending (2-order limit)`);
            } else {
              console.log(`[State] Recovered missing order: ${cleanOrd.batchNumber} on ${cleanOrd.machineId}`);
            }
          } else {
            console.log(`[State] Recovered missing order: ${cleanOrd.batchNumber} on ${cleanOrd.machineId}`);
          }
          state.orders.push(cleanOrd);
          stateOrderById.set(dbOrd.id, cleanOrd);
          if (dbOrd.batchNumber && dbOrd.machineId) stateOrderByBatchMc.set(bmKey, cleanOrd);
        });
      }
    } catch(e) { console.warn('[State] Order recovery failed:', e.message); }

    // v40 P18.14i Fix 2: actualProd refresh only — auto-promote pending→running REMOVED.
    // Previously the server flipped any pending order with actuals > 0 to 'running'
    // silently on every GET. Combined with the broken merge in /api/planning/state,
    // this caused closed→running and stuck >2 running orders per machine. With the new
    // DPR gate at /api/dpr/save, non-running orders can no longer receive actuals,
    // so the auto-promote is unnecessary. Status is now fully user-controlled.
    // v41ZI Item 4+6: refresh the actuals/gross/override caches in the BACKGROUND (fire-and-forget,
    // throttled to 60s) and inject from whatever is already cached. This MUST NOT be awaited: this
    // endpoint is on the Tracking app's critical path (pullFromServer STEP 1, 8s timeout) and the
    // actuals aggregation can exceed that on a busy DB — awaiting it made the Tracking fetch abort,
    // leaving state.batches empty so every dashboard/WIP/label count read 0. The cache stays warm via
    // the startup warm + frequent polling; on a cold cache we simply skip injection this once (orders
    // keep their stored actualProd, exactly as before Item 4) and the next warmed read injects.
    warmActualsCache().catch(()=>{});
    if (state.orders && _actualsCache) {
      for (const ord of state.orders) {
        // v41ZI Item 4+6 / v41ZJ perf: prefer the authoritative per-batch DPR gross using ONLY the
        // in-memory caches (override → pure batch sum) — never the synchronous SQLite fallback inside
        // effectiveGross(). This endpoint is polled by every client; a per-order DB query here would
        // block the event loop on the hot path. An explicit override always wins (even 0); otherwise
        // the pure batch sum if present; else the legacy (order_id|batch) cache. A batch absent from
        // both gross maps has no actuals, so the legacy/0 fallback is exactly what effectiveGross
        // would have returned in PG mode (SQLite is dormant) — behaviour is unchanged, just faster.
        const bn = ord.batchNumber;
        const hasOverride = bn != null && Object.prototype.hasOwnProperty.call(_grossOverride, bn);
        let eff = 0;
        if (hasOverride) eff = _grossOverride[bn] || 0;
        else if (bn != null && _grossByBatch && Object.prototype.hasOwnProperty.call(_grossByBatch, bn)) eff = _grossByBatch[bn] || 0;
        const legacy = (_actualsCache[ord.id] || _actualsCache[ord.batchNumber] || 0);
        ord.actualProd = (hasOverride || eff > 0) ? eff : legacy;
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

          // v41ZN: debounce — skip if a background merge ran within the window. Set the timestamp
          // BEFORE the awaits below so a concurrent save can't slip a second merge through. The blob
          // is already persisted by this POST; production_orders just catches up on the next merge.
          const _bgNow = Date.now();
          if (_bgNow - _lastBgMerge < BG_MERGE_DEBOUNCE_MS) return;
          _lastBgMerge = _bgNow;

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

          // CRITICAL: Count running orders per machine from DB — authoritative 2-order limit
          // Sort oldest-first so the 2 most established running orders are protected
          const dbRunningPerMachine = {};
          const _bgRunningOrderIds = {};
          const _bgAllRunning = Object.entries(existingMap)
            .filter(([, o]) => o && o.status === 'running' && o.machineId && !o.deleted)
            .sort((a, b) => {
              const ta = a[1].updated_at ? new Date(a[1].updated_at).getTime() : 0;
              const tb = b[1].updated_at ? new Date(b[1].updated_at).getTime() : 0;
              return ta - tb;
            });
          _bgAllRunning.forEach(([id, o]) => {
            dbRunningPerMachine[o.machineId] = (dbRunningPerMachine[o.machineId] || 0) + 1;
            if (!_bgRunningOrderIds[o.machineId]) _bgRunningOrderIds[o.machineId] = [];
            _bgRunningOrderIds[o.machineId].push(id);
          });
          // ACTIVE ENFORCEMENT: downgrade newest orders on machines already over limit
          const _bgForcePendingIds = new Set();
          Object.entries(_bgRunningOrderIds).forEach(([machineId, ids]) => {
            if (ids.length > 2) {
              ids.slice(2).forEach(id => {
                _bgForcePendingIds.add(id);
                console.log('[v41z bg-merge] MC ' + machineId + ' has ' + ids.length + ' running — downgrading ' + id + ' to pending (2-order limit)');
              });
            }
          });

          // v40 P18.14i Fix 1: status merge — client wins.
          // The PRIOR rule preserved DB's non-pending status, which caused two bugs:
          //   a) closed → running silently when DB had stale 'running'
          //   b) any user-initiated demotion to pending got reverted
          // New rule: incoming client status is authoritative for the order's intent.
          // Safety net: if the DB row's updated_at is NEWER than the incoming client's
          // _localEditedAt (which the client now stamps), the DB wins — protects against
          // stale tabs overwriting a fresh decision from another device.

          // v41 PERF FIX: previously this issued one INSERT per order via Promise.all — with
          // 479 orders that fired 479 concurrent queries against a 5-connection pool EVERY 30s
          // (Planning auto-sync cadence), saturating the pool and starving all other endpoints
          // (Tracking tabs slowed to a crawl). Now: a single batched multi-row upsert per save.
          // Chunked to stay well under PostgreSQL's 65535-parameter limit (6 params/row → ~10000 rows/chunk).
          const mergedList = await Promise.all(orders.map(async ord => {
            const ex = existingMap[ord.id];
            let mergedOrd = ord;
            if (ex) {
              const hasManualDate = ex.manualEndDate || ex.manualStartDate;
              const clientEdit = parseInt(ord._localEditedAt || 0);
              const dbUpdated  = ex.updated_at ? new Date(ex.updated_at).getTime() : 0;
              let finalStatus;
              // PERMANENT STATUS PROTECTION: running and closed are user-set, never change automatically
              const alreadyRunningInDB = ex.status === 'running';
              const machineRunCount = dbRunningPerMachine[ord.machineId] || 0;
              const _bgDbProtected = ex.status === 'running' || ex.status === 'closed';
              const _bgClientProtected = ord.status === 'running' || ord.status === 'closed';
              if (_bgDbProtected && ord.status !== ex.status) {
                // DB has running/closed — preserve it, client cannot change it
                finalStatus = ex.status;
              } else if (_bgClientProtected && !_bgDbProtected) {
                // Client setting running/closed — accept it (user action)
                finalStatus = ord.status;
              } else if (_bgForcePendingIds.has(ord.id) && ord.status === 'running' && ex.status !== 'closed') {
                finalStatus = 'pending';
              } else {
              const wouldExceedLimit = ord.status === 'running' && !alreadyRunningInDB && machineRunCount >= 2;
              if (wouldExceedLimit) {
                finalStatus = 'pending';
              } else if (ex.status && ord.status && ex.status !== ord.status) {
                // Protect running/closed from being reverted
                const clientIsProtected = ord.status === 'running' || ord.status === 'closed';
                const dbIsProtected = ex.status === 'running' || ex.status === 'closed';
                if (clientIsProtected) {
                  finalStatus = ord.status;
                  // Update running count if newly running
                  if (ord.status === 'running' && !alreadyRunningInDB && ord.machineId) {
                    dbRunningPerMachine[ord.machineId] = (dbRunningPerMachine[ord.machineId] || 0) + 1;
                  }
                } else if (dbIsProtected) {
                  finalStatus = ex.status;
                } else if (clientEdit && dbUpdated && dbUpdated > clientEdit + 5000) {
                  finalStatus = ex.status;
                } else {
                  finalStatus = ord.status;
                }
              } else {
                finalStatus = ord.status || ex.status || 'pending';
              }
              } // end _bgForcePendingIds else
              mergedOrd = {
                ...ord,
                startDate:       hasManualDate ? ex.startDate   : ord.startDate,
                endDate:         hasManualDate ? ex.endDate     : ord.endDate,
                manualEndDate:   ex.manualEndDate   || ord.manualEndDate,
                manualStartDate: ex.manualStartDate || ord.manualStartDate,
                status: finalStatus,
                actualProd: Math.max(ord.actualProd||0, ex.actualProd||0),
                // v41z: protect SAP refs and PO number — DB wins if set; client cannot blank them via stale tab
                sapDocEntry: ex.sapDocEntry || ord.sapDocEntry || null,
                sapDocNum:   ex.sapDocNum   || ord.sapDocNum   || '',
                poNumber:    ex.poNumber    || ord.poNumber    || '',
                // v41z2: bg-merge — DB always wins over stale blob for user-editable fields
                qty:      ex.qty      != null ? ex.qty      : (ord.qty      != null ? ord.qty      : null),
                grossQty: ex.grossQty != null ? ex.grossQty : (ord.grossQty != null ? ord.grossQty : null),
                aGrade:   ex.aGrade   != null ? ex.aGrade   : (ord.aGrade   != null ? ord.aGrade   : null),
                zone:     ex.zone     || ord.zone     || null,
                packing:  ex.packing  || ord.packing  || null,
                pcCode:    ex.pcCode    || ord.pcCode    || null,
                startDate: ex.startDate || ord.startDate || null,
                endDate:   ex.endDate   || ord.endDate   || null,
              };
            }
            return mergedOrd;
          }));

          const CHUNK = 500;
          for (let i = 0; i < mergedList.length; i += CHUNK) {
            const chunk = mergedList.slice(i, i + CHUNK);
            const vals = [];
            const params = [];
            chunk.forEach((m, idx) => {
              const b = idx * 6;
              vals.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},NOW()::TEXT)`);
              params.push(m.id, JSON.stringify(m), m.machineId||null,
                          m.batchNumber||null, m.status||'pending', m.deleted||false);
            });
            await pgPool.query(`
              INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at)
              VALUES ${vals.join(',')}
              ON CONFLICT(id) DO UPDATE SET data_json=EXCLUDED.data_json,machine_id=EXCLUDED.machine_id,
                batch_number=EXCLUDED.batch_number,status=EXCLUDED.status,deleted=EXCLUDED.deleted,
                updated_at=NOW()::TEXT
            `, params);
          }
          console.log(`[State] Background merged ${orders.length} orders into production_orders (batched)`);
        } catch(e) { console.warn('[State] Background order merge failed:', e.message); }
      });
    }

    // v41w CRITICAL FIX (Item 6): the blob is the GET source-of-truth for order status. If a
    // stale client posts the blob with old statuses, the corrupted blob is returned on next GET
    // and orders that were correctly closed flip back to running. Apply the SAME per-order
    // conflict resolution that the background merge uses BEFORE writing the blob — read each
    // order's current DB row, and if DB.updated_at > client._localEditedAt + 5s, preserve DB
    // status in the blob. This protects against stale-tab regressions.
    //
    // v41y CRITICAL FIX: the v41w guard silently corrected stale writes server-side BUT did not
    // tell the client. The client kept its own (stale) in-memory state and on next save replayed
    // the same stale values — guard fires again, count grows, loop continues forever until the
    // client's 60s skip-overwrite window expires. Even worse, line 1776 of planning.html writes
    // the stale `toSave` to localStorage immediately after save success, so on page refresh the
    // user's localStorage still had the stale state. NOW: we return the list of preserved
    // {id, status} corrections in the response so the client can patch its in-memory state and
    // rewrite localStorage immediately.
    let blobPreservedCount = 0;
    const preservedOrders = [];
    if (state.orders && state.orders.length > 0 && pgPool) {
      try {
        const ids = state.orders.map(o => o.id).filter(Boolean);
        if (ids.length > 0) {
          const dbRes = await pgPool.query(
            `SELECT id, status, deleted, updated_at, data_json FROM production_orders WHERE id = ANY($1)`, [ids]
          );
          const dbMap = {};
          dbRes.rows.forEach(r => {
            let exData = {};
            try { exData = r.data_json ? (typeof r.data_json === 'string' ? JSON.parse(r.data_json) : r.data_json) : {}; } catch(e) {}
            dbMap[r.id] = { ...r, _exData: exData };
          });
          state.orders = state.orders.map(ord => {
            const dbRow = dbMap[ord.id];
            if (!dbRow) return ord;
            const clientEdit = parseInt(ord._localEditedAt || 0);
            const dbUpdated  = dbRow.updated_at ? new Date(dbRow.updated_at).getTime() : 0;
            let result = ord;
            // Status: DB wins if meaningfully newer
            if (dbRow.status && ord.status && dbRow.status !== ord.status && dbUpdated > clientEdit + 5000) {
              result = { ...result, status: dbRow.status };
              blobPreservedCount++;
              preservedOrders.push({
                id: ord.id,
                batchNumber: ord.batchNumber || null,
                machineId: ord.machineId || null,
                clientStatus: ord.status,
                dbStatus: dbRow.status,
              });
            }
            // Deleted is sticky
            if (dbRow.deleted && !result.deleted) {
              result = { ...result, deleted: true };
              // Record deletion too so client can drop the order from state
              preservedOrders.push({
                id: ord.id,
                batchNumber: ord.batchNumber || null,
                machineId: ord.machineId || null,
                clientStatus: ord.status || null,
                dbStatus: ord.status || null,
                deleted: true,
              });
            }
            // v41z: protect SAP refs and PO number — DB wins if set; stale client cannot blank them
            const dbSapEntry = dbRow._exData.sapDocEntry || null;
            const dbSapNum   = dbRow._exData.sapDocNum   || '';
            const dbPoNumber = dbRow._exData.poNumber    || '';
            if (dbSapEntry && !result.sapDocEntry) result = { ...result, sapDocEntry: dbSapEntry };
            if (dbSapNum   && !result.sapDocNum)   result = { ...result, sapDocNum:   dbSapNum };
            if (dbPoNumber && !result.poNumber)    result = { ...result, poNumber:    dbPoNumber };
            // v41z2: user-editable fields — DB always wins in blob-save so refresh never reverts changes
            const dbQty      = dbRow._exData.qty      != null ? dbRow._exData.qty      : null;
            const dbGrossQty = dbRow._exData.grossQty != null ? dbRow._exData.grossQty : null;
            const dbAGrade   = dbRow._exData.aGrade   != null ? dbRow._exData.aGrade   : null;
            const dbZone     = dbRow._exData.zone     || null;
            const dbPacking   = dbRow._exData.packing   || null;
            const dbPcCode    = dbRow._exData.pcCode    || null;
            const dbStartDate = dbRow._exData.startDate || null;
            const dbEndDate   = dbRow._exData.endDate   || null;
            if (dbQty      != null) result = { ...result, qty:      dbQty };
            if (dbGrossQty != null) result = { ...result, grossQty: dbGrossQty };
            if (dbAGrade   != null) result = { ...result, aGrade:   dbAGrade };
            if (dbZone)             result = { ...result, zone:     dbZone };
            if (dbEndDate)    result = { ...result, endDate:   dbEndDate };
            if (dbPacking)   result = { ...result, packing:  dbPacking };
            if (dbPcCode)    result = { ...result, pcCode:   dbPcCode };
            if (dbStartDate) result = { ...result, startDate: dbStartDate };
            return result;
          });
          if (blobPreservedCount > 0) {
            console.log(`[v41w blob-save] Preserved DB status on ${blobPreservedCount}/${state.orders.length} orders before blob write`);
          }
        }
      } catch (e) {
        console.warn('[v41w blob-save] Pre-save conflict check failed (saving as-is):', e.message);
      }
    }

    // v41z FINAL FIX: Enforce 2-order limit directly in the blob before saving.
    // This stops stale browsers from re-corrupting the blob with 3+ running orders.
    // Also adds downgraded orders to preservedOrders so client clears _localOrderChanges.
    if (state.orders && state.orders.length > 0) {
      const _blobRunningPerMC = {};
      // First pass: count running per machine (sort by _localEditedAt desc — newest protected first)
      const _blobRunning = state.orders
        .filter(o => o && o.status === 'running' && o.machineId && !o.deleted)
        .sort((a, b) => (parseInt(b._localEditedAt||0)) - (parseInt(a._localEditedAt||0)));
      const _blobAllowedIds = new Set();
      for (const o of _blobRunning) {
        const cnt = _blobRunningPerMC[o.machineId] || 0;
        if (cnt < 2) {
          _blobAllowedIds.add(o.id);
          _blobRunningPerMC[o.machineId] = cnt + 1;
        }
      }
      let blobLimitDowngraded = 0;
      state.orders = state.orders.map(o => {
        // NEVER downgrade closed orders — only running orders subject to 2-order limit
        if (o && o.status === 'running' && o.machineId && !o.deleted && !_blobAllowedIds.has(o.id)) {
          blobLimitDowngraded++;
          console.log(`[v41z blob-save] MC ${o.machineId} over limit — downgrading ${o.batchNumber||o.id} to pending`);
          preservedOrders.push({
            id: o.id,
            batchNumber: o.batchNumber || null,
            machineId: o.machineId || null,
            clientStatus: 'running',
            dbStatus: 'pending',
          });
          return { ...o, status: 'pending' };
        }
        // CLOSED orders are permanent — never touch them
        return o;
      });
      if (blobLimitDowngraded > 0) {
        console.log(`[v41z blob-save] Downgraded ${blobLimitDowngraded} over-limit running orders to pending`);
      }
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
    res.json({ ok: true, savedAt: new Date().toISOString(), preservedOrders });
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
    // v40 P18.14i Fix 2c: admins can request all orders (not just running) so the DPR UI
    // can mark non-running orders visually + warn before entering data against them.
    // Triggered by ?includeAll=1 query param OR X-User-Role: admin header.
    const _isAdmin = req.query.includeAll === '1' || req.headers['x-user-role'] === 'admin';
    const baseSet = _isAdmin
      ? (state.orders || []).filter(o => !o.deleted)
      : (state.orders || []).filter(o => o.status === 'running' && !o.deleted);

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
        packing: o.packing || '',
      };
    };

    if (_isAdmin) {
      // Admin sees everything — no max-2-per-machine cap. Status field tells the client what to mark.
      const orders = baseSet.map(mapOrder);
      return res.json({ ok: true, orders });
    }

    // Default (non-admin): only running orders, capped at 2 per machine for non-legacy
    const running = baseSet;
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

// v40 P18.14i Fix 3: cleanup-banner data endpoint.
// Returns list of machines with >2 running orders so the Planning page can show
// a non-mutating warning banner. No automatic mutation — purely informational.
// Called by planning.html on page load and every 5 minutes by the auto-sync poller.
app.get('/api/planning/overlimit-machines', async (req, res) => {
  try {
    const state = await getPlanningStateAsync();
    if (!state.orders) return res.json({ ok: true, machines: [] });

    // v41z STRUCTURAL FIX: same reconciliation as GET /api/planning/state — if production_orders
    // has a newer updated_at than the blob saved_at AND statuses differ, DB wins. The blob can
    // get stuck with stale 'running' status if a saveState fails after a successful upsertOrderToDB.
    // Reconciling here ensures the banner only fires for ACTUAL over-limit conditions, not stale
    // blob entries.
    let blobSavedAt = 0;
    try {
      const r2 = pgPool
        ? (await pgPool.query('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1')).rows[0]
        : db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get();
      blobSavedAt = r2?.saved_at ? new Date(r2.saved_at).getTime() : 0;
    } catch(e) {}
    let dbStatusMap = {};
    try {
      if (pgPool) {
        const r = await pgPool.query('SELECT id, status, deleted, updated_at FROM production_orders');
        r.rows.forEach(row => { dbStatusMap[row.id] = row; });
      } else {
        db.prepare('SELECT id, status, deleted, updated_at FROM production_orders').all()
          .forEach(row => { dbStatusMap[row.id] = row; });
      }
    } catch(e) {}
    // Reconcile each order's effective status before counting
    const effectiveStatus = (o) => {
      const dbRow = dbStatusMap[o.id];
      if (!dbRow) return o.status;
      // Deleted is sticky
      if (dbRow.deleted) return 'deleted';
      const dbUpd = dbRow.updated_at ? new Date(dbRow.updated_at).getTime() : 0;
      if (dbRow.status && o.status && dbRow.status !== o.status && dbUpd > blobSavedAt + 30000) {
        return dbRow.status;
      }
      return o.status;
    };

    // Group running orders by machine (using reconciled effective status)
    const byMc = {};
    for (const o of state.orders) {
      if (o.deleted) continue;
      const eff = effectiveStatus(o);
      if (eff !== 'running') continue;
      const mc = o.machineId || '(unassigned)';
      if (!byMc[mc]) byMc[mc] = [];
      byMc[mc].push(o);
    }
    // Pick over-limit machines
    const machines = [];
    for (const [mcId, orders] of Object.entries(byMc)) {
      if (orders.length <= 2) continue;
      // Sort by startDate ASC so oldest are listed first (likely candidates for closure)
      orders.sort((a,b) => String(a.startDate||'').localeCompare(String(b.startDate||'')));
      machines.push({
        machineId: mcId,
        runningCount: orders.length,
        runningBatches: orders.map(o => {
          const actual = (_actualsCache?.[o.id] || _actualsCache?.[o.batchNumber] || o.actualProd || o.actualQty || 0);
          return {
            id: o.id,
            batchNumber: o.batchNumber || '',
            customer: o.customer || '',
            startDate: o.startDate || null,
            actualProd: actual,
            grossQty: o.grossQty || o.qty || 0,
          };
        }),
      });
    }
    res.json({ ok: true, machines, scannedAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// v41 P19.3: Invoice flow rework — new endpoints
// ═══════════════════════════════════════════════════════════════
// 1. GET  /api/invoice/pending-reconciliation — queue view (dispatch/planning/admin)
// 2. GET  /api/invoice/so-consumption          — full SO consumption ledger
// 3. GET  /api/invoice/so-consumption/:docEntry — single SO consumption detail
// 4. POST /api/invoice/close-legacy/:id        — mark invoices_received as legacy_closed
// 5. POST /api/invoice/scan-out-eligible/:requestId — check if request is ready for scan-out
// ═══════════════════════════════════════════════════════════════

function _v41_requireInvoiceRole(req, res) {
  const session = verifyToken(req.headers['x-session-token'] || req.query.token || req.body?.token);
  if (!session) {
    res.status(401).json({ ok: false, error: 'Not authenticated' });
    return null;
  }
  // v44L #2 FIX: the Tracking app's Dispatch Officer (role 'tracking_dispatch') operates the
  // Dispatch page where invoice generation, the Pending Reconciliation queue and scan-out live.
  // The generate endpoint is ungated (so the officer could CREATE a request, e.g. 26ZG119) but
  // these read/scan-out endpoints previously allowed only the planning-app manager roles, so the
  // officer was silently 403'd and the Pending Reconciliation queue showed empty. Add the tracking
  // dispatch role so the officer can see and act on the requests they create. ('admin' covers
  // Track_Admin too.)
  const allowed = ['dispatch_manager', 'planning_manager', 'admin', 'tracking_dispatch'];
  if (!allowed.includes(session.role)) {
    res.status(403).json({ ok: false, error: 'Forbidden — dispatch/planning/admin only' });
    return null;
  }
  return session;
}

// GET /api/invoice/pending-reconciliation — list invoice_requests awaiting SAP reconcile
app.get('/api/invoice/pending-reconciliation', async (req, res) => {
  const session = _v41_requireInvoiceRole(req, res);
  if (!session) return;
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(
        `SELECT * FROM invoice_requests
         WHERE status='pending_reconciliation'
         ORDER BY created_at DESC
         LIMIT 500`
      );
      rows = r.rows;
    } else {
      rows = db.prepare(
        `SELECT * FROM invoice_requests
         WHERE status='pending_reconciliation'
         ORDER BY created_at DESC
         LIMIT 500`
      ).all();
    }
    // v44ZB (v44AB): before returning, run the batch+customer+qty fallback for each pending
    // request. Clean single matches against a dispatched, unlinked invoice auto-reconcile and drop
    // off the list (this clears shipped orphans whose invoice arrived on a different SO than the
    // request recorded — the strict sap_doc_entry match the poller relies on misses these). Ambiguous
    // requests (multiple candidates or out-of-band qty) stay, annotated with _fallbackProposal so an
    // admin can confirm. Idempotent + race-safe via the UPDATE guards inside _applyFallbackReconcile.
    const out = [];
    for (const row of rows) {
      let outcome = null;
      try { outcome = await _fallbackReconcileRequest(row); }
      catch (e) { console.warn('[v44ZB fallback-recon] list sweep error:', e.message); }
      if (outcome && outcome.reconciled) continue;            // auto-cleared — omit from the list
      if (outcome && outcome.proposal) row._fallbackProposal = outcome.proposal;
      out.push(row);
    }
    res.json({ ok: true, count: out.length, requests: out });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/invoice/so-consumption — full sales order consumption ledger.
// Optional ?cardCode= filter to scope to one customer.
app.get('/api/invoice/so-consumption', async (req, res) => {
  const session = _v41_requireInvoiceRole(req, res);
  if (!session) return;
  try {
    const cardCode = (req.query.cardCode || '').toString();
    const whereCard = pgPool ? 'WHERE card_code=$1' : 'WHERE card_code=?';
    let rows;
    if (pgPool) {
      const r = cardCode
        ? await pgPool.query(`SELECT * FROM sales_order_consumption ${whereCard} ORDER BY updated_at DESC LIMIT 500`, [cardCode])
        : await pgPool.query(`SELECT * FROM sales_order_consumption ORDER BY updated_at DESC LIMIT 500`);
      rows = r.rows;
    } else {
      rows = cardCode
        ? db.prepare(`SELECT * FROM sales_order_consumption ${whereCard} ORDER BY updated_at DESC LIMIT 500`).all(cardCode)
        : db.prepare(`SELECT * FROM sales_order_consumption ORDER BY updated_at DESC LIMIT 500`).all();
    }
    // Compute remaining + tolerance per row
    const ledger = rows.map(r => {
      const original = parseFloat(r.original_qty_lakhs) || 0;
      const dispatched = parseFloat(r.dispatched_qty_lakhs) || 0;
      const tolerance = original * 1.15;
      const remaining = Math.max(0, original - dispatched);
      const headroom = Math.max(0, tolerance - dispatched);
      const pctDispatched = original > 0 ? (dispatched / original * 100) : 0;
      return {
        ...r,
        remainingQty: parseFloat(remaining.toFixed(3)),
        toleranceQty: parseFloat(tolerance.toFixed(3)),
        headroomQty: parseFloat(headroom.toFixed(3)),
        pctDispatched: parseFloat(pctDispatched.toFixed(2)),
        isExhausted: pctDispatched >= 100,
        isOverDispatched: pctDispatched > 100,
        isAtTolerance: pctDispatched >= 115,
      };
    });
    res.json({ ok: true, count: ledger.length, ledger });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/invoice/so-consumption/:docEntry — single SO consumption detail
app.get('/api/invoice/so-consumption/:docEntry', async (req, res) => {
  const session = _v41_requireInvoiceRole(req, res);
  if (!session) return;
  try {
    const docEntry = parseInt(req.params.docEntry, 10);
    if (!docEntry) return res.status(400).json({ ok: false, error: 'invalid docEntry' });
    let soc;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM sales_order_consumption WHERE sap_doc_entry=$1`, [docEntry]);
      soc = r.rows[0];
    } else {
      soc = db.prepare(`SELECT * FROM sales_order_consumption WHERE sap_doc_entry=?`).get(docEntry);
    }
    if (!soc) return res.json({ ok: true, soc: null, message: 'No consumption ledger entry yet — no invoices reconciled' });
    // Fetch all linked invoices for context
    let invoices = [];
    try {
      if (pgPool) {
        const ri = await pgPool.query(
          `SELECT id, sap_doc_num, invoice_date, total_qty_lakhs, total_amount, dispatch_status
           FROM invoices_received WHERE sap_doc_entry IN (
             SELECT DISTINCT sap_response_doc_entry FROM invoice_requests WHERE sap_doc_entry=$1 AND reconciled_with_invoice_id IS NOT NULL
           ) ORDER BY invoice_date DESC`,
          [docEntry]
        );
        invoices = ri.rows;
      }
    } catch {}
    const original = parseFloat(soc.original_qty_lakhs) || 0;
    const dispatched = parseFloat(soc.dispatched_qty_lakhs) || 0;
    const tolerance = original * 1.15;
    const result = {
      ...soc,
      remainingQty: parseFloat(Math.max(0, original - dispatched).toFixed(3)),
      toleranceQty: parseFloat(tolerance.toFixed(3)),
      headroomQty: parseFloat(Math.max(0, tolerance - dispatched).toFixed(3)),
      pctDispatched: original > 0 ? parseFloat((dispatched / original * 100).toFixed(2)) : 0,
      invoices
    };
    res.json({ ok: true, soc: result });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/invoice/close-legacy/:id — mark invoices_received as legacy_closed.
// Used when an invoice doesn't match any pending invoice_request (legacy stock, return, etc.)
app.post('/api/invoice/close-legacy/:id', async (req, res) => {
  const session = _v41_requireInvoiceRole(req, res);
  if (!session) return;
  try {
    const id = req.params.id;
    const { reason } = req.body || {};
    if (pgPool) {
      const r = await pgPool.query(
        `UPDATE invoices_received SET is_legacy_closed=1, legacy_closed_by=$1, legacy_closed_at=NOW()::TEXT, legacy_close_reason=$2 WHERE id=$3 AND is_legacy_closed=0`,
        [session.username, reason || '', id]
      );
      if (r.rowCount === 0) return res.status(404).json({ ok: false, error: 'Invoice not found or already legacy-closed' });
    } else {
      const r = db.prepare(
        `UPDATE invoices_received SET is_legacy_closed=1, legacy_closed_by=?, legacy_closed_at=datetime('now'), legacy_close_reason=? WHERE id=? AND is_legacy_closed=0`
      ).run(session.username, reason || '', id);
      if (r.changes === 0) return res.status(404).json({ ok: false, error: 'Invoice not found or already legacy-closed' });
    }
    logAudit(session.username, session.role, session.app || 'planning', 'INVOICE_LEGACY_CLOSED',
      `Marked invoice ${id} as legacy: ${reason || '(no reason)'}`, req.ip);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/invoice/scan-out-eligible/:requestId — check if a request is ready for scan-out.
// Returns { eligible, status, reason } so the client can gate the scan-out UI.
app.post('/api/invoice/scan-out-eligible/:requestId', async (req, res) => {
  const session = _v41_requireInvoiceRole(req, res);
  if (!session) return;
  try {
    const reqId = req.params.requestId;
    let row;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM invoice_requests WHERE id=$1`, [reqId]);
      row = r.rows[0];
    } else {
      row = db.prepare(`SELECT * FROM invoice_requests WHERE id=?`).get(reqId);
    }
    if (!row) return res.status(404).json({ ok: false, error: 'invoice_request not found' });

    // Gating logic
    let eligible = false;
    let reason = '';
    if (row.status === 'reconciled') {
      eligible = true;
      reason = 'OK — invoice reconciled with SAP. Proceed to scan-out.';
    } else if (row.status === 'pending_reconciliation') {
      eligible = false;
      reason = 'Waiting for SAP reconciliation. SAP user must create the invoice in SAP; Sunloc will pick it up on next poll (~5 min).';
    } else if (row.status === 'ready_to_scan_out' || row.status === 'dispatched') {
      eligible = (row.status === 'ready_to_scan_out');
      reason = row.status === 'dispatched' ? 'Already dispatched.' : 'OK — proceed to scan-out.';
    } else {
      eligible = false;
      reason = `Cannot scan out — invoice request status: ${row.status}`;
    }
    res.json({
      ok: true,
      eligible,
      status: row.status,
      reason,
      request: {
        id: row.id,
        batchNumber: row.batch_number,
        customer: row.customer,
        sapDocEntry: row.sap_doc_entry,
        qtyLakhs: parseFloat(row.qty_lakhs),
        boxes: row.boxes,
        reconciledAt: row.reconciled_at,
        reconciledWithInvoiceId: row.reconciled_with_invoice_id
      }
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ─── end v41 P19.3 invoice flow endpoints ────────────────────────


// ═══════════════════════════════════════════════════════════════
// DPR APP ROUTES
// ═══════════════════════════════════════════════════════════════

// POST bulk import DPR records from backup
app.post('/api/dpr/bulk-import', async (req, res) => {
  try {
    const { records } = req.body;
    if (!records || !Array.isArray(records)) return res.status(400).json({ ok: false, error: 'No records provided' });
    // v40 P18.14i Fix 2: bulk import gate — reject rows targeting non-running orders unless force=true (admin)
    const _isAdminCaller = (req.body.userRole === 'admin') || (req.headers['x-user-role'] === 'admin');
    const _forceEntry    = !!req.body.forceEntry;
    let _planForGate = null;
    let _orderStatusById = {};
    let _orderStatusByBatch = {};
    const _orderBatchById = {}; // v41ZY: explicit-batch resolution (mirrors /api/dpr/save)
    try {
      _planForGate = await getPlanningStateAsync();
      for (const o of (_planForGate.orders || [])) {
        if (o.id) _orderStatusById[o.id] = { status: o.status, deleted: o.deleted };
        if (o.batchNumber) _orderStatusByBatch[o.batchNumber] = { status: o.status, deleted: o.deleted };
        if (o.id && o.batchNumber) _orderBatchById[o.id] = o.batchNumber;
      }
    } catch(e) { console.warn('[v40 P18.14i bulk-import] planning state fetch failed; gate will allow all:', e.message); }
    const _rejected = [];
    const _gateBulk = (orderId, batchNumber, machineId, date, shift, qty) => {
      if ((orderId && String(orderId).startsWith('TEMP-')) ||
          (batchNumber && String(batchNumber).startsWith('TEMP-'))) return true;
      const meta = _orderStatusById[orderId] || _orderStatusByBatch[batchNumber];
      if (!meta) return true;
      if (meta.deleted) { _rejected.push({ orderId, batchNumber, machineId, date, shift, qty, reason: 'deleted' }); return false; }
      if (meta.status === 'running') return true;
      // v41ZR Issue 4: a batch wound down mid-shift (its final partial production recorded while the
      // next batch takes the machine's running slot) has its planning status moved off 'running'. Its
      // last entry was previously dropped (DELETE+skip) — leaving the batch short of gross → perpetual
      // phantom pending → forced manual close. Allow the entry when the batch already has accumulated
      // production (a real, previously-running batch finishing up). Never-started orders in a wrong
      // status have no prior actuals and stay gated; DPR-closed/deleted are already blocked above.
      if ((batchNumber && _grossByBatch && (_grossByBatch[batchNumber] || 0) > 0) ||
          (orderId && _actualsCache && (_actualsCache[orderId] || 0) > 0)) return true;
      if (_isAdminCaller && _forceEntry) {
        console.warn(`[v40 P18.14i Fix 2 bulk] force-import: ${qty}L to ${meta.status} order ${orderId||batchNumber} on ${machineId} ${date}/${shift}`);
        return true;
      }
      _rejected.push({ orderId, batchNumber, machineId, date, shift, qty, reason: `status='${meta.status}' — not running` });
      return false;
    };
    let saved = 0, skipped = 0;
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
                if (!_gateBulk(run.orderId, run.batchNumber, machineId, date, shiftName, qty)) { skipped++; continue; }
                await client.query(`INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                  ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET
                  order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number, qty_lakhs=EXCLUDED.qty_lakhs`,
                  [run.orderId||null, (run.batchNumber || _orderBatchById[run.orderId] || null), machineId, date, shiftName, ri, qty, floor]);
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
    res.json({ ok: true, saved, skipped, rejected: _rejected });
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

      // v43 #1: snapshot existing actuals BEFORE the blanket delete so a post-close re-save cannot
      // silently erase a DPR-closed batch's already-recorded production. After the gated re-insert we
      // restore any snapshot row that (a) was NOT re-saved this round and (b) belongs to a DPR-closed
      // batch. New entry to a closed batch (no prior row) stays blocked; legitimate slot reassignment
      // (same machine/shift/run re-saved) still wins. One scoped SELECT per save; restores are rare.
      let _preDeleteActuals = [];
      try {
        const _snap = await pgPool.query(
          'SELECT order_id, batch_number, machine_id, shift, run_index, qty_lakhs, floor FROM production_actuals WHERE floor = $1 AND date = $2',
          [floor, date]
        );
        _preDeleteActuals = _snap.rows || [];
      } catch (e) { console.warn('[v43 #1] pre-delete snapshot failed:', e.message); }

      // Delete old actuals for this floor+date, then re-insert
      await pgPool.query('DELETE FROM production_actuals WHERE floor = $1 AND date = $2', [floor, date]);

      // v41h FIX (issues 1 & 2): build the gate status map from the AUTHORITATIVE production_orders
      // table — that is where changeOrderStatus → upsertOrderToDB writes the planner's "In Production"
      // status. The planning_state JSON blob lags (only rewritten on full saveState), so reading
      // status from the blob alone made freshly-promoted "running" orders still look "pending",
      // wrongly rejecting their DPR entries. We seed from the blob, then OVERLAY production_orders
      // (newest wins) so the latest planner-set status is always honoured.
      const _planForGate = await getPlanningStateAsync();
      const _orderStatusById = {};
      const _orderStatusByBatch = {};
      // v41ZY: order -> batch map so every production_actuals row can be stored with an EXPLICIT
      // batch_number. Previously a run with a blank batch was saved as NULL and the per-batch DPR
      // gross relied on a LIVE-planning-state fallback that broke when the batch closed (its order
      // left active state) — collapsing the cumulative to only the dates that had an explicit batch.
      const _orderBatchById = {};
      for (const o of (_planForGate.orders || [])) {
        if (o.id) _orderStatusById[o.id] = { status: o.status, deleted: o.deleted };
        if (o.batchNumber) _orderStatusByBatch[o.batchNumber] = { status: o.status, deleted: o.deleted };
        if (o.id && o.batchNumber) _orderBatchById[o.id] = o.batchNumber;
      }
      // Overlay authoritative production_orders rows (status column is the source of truth).
      try {
        let _poRows;
        if (pgPool) {
          const _r = await pgPool.query(`SELECT id, batch_number, status, deleted FROM production_orders`);
          _poRows = _r.rows;
        } else {
          _poRows = db.prepare(`SELECT id, batch_number, status, deleted FROM production_orders`).all();
        }
        for (const r of (_poRows || [])) {
          const meta = { status: r.status, deleted: (r.deleted === true || r.deleted === 1) };
          if (r.id) _orderStatusById[r.id] = meta;
          if (r.batch_number) _orderStatusByBatch[r.batch_number] = meta;
          // production_orders is authoritative for an order's batch and includes CLOSED orders, so
          // this resolves the batch even after close (the case that previously broke attribution).
          if (r.id && r.batch_number) _orderBatchById[r.id] = r.batch_number;
        }
      } catch (e) {
        console.warn('[v41h DPR gate] production_orders overlay failed, using blob status:', e.message);
      }
      // v41l (point 2): load DPR-closed batches — entries to a closed batch are rejected (unless it
      // was reopened, in which case the dpr_batch_closed row is gone and it's not in this set).
      const _closedSet = new Set();
      try {
        let _cRows;
        if (pgPool) _cRows = (await pgPool.query('SELECT order_id, batch_number FROM dpr_batch_closed')).rows;
        else _cRows = db.prepare('SELECT order_id, batch_number FROM dpr_batch_closed').all();
        for (const r of (_cRows || [])) { if (r.order_id) _closedSet.add('id:'+r.order_id); if (r.batch_number) _closedSet.add('bn:'+r.batch_number); }
      } catch (e) { console.warn('[v41l DPR gate] closed-batch load failed:', e.message); }
      const _isAdminCaller = (req.body.userRole === 'admin') || (req.headers['x-user-role'] === 'admin');
      const _forceEntry    = !!req.body.forceEntry;
      const _rejected = [];

      const actualsToSave = [];
      const _gateRow = (orderId, batchNumber, machineId, shift, qty) => {
        // Allow TEMP batches unconditionally (fallback path when no real order assigned)
        if ((orderId && String(orderId).startsWith('TEMP-')) ||
            (batchNumber && String(batchNumber).startsWith('TEMP-'))) return true;
        // v41l (point 2): block entries to DPR-closed batches (reopen removes them from this set).
        if (_closedSet.has('id:'+orderId) || (batchNumber && _closedSet.has('bn:'+batchNumber))) {
          if (_isAdminCaller && _forceEntry) {
            try { logAudit(req.body.userName||'admin','admin','dpr','DPR_FORCE_ENTRY_CLOSED',`Wrote ${qty}L to CLOSED batch ${batchNumber||orderId} on ${machineId} ${date}/${shift}`); } catch {}
            return true;
          }
          _rejected.push({ orderId, batchNumber, machineId, shift, qty, reason: `Batch ${batchNumber||orderId} is CLOSED in DPR — reopen it (same day, once) before entering data` });
          return false;
        }
        const meta = _orderStatusById[orderId] || _orderStatusByBatch[batchNumber];
        if (!meta) return true;   // unknown order → don't block (could be a legacy/orphan)
        if (meta.deleted) {
          _rejected.push({ orderId, batchNumber, machineId, shift, qty, reason: 'Order is deleted' });
          return false;
        }
        if (meta.status === 'running') return true;
        // v41ZR Issue 4: a batch wound down mid-shift (its final partial production recorded while the
        // next batch takes the machine's running slot) has its planning status moved off 'running'. Its
        // last entry was previously dropped (DELETE+skip) — leaving the batch short of gross → perpetual
        // phantom pending → forced manual close. Allow the entry when the batch already has accumulated
        // production (a real, previously-running batch finishing up). Never-started orders in a wrong
        // status have no prior actuals and stay gated; DPR-closed/deleted are already blocked above.
        if ((batchNumber && _grossByBatch && (_grossByBatch[batchNumber] || 0) > 0) ||
            (orderId && _actualsCache && (_actualsCache[orderId] || 0) > 0)) return true;
        // Non-running order
        if (_isAdminCaller && _forceEntry) {
          // Admin force-entry — allow but audit
          console.warn(`[v40 P18.14i Fix 2] DPR force-entry: admin wrote ${qty}L to ${meta.status} order ${orderId||batchNumber} on ${machineId} ${date}/${shift}. caller=${req.body.userName||'unknown'}`);
          try { logAudit(req.body.userName||'admin','admin','dpr','DPR_FORCE_ENTRY',`Wrote ${qty}L to ${meta.status} order ${orderId||batchNumber} on ${machineId} ${date}/${shift}`); } catch {}
          return true;
        }
        _rejected.push({ orderId, batchNumber, machineId, shift, qty, reason: `Order status is '${meta.status}' — only running orders accept DPR entries` });
        return false;
      };

      if (actuals && actuals.length > 0) {
        for (const a of actuals) {
          if (!a.qty || a.qty <= 0) continue;
          if (!_gateRow(a.orderId, a.batchNumber, a.machineId, a.shift, a.qty)) continue;
          actualsToSave.push([a.orderId||null, (a.batchNumber || _orderBatchById[a.orderId] || null), a.machineId, date, a.shift, a.runIndex||0, a.qty, a.floor||floor]);
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
              if (!_gateRow(run.orderId, run.batchNumber, machineId, shiftName, qty)) return;
              actualsToSave.push([run.orderId||null, (run.batchNumber || _orderBatchById[run.orderId] || null), machineId, date, shiftName, ri, qty, floor]);
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

      // v43 #1: restore DPR-closed batches' production that this re-save would have dropped. A snapshot
      // row is "dropped" when its (machine,shift,run_index) slot was NOT re-saved this round; restore it
      // only when its batch/order is DPR-closed (preserve historical production). This is what fixes the
      // "close running batch → select next → recent days vanish" loss: the gate refuses to re-insert the
      // closed batch's runs after the blanket delete, so we put back exactly those that belonged to it.
      if (_preDeleteActuals.length) {
        const _savedKeys = new Set(actualsToSave.map(r => `${r[2]}|${r[4]}|${r[5]}`));
        let _restored = 0;
        for (const s of _preDeleteActuals) {
          if (_savedKeys.has(`${s.machine_id}|${s.shift}|${s.run_index}`)) continue;  // slot legitimately re-saved
          const _closed = _closedSet.has('id:'+s.order_id) || (s.batch_number && _closedSet.has('bn:'+s.batch_number));
          if (!_closed) continue;                                                      // only preserve closed-batch history
          if (!(parseFloat(s.qty_lakhs) > 0)) continue;
          await pgPool.query(
            `INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
             ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET
               order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number,
               qty_lakhs=EXCLUDED.qty_lakhs, synced_at=NOW()`,
            [s.order_id||null, s.batch_number||null, s.machine_id, date, s.shift, s.run_index, s.qty_lakhs, s.floor||floor]
          );
          _restored++;
        }
        if (_restored) console.log(`[v43 #1] preserved ${_restored} closed-batch actual row(s) on ${floor}/${date} a re-save would have dropped`);
      }

      // Attach rejected list to res so client can show error
      if (_rejected.length > 0) {
        res._dprRejected = _rejected;
      }

      // Update planning actuals cache (two-way sync) — warm cache so Planning sees fresh data
      try {
        await warmActualsCache();
      } catch(e) { console.warn('Planning sync error:', e.message); }

    } else {
      // SQLite fallback — same gate logic
      const _planForGateSq = await getPlanningStateAsync();
      const _orderStatusByIdSq = {};
      const _orderStatusByBatchSq = {};
      const _orderBatchByIdSq = {}; // v41ZY: explicit-batch resolution (mirrors PG path)
      for (const o of (_planForGateSq.orders || [])) {
        if (o.id) _orderStatusByIdSq[o.id] = { status: o.status, deleted: o.deleted };
        if (o.batchNumber) _orderStatusByBatchSq[o.batchNumber] = { status: o.status, deleted: o.deleted };
        if (o.id && o.batchNumber) _orderBatchByIdSq[o.id] = o.batchNumber;
      }
      // v41h FIX (issues 1 & 2): overlay authoritative production_orders status (see PG-path note).
      try {
        let _poRowsSq;
        if (pgPool) {
          const _r = await pgPool.query(`SELECT id, batch_number, status, deleted FROM production_orders`);
          _poRowsSq = _r.rows;
        } else {
          _poRowsSq = db.prepare(`SELECT id, batch_number, status, deleted FROM production_orders`).all();
        }
        for (const r of (_poRowsSq || [])) {
          const meta = { status: r.status, deleted: (r.deleted === true || r.deleted === 1) };
          if (r.id) _orderStatusByIdSq[r.id] = meta;
          if (r.batch_number) _orderStatusByBatchSq[r.batch_number] = meta;
          if (r.id && r.batch_number) _orderBatchByIdSq[r.id] = r.batch_number;
        }
      } catch (e) {
        console.warn('[v41h DPR gate Sq] production_orders overlay failed, using blob status:', e.message);
      }
      // v41l (point 2): closed-batch set for the SQLite path.
      const _closedSetSq = new Set();
      try {
        let _cRowsSq;
        if (pgPool) _cRowsSq = (await pgPool.query('SELECT order_id, batch_number FROM dpr_batch_closed')).rows;
        else _cRowsSq = db.prepare('SELECT order_id, batch_number FROM dpr_batch_closed').all();
        for (const r of (_cRowsSq || [])) { if (r.order_id) _closedSetSq.add('id:'+r.order_id); if (r.batch_number) _closedSetSq.add('bn:'+r.batch_number); }
      } catch (e) { console.warn('[v41l DPR gate Sq] closed-batch load failed:', e.message); }
      const _isAdminCallerSq = (req.body.userRole === 'admin') || (req.headers['x-user-role'] === 'admin');
      const _forceEntrySq    = !!req.body.forceEntry;
      const _rejectedSq = [];
      const _gateSq = (orderId, batchNumber, machineId, shift, qty) => {
        if ((orderId && String(orderId).startsWith('TEMP-')) ||
            (batchNumber && String(batchNumber).startsWith('TEMP-'))) return true;
        // v41l (point 2): block entries to DPR-closed batches.
        if (_closedSetSq.has('id:'+orderId) || (batchNumber && _closedSetSq.has('bn:'+batchNumber))) {
          if (_isAdminCallerSq && _forceEntrySq) {
            try { logAudit(req.body.userName||'admin','admin','dpr','DPR_FORCE_ENTRY_CLOSED',`Wrote ${qty}L to CLOSED batch ${batchNumber||orderId} on ${machineId} ${date}/${shift}`); } catch {}
            return true;
          }
          _rejectedSq.push({ orderId, batchNumber, machineId, shift, qty, reason: `Batch ${batchNumber||orderId} is CLOSED in DPR — reopen it (same day, once) before entering data` });
          return false;
        }
        const meta = _orderStatusByIdSq[orderId] || _orderStatusByBatchSq[batchNumber];
        if (!meta) return true;
        if (meta.deleted) {
          _rejectedSq.push({ orderId, batchNumber, machineId, shift, qty, reason: 'Order is deleted' });
          return false;
        }
        if (meta.status === 'running') return true;
        // v41ZR Issue 4: a batch wound down mid-shift (its final partial production recorded while the
        // next batch takes the machine's running slot) has its planning status moved off 'running'. Its
        // last entry was previously dropped (DELETE+skip) — leaving the batch short of gross → perpetual
        // phantom pending → forced manual close. Allow the entry when the batch already has accumulated
        // production (a real, previously-running batch finishing up). Never-started orders in a wrong
        // status have no prior actuals and stay gated; DPR-closed/deleted are already blocked above.
        if ((batchNumber && _grossByBatch && (_grossByBatch[batchNumber] || 0) > 0) ||
            (orderId && _actualsCache && (_actualsCache[orderId] || 0) > 0)) return true;
        if (_isAdminCallerSq && _forceEntrySq) {
          console.warn(`[v40 P18.14i Fix 2 SQLite] DPR force-entry: admin wrote ${qty}L to ${meta.status} order ${orderId||batchNumber}`);
          return true;
        }
        _rejectedSq.push({ orderId, batchNumber, machineId, shift, qty, reason: `Order status is '${meta.status}' — only running orders accept DPR entries` });
        return false;
      };
      db.prepare(`INSERT INTO dpr_records (floor, date, data_json) VALUES (?, ?, ?) ON CONFLICT(floor, date) DO UPDATE SET data_json = excluded.data_json, saved_at = datetime('now')`).run(floor, date, JSON.stringify(data));
      // v43 #1: snapshot before the blanket delete so a post-close re-save can't erase a DPR-closed
      // batch's recorded production (see PG path for rationale). Restored after the gated re-insert.
      let _preDeleteActualsSq = [];
      try { _preDeleteActualsSq = db.prepare('SELECT order_id, batch_number, machine_id, shift, run_index, qty_lakhs, floor FROM production_actuals WHERE floor = ? AND date = ?').all(floor, date) || []; }
      catch (e) { console.warn('[v43 #1 SQLite] pre-delete snapshot failed:', e.message); }
      db.prepare('DELETE FROM production_actuals WHERE floor = ? AND date = ?').run(floor, date);
      const upsert = db.prepare(`INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET order_id=excluded.order_id, batch_number=excluded.batch_number, qty_lakhs=excluded.qty_lakhs, synced_at=datetime('now')`);
      const rows = (actuals && actuals.length > 0)
        ? actuals.filter(a => a.qty > 0 && _gateSq(a.orderId, a.batchNumber, a.machineId, a.shift, a.qty))
                 .map(a => [a.orderId||null, (a.batchNumber || _orderBatchByIdSq[a.orderId] || null), a.machineId, date, a.shift, a.runIndex||0, a.qty, a.floor||floor])
        : [];
      db.transaction(rows => rows.forEach(r => upsert.run(...r)))(rows);
      // v43 #1: restore DPR-closed batches' production dropped by this re-save (mirror of PG path).
      if (_preDeleteActualsSq.length) {
        const _savedKeysSq = new Set(rows.map(r => `${r[2]}|${r[4]}|${r[5]}`));
        let _restoredSq = 0;
        for (const s of _preDeleteActualsSq) {
          if (_savedKeysSq.has(`${s.machine_id}|${s.shift}|${s.run_index}`)) continue;
          const _closed = _closedSetSq.has('id:'+s.order_id) || (s.batch_number && _closedSetSq.has('bn:'+s.batch_number));
          if (!_closed) continue;
          if (!(parseFloat(s.qty_lakhs) > 0)) continue;
          upsert.run(s.order_id||null, s.batch_number||null, s.machine_id, date, s.shift, s.run_index, s.qty_lakhs, s.floor||floor);
          _restoredSq++;
        }
        if (_restoredSq) console.log(`[v43 #1 SQLite] preserved ${_restoredSq} closed-batch actual row(s) on ${floor}/${date}`);
      }
      if (_rejectedSq.length > 0) res._dprRejected = _rejectedSq;
    }

    // Refresh actuals cache so Planning sees new DPR data immediately (force — bypass throttle)
    _actualsCacheTime = 0; // bypass 60s throttle so save is visible immediately
    warmActualsCache().catch(e => console.warn('[DPR] cache warm failed:', e.message));
    // v40 P18.14i Fix 2: include any gate rejections so client can alert the operator
    const rejected = res._dprRejected || [];
    res.json({
      ok: true,
      savedAt: new Date().toISOString(),
      rejectedCount: rejected.length,
      rejected: rejected,
    });
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

    // v41ZY FIX: Before marking as closed, flush any pending actuals for this batch
    // from ALL dpr_records. This ensures today's qty is written to production_actuals
    // before the gate starts rejecting saves for this closed batch.
    if (pgPool) {
      try {
        const allRecords = await pgPool.query(`SELECT floor, date, data_json FROM dpr_records`);
        for (const rec of allRecords.rows) {
          const data = typeof rec.data_json === 'string' ? JSON.parse(rec.data_json) : rec.data_json;
          const shifts = data.shifts || {};
          for (const [shiftName, shiftData] of Object.entries(shifts)) {
            if (!shiftData.machines) continue;
            for (const [machineId, machineData] of Object.entries(shiftData.machines)) {
              const runs = machineData.runs || [];
              for (let ri = 0; ri < runs.length; ri++) {
                const run = runs[ri];
                if ((run.orderId !== orderId) && (run.batchNumber !== batchNumber)) continue;
                const qty = parseFloat(run.qty) || 0;
                if (qty <= 0) continue;
                await pgPool.query(
                  `INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET
                   order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number, qty_lakhs=EXCLUDED.qty_lakhs`,
                  [run.orderId||orderId, batchNumber||run.batchNumber||null, machineId, rec.date, shiftName, ri, qty, rec.floor]
                );
              }
            }
          }
        }
        console.log(`[batch-close] flushed actuals for ${batchNumber||orderId} before close`);
      } catch (flushErr) {
        console.warn('[batch-close] actuals flush warning:', flushErr.message);
        // Non-fatal — proceed with close even if flush has issues
      }

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

    // Refresh actuals cache after flush+close
    try { await warmActualsCache(); } catch {}

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

// v41l: REOPEN a batch in DPR — Production Manager gets ONE reopen per batch, same IST day only.
// Guards: (1) the batch must currently be closed; (2) it must not have been reopened before
// (dpr_batch_reopen_log row absent); (3) the close must be on the SAME IST calendar day as now.
// On success: delete the dpr_batch_closed row (so data entry is allowed again) AND write a
// reopen-log row (so a second reopen is permanently blocked). The client also audit-logs it.
function _istYMD(dtStr) {
  // Convert a stored timestamp (UTC-ish 'YYYY-MM-DD HH:MM:SS' or ISO) to the IST calendar date.
  // All users are IST (UTC+5:30). datetime('now') / NOW() store UTC; add 5h30m then take the date.
  try {
    let d = dtStr ? new Date(dtStr.replace(' ', 'T') + (/[zZ]|[+\-]\d\d:?\d\d$/.test(dtStr) ? '' : 'Z')) : new Date();
    if (isNaN(d.getTime())) d = new Date(dtStr);
    const ist = new Date(d.getTime() + (5 * 60 + 30) * 60000);
    return ist.toISOString().slice(0, 10);
  } catch { return null; }
}
app.post('/api/dpr/batch-reopen', async (req, res) => {
  try {
    const { orderId, batchNumber, reopenedBy } = req.body;
    if (!orderId) return res.status(400).json({ ok: false, error: 'orderId required' });

    // Look up the current closed row + any prior reopen.
    let closedRow, reopenRow;
    if (pgPool) {
      closedRow = (await pgPool.query('SELECT order_id, batch_number, closed_at FROM dpr_batch_closed WHERE order_id=$1', [orderId])).rows[0];
      reopenRow = (await pgPool.query('SELECT order_id FROM dpr_batch_reopen_log WHERE order_id=$1', [orderId])).rows[0];
    } else {
      closedRow = db.prepare('SELECT order_id, batch_number, closed_at FROM dpr_batch_closed WHERE order_id=?').get(orderId);
      reopenRow = db.prepare('SELECT order_id FROM dpr_batch_reopen_log WHERE order_id=?').get(orderId);
    }

    if (!closedRow) return res.status(409).json({ ok: false, error: 'Batch is not currently closed in DPR.' });
    if (reopenRow)  return res.status(409).json({ ok: false, error: 'This batch has already been reopened once and cannot be reopened again. Contact Admin.' });

    // Same-IST-day guard: the close date (IST) must equal today (IST).
    const closeDayIST = _istYMD(closedRow.closed_at);
    const todayIST    = _istYMD(new Date().toISOString());
    if (closeDayIST && todayIST && closeDayIST !== todayIST) {
      return res.status(409).json({ ok: false, error: `Reopen window expired. A batch can only be reopened on the same day it was closed (closed ${closeDayIST}, today ${todayIST}). Contact Admin.` });
    }

    // Perform: record the reopen (blocks future reopens) then delete the closed row.
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO dpr_batch_reopen_log (order_id, batch_number, closed_at, reopened_at, reopened_by)
         VALUES ($1,$2,$3,NOW(),$4) ON CONFLICT(order_id) DO NOTHING`,
        [orderId, batchNumber || closedRow.batch_number || null, closedRow.closed_at || null, reopenedBy || null]
      );
      await pgPool.query('DELETE FROM dpr_batch_closed WHERE order_id=$1', [orderId]);
    } else {
      db.prepare(`INSERT OR IGNORE INTO dpr_batch_reopen_log (order_id, batch_number, closed_at, reopened_at, reopened_by)
        VALUES (?, ?, ?, datetime('now'), ?)`).run(orderId, batchNumber || closedRow.batch_number || null, closedRow.closed_at || null, reopenedBy || null);
      db.prepare('DELETE FROM dpr_batch_closed WHERE order_id=?').run(orderId);
    }
    res.json({ ok: true, reopenedAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════ v41ZZ: Retire stale / legacy batches ═══════════════
// Retire = "as-is-where-is" close of an unclosed/never-dispatched batch. It is STATUS-ONLY:
//   • records a retired marker (audited: who/when/reason; reversible via prev_* snapshot)
//   • sets Planning status=closed and inserts dpr_batch_closed
//   • the batch's WIP is EXCLUDED (treated as 0) at every WIP site (server wipLakhs + the three
//     client sites), so phantom WIP for a physically-gone batch disappears.
// It touches NO production_actuals / scan / wastage data — so A-Grade, gross and average production
// stay exactly as-is. (This is deliberately NOT /api/dpr/batch-close, so IT's pre-close actuals
// flush does NOT run — retire must never materialise new numbers.) Reconcile WIP remains the
// separate data-entry path that DOES move A-Grade/WIP.
let _retiredBatchSet = new Set();
async function loadRetiredBatches() {
  try {
    let rows;
    if (pgPool) rows = (await pgPool.query('SELECT batch_number FROM retired_batches')).rows;
    else rows = db.prepare('SELECT batch_number FROM retired_batches').all();
    _retiredBatchSet = new Set(rows.map(r => (r.batch_number || '').toUpperCase()));
  } catch (e) { console.warn('[retire] loadRetiredBatches failed:', e.message); }
}

// POST /api/batch/retire — bulk. body: { batches:[{batchNumber,orderId,prodMonth,residualWip}], by, reason }
app.post('/api/batch/retire', async (req, res) => {
  try {
    const list = Array.isArray(req.body && req.body.batches) ? req.body.batches : [];
    const by = ((req.body && req.body.by) || 'admin').toString().slice(0, 120);
    const reason = ((req.body && req.body.reason) || '').toString().slice(0, 500);
    if (!list.length) return res.status(400).json({ ok: false, error: 'no batches' });
    const nowIso = new Date().toISOString();
    let retired = 0;
    for (const item of list) {
      const batchNumber = ((item && item.batchNumber) || '').toString().trim();
      if (!batchNumber) continue;
      let orderId = ((item && item.orderId) || '').toString().trim() || null;
      if (!orderId) { // robustness: resolve the production order from the batch number
        try {
          if (pgPool) orderId = (await pgPool.query('SELECT id FROM production_orders WHERE batch_number=$1 LIMIT 1', [batchNumber])).rows[0]?.id || null;
          else orderId = (db.prepare('SELECT id FROM production_orders WHERE batch_number=? LIMIT 1').get(batchNumber) || {}).id || null;
        } catch (e) {}
      }
      const prodMonth = ((item && item.prodMonth) || '').toString().slice(0, 7) || null;
      const residualWip = parseFloat(item && item.residualWip || 0) || 0;
      let prevStatus = null, prevDprClosed = 0;
      if (pgPool) {
        if (orderId) {
          prevStatus = (await pgPool.query('SELECT status FROM production_orders WHERE id=$1', [orderId])).rows[0]?.status || null;
          prevDprClosed = (await pgPool.query('SELECT 1 FROM dpr_batch_closed WHERE order_id=$1', [orderId])).rows.length ? 1 : 0;
        }
        await pgPool.query(
          `INSERT INTO retired_batches (batch_number, order_id, retired_at, retired_by, reason, prod_month, residual_wip, prev_order_status, prev_dpr_closed)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
           ON CONFLICT(batch_number) DO UPDATE SET retired_at=EXCLUDED.retired_at, retired_by=EXCLUDED.retired_by, reason=EXCLUDED.reason, residual_wip=EXCLUDED.residual_wip`,
          [batchNumber, orderId, nowIso, by, reason, prodMonth, residualWip, prevStatus, prevDprClosed]);
        if (orderId) {
          await pgPool.query(`UPDATE production_orders SET status='closed' WHERE id=$1`, [orderId]);
          if (!prevDprClosed) await pgPool.query(
            `INSERT INTO dpr_batch_closed (order_id, batch_number, closed_at, closed_by, notes)
             VALUES ($1,$2,$3,$4,$5) ON CONFLICT(order_id) DO NOTHING`,
            [orderId, batchNumber, nowIso, by, 'retired (legacy cleanup)']);
        }
      } else {
        if (orderId) {
          prevStatus = (db.prepare('SELECT status FROM production_orders WHERE id=?').get(orderId) || {}).status || null;
          prevDprClosed = db.prepare('SELECT 1 FROM dpr_batch_closed WHERE order_id=?').get(orderId) ? 1 : 0;
        }
        db.prepare(`INSERT OR REPLACE INTO retired_batches (batch_number, order_id, retired_at, retired_by, reason, prod_month, residual_wip, prev_order_status, prev_dpr_closed)
                    VALUES (?,?,?,?,?,?,?,?,?)`)
          .run(batchNumber, orderId, nowIso, by, reason, prodMonth, residualWip, prevStatus, prevDprClosed);
        if (orderId) {
          db.prepare(`UPDATE production_orders SET status='closed' WHERE id=?`).run(orderId);
          if (!prevDprClosed) db.prepare(`INSERT OR IGNORE INTO dpr_batch_closed (order_id, batch_number, closed_at, closed_by, notes) VALUES (?,?,?,?,?)`)
            .run(orderId, batchNumber, nowIso, by, 'retired (legacy cleanup)');
        }
      }
      retired++;
    }
    await loadRetiredBatches();
    try { await warmPlanningCache(); } catch (e) {}
    console.log(`[retire] ${retired} batch(es) retired by ${by}`);
    res.json({ ok: true, retired });
  } catch (err) { console.error('[retire] error', err.message); res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/batch/unretire — reverse a retire. body: { batches:[batchNumber,...] }
app.post('/api/batch/unretire', async (req, res) => {
  try {
    const list = Array.isArray(req.body && req.body.batches) ? req.body.batches : [];
    if (!list.length) return res.status(400).json({ ok: false, error: 'no batches' });
    let restored = 0;
    for (const bn of list) {
      const batchNumber = (bn || '').toString().trim();
      if (!batchNumber) continue;
      let row;
      if (pgPool) row = (await pgPool.query('SELECT * FROM retired_batches WHERE batch_number=$1', [batchNumber])).rows[0];
      else row = db.prepare('SELECT * FROM retired_batches WHERE batch_number=?').get(batchNumber);
      if (!row) continue;
      const orderId = row.order_id;
      if (pgPool) {
        if (orderId && row.prev_order_status) await pgPool.query(`UPDATE production_orders SET status=$1 WHERE id=$2`, [row.prev_order_status, orderId]);
        if (orderId && !row.prev_dpr_closed) await pgPool.query('DELETE FROM dpr_batch_closed WHERE order_id=$1', [orderId]);
        await pgPool.query('DELETE FROM retired_batches WHERE batch_number=$1', [batchNumber]);
      } else {
        if (orderId && row.prev_order_status) db.prepare(`UPDATE production_orders SET status=? WHERE id=?`).run(row.prev_order_status, orderId);
        if (orderId && !row.prev_dpr_closed) db.prepare('DELETE FROM dpr_batch_closed WHERE order_id=?').run(orderId);
        db.prepare('DELETE FROM retired_batches WHERE batch_number=?').run(batchNumber);
      }
      restored++;
    }
    await loadRetiredBatches();
    try { await warmPlanningCache(); } catch (e) {}
    console.log(`[retire] ${restored} batch(es) un-retired`);
    res.json({ ok: true, restored });
  } catch (err) { console.error('[unretire] error', err.message); res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/batch/retired — list retired batches (client uses for WIP exclusion + Report Z display)
app.get('/api/batch/retired', async (req, res) => {
  try {
    let rows;
    if (pgPool) rows = (await pgPool.query('SELECT batch_number, order_id, retired_at, retired_by, reason, prod_month, residual_wip FROM retired_batches')).rows;
    else rows = db.prepare('SELECT batch_number, order_id, retired_at, retired_by, reason, prod_month, residual_wip FROM retired_batches').all();
    res.json({ ok: true, retired: rows });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── v44E Issue#1: WIP reconciliation OVERRIDE (admin-typed authoritative values) ──
// Upsert a per-batch override of Gross/A-Grade/Packing/WIP/Wastage (all Lakhs). Reports read
// these in place of scan-derived values. Reversible via /clear. No scan/DPR/wastage rows touched.
app.post('/api/batch/reconcile-override', async (req, res) => {
  try {
    const { batchNumber, gross, aGrade, packing, wip, wastage, reason, by } = req.body || {};
    if (!batchNumber) return res.status(400).json({ ok:false, error:'batchNumber required' });
    const num = v => (v===''||v===null||v===undefined||isNaN(parseFloat(v))) ? null : parseFloat(v);
    const g=num(gross), a=num(aGrade), p=num(packing), w=num(wip), ws=num(wastage);
    const who = (by||'admin').toString().slice(0,60);
    const rsn = (reason||'').toString().slice(0,300);
    const ts = new Date().toISOString();
    const details = JSON.stringify({ batchNumber, gross:g, aGrade:a, packing:p, wip:w, wastage:ws, reason:rsn, ts });
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO batch_reconcile_override (batch_number,gross,a_grade,packing,wip,wastage,reason,by_user,ts)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT (batch_number) DO UPDATE SET gross=$2,a_grade=$3,packing=$4,wip=$5,wastage=$6,reason=$7,by_user=$8,ts=$9`,
        [batchNumber,g,a,p,w,ws,rsn,who,ts]);
      await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,'admin','tracking','RECONCILE_OVERRIDE_SET',$2)`, [who, details]);
    } else {
      db.prepare(`INSERT INTO batch_reconcile_override (batch_number,gross,a_grade,packing,wip,wastage,reason,by_user,ts)
         VALUES (?,?,?,?,?,?,?,?,?)
         ON CONFLICT(batch_number) DO UPDATE SET gross=excluded.gross,a_grade=excluded.a_grade,packing=excluded.packing,wip=excluded.wip,wastage=excluded.wastage,reason=excluded.reason,by_user=excluded.by_user,ts=excluded.ts`)
        .run(batchNumber,g,a,p,w,ws,rsn,who,ts);
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,'admin','tracking','RECONCILE_OVERRIDE_SET',?)`).run(who, details);
    }
    res.json({ ok:true, ts });
  } catch (err) { res.status(500).json({ ok:false, error: err.message }); }
});

app.post('/api/batch/reconcile-override/clear', async (req, res) => {
  try {
    const { batchNumber, by } = req.body || {};
    if (!batchNumber) return res.status(400).json({ ok:false, error:'batchNumber required' });
    const who = (by||'admin').toString().slice(0,60);
    if (pgPool) {
      await pgPool.query(`DELETE FROM batch_reconcile_override WHERE batch_number=$1`, [batchNumber]);
      await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,'admin','tracking','RECONCILE_OVERRIDE_CLEAR',$2)`, [who, JSON.stringify({batchNumber})]);
    } else {
      db.prepare(`DELETE FROM batch_reconcile_override WHERE batch_number=?`).run(batchNumber);
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,'admin','tracking','RECONCILE_OVERRIDE_CLEAR',?)`).run(who, JSON.stringify({batchNumber}));
    }
    res.json({ ok:true });
  } catch (err) { res.status(500).json({ ok:false, error: err.message }); }
});

app.get('/api/batch/reconcile-overrides', async (req, res) => {
  try {
    let rows;
    if (pgPool) rows = (await pgPool.query('SELECT batch_number,gross,a_grade,packing,wip,wastage,reason,by_user,ts FROM batch_reconcile_override')).rows;
    else rows = db.prepare('SELECT batch_number,gross,a_grade,packing,wip,wastage,reason,by_user,ts FROM batch_reconcile_override').all();
    res.json({ ok:true, overrides: rows });
  } catch (err) { res.status(500).json({ ok:false, error: err.message }); }
});

// GET all DPR-closed batches (used by Planning to gate close button)
app.get('/api/dpr/batch-closed', async (req, res) => {
  try {
    let rows, reopened;
    if (pgPool) {
      const r = await pgPool.query('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed');
      rows = r.rows;
      reopened = (await pgPool.query('SELECT order_id FROM dpr_batch_reopen_log')).rows;
    } else {
      rows = db.prepare('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed').all();
      reopened = db.prepare('SELECT order_id FROM dpr_batch_reopen_log').all();
    }
    const reopenedSet = new Set((reopened || []).map(r => r.order_id));
    // Annotate each closed row with whether this batch has already used its one reopen.
    rows = (rows || []).map(r => ({ ...r, alreadyReopened: reopenedSet.has(r.order_id) }));
    res.json({ ok: true, closed: rows, reopenedOrderIds: Array.from(reopenedSet) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// v41ZI Item 6: DPR Closed-Batches report + batch-level gross override
// MUST be declared BEFORE the /api/dpr/:floor/:date catch-all below, otherwise
// "closed-batches" / "gross-override" would be captured as a :floor param.
// ═══════════════════════════════════════════════════════════════

// GET /api/dpr/closed-batches — machine-wise list of all closed batches with planned vs actual DPR gross.
// One row per closed production order. "Actual DPR Gross" = override (if set) else SUM(production_actuals).
app.get('/api/dpr/closed-batches', async (req, res) => {
  try {
    // v41ZK: do NOT await the heavy actuals aggregation here — this is an on-demand report and the
    // client aborts at 12s. warmActualsCache() blocked long enough on the production DB to time the
    // request out ("Failed to load closed batches: The operation timed out"). The gross maps are kept
    // warm by the startup warm + planning/state polling, so we read them as-is and only await the
    // cheap single-row override refresh below for correctness.
    warmActualsCache().catch(()=>{});
    await loadGrossOverrides();
    const ps = await getPlanningStateAsync();
    const orders = (ps.orders || []).filter(o => o && !o.deleted);

    // closed set from dpr_batch_closed (keyed by order_id and batch_number) + closed_at lookup
    let closedRows;
    if (pgPool) closedRows = (await pgPool.query('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed')).rows;
    else closedRows = db.prepare('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed').all();
    const closedById = new Map(), closedByBatch = new Map();
    for (const c of (closedRows || [])) {
      if (c.order_id) closedById.set(c.order_id, c);
      if (c.batch_number) closedByBatch.set(c.batch_number, c);
    }

    // override metadata (reason/by/at) for display
    let ovRows;
    if (pgPool) ovRows = (await pgPool.query('SELECT batch_number, gross_lakhs, reason, updated_by, updated_at FROM batch_gross_override')).rows;
    else ovRows = db.prepare('SELECT batch_number, gross_lakhs, reason, updated_by, updated_at FROM batch_gross_override').all();
    const ovByBatch = new Map((ovRows || []).map(r => [r.batch_number, r]));

    const out = [];
    for (const o of orders) {
      const cRow = closedById.get(o.id) || closedByBatch.get(o.batchNumber);
      const isClosed = !!cRow || o.status === 'closed';
      if (!isClosed) continue;
      const ov = ovByBatch.get(o.batchNumber) || null;
      const rawGross = _grossByBatch && Object.prototype.hasOwnProperty.call(_grossByBatch, o.batchNumber)
        ? _grossByBatch[o.batchNumber] : effectiveGross(o.batchNumber);
      out.push({
        orderId: o.id,
        batchNumber: o.batchNumber || '',
        machineId: o.machineId || '',
        size: (o.size != null ? String(o.size) : ''),
        colour: o.colour || o.color || '',
        pcCode: o.pcCode || '',
        customer: o.customer || '',
        startDate: o.startDate || '',
        endDate: o.endDate || '',
        plannedGross: parseFloat(o.grossQty || 0) || 0,
        rawDprGross: parseFloat(rawGross || 0) || 0,                 // pure SUM(production_actuals)
        actualGross: effectiveGross(o.batchNumber),                 // override (if any) else raw sum
        hasOverride: !!ov,
        overrideReason: ov ? (ov.reason || '') : '',
        overrideBy: ov ? (ov.updated_by || '') : '',
        overrideAt: ov ? (ov.updated_at || '') : '',
        closedAt: cRow ? (cRow.closed_at || '') : '',
        status: o.status || ''
      });
    }
    // Machine then batch ordering (report is grouped/filtered client-side)
    out.sort((a,b) => (a.machineId||'').localeCompare(b.machineId||'') || (a.batchNumber||'').localeCompare(b.batchNumber||''));
    res.json({ ok: true, batches: out });
  } catch (err) {
    console.error('[closed-batches]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/dpr/gross-override — set/replace a batch-level DPR gross correction (Production Manager / Admin).
// No time gate (per Ishan: 24h requirement discarded; existing reopen gates left untouched).
// Body: { batchNumber, grossLakhs, reason, updatedBy, userRole }. Cascades to Planning + Reports D/E.
app.post('/api/dpr/gross-override', async (req, res) => {
  try {
    const { batchNumber, grossLakhs, reason, updatedBy, userRole } = req.body || {};
    if (!batchNumber) return res.status(400).json({ ok: false, error: 'batchNumber required' });
    const g = parseFloat(grossLakhs);
    if (!Number.isFinite(g) || g < 0) return res.status(400).json({ ok: false, error: 'grossLakhs must be a number ≥ 0' });
    const by = (updatedBy || userRole || 'unknown').toString().slice(0, 120);
    const reasonStr = (reason || '').toString().slice(0, 500);

    if (pgPool) {
      await pgPool.query(
        `INSERT INTO batch_gross_override (batch_number, gross_lakhs, reason, updated_by, updated_at)
         VALUES ($1,$2,$3,$4,NOW()::TEXT)
         ON CONFLICT(batch_number) DO UPDATE SET gross_lakhs=EXCLUDED.gross_lakhs, reason=EXCLUDED.reason,
           updated_by=EXCLUDED.updated_by, updated_at=NOW()::TEXT`,
        [batchNumber, g, reasonStr, by]
      );
    } else {
      db.prepare(
        `INSERT INTO batch_gross_override (batch_number, gross_lakhs, reason, updated_by, updated_at)
         VALUES (?,?,?,?,datetime('now'))
         ON CONFLICT(batch_number) DO UPDATE SET gross_lakhs=excluded.gross_lakhs, reason=excluded.reason,
           updated_by=excluded.updated_by, updated_at=datetime('now')`
      ).run(batchNumber, g, reasonStr, by);
    }
    await loadGrossOverrides();                 // refresh override map → effectiveGross() picks it up immediately
    _planningStateCacheTime = 0;                // force planning state cache to re-serve with new gross
    try { logAudit(by, userRole || '', 'dpr', 'DPR_GROSS_OVERRIDE', `Set batch ${batchNumber} gross → ${g}L. ${reasonStr}`, req.ip); } catch {}
    res.json({ ok: true, batchNumber, actualGross: effectiveGross(batchNumber) });
  } catch (err) {
    console.error('[gross-override]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// DELETE /api/dpr/gross-override/:batchNumber — revert to the raw SUM(production_actuals) for this batch.
app.delete('/api/dpr/gross-override/:batchNumber', async (req, res) => {
  try {
    const bn = req.params.batchNumber;
    const by = (req.query.by || req.body?.updatedBy || 'unknown').toString().slice(0,120);
    if (pgPool) await pgPool.query('DELETE FROM batch_gross_override WHERE batch_number=$1', [bn]);
    else db.prepare('DELETE FROM batch_gross_override WHERE batch_number=?').run(bn);
    await loadGrossOverrides();
    _planningStateCacheTime = 0;
    try { logAudit(by, '', 'dpr', 'DPR_GROSS_OVERRIDE_CLEAR', `Cleared gross override for batch ${bn}`, req.ip); } catch {}
    res.json({ ok: true, batchNumber: bn, actualGross: effectiveGross(bn) });
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
    // v40 P18.16: refuse disabled accounts
    if (user.is_active === 0 || user.is_active === false) {
      return res.status(403).json({ ok: false, error: 'Account is disabled. Contact your administrator.' });
    }
    if (user.pin_hash !== hashPin(pin)) return res.status(401).json({ ok: false, error: 'Invalid PIN' });
    const token = generateToken();
    const expires = new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString().replace('T',' ').slice(0,19);
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

// GET /api/auth/users?app=<app> — PUBLIC (pre-login). Returns the minimal list of ACTIVE usernames
// for an app so the login screen can list EVERY defined account (not just the seeded defaults) —
// fixes admin-created users (e.g. "Marketing") being unable to sign in because their username never
// appeared in the picker. Returns only username + role; never PINs or hashes.
app.get('/api/auth/login-users', async (req, res) => {
  try {
    const appName = req.query.app;
    if (!appName) return res.status(400).json({ ok: false, error: 'app required' });
    let rows;
    if (pgPool) rows = (await pgPool.query('SELECT username, role, is_active FROM app_users WHERE app=$1 ORDER BY username ASC', [appName])).rows;
    else rows = db.prepare('SELECT username, role, is_active FROM app_users WHERE app=? ORDER BY username ASC').all(appName);
    const users = (rows || [])
      .filter(r => r.is_active !== 0 && r.is_active !== false)
      .map(r => ({ username: r.username, role: r.role }));
    res.json({ ok: true, users });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
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
    // v40 P18.16: targetApp accepted so admin can change PINs across all 3 apps.
    // Non-admin users can still change ONLY their own PIN (within their own app).
    const { token, username, newPin, targetApp } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    const isSelfEdit = session.username === username;
    if (session.role !== 'admin' && !isSelfEdit) {
      return res.status(403).json({ ok: false, error: 'Only admin can change other users PINs' });
    }
    if (!newPin || String(newPin).length < 4) {
      return res.status(400).json({ ok: false, error: 'PIN must be at least 4 characters' });
    }
    // Cross-app PIN change is admin-only. Self-edits restricted to session.app.
    const effectiveApp = (session.role === 'admin' && targetApp) ? targetApp : session.app;
    if (pgPool) {
      const r = await pgPool.query('UPDATE app_users SET pin_hash=$1, updated_at=NOW() WHERE username=$2 AND app=$3', [hashPin(newPin), username, effectiveApp]);
      if (r.rowCount === 0) return res.status(404).json({ ok: false, error: `User ${username} not found in app ${effectiveApp}` });
    } else {
      const info = db.prepare(`UPDATE app_users SET pin_hash=?, updated_at=datetime('now') WHERE username=? AND app=?`).run(hashPin(newPin), username, effectiveApp);
      if (info.changes === 0) return res.status(404).json({ ok: false, error: `User ${username} not found in app ${effectiveApp}` });
    }
    logAudit(session.username, session.role, session.app, 'CHANGE_PIN', `Changed PIN for ${username} (app=${effectiveApp})`, req.ip);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});


// ─────────────────────────────────────────────────────────────────
// v40 Phase 18.16: ADMIN USER MANAGEMENT
//
// Endpoints to manage users across all 3 apps from a single Plan_Admin login.
//   GET  /api/admin/users           — list all users (admin-only)
//   POST /api/admin/users/create    — add a new user
//   POST /api/admin/users/toggle-active — enable/disable an account
//   (PIN change handled by existing /api/auth/change-pin with targetApp param)
//
// All actions are audit-logged. Disabled users are blocked at login (P18.16 login gate).
// ─────────────────────────────────────────────────────────────────

// GET /api/admin/users — admin-only — returns all users across apps with last_login + status
app.get('/api/admin/users', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    // Pull users + their most recent login from audit_log
    let users = [];
    if (pgPool) {
      const r = await pgPool.query(`
        SELECT u.id, u.username, u.role, u.app, u.is_active, u.created_at, u.updated_at,
          (SELECT MAX(ts) FROM audit_log WHERE username=u.username AND app=u.app AND action='LOGIN') AS last_login
        FROM app_users u
        ORDER BY u.app ASC, u.username ASC
      `);
      users = r.rows;
    } else {
      users = db.prepare(`
        SELECT u.id, u.username, u.role, u.app, u.is_active, u.created_at, u.updated_at,
          (SELECT MAX(ts) FROM audit_log WHERE username=u.username AND app=u.app AND action='LOGIN') AS last_login
        FROM app_users u
        ORDER BY u.app ASC, u.username ASC
      `).all();
    }
    res.json({ ok: true, users });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/admin/users/create — admin-only — add a new user to any app
app.post('/api/admin/users/create', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const { username, role, app: appName, pin } = req.body;
    if (!username || !String(username).trim()) return res.status(400).json({ ok: false, error: 'username required' });
    if (!role || !String(role).trim()) return res.status(400).json({ ok: false, error: 'role required' });
    if (!appName || !['dpr','planning','tracking'].includes(appName)) {
      return res.status(400).json({ ok: false, error: 'app must be one of: dpr, planning, tracking' });
    }
    if (!pin || String(pin).length < 4) return res.status(400).json({ ok: false, error: 'PIN must be at least 4 characters' });
    // Username uniqueness check (app_users.username has UNIQUE constraint, but give clearer error)
    let existing;
    if (pgPool) {
      const r = await pgPool.query('SELECT id FROM app_users WHERE username=$1', [username.trim()]);
      existing = r.rows[0];
    } else {
      existing = db.prepare('SELECT id FROM app_users WHERE username=?').get(username.trim());
    }
    if (existing) return res.status(409).json({ ok: false, error: `Username "${username}" already exists` });
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO app_users (username, pin_hash, role, app, is_active) VALUES ($1,$2,$3,$4,1)`,
        [username.trim(), hashPin(pin), role.trim(), appName]
      );
    } else {
      db.prepare(`INSERT INTO app_users (username, pin_hash, role, app, is_active) VALUES (?,?,?,?,1)`)
        .run(username.trim(), hashPin(pin), role.trim(), appName);
    }
    logAudit(session.username, session.role, session.app, 'USER_CREATED', `Created ${username} (${role}/${appName})`, req.ip);
    res.json({ ok: true, message: `User ${username} created.` });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/admin/users/toggle-active — admin-only — enable/disable account
app.post('/api/admin/users/toggle-active', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const { username, app: appName, isActive } = req.body;
    if (!username || !appName) return res.status(400).json({ ok: false, error: 'username and app required' });
    // Safety: refuse to disable yourself
    if (username === session.username && appName === session.app && !isActive) {
      return res.status(400).json({ ok: false, error: 'Cannot disable your own account' });
    }
    // Safety: refuse to disable the last admin in any app
    if (!isActive) {
      let adminCount;
      if (pgPool) {
        const r = await pgPool.query(`SELECT COUNT(*)::int AS c FROM app_users WHERE app=$1 AND role='admin' AND is_active=1 AND username<>$2`, [appName, username]);
        adminCount = r.rows[0]?.c || 0;
      } else {
        adminCount = db.prepare(`SELECT COUNT(*) c FROM app_users WHERE app=? AND role='admin' AND is_active=1 AND username<>?`).get(appName, username)?.c || 0;
      }
      // Check if THIS user is currently an admin
      let thisUser;
      if (pgPool) { const r = await pgPool.query(`SELECT role FROM app_users WHERE username=$1 AND app=$2`, [username, appName]); thisUser = r.rows[0]; }
      else { thisUser = db.prepare(`SELECT role FROM app_users WHERE username=? AND app=?`).get(username, appName); }
      if (thisUser?.role === 'admin' && adminCount === 0) {
        return res.status(400).json({ ok: false, error: `Cannot disable the last active admin for app ${appName}` });
      }
    }
    const newVal = isActive ? 1 : 0;
    if (pgPool) {
      const r = await pgPool.query(`UPDATE app_users SET is_active=$1, updated_at=NOW() WHERE username=$2 AND app=$3`, [newVal, username, appName]);
      if (r.rowCount === 0) return res.status(404).json({ ok: false, error: 'User not found' });
    } else {
      const info = db.prepare(`UPDATE app_users SET is_active=?, updated_at=datetime('now') WHERE username=? AND app=?`).run(newVal, username, appName);
      if (info.changes === 0) return res.status(404).json({ ok: false, error: 'User not found' });
    }
    logAudit(session.username, session.role, session.app, isActive ? 'USER_ENABLED' : 'USER_DISABLED', `${isActive?'Enabled':'Disabled'} ${username} (app=${appName})`, req.ip);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ─── end v40 P18.16: Admin User Management ────────────────────

// ─────────────────────────────────────────────────────────────────
// v40 Phase 18.17: DATA INTEGRITY DASHBOARD
//
// Background hourly scan over the last 30 days across 12 check functions.
// Findings persist in integrity_findings (deduped by finding_key). Admin
// reviews and either acknowledges (24h auto-expiry), assigns to an operator
// for fix action, or mutes the whole check_type for systemic false positives.
//
// Operators in DPR/Planning/Tracking see assigned tasks as a banner in their
// own app. Findings self-resolve on the next scan when the underlying
// condition no longer triggers the check.
// ─────────────────────────────────────────────────────────────────

const integrityEngine = require('./integrity-engine');

let _integrityLastRunAt = null;
let _integrityLastRunResult = null;
let _integrityIsRunning = false;

async function _runIntegrityScan(opts = {}) {
  if (_integrityIsRunning) {
    return { skipped: true, reason: 'Already running' };
  }
  _integrityIsRunning = true;
  try {
    const planningState = await getPlanningStateAsync();
    const ctx = {
      pgPool: pgPool || null,
      db: db,
      planningState,
      lookbackDays: opts.lookbackDays || 30,
    };
    const result = await integrityEngine.runAllChecks(ctx);
    _integrityLastRunAt = new Date().toISOString();
    _integrityLastRunResult = result;
    console.log(`[Integrity] Scan complete: ${result.findingsFound} findings (${result.upserted} upserted, ${result.resolved} resolved) in ${result.durationMs}ms`);
    if (result.errors.length > 0) {
      console.warn(`[Integrity] ${result.errors.length} check(s) errored:`, result.errors);
    }
    return result;
  } finally {
    _integrityIsRunning = false;
  }
}

// POST /api/integrity/run-now — admin-only, triggers an immediate scan
app.post('/api/integrity/run-now', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const result = await _runIntegrityScan({ lookbackDays: req.body.lookbackDays || 30 });
    if (result.skipped) return res.json({ ok: true, skipped: true, message: 'Scan already in progress' });
    logAudit(session.username, session.role, session.app, 'INTEGRITY_SCAN_TRIGGERED',
      `Manual scan: ${result.findingsFound} findings`, req.ip);
    res.json({ ok: true, lastRunAt: _integrityLastRunAt, ...result });
  } catch (err) {
    console.error('[Integrity] run-now failed:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/integrity/findings — admin-only — list findings with optional filters
app.get('/api/integrity/findings', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });

    const severity = req.query.severity;  // critical|warning|info or all
    const checkType = req.query.checkType; // specific check_type or 'all'/empty
    const includeAcked = req.query.includeAcked === '1';
    const includeResolved = req.query.includeResolved === '1';

    let sql = `SELECT * FROM integrity_findings WHERE 1=1`;
    const params = [];
    if (severity && severity !== 'all') {
      sql += ` AND severity = ?`;
      params.push(severity);
    }
    if (checkType && checkType !== 'all') {
      sql += ` AND check_type = ?`;
      params.push(checkType);
    }
    if (!includeResolved) sql += ` AND resolved = 0`;
    if (!includeAcked) {
      if (pgPool) sql += ` AND (ack_until IS NULL OR ack_until < NOW()::TEXT)`;
      else sql += ` AND (ack_until IS NULL OR ack_until < datetime('now'))`;
    }
    sql += ` ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END, last_seen DESC LIMIT 500`;

    let rows = [];
    if (pgPool) {
      let i = 0; const pgSql = sql.replace(/\?/g, () => `$${++i}`);
      const r = await pgPool.query(pgSql, params);
      rows = r.rows;
    } else {
      rows = db.prepare(sql).all(...params);
    }
    // Parse raw_data_json for client convenience
    for (const r of rows) {
      if (r.raw_data_json) {
        try { r.raw_data = JSON.parse(r.raw_data_json); delete r.raw_data_json; } catch (e) {}
      }
    }
    // v41ZB: attach assignment/action history per finding (read-only join, no schema change).
    // Surfaces "status against each action + timestamp" in the Data Integrity dashboard.
    try {
      const ids = rows.map(r => r.id).filter(Boolean);
      if (ids.length) {
        let taskRows = [];
        if (pgPool) {
          const r2 = await pgPool.query(`SELECT * FROM integrity_tasks WHERE finding_id = ANY($1) ORDER BY assigned_at ASC`, [ids]);
          taskRows = r2.rows;
        } else {
          const ph = ids.map(() => '?').join(',');
          taskRows = db.prepare(`SELECT * FROM integrity_tasks WHERE finding_id IN (${ph}) ORDER BY assigned_at ASC`).all(...ids);
        }
        const byFinding = {};
        for (const t of taskRows) { (byFinding[t.finding_id] = byFinding[t.finding_id] || []).push(t); }
        for (const r of rows) { r.tasks = byFinding[r.id] || []; }
      } else {
        for (const r of rows) { r.tasks = []; }
      }
    } catch (e) {
      console.error('[Integrity] task attach failed:', e.message);
      for (const r of rows) { if (!r.tasks) r.tasks = []; }
    }
    // Summary counts (unack, unresolved)
    let summarySql = `SELECT severity, COUNT(*)::int AS c FROM integrity_findings WHERE resolved=0 AND (ack_until IS NULL OR ack_until < `;
    summarySql += pgPool ? `NOW()::TEXT` : `datetime('now')`;
    summarySql += `) GROUP BY severity`;
    let summaryRows = [];
    if (pgPool) {
      const r = await pgPool.query(summarySql.replace('COUNT(*)::int', 'COUNT(*)::int'));
      summaryRows = r.rows;
    } else {
      summaryRows = db.prepare(summarySql.replace('::int', '')).all();
    }
    const summary = { critical: 0, warning: 0, info: 0 };
    for (const s of summaryRows) summary[s.severity] = parseInt(s.c) || 0;

    // v41ZC: total rows matching the ACTIVE filter (pre-LIMIT), so the UI can show "showing 500 of N".
    let totalMatching = rows.length;
    try {
      let countSql = sql.replace('SELECT *', 'SELECT COUNT(*) AS c').replace(/ ORDER BY[\s\S]*$/, '');
      if (pgPool) {
        let i = 0; const pgCount = countSql.replace(/\?/g, () => `$${++i}`);
        const cr = await pgPool.query(pgCount, params);
        totalMatching = parseInt(cr.rows[0]?.c) || rows.length;
      } else {
        const cr = db.prepare(countSql).get(...params);
        totalMatching = (cr && (cr.c|0)) || rows.length;
      }
    } catch (e) { /* fall back to rows.length */ }

    res.json({
      ok: true,
      findings: rows,
      summary,
      totalMatching,
      capped: totalMatching > rows.length,
      lastRunAt: _integrityLastRunAt,
      isRunning: _integrityIsRunning,
    });
  } catch (err) {
    console.error('[Integrity] findings list failed:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/integrity/ack/:id — admin-only — acknowledge a finding for N hours
app.post('/api/integrity/ack/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const reason = req.body.reason || 'Acknowledged';
    const hours = parseInt(req.body.hours) || 24;
    const ackUntil = new Date(Date.now() + hours * 3600000).toISOString();
    if (pgPool) {
      await pgPool.query(`UPDATE integrity_findings SET ack_by=$1, ack_at=NOW()::TEXT, ack_reason=$2, ack_until=$3 WHERE id=$4`,
        [session.username, reason, ackUntil, req.params.id]);
    } else {
      db.prepare(`UPDATE integrity_findings SET ack_by=?, ack_at=datetime('now'), ack_reason=?, ack_until=? WHERE id=?`)
        .run(session.username, reason, ackUntil, req.params.id);
    }
    logAudit(session.username, session.role, session.app, 'INTEGRITY_ACK', `Acknowledged ${req.params.id}: ${reason}`, req.ip);
    res.json({ ok: true, ackUntil });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/integrity/ack-bulk — admin-only — bulk-acknowledge warning/info findings
// Critical findings always require individual acknowledgment.
app.post('/api/integrity/ack-bulk', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const reason = req.body.reason || 'Historical backfill — bulk acknowledged';
    const hours = parseInt(req.body.hours) || (7 * 24);  // default 7 days for backfill
    const ackUntil = new Date(Date.now() + hours * 3600000).toISOString();
    let count = 0;
    if (pgPool) {
      const r = await pgPool.query(
        `UPDATE integrity_findings SET ack_by=$1, ack_at=NOW()::TEXT, ack_reason=$2, ack_until=$3
         WHERE severity IN ('warning','info') AND resolved=0 AND (ack_until IS NULL OR ack_until < NOW()::TEXT)`,
        [session.username, reason, ackUntil]
      );
      count = r.rowCount || 0;
    } else {
      const info = db.prepare(
        `UPDATE integrity_findings SET ack_by=?, ack_at=datetime('now'), ack_reason=?, ack_until=?
         WHERE severity IN ('warning','info') AND resolved=0 AND (ack_until IS NULL OR ack_until < datetime('now'))`
      ).run(session.username, reason, ackUntil);
      count = info.changes;
    }
    logAudit(session.username, session.role, session.app, 'INTEGRITY_ACK_BULK', `Bulk ack ${count} warning/info findings`, req.ip);
    res.json({ ok: true, count, ackUntil });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/integrity/mute — admin-only — mute or unmute a check_type entirely
app.post('/api/integrity/mute', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const { checkType, mute, reason } = req.body;
    if (!checkType) return res.status(400).json({ ok: false, error: 'checkType required' });
    if (mute) {
      if (pgPool) {
        await pgPool.query(
          `INSERT INTO integrity_mutes (check_type, muted_by, reason) VALUES ($1,$2,$3)
           ON CONFLICT(check_type) DO UPDATE SET muted_by=$2, muted_at=NOW()::TEXT, reason=$3`,
          [checkType, session.username, reason || '']
        );
      } else {
        db.prepare(
          `INSERT INTO integrity_mutes (check_type, muted_by, reason) VALUES (?,?,?)
           ON CONFLICT(check_type) DO UPDATE SET muted_by=excluded.muted_by, muted_at=datetime('now'), reason=excluded.reason`
        ).run(checkType, session.username, reason || '');
      }
      logAudit(session.username, session.role, session.app, 'INTEGRITY_MUTE', `Muted check_type ${checkType}: ${reason||''}`, req.ip);
    } else {
      if (pgPool) await pgPool.query(`DELETE FROM integrity_mutes WHERE check_type=$1`, [checkType]);
      else db.prepare(`DELETE FROM integrity_mutes WHERE check_type=?`).run(checkType);
      logAudit(session.username, session.role, session.app, 'INTEGRITY_UNMUTE', `Unmuted check_type ${checkType}`, req.ip);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/integrity/mutes — admin-only — list currently muted check_types
app.get('/api/integrity/mutes', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    let rows = [];
    if (pgPool) { const r = await pgPool.query(`SELECT * FROM integrity_mutes ORDER BY muted_at DESC`); rows = r.rows; }
    else rows = db.prepare(`SELECT * FROM integrity_mutes ORDER BY muted_at DESC`).all();
    res.json({ ok: true, mutes: rows });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/integrity/assign-task — admin-only — assign a finding (or freeform note) to a user/role
app.post('/api/integrity/assign-task', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const { findingId, assignedTo, app: appName, note } = req.body;
    if (!assignedTo) return res.status(400).json({ ok: false, error: 'assignedTo required' });
    const taskId = `IT-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO integrity_tasks (id, finding_id, assigned_to, assigned_by, app, note, status)
         VALUES ($1,$2,$3,$4,$5,$6,'pending')`,
        [taskId, findingId || null, assignedTo, session.username, appName || null, note || null]
      );
    } else {
      db.prepare(
        `INSERT INTO integrity_tasks (id, finding_id, assigned_to, assigned_by, app, note, status)
         VALUES (?,?,?,?,?,?, 'pending')`
      ).run(taskId, findingId || null, assignedTo, session.username, appName || null, note || null);
    }
    logAudit(session.username, session.role, session.app, 'INTEGRITY_TASK_ASSIGNED',
      `Assigned task ${taskId} to ${assignedTo}${findingId?` for finding ${findingId}`:''}`, req.ip);
    res.json({ ok: true, taskId });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/integrity/my-tasks — any logged-in user — returns their pending/seen tasks
app.get('/api/integrity/my-tasks', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    // Match assigned_to by exact username or by role:<role>
    let rows = [];
    const sql = `
      SELECT t.*, f.severity, f.description, f.batch_number, f.order_id, f.machine_id, f.suggested_app, f.suggested_page, f.suggested_action,
             f.resolved AS finding_resolved
      FROM integrity_tasks t
      LEFT JOIN integrity_findings f ON f.id = t.finding_id
      WHERE t.status IN ('pending','seen') AND (LOWER(TRIM(t.assigned_to)) = LOWER(TRIM(?)) OR LOWER(TRIM(t.assigned_to)) = LOWER(TRIM(?)))
      ORDER BY t.assigned_at DESC LIMIT 50
    `;
    const roleKey = `role:${session.role}`;
    if (pgPool) {
      let i = 0; const pgSql = sql.replace(/\?/g, () => `$${++i}`);
      const r = await pgPool.query(pgSql, [session.username, roleKey]);
      rows = r.rows;
    } else {
      rows = db.prepare(sql).all(session.username, roleKey);
    }
    // Filter out tasks whose findings have been auto-resolved
    rows = rows.filter(r => !r.finding_id || r.finding_resolved === 0 || r.finding_resolved === false);
    res.json({ ok: true, tasks: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/integrity/task/:id/seen — operator marks a task as seen
app.post('/api/integrity/task/:id/seen', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (pgPool) {
      await pgPool.query(`UPDATE integrity_tasks SET status='seen', seen_at=NOW()::TEXT, seen_by=$1 WHERE id=$2`,
        [session.username, req.params.id]);
    } else {
      db.prepare(`UPDATE integrity_tasks SET status='seen', seen_at=datetime('now'), seen_by=? WHERE id=?`)
        .run(session.username, req.params.id);
    }
    logAudit(session.username, session.role, session.app, 'INTEGRITY_TASK_SEEN', `Marked task ${req.params.id} as seen`, req.ip);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/integrity/task/:id/dismiss — admin withdraws a task
app.post('/api/integrity/task/:id/dismiss', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    if (pgPool) {
      await pgPool.query(`UPDATE integrity_tasks SET status='dismissed', dismissed_at=NOW()::TEXT, dismissed_by=$1 WHERE id=$2`,
        [session.username, req.params.id]);
    } else {
      db.prepare(`UPDATE integrity_tasks SET status='dismissed', dismissed_at=datetime('now'), dismissed_by=? WHERE id=?`)
        .run(session.username, req.params.id);
    }
    logAudit(session.username, session.role, session.app, 'INTEGRITY_TASK_DISMISSED', `Dismissed task ${req.params.id}`, req.ip);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ─── Hourly scheduler ────────────────────────────────────────────
// First scan runs 180 seconds after server boot, then every 60 minutes.
// v41ZJ: deferred from 30s → 180s. The scan holds DB connections for ~50s (1697 findings); running
// it 30s after boot collided with the post-deploy reconnection storm (every client re-syncing at
// once) + startup cache warms, starving the pool and causing client-side request aborts. 180s lets
// startup settle first. Configurable via env vars (interval + first-delay).
const _intervalMin = parseInt(process.env.INTEGRITY_SCAN_INTERVAL_MIN) || 60;
const _firstScanDelayMs = parseInt(process.env.INTEGRITY_SCAN_FIRST_DELAY_MS) || 180000;
setTimeout(() => {
  console.log(`[Integrity] First scan starting (interval: ${_intervalMin}min)...`);
  _runIntegrityScan().catch(e => console.error('[Integrity] First scan failed:', e.message));
}, _firstScanDelayMs);
setInterval(() => {
  _runIntegrityScan().catch(e => console.error('[Integrity] Periodic scan failed:', e.message));
}, _intervalMin * 60 * 1000);

// ─── end v40 P18.17: Data Integrity Dashboard ──────────────────

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


// ─────────────────────────────────────────────────────────────────
// v40 Phase 18.15: WO MULTI-CUSTOMER SPLIT
//
// Use case: A 50-box WO batch 26ZC100 was produced before any customer was
// confirmed. After production, three real orders arrive: 20+25+5 boxes for
// three different customers. Planner uses Split & Assign to break the parent
// WO into 1..N child customer orders. Admin approves; on approval:
//   1. Create N new "child" production orders, one per customer line
//   2. Each child gets its own batchNumber = parent + suffix (A, B, C, ...)
//   3. Rebatch label rows + scan rows from parent -> child by box position
//      (boxes 1..20 -> child A, 21..45 -> child B, 46..50 -> child C)
//   4. Parent order qty reduced by sum-of-children; if 0, parent marked wo-split
//   5. Each child status = 'closed' (production was done at parent, children
//      live on the same machine for traceability per user-confirmed design)
//   6. Production data inherited proportionally per child's share of boxes
//
// Approval flow stays 2-step (planner proposes, admin approves) - same as the
// existing 1:1 wo_reconciliation flow.
// ─────────────────────────────────────────────────────────────────

// POST /api/wo/split/propose - Planner proposes a multi-customer split
app.post('/api/wo/split/propose', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.body.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
    }
    const { sourceOrderId, lines } = req.body;
    if (!sourceOrderId) return res.status(400).json({ ok: false, error: 'sourceOrderId required' });
    if (!Array.isArray(lines) || lines.length === 0) {
      return res.status(400).json({ ok: false, error: 'At least one customer line required' });
    }

    const planState = await getPlanningStateAsync();
    const sourceOrd = (planState.orders || []).find(o => o.id === sourceOrderId);
    if (!sourceOrd) return res.status(404).json({ ok: false, error: 'Source order not found' });
    if (sourceOrd.woStatus !== 'wo' && sourceOrd.woStatus !== 'wo-split-partial') {
      return res.status(400).json({ ok: false, error: `Order is not a W/O order (woStatus=${sourceOrd.woStatus||'none'})` });
    }
    if (sourceOrd.deleted) return res.status(400).json({ ok: false, error: 'Source order is deleted' });

    let totalBoxesInBatch = 0;
    try {
      if (pgPool) {
        const r = await pgPool.query(`SELECT COUNT(*)::int AS c FROM tracking_labels WHERE batch_number=$1 AND (voided IS NULL OR voided=0)`, [sourceOrd.batchNumber]);
        totalBoxesInBatch = r.rows[0]?.c || 0;
      } else {
        totalBoxesInBatch = db.prepare(`SELECT COUNT(*) c FROM tracking_labels WHERE batch_number=? AND (voided IS NULL OR voided=0)`).get(sourceOrd.batchNumber)?.c || 0;
      }
    } catch(e) {}

    const conflicts = [];
    const lineNorm = [];
    let nextBoxStart = 1;
    for (let i = 0; i < lines.length; i++) {
      const L = lines[i];
      if (!L.customer || String(L.customer).trim() === '') {
        conflicts.push({ lineIndex: i, reason: 'Customer name required' });
        continue;
      }
      const boxes = parseInt(L.boxes);
      if (!boxes || boxes <= 0) {
        conflicts.push({ lineIndex: i, customer: L.customer, reason: 'boxes must be a positive integer' });
        continue;
      }
      const suffix = String(L.suffix || String.fromCharCode(64 + i + 1)).toUpperCase();
      if (!/^[A-Z0-9]+$/.test(suffix)) {
        conflicts.push({ lineIndex: i, customer: L.customer, reason: 'suffix must be alphanumeric (A-Z, 0-9)' });
        continue;
      }
      const boxStart = nextBoxStart;
      const boxEnd = nextBoxStart + boxes - 1;
      nextBoxStart = boxEnd + 1;

      const sizeQty = boxes * (sourceOrd.qty / Math.max(1, totalBoxesInBatch || (parseInt(sourceOrd.totalBoxes)||0) || 1));
      const qtyLakhs = parseFloat(L.qtyLakhs) || sizeQty;
      const childBatchNumber = `${sourceOrd.batchNumber}-${suffix}`;
      lineNorm.push({
        line_index: i,
        customer: String(L.customer).trim(),
        bill_to: String(L.billTo || '').trim() || null,
        po_number: String(L.poNumber || '').trim() || null,
        zone: String(L.zone || '').trim() || null,
        boxes,
        qty_lakhs: qtyLakhs,
        box_start: boxStart,
        box_end: boxEnd,
        child_batch_suffix: suffix,
        child_batch_number: childBatchNumber,
        sap_doc_entry: L.sapDocEntry ? parseInt(L.sapDocEntry) : null,
        sap_doc_num: L.sapDocNum ? String(L.sapDocNum).trim() : null,
      });
    }
    if (conflicts.length > 0) return res.status(400).json({ ok: false, error: 'Validation failed', conflicts });

    const totalBoxesSplit = lineNorm.reduce((s,l) => s + l.boxes, 0);
    const cap = totalBoxesInBatch || parseInt(sourceOrd.totalBoxes) || 0;
    if (cap > 0 && totalBoxesSplit > cap) {
      return res.status(400).json({ ok: false, error: `Sum of split boxes (${totalBoxesSplit}) exceeds boxes in batch (${cap})` });
    }
    const seen = new Set();
    for (const L of lineNorm) {
      if (seen.has(L.child_batch_suffix)) {
        return res.status(400).json({ ok: false, error: `Duplicate suffix: ${L.child_batch_suffix}` });
      }
      seen.add(L.child_batch_suffix);
    }
    for (const L of lineNorm) {
      const collision = (planState.orders || []).find(o => o.batchNumber === L.child_batch_number && !o.deleted);
      if (collision) {
        return res.status(409).json({ ok: false, error: `Child batch number ${L.child_batch_number} already exists (order ${collision.id})` });
      }
    }

    const reqId = `WOSPLIT-${Date.now()}`;
    const residualBoxes = cap > 0 ? Math.max(0, cap - totalBoxesSplit) : 0;

    if (pgPool) {
      await pgPool.query(
        `INSERT INTO wo_split_requests (id, source_order_id, source_batch_number, proposed_by, status, total_boxes_split, residual_boxes)
         VALUES ($1,$2,$3,$4,'pending',$5,$6)`,
        [reqId, sourceOrderId, sourceOrd.batchNumber, session.username, totalBoxesSplit, residualBoxes]
      );
      for (const L of lineNorm) {
        await pgPool.query(
          `INSERT INTO wo_split_lines (id, split_request_id, line_index, customer, bill_to, po_number, zone, boxes, qty_lakhs, box_start, box_end, child_batch_suffix, child_batch_number, sap_doc_entry, sap_doc_num)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
          [`${reqId}-L${L.line_index}`, reqId, L.line_index, L.customer, L.bill_to, L.po_number, L.zone,
           L.boxes, L.qty_lakhs, L.box_start, L.box_end, L.child_batch_suffix, L.child_batch_number,
           L.sap_doc_entry, L.sap_doc_num]
        );
      }
    } else {
      db.prepare(`INSERT INTO wo_split_requests (id, source_order_id, source_batch_number, proposed_by, status, total_boxes_split, residual_boxes) VALUES (?,?,?,?, 'pending', ?, ?)`)
        .run(reqId, sourceOrderId, sourceOrd.batchNumber, session.username, totalBoxesSplit, residualBoxes);
      const insLine = db.prepare(`INSERT INTO wo_split_lines (id, split_request_id, line_index, customer, bill_to, po_number, zone, boxes, qty_lakhs, box_start, box_end, child_batch_suffix, child_batch_number, sap_doc_entry, sap_doc_num) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
      for (const L of lineNorm) {
        insLine.run(`${reqId}-L${L.line_index}`, reqId, L.line_index, L.customer, L.bill_to, L.po_number, L.zone,
                    L.boxes, L.qty_lakhs, L.box_start, L.box_end, L.child_batch_suffix, L.child_batch_number,
                    L.sap_doc_entry, L.sap_doc_num);
      }
    }

    logAudit(session.username, session.role, 'planning', 'WO_SPLIT_PROPOSED',
      `Proposed split of ${sourceOrd.batchNumber} into ${lineNorm.length} customer order(s): ${lineNorm.map(L=>`${L.child_batch_suffix}=${L.customer}/${L.boxes}b`).join(', ')}`);
    res.json({ ok: true, requestId: reqId, status: 'pending', message: 'Awaiting Admin approval' });
  } catch (err) {
    console.error('[v40 P18.15] split/propose failed:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/wo/split/pending - admin sees pending; planner sees only their own
app.get('/api/wo/split/pending', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    let rows = [];
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM wo_split_requests WHERE status='pending' ORDER BY proposed_at DESC`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM wo_split_requests WHERE status='pending' ORDER BY proposed_at DESC`).all();
    }
    if (session.role !== 'admin') rows = rows.filter(r => r.proposed_by === session.username);
    const planState = await getPlanningStateAsync();
    const enriched = [];
    for (const r of rows) {
      let lines = [];
      if (pgPool) { const lr = await pgPool.query(`SELECT * FROM wo_split_lines WHERE split_request_id=$1 ORDER BY line_index ASC`, [r.id]); lines = lr.rows; }
      else { lines = db.prepare(`SELECT * FROM wo_split_lines WHERE split_request_id=? ORDER BY line_index ASC`).all(r.id); }
      const sourceOrd = (planState.orders || []).find(o => o.id === r.source_order_id) || {};
      enriched.push({ ...r, lines, sourceOrder: sourceOrd });
    }
    res.json({ ok: true, requests: enriched });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/wo/split/approve/:id - Admin approves; performs atomic split
app.post('/api/wo/split/approve/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const reqId = req.params.id;
    let request, lines;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM wo_split_requests WHERE id=$1`, [reqId]);
      request = r.rows[0];
      const lr = await pgPool.query(`SELECT * FROM wo_split_lines WHERE split_request_id=$1 ORDER BY line_index ASC`, [reqId]);
      lines = lr.rows;
    } else {
      request = db.prepare(`SELECT * FROM wo_split_requests WHERE id=?`).get(reqId);
      lines = db.prepare(`SELECT * FROM wo_split_lines WHERE split_request_id=? ORDER BY line_index ASC`).all(reqId);
    }
    if (!request) return res.status(404).json({ ok: false, error: 'Split request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok: false, error: `Already ${request.status}` });
    if (!lines || lines.length === 0) return res.status(400).json({ ok: false, error: 'Split request has no lines' });

    const planState = await getPlanningStateAsync();
    const parent = (planState.orders || []).find(o => o.id === request.source_order_id);
    if (!parent) return res.status(404).json({ ok: false, error: 'Source parent order no longer exists' });
    if (parent.woStatus !== 'wo' && parent.woStatus !== 'wo-split-partial') {
      return res.status(400).json({ ok: false, error: `Parent order is no longer a W/O (woStatus=${parent.woStatus||'none'}). Cannot split.` });
    }
    for (const L of lines) {
      const collision = (planState.orders || []).find(o => o.batchNumber === L.child_batch_number && !o.deleted);
      if (collision) return res.status(409).json({ ok: false, error: `Child batch number ${L.child_batch_number} now collides with order ${collision.id}` });
    }

    const parentActual = parseFloat(parent.actualProd || 0);
    const totalBoxesSplit = lines.reduce((s,l) => s + (l.boxes||0), 0);
    const now = new Date().toISOString();
    const childOrders = [];
    for (const L of lines) {
      const proportional = totalBoxesSplit > 0 ? (parentActual * L.boxes / totalBoxesSplit) : 0;
      const childId = `${parent.id}-${L.child_batch_suffix}`;
      const child = {
        ...parent,
        id: childId,
        batchNumber: L.child_batch_number,
        customer: L.customer,
        shipTo: L.customer,
        billTo: L.bill_to || '',
        poNumber: L.po_number || '',
        zone: L.zone || parent.zone,
        qty: L.qty_lakhs,
        actualProd: parseFloat(proportional.toFixed(3)),
        actualQty: parseFloat(proportional.toFixed(3)),
        totalBoxes: L.boxes,
        woStatus: 'wo-split-child',
        woSplitParentId: parent.id,
        woSplitFromBatch: parent.batchNumber,
        woSplitLineId: L.id,
        status: 'closed',
        closedDate: now,
        deleted: false,
        sapDocEntry: L.sap_doc_entry || null,
        sapDocNum: L.sap_doc_num || '',
        _localEditedAt: Date.now(),
      };
      childOrders.push({ child, line: L });
    }

    const performSplit = async () => {
      for (const c of childOrders) planState.orders.push(c.child);
      const residualBoxes = request.residual_boxes || 0;
      if (residualBoxes > 0) {
        const cap = parseInt(parent.totalBoxes) || (totalBoxesSplit + residualBoxes);
        parent.qty = parent.qty * (residualBoxes / cap);
        parent.totalBoxes = residualBoxes;
        parent.woStatus = 'wo-split-partial';
        parent._localEditedAt = Date.now();
      } else {
        parent.qty = 0;
        parent.woStatus = 'wo-split';
        parent.deleted = false;
        parent.status = 'closed';
        parent.closedDate = now;
        parent._localEditedAt = Date.now();
      }
      parent.woSplitRequestId = reqId;

      if (pgPool) {
        await pgPool.query(
          `INSERT INTO planning_state (id, state_json) VALUES (1, $1)
           ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json, saved_at=NOW()::TEXT`,
          [JSON.stringify(planState)]
        );
      } else {
        db.prepare(`INSERT INTO planning_state (id, state_json, saved_at) VALUES (1, ?, datetime('now'))
                    ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json, saved_at=datetime('now')`).run(JSON.stringify(planState));
      }
      _planningStateCache = planState;
      _planningStateCacheTime = Date.now();

      for (const c of childOrders) {
        const j = JSON.stringify(c.child);
        if (pgPool) {
          await pgPool.query(
            `INSERT INTO production_orders (id, data_json, machine_id, batch_number, status, deleted, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,NOW()::TEXT)
             ON CONFLICT(id) DO UPDATE SET data_json=$2, machine_id=$3, batch_number=$4, status=$5, deleted=$6, updated_at=NOW()::TEXT`,
            [c.child.id, j, c.child.machineId||null, c.child.batchNumber, c.child.status, false]
          );
        } else {
          db.prepare(`INSERT INTO production_orders (id, data_json, machine_id, batch_number, status, deleted, updated_at)
                      VALUES (?,?,?,?,?,?,datetime('now'))
                      ON CONFLICT(id) DO UPDATE SET data_json=?, machine_id=?, batch_number=?, status=?, deleted=?, updated_at=datetime('now')`)
            .run(c.child.id, j, c.child.machineId||null, c.child.batchNumber, c.child.status, 0,
                 j, c.child.machineId||null, c.child.batchNumber, c.child.status, 0);
        }
      }

      let parentLabels = [];
      if (pgPool) {
        const r = await pgPool.query(
          `SELECT id, batch_number, box_number FROM tracking_labels WHERE batch_number=$1 ORDER BY box_number ASC`,
          [parent.batchNumber]
        );
        parentLabels = r.rows;
      } else {
        parentLabels = db.prepare(`SELECT id, batch_number, box_number FROM tracking_labels WHERE batch_number=? ORDER BY box_number ASC`).all(parent.batchNumber);
      }
      const lineByBoxPos = {};
      for (const L of lines) {
        for (let b = L.box_start; b <= L.box_end; b++) lineByBoxPos[b] = L;
      }
      let relabeled = 0;
      for (let pos = 0; pos < parentLabels.length; pos++) {
        const lbl = parentLabels[pos];
        const boxPos = pos + 1;
        const L = lineByBoxPos[boxPos];
        if (!L) continue;
        const newBatch = L.child_batch_number;
        if (pgPool) {
          await pgPool.query(`UPDATE tracking_labels SET batch_number=$1, customer=$2, wo_status='wo-split-child' WHERE id=$3`,
            [newBatch, L.customer, lbl.id]);
          await pgPool.query(`UPDATE tracking_scans SET batch_number=$1 WHERE batch_number=$2 AND label_id=$3`,
            [newBatch, parent.batchNumber, lbl.id]);
        } else {
          db.prepare(`UPDATE tracking_labels SET batch_number=?, customer=?, wo_status='wo-split-child' WHERE id=?`).run(newBatch, L.customer, lbl.id);
          db.prepare(`UPDATE tracking_scans SET batch_number=? WHERE batch_number=? AND label_id=?`).run(newBatch, parent.batchNumber, lbl.id);
        }
        relabeled++;
      }

      const approvedAt = new Date().toISOString();
      if (pgPool) {
        await pgPool.query(`UPDATE wo_split_requests SET status='approved', approved_by=$1, approved_at=$2 WHERE id=$3`,
          [session.username, approvedAt, reqId]);
        for (const c of childOrders) {
          await pgPool.query(`UPDATE wo_split_lines SET child_order_id=$1 WHERE id=$2`, [c.child.id, c.line.id]);
        }
      } else {
        db.prepare(`UPDATE wo_split_requests SET status='approved', approved_by=?, approved_at=? WHERE id=?`)
          .run(session.username, approvedAt, reqId);
        const upL = db.prepare(`UPDATE wo_split_lines SET child_order_id=? WHERE id=?`);
        for (const c of childOrders) upL.run(c.child.id, c.line.id);
      }

      return { childCount: childOrders.length, relabeled };
    };

    const result = await performSplit();
    logAudit(session.username, session.role, 'planning', 'WO_SPLIT_APPROVED',
      `Approved split ${reqId} of ${parent.batchNumber}: created ${result.childCount} child order(s), rebatched ${result.relabeled} label(s)`);
    res.json({
      ok: true,
      childOrdersCreated: result.childCount,
      labelsRebatched: result.relabeled,
      message: `${result.childCount} customer order(s) created. Print fresh labels for each child batch and physically replace on boxes before dispatch.`
    });
  } catch (err) {
    console.error('[v40 P18.15] split/approve failed:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/wo/split/reject/:id - Admin rejects with reason
app.post('/api/wo/split/reject/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const reason = req.body.reason || 'No reason given';
    const now = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(`UPDATE wo_split_requests SET status='rejected', approved_by=$1, approved_at=$2, rejection_reason=$3 WHERE id=$4`,
        [session.username, now, reason, req.params.id]);
    } else {
      db.prepare(`UPDATE wo_split_requests SET status='rejected', approved_by=?, approved_at=?, rejection_reason=? WHERE id=?`)
        .run(session.username, now, reason, req.params.id);
    }
    logAudit(session.username, session.role, 'planning', 'WO_SPLIT_REJECTED', `Rejected split ${req.params.id}: ${reason}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/split/history - recent split history
app.get('/api/wo/split/history', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    let rows = [];
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM wo_split_requests WHERE status IN ('approved','rejected') ORDER BY proposed_at DESC LIMIT 50`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM wo_split_requests WHERE status IN ('approved','rejected') ORDER BY proposed_at DESC LIMIT 50`).all();
    }
    if (session.role !== 'admin') rows = rows.filter(r => r.proposed_by === session.username);
    res.json({ ok: true, requests: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ─── end v40 Phase 18.15: WO Split endpoints ─────────────────

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
      // v40 P18.14f: Respect max-2 running per machine. If 2 already running on this machine
      // and this is a NEW order being created, default to 'pending' (manual promotion later).
      // For existing orders we honor their current status to avoid accidental demotions.
      const planState = getPlanningState();
      if (planState.orders) {
        // Check if order already exists (Planning Manager may have pre-entered it)
        const existingIdx = planState.orders.findIndex(o => o.id === orderId);
        const targetMachineId = orderDetails.machineId;
        const runningOnMachine = (planState.orders || []).filter(o =>
          o.machineId === targetMachineId &&
          o.status === 'running' &&
          o.id !== orderId &&
          !o.deleted
        ).length;
        const proposedStatus = (runningOnMachine >= 2 && existingIdx < 0) ? 'pending' : 'running';
        if (runningOnMachine >= 2 && existingIdx < 0) {
          console.warn(`[v40 P18.14f] Reconciliation: machine ${targetMachineId} already has 2 running orders. Reconciled order ${orderId} created as 'pending' — manual promotion required.`);
        }
        const orderToSave = {
          ...orderDetails,
          id: orderId,
          startDate: request.back_date,
          actualQty: mappings.reduce((s,m) => s + (m.actualLakhs || 0), 0),
          status: proposedStatus
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
      build: 'v44ZL',
      db: DB_PATH,
      planningSavedAt: planningRow?.saved_at || null,
      dprRecords: dprCount?.c || 0,
      actualsEntries: actualsCount?.c || 0,
      uptime: Math.floor(process.uptime()) + 's',
    });
  } catch(err) {
    // Server is alive even if DB query fails (e.g. still warming up)
    res.json({ ok: true, server: 'Sunloc Integrated Server v1.0', build: 'v44ZL', db: DB_PATH, uptime: Math.floor(process.uptime())+'s', note: 'DB initialising: '+err.message });
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

// GET /api/daily-printing/salvage-pct  (v44J #2 — Report E transport, READ-ONLY)
// Returns per-batch WHOLE-batch cumulative printing salvage % = 100·Σsalvage / ΣtotalOutput,
// summed across ALL of that batch's Daily Printing Log rows. Keyed by UPPER-CASED batch number
// (the denormalized data_json.batchNumber, which is what Report E keys batches by). Tracking's
// Report E multiplies this % by AIM Out (Lakhs) to obtain printing salvage in Lakhs — no KG→Lakh
// conversion needed. Rows with no batchNumber are irrelevant to Report E (its batches all have one)
// and are skipped. Aggregation done in JS so the pgPool and SQLite paths are identical.
app.get('/api/daily-printing/salvage-pct', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT data_json FROM daily_printing');
      rows = r.rows.map(x => typeof x.data_json === 'string' ? JSON.parse(x.data_json) : x.data_json);
    } else {
      rows = db.prepare('SELECT data_json FROM daily_printing').all().map(x => JSON.parse(x.data_json));
    }
    const agg = {}; // batchUpper -> { sal, out }
    for (const l of rows) {
      if (!l) continue;
      const b = (l.batchNumber || '').trim();
      if (!b) continue;
      const k = b.toUpperCase();
      if (!agg[k]) agg[k] = { sal: 0, out: 0 };
      agg[k].sal += parseFloat(l.salvage || 0) || 0;
      agg[k].out += parseFloat(l.totalOutput || 0) || 0;
    }
    const pct = {};
    for (const k in agg) pct[k] = agg[k].out > 0 ? (agg[k].sal / agg[k].out * 100) : 0;
    res.json({ ok: true, pct });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});
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

// DELETE /api/daily-printing/:id — remove a single daily printing log (edit-typo / dedup support)
// v41ZE #5/#6: deletes must reach the dedicated daily_printing table, otherwise a row removed in
// the client reappears on the next load (the bulk endpoint only upserts, never deletes).
app.delete('/api/daily-printing/:id', async (req, res) => {
  try {
    const id = req.params.id;
    if (!id) return res.status(400).json({ ok: false, error: 'id required' });
    if (pgPool) {
      await pgPool.query('DELETE FROM daily_printing WHERE id=$1', [id]);
    } else {
      db.prepare('DELETE FROM daily_printing WHERE id=?').run(id);
    }
    res.json({ ok: true, deleted: id });
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
    // v40 P18.16: refuse disabled accounts
    if (user.is_active === 0 || user.is_active === false) {
      return res.status(403).json({ ok: false, error: 'Account is disabled. Contact your administrator.' });
    }
    if (user.pin_hash !== hashPin(pin)) return res.status(401).json({ ok: false, error: 'Invalid PIN' });
    const token = generateToken();
    const expires = new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString().replace('T',' ').slice(0,19);
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
    // v40 P18.16: targetApp accepted so admin can change PINs across all 3 apps.
    // Non-admin users can still change ONLY their own PIN (within their own app).
    const { token, username, newPin, targetApp } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    const isSelfEdit = session.username === username;
    if (session.role !== 'admin' && !isSelfEdit) {
      return res.status(403).json({ ok: false, error: 'Only admin can change other users PINs' });
    }
    if (!newPin || String(newPin).length < 4) {
      return res.status(400).json({ ok: false, error: 'PIN must be at least 4 characters' });
    }
    // Cross-app PIN change is admin-only. Self-edits restricted to session.app.
    const effectiveApp = (session.role === 'admin' && targetApp) ? targetApp : session.app;
    if (pgPool) {
      const r = await pgPool.query('UPDATE app_users SET pin_hash=$1, updated_at=NOW() WHERE username=$2 AND app=$3', [hashPin(newPin), username, effectiveApp]);
      if (r.rowCount === 0) return res.status(404).json({ ok: false, error: `User ${username} not found in app ${effectiveApp}` });
    } else {
      const info = db.prepare(`UPDATE app_users SET pin_hash=?, updated_at=datetime('now') WHERE username=? AND app=?`).run(hashPin(newPin), username, effectiveApp);
      if (info.changes === 0) return res.status(404).json({ ok: false, error: `User ${username} not found in app ${effectiveApp}` });
    }
    logAudit(session.username, session.role, session.app, 'CHANGE_PIN', `Changed PIN for ${username} (app=${effectiveApp})`, req.ip);
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
      // v40 P18.14f: Respect max-2 running per machine. If 2 already running on this machine
      // and this is a NEW order being created, default to 'pending' (manual promotion later).
      // For existing orders we honor their current status to avoid accidental demotions.
      const planState = getPlanningState();
      if (planState.orders) {
        // Check if order already exists (Planning Manager may have pre-entered it)
        const existingIdx = planState.orders.findIndex(o => o.id === orderId);
        const targetMachineId = orderDetails.machineId;
        const runningOnMachine = (planState.orders || []).filter(o =>
          o.machineId === targetMachineId &&
          o.status === 'running' &&
          o.id !== orderId &&
          !o.deleted
        ).length;
        const proposedStatus = (runningOnMachine >= 2 && existingIdx < 0) ? 'pending' : 'running';
        if (runningOnMachine >= 2 && existingIdx < 0) {
          console.warn(`[v40 P18.14f] Reconciliation: machine ${targetMachineId} already has 2 running orders. Reconciled order ${orderId} created as 'pending' — manual promotion required.`);
        }
        const orderToSave = {
          ...orderDetails,
          id: orderId,
          startDate: request.back_date,
          actualQty: mappings.reduce((s,m) => s + (m.actualLakhs || 0), 0),
          status: proposedStatus
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
      build: 'v44ZL',
      db: DB_PATH,
      planningSavedAt: planningRow?.saved_at || null,
      dprRecords: dprCount?.c || 0,
      actualsEntries: actualsCount?.c || 0,
      uptime: Math.floor(process.uptime()) + 's',
    });
  } catch(err) {
    // Server is alive even if DB query fails (e.g. still warming up)
    res.json({ ok: true, server: 'Sunloc Integrated Server v1.0', build: 'v44ZL', db: DB_PATH, uptime: Math.floor(process.uptime())+'s', note: 'DB initialising: '+err.message });
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
    // v41 PERF: ?light=1 omits the heavy scans + labels arrays (SELECT * over ~29.5k scan rows).
    // The client requests light mode once it has already loaded scans (STEP 3) and labels (STEP 2)
    // via their own endpoints — it only needs stageClosure/dispatchRecs/wastage/alerts from here.
    // This removes a large per-sync payload that was a primary cause of Tracking slowness.
    const light = req.query.light === '1' || req.query.light === 'true';
    if (pgPool) {
      const mapClosure = r => ({ ...r, batchNumber: r.batch_number, closedAt: r.closed_at, closedBy: r.closed_by, shortClose: r.short_close, shortReason: r.short_reason, shortBoxes: r.short_boxes });
      const mapWastage = r => ({ ...r, batchNumber: r.batch_number });
      const mapDispatch = r => ({ ...r, batchNumber: r.batch_number, vehicleNo: r.vehicle_no, invoiceNo: r.invoice_no });
      const mapAlert = r => ({ ...r, labelId: r.label_id, batchNumber: r.batch_number, scanInTs: r.scan_in_ts, hoursStuck: r.hours_stuck });
      const mapRev = r => ({ reversedScanId: r.reversed_scan_id, labelId: r.label_id, batchNumber: r.batch_number, dept: r.dept, type: r.type });
      if (light) {
        const [closure, wastage, dispatch, alerts, revs] = await Promise.all([
          pgPool.query('SELECT * FROM tracking_stage_closure'),
          pgPool.query('SELECT * FROM tracking_wastage ORDER BY ts ASC'),
          pgPool.query('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC'),
          pgPool.query('SELECT * FROM tracking_alerts WHERE resolved = 0'),
          pgPool.query('SELECT reversed_scan_id, label_id, batch_number, dept, type FROM tracking_scan_reversals'),
        ]);
        return res.json({ ok: true, light: true, state: {
          stageClosure: closure.rows.map(mapClosure), wastage: wastage.rows.map(mapWastage),
          dispatchRecs: dispatch.rows.map(mapDispatch), alerts: alerts.rows.map(mapAlert),
          scanReversals: revs.rows.map(mapRev)
        }});
      }
      const [labels, scans, closure, wastage, dispatch, alerts, revs] = await Promise.all([
        pgPool.query('SELECT * FROM tracking_labels ORDER BY generated DESC'),
        pgPool.query('SELECT * FROM tracking_scans ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_stage_closure'),
        pgPool.query('SELECT * FROM tracking_wastage ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_alerts WHERE resolved = 0'),
        pgPool.query('SELECT reversed_scan_id, label_id, batch_number, dept, type FROM tracking_scan_reversals'),
      ]);
      const mapLabel = r => ({ ...r, batchNumber: r.batch_number, labelNumber: r.label_number, isPartial: r.is_partial, isOrange: r.is_orange, parentLabelId: r.parent_label_id, pcCode: r.pc_code, poNumber: r.po_number, machineId: r.machine_id, printingMatter: r.printing_matter, printedAt: r.printed_at, voidReason: r.void_reason, voidedAt: r.voided_at, voidedBy: r.voided_by, qrData: r.qr_data, woStatus: r.wo_status, shipTo: r.ship_to, billTo: r.bill_to, isExcess: r.is_excess, excessNum: r.excess_num, excessTotal: r.excess_total, normalTotal: r.normal_total });
      const mapScan = r => ({ ...r, labelId: r.label_id, batchNumber: r.batch_number, labelNumber: r.label_number });
      res.json({ ok: true, state: {
        labels: labels.rows.map(mapLabel), scans: scans.rows.map(mapScan),
        stageClosure: closure.rows.map(mapClosure), wastage: wastage.rows.map(mapWastage),
        dispatchRecs: dispatch.rows.map(mapDispatch), alerts: alerts.rows.map(mapAlert),
        scanReversals: revs.rows.map(mapRev)
      }});
    } else {
      const closure = db.prepare('SELECT * FROM tracking_stage_closure').all();
      const wastage = db.prepare('SELECT * FROM tracking_wastage ORDER BY ts ASC').all();
      const dispatch= db.prepare('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC').all();
      const alerts  = db.prepare('SELECT * FROM tracking_alerts WHERE resolved = 0').all();
      const revs = db.prepare('SELECT reversed_scan_id AS reversedScanId, label_id AS labelId, batch_number AS batchNumber, dept, type FROM tracking_scan_reversals').all();
      if (light) {
        return res.json({ ok: true, light: true, state: { stageClosure: closure, wastage, dispatchRecs: dispatch, alerts, scanReversals: revs } });
      }
      const labels  = db.prepare('SELECT * FROM tracking_labels ORDER BY generated DESC').all();
      const scans   = db.prepare('SELECT * FROM tracking_scans ORDER BY ts ASC').all();
      res.json({ ok: true, state: { labels, scans, stageClosure: closure, wastage, dispatchRecs: dispatch, alerts, scanReversals: revs } });
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
        safeQuery(`SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans s WHERE NOT EXISTS (SELECT 1 FROM tracking_scan_reversals r WHERE r.reversed_scan_id=s.id) GROUP BY batch_number, dept, type`),
        safeQuery('SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type'),
        safeQuery('SELECT batch_number, SUM(qty) as total_qty FROM tracking_dispatch_records GROUP BY batch_number')
      ]);
    } else {
      scanRows     = db.prepare(`SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans s WHERE NOT EXISTS (SELECT 1 FROM tracking_scan_reversals r WHERE r.reversed_scan_id=s.id) GROUP BY batch_number, dept, type`).all();
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

    // v41ZI Item 4: authoritative per-batch DPR gross (override → pure batch sum), so Tracking
    // Reports D & E show the DPR-entered gross for EVERY batch. The warm is fire-and-forget (NOT
    // awaited) — this endpoint is on the Tracking sync path (15s timeout) and must stay fast. The
    // gross maps are kept warm by the startup warm + planning/state polling, so they're populated
    // here in practice; on a rare cold read grossByBatch is briefly empty and self-heals next sync.
    warmActualsCache().catch(()=>{});
    const grossByBatch = {};
    for (const bn of Object.keys(_grossByBatch || {})) grossByBatch[bn] = effectiveGross(bn);
    for (const bn of Object.keys(_grossOverride || {})) grossByBatch[bn] = _grossOverride[bn];

    res.json({ ok: true, summary, wastage, dispatched, grossByBatch });
  } catch(err) {
    console.error('[scan-summary]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══ v41 P19.6 (Q3): Month-attributed A-Grade ═══
// Attributes A-Grade to the MONTH OF PRODUCTION using a 6 AM boundary on scan ts.
// Production window for month YYYY-MM = [YYYY-MM-01 06:00:00, nextMonth-01 06:00:00).
// Rationale (per Ishan): a batch produced across a month boundary (e.g. 31 May–2 Jun)
// has its May A-Grade computed from ONLY the scans/wastage timestamped before 6 AM on
// 1 Jun (end of C-shift). Everything after rolls into June. OUT, salvage and remelt are
// ALL sliced at the same boundary (option A — symmetric slice) so each month's A-Grade %
// is internally consistent: pct = OUT / (OUT + Salvage + Remelt), all within-window.
// Read-only aggregation from tracking_scans + tracking_wastage (both carry `ts`).
// No migration, no schema change.
function _v41_monthWindow(ym){
  // ym = 'YYYY-MM' → { start, end } as 'YYYY-MM-DD HH:MM:SS' strings (local clock, 06:00 boundary)
  const [y, m] = ym.split('-').map(Number);
  const pad = n => String(n).padStart(2,'0');
  const start = `${y}-${pad(m)}-01 06:00:00`;
  // next month
  const ny = m === 12 ? y+1 : y;
  const nm = m === 12 ? 1   : m+1;
  const end = `${ny}-${pad(nm)}-01 06:00:00`;
  return { start, end };
}

app.get('/api/tracking/agrade-by-month', async (req, res) => {
  try {
    const ym = String(req.query.month||'').trim();
    if (!/^\d{4}-\d{2}$/.test(ym)) return res.status(400).json({ ok:false, error:'month=YYYY-MM required' });
    const { start, end } = _v41_monthWindow(ym);

    // Also need to know which batches had ANY production activity that SPANS the boundary,
    // so the client can flag cross-month batches. A batch is cross-month if it has scans
    // both before `start`+window AND after `end` boundary anywhere in its history.
    let scanRows, wastageRows, spanRows;
    if (pgPool) {
      const sq = async (sql, params) => {
        try { return (await pgPool.query(sql, params)).rows; }
        catch(e){ console.warn('[agrade-by-month] pg query failed:', e.message); return []; }
      };
      [scanRows, wastageRows, spanRows] = await Promise.all([
        sq(`SELECT batch_number, dept, type,
              COUNT(*) FILTER (WHERE label_id NOT LIKE 'recon-%') AS box_cnt,
              COALESCE(SUM(qty) FILTER (WHERE label_id LIKE 'recon-%'),0) AS recon_qty
            FROM tracking_scans WHERE ts >= $1 AND ts < $2
            GROUP BY batch_number, dept, type`, [start, end]),
        sq(`SELECT batch_number, dept, type, COALESCE(SUM(qty),0) AS total_qty
            FROM tracking_wastage WHERE ts >= $1 AND ts < $2
            GROUP BY batch_number, dept, type`, [start, end]),
        // batches with scans before window start AND scans on/after window end (true span across this month's boundaries)
        sq(`SELECT batch_number,
              MIN(ts) AS first_ts, MAX(ts) AS last_ts
            FROM tracking_scans GROUP BY batch_number`, [])
      ]);
    } else {
      const sq = (sql, params) => { try { return db.prepare(sql).all(...params); } catch(e){ console.warn('[agrade-by-month] sqlite query failed:', e.message); return []; } };
      scanRows    = sq(`SELECT batch_number, dept, type,
                          SUM(CASE WHEN label_id NOT LIKE 'recon-%' THEN 1 ELSE 0 END) AS box_cnt,
                          COALESCE(SUM(CASE WHEN label_id LIKE 'recon-%' THEN qty ELSE 0 END),0) AS recon_qty
                        FROM tracking_scans WHERE ts >= ? AND ts < ?
                        GROUP BY batch_number, dept, type`, [start, end]);
      wastageRows = sq(`SELECT batch_number, dept, type, COALESCE(SUM(qty),0) AS total_qty
                        FROM tracking_wastage WHERE ts >= ? AND ts < ?
                        GROUP BY batch_number, dept, type`, [start, end]);
      spanRows    = sq(`SELECT batch_number, MIN(ts) AS first_ts, MAX(ts) AS last_ts
                        FROM tracking_scans GROUP BY batch_number`, []);
    }

    // Build per-batch, per-dept windowed scan summary.
    // boxes = real scan count (box-count path, matches canonical A-Grade via boxToLakh client-side)
    // reconQty = Lakhs from synthetic reconciliation 'output' scans (already in Lakhs, added directly)
    const summary = {};
    const ensure = (bn, dept) => {
      if (!summary[bn]) summary[bn] = {};
      if (!summary[bn][dept]) summary[bn][dept] = { inBoxes:0, outBoxes:0, inReconQty:0, outReconQty:0 };
    };
    scanRows.forEach(r => {
      const bn = r.batch_number; if (!bn) return;
      ensure(bn, r.dept);
      const boxes = parseInt(r.box_cnt||0,10);
      const reconQ = parseFloat(r.recon_qty||0);
      if (r.type === 'in')  { summary[bn][r.dept].inBoxes  += boxes; summary[bn][r.dept].inReconQty  += reconQ; }
      if (r.type === 'out') { summary[bn][r.dept].outBoxes += boxes; summary[bn][r.dept].outReconQty += reconQ; }
    });
    const wastage = {};
    wastageRows.forEach(r => {
      const bn = r.batch_number; if (!bn) return;
      if (!wastage[bn]) wastage[bn] = {};
      if (!wastage[bn][r.dept]) wastage[bn][r.dept] = { salvage:0, remelt:0 };
      if (r.type === 'salvage') wastage[bn][r.dept].salvage += parseFloat(r.total_qty||0);
      if (r.type === 'remelt')  wastage[bn][r.dept].remelt  += parseFloat(r.total_qty||0);
    });
    // Cross-month flag: batch whose scan history starts before this window's end but also
    // continues at/after the window end boundary → production straddled the boundary.
    const crossMonth = {};
    spanRows.forEach(r => {
      const bn = r.batch_number; if (!bn) return;
      const first = r.first_ts || '', last = r.last_ts || '';
      // straddles if first scan is before window end AND last scan is on/after window end,
      // OR first scan is before window start AND last scan is on/after window start
      const straddlesEnd   = first && last && first < end   && last >= end;
      const straddlesStart = first && last && first < start && last >= start;
      if (straddlesEnd || straddlesStart) crossMonth[bn] = true;
    });

    // v41i FIX (issue 4): month-sliced GROSS production per batch, summed from production_actuals
    // by CALENDAR month of the entry date — exactly the basis the DPR "Produced" report uses
    // (DPR sums daily shift entries whose date is in the selected YYYY-MM). Report E previously used
    // the batch's ALL-TIME actualProd attributed to one month, so batches spanning April→May didn't
    // reconcile with DPR. With this, Report E gross (month mode) = sum of that batch's DPR entries
    // dated within YYYY-MM, matching DPR per-machine totals.
    const monthGross = {};
    try {
      let grossRows;
      if (pgPool) {
        grossRows = (await pgPool.query(
          `SELECT batch_number, COALESCE(SUM(qty_lakhs),0) AS g
             FROM production_actuals WHERE date LIKE $1 GROUP BY batch_number`, [ym + '%'])).rows;
      } else {
        grossRows = db.prepare(
          `SELECT batch_number, COALESCE(SUM(qty_lakhs),0) AS g
             FROM production_actuals WHERE date LIKE ? GROUP BY batch_number`).all(ym + '%');
      }
      for (const r of (grossRows||[])) {
        if (r.batch_number) monthGross[r.batch_number] = parseFloat(r.g) || 0;
      }
    } catch(e) { console.warn('[agrade-by-month] monthGross query failed:', e.message); }

    res.json({ ok:true, month:ym, window:{ start, end }, summary, wastage, crossMonth, monthGross });
  } catch(err) {
    console.error('[agrade-by-month]', err.message);
    res.status(500).json({ ok:false, error: err.message });
  }
});

app.get('/api/tracking/wip-summary', async (req, res) => {
  try {
    // v41ZH #1a: Planning's loadTrackingStatus (every 60s, on every open Planning tab) only reads
    // `closures` from this response — it explicitly discards `scanSummary`. The scan GROUP BY over
    // the full, unbounded tracking_scans table was therefore pure wasted work every minute. When
    // ?closuresOnly=1 is passed we skip that aggregation entirely and return closures only. DPR
    // still calls without the flag (it genuinely consumes scanSummary for per-machine A-Grade), so
    // its behaviour is byte-identical to before.
    const closuresOnly = req.query.closuresOnly === '1' || req.query.closuresOnly === 'true';
    let summary = [], closures;
    if (pgPool) {
      if (!closuresOnly) {
        const r1 = await pgPool.query('SELECT batch_number, dept, type, COUNT(*) as cnt FROM tracking_scans GROUP BY batch_number, dept, type');
        summary = r1.rows;
      }
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
      if (!closuresOnly) {
        summary = db.prepare('SELECT batch_number, dept, type, COUNT(*) as cnt FROM tracking_scans GROUP BY batch_number, dept, type').all();
      }
      closures = db.prepare("SELECT batch_number, dept, closed, closed_at FROM tracking_stage_closure WHERE closed = 1").all();
    }
    res.json({ ok: true, scanSummary: summary, closures });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── v41ZH #1: Lightweight sync-version probe ──────────────────
// Returns cheap aggregate signatures so the tracking client can decide which heavy endpoints
// actually need re-fetching on its 2-minute auto-sync. The goal: never re-pull a large quantum
// of data (all ~10k labels, the full box-stages map, the scan-summary aggregation) when nothing
// has changed. Each signature changes on the mutations that matter:
//   labels   — count (insert), voided (void), printed (print) → covers display-affecting changes
//   scans    — count + max(ts) (scans are append-only) → drives scan-summary + box-stages + recent
//   wastage  — count (append-only)
//   dispatch — count (Phase 18 truck dispatches; affects box-stages 'dispatched' promotion)
// All four are COUNT/MAX aggregates with tiny result payloads. No history is dropped anywhere —
// the heavy endpoints themselves are unchanged; the client just skips calling them when the
// signature is identical to the last successfully-applied pull.
app.get('/api/tracking/sync-version', async (req, res) => {
  try {
    const one = async (sql) => {
      try {
        if (pgPool) { const r = await pgPool.query(sql); return r.rows[0] || {}; }
        return db.prepare(sql).get() || {};
      } catch (e) { return {}; }
    };
    // voided/printed flags are stored as 0/1 (SQLite) or boolean/0-1 (PG). COALESCE→0 then
    // compare > 0 / truthy works in both dialects without dialect-specific literals.
    const labSql = pgPool
      ? "SELECT COUNT(*) AS count, COALESCE(SUM(CASE WHEN COALESCE(voided,0)::int <> 0 THEN 1 ELSE 0 END),0) AS voided, COALESCE(SUM(CASE WHEN COALESCE(printed,0)::int <> 0 THEN 1 ELSE 0 END),0) AS printed FROM tracking_labels"
      : "SELECT COUNT(*) AS count, COALESCE(SUM(CASE WHEN COALESCE(voided,0)<>0 THEN 1 ELSE 0 END),0) AS voided, COALESCE(SUM(CASE WHEN COALESCE(printed,0)<>0 THEN 1 ELSE 0 END),0) AS printed FROM tracking_labels";
    const lab = await one(labSql);
    const scn = await one('SELECT COUNT(*) AS count, MAX(ts) AS maxts FROM tracking_scans');
    const wst = await one('SELECT COUNT(*) AS count FROM tracking_wastage');
    const dsp = await one('SELECT COUNT(*) AS count FROM tracking_dispatch_records');
    res.json({
      ok: true,
      labels:   { count: parseInt(lab.count || 0, 10), voided: parseInt(lab.voided || 0, 10), printed: parseInt(lab.printed || 0, 10) },
      scans:    { count: parseInt(scn.count || 0, 10), maxTs: scn.maxts || scn.maxTs || null },
      wastage:  { count: parseInt(wst.count || 0, 10) },
      dispatch: { count: parseInt(dsp.count || 0, 10) }
    });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});


// ── Labels lookup by batchNumber (scanning fallback) ──
// ── Save new labels to PostgreSQL directly ──────────────────
app.post('/api/tracking/labels', async (req, res) => {
  try {
    const { labels } = req.body;
    if (!labels || !labels.length) return res.status(400).json({ ok: false, error: 'No labels' });

    // v41y FIX Item 1 (defense in depth): refuse to insert a NEW label whose
    // (batch_number, label_number, is_orange, is_excess) collides with an existing
    // non-voided row that has a different id. Catches the rare case where two devices
    // (or two rapid clicks bypassing the client _v41y_labelGenInFlight flag) try to
    // create labels with the same number for the same batch. Updates to an existing id
    // (reprint state, void, etc.) still pass through.
    const parseLabelNum = n => { if (n == null) return null; const s = String(n).replace(/^OL-/i, ''); const p = parseInt(s); return isNaN(p) ? null : p; };
    const skipped = [];
    const accepted = [];
    if (pgPool) {
      for (const l of labels) {
        const lnum = parseLabelNum(l.labelNumber || l.label_number);
        const bn = l.batchNumber || l.batch_number;
        if (!bn || lnum == null) { accepted.push(l); continue; }
        const r = await pgPool.query(
          `SELECT id FROM tracking_labels
             WHERE batch_number=$1 AND label_number=$2
               AND is_orange=$3 AND is_excess=$4
               AND COALESCE(voided,0)=0 AND id <> $5
             LIMIT 1`,
          [bn, lnum, l.isOrange ? 1 : 0, l.isExcess ? 1 : 0, l.id]
        );
        if (r.rows.length > 0) {
          skipped.push({ id: l.id, batchNumber: bn, labelNumber: lnum, reason: 'duplicate', existingId: r.rows[0].id });
        } else {
          accepted.push(l);
        }
      }
    } else {
      for (const l of labels) {
        const lnum = parseLabelNum(l.labelNumber || l.label_number);
        const bn = l.batchNumber || l.batch_number;
        if (!bn || lnum == null) { accepted.push(l); continue; }
        const ex = db.prepare(
          `SELECT id FROM tracking_labels
             WHERE batch_number=? AND label_number=?
               AND is_orange=? AND is_excess=?
               AND COALESCE(voided,0)=0 AND id <> ?
             LIMIT 1`
        ).get(bn, lnum, l.isOrange ? 1 : 0, l.isExcess ? 1 : 0, l.id);
        if (ex) {
          skipped.push({ id: l.id, batchNumber: bn, labelNumber: lnum, reason: 'duplicate', existingId: ex.id });
        } else {
          accepted.push(l);
        }
      }
    }
    if (skipped.length > 0) {
      console.warn(`[v41y label-dup] Skipped ${skipped.length} duplicate label(s):`, skipped.slice(0, 5).map(s => `${s.batchNumber}#${s.labelNumber}`).join(', '));
    }
    if (accepted.length === 0) {
      return res.json({ ok: true, count: 0, skipped: skipped.length, duplicates: skipped });
    }
    const labelsToWrite = accepted;
    if (pgPool) {
      for (const l of labelsToWrite) {
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
      labelsToWrite.forEach(l => stmt.run(
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
    res.json({ ok: true, saved: labelsToWrite.length, skipped: skipped.length, duplicates: skipped });
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
// v41b PAYLOAD SLIM: qr_data (the full QR string per label) is intentionally EXCLUDED here.
// It is only needed when actually printing a label, and the client rebuilds it deterministically
// from batchNumber|labelNumber|size|qty|id via generateQRData() (identical to what was stored).
// Excluding it roughly halves the ~10.2k-row bulk response, removing the remaining timeout risk
// on the label load. Explicit column list (no SELECT *) keeps the payload to exactly what the
// label list / dashboard / print queue need.
app.get('/api/tracking/labels-all', async (req, res) => {
  try {
    const COLS = `id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,wo_status,ship_to,bill_to,is_excess,excess_num,excess_total,normal_total`;
    const m=r=>({id:r.id,batchNumber:r.batch_number,labelNumber:r.label_number,size:r.size,qty:r.qty,isPartial:!!r.is_partial,isOrange:!!r.is_orange,parentLabelId:r.parent_label_id||null,customer:r.customer||'',colour:r.colour||'',pcCode:r.pc_code||'',poNumber:r.po_number||'',machineId:r.machine_id||'',printingMatter:r.printing_matter||'',generated:r.generated,printed:!!r.printed,printedAt:r.printed_at||null,voided:!!r.voided,voidReason:r.void_reason||'',voidedAt:r.voided_at||null,voidedBy:r.voided_by||null,woStatus:r.wo_status||null,shipTo:r.ship_to||'',billTo:r.bill_to||'',isExcess:!!r.is_excess,excessNum:r.excess_num||null,excessTotal:r.excess_total||null,normalTotal:r.normal_total||null});
    if(pgPool){const r=await pgPool.query(`SELECT ${COLS} FROM tracking_labels ORDER BY generated DESC`);res.json({ok:true,labels:r.rows.map(m)});}
    else{const labels=db.prepare(`SELECT ${COLS} FROM tracking_labels ORDER BY generated DESC`).all();res.json({ok:true,labels:labels.map(m)});}
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
    const since = req.query.since || null;   // optional: 'YYYY-MM-DD' window start
    // v41 PERF FIX: optional ?limit=N caps the number of most-recent rows returned.
    // The 5-second operator-UI refresh only needs the latest handful of scans + recent
    // alerts; it does NOT need full history (reports use scan-summary, per-box uses
    // box-stages). Capping it keeps the frequent poll tiny so it never aborts/starves
    // the rest of the sync. When neither since nor limit is supplied behaviour is
    // unchanged (returns all) for backward compatibility with any other caller.
    let limit = parseInt(req.query.limit, 10);
    if (!Number.isFinite(limit) || limit <= 0) limit = 0; // 0 = no cap
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
    // v44F PERF: cap since-queries at 2000 rows
    if (since && (limit <= 0 || limit > 2000)) limit = 2000;
    const limitClause = limit > 0 ? ` LIMIT ${limit}` : '';
    if (pgPool) {
      // Try with label_number column first (after migration v10)
      let rows;
      try {
        const r = await pgPool.query(
          `SELECT * FROM tracking_scans ${whereClause} ORDER BY ts DESC${limitClause}`
        );
        rows = r.rows;
      } catch(e) {
        // Fallback if column issues — select without label_number
        const r = await pgPool.query(
          `SELECT id,label_id,batch_number,dept,type,ts,operator,size,qty FROM tracking_scans ${whereClause} ORDER BY ts DESC${limitClause}`
        );
        rows = r.rows;
      }
      res.json({ ok: true, scans: rows.map(mapScan), count: rows.length });
    } else {
      let scans;
      try {
        scans = db.prepare(`SELECT * FROM tracking_scans ${whereClause} ORDER BY ts DESC${limitClause}`).all();
      } catch(e) {
        scans = db.prepare(`SELECT id,label_id,batch_number,dept,type,ts,operator,size,qty FROM tracking_scans ${whereClause} ORDER BY ts DESC${limitClause}`).all();
      }
      res.json({ ok: true, scans: scans.map(mapScan), count: scans.length });
    }
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// v44S Issue 1: IST-aware day-range → UTC ISO bounds. Scans store ts as UTC ISO
// (new Date().toISOString()), but the UI shows the IST date. A from/to day range must therefore be
// converted to IST-midnight boundaries (IST = UTC + 5:30), or early-IST-morning scans (which carry
// the PREVIOUS UTC calendar date) leak across the visible day boundary and the range looks wrong
// even though the totals are right. fromTs = from-day 00:00 IST; toTs = (to-day +1) 00:00 IST, so
// `ts >= fromTs AND ts < toTs` captures exactly the IST days the user picked. Both are full ISO
// strings compared against the full ISO ts (lexicographic == chronological for same-format ISO).
function _istRangeBounds(from, to) {
  const out = {};
  if (from) out.fromTs = new Date(from + 'T00:00:00+05:30').toISOString();
  if (to)   out.toTs   = new Date(new Date(to + 'T00:00:00+05:30').getTime() + 86400000).toISOString();
  return out;
}
// v44S: IST calendar date (YYYY-MM-DD) of a UTC ISO timestamp — shift the instant +5:30 then take
// the UTC date, so per-day grouping/labels match what the operator saw on the IST clock.
function _istDate(ts) {
  if (!ts) return '';
  const t = new Date(ts).getTime();
  if (!Number.isFinite(t)) return (ts || '').slice(0, 10);
  return new Date(t + (5 * 60 + 30) * 60000).toISOString().slice(0, 10);
}

// GET /api/tracking/scans-filtered  (v44K #7 — recent-scans filter panel, READ-ONLY)
// Filters tracking_scans by dept (required) + optional from/to date range, batch, type.
// PARAMETERIZED queries (no string interpolation of user-supplied values). Capped 5000, newest first.
app.get('/api/tracking/scans-filtered', async (req, res) => {
  try {
    const dept = (req.query.dept || '').trim();
    if (!dept) return res.status(400).json({ ok:false, error:'dept required' });
    const from  = (req.query.from  || '').trim();   // 'YYYY-MM-DD' inclusive (IST day)
    const to    = (req.query.to    || '').trim();    // 'YYYY-MM-DD' inclusive (IST day)
    const batch = (req.query.batch || '').trim();
    const type  = (req.query.type  || '').trim();    // 'in' | 'out' | '' (all)
    const { fromTs, toTs } = _istRangeBounds(from, to);
    let cap = parseInt(req.query.limit, 10);
    if (!Number.isFinite(cap) || cap <= 0 || cap > 5000) cap = 5000;
    const mapScan = r => ({ id:r.id, labelId:r.label_id, batchNumber:r.batch_number, dept:r.dept, type:r.type, ts:r.ts, operator:r.operator||null, size:r.size||null, qty:r.qty||null });
    const cols = 'id,label_id,batch_number,dept,type,ts,operator,size,qty';
    if (pgPool) {
      const cond=['dept=$1']; const params=[dept]; let i=2;
      if(fromTs){ cond.push(`ts >= $${i++}`); params.push(fromTs); }
      if(toTs){ cond.push(`ts < $${i++}`); params.push(toTs); }
      if(batch){ cond.push(`batch_number=$${i++}`); params.push(batch); }
      if(type==='in'||type==='out'){ cond.push(`type=$${i++}`); params.push(type); }
      const r = await pgPool.query(`SELECT ${cols} FROM tracking_scans WHERE ${cond.join(' AND ')} ORDER BY ts DESC LIMIT ${cap}`, params);
      res.json({ ok:true, scans:r.rows.map(mapScan), count:r.rows.length });
    } else {
      const cond=['dept=?']; const params=[dept];
      if(fromTs){ cond.push('ts >= ?'); params.push(fromTs); }
      if(toTs){ cond.push('ts < ?'); params.push(toTs); }
      if(batch){ cond.push('batch_number=?'); params.push(batch); }
      if(type==='in'||type==='out'){ cond.push('type=?'); params.push(type); }
      const scans = db.prepare(`SELECT ${cols} FROM tracking_scans WHERE ${cond.join(' AND ')} ORDER BY ts DESC LIMIT ${cap}`).all(...params);
      res.json({ ok:true, scans:scans.map(mapScan), count:scans.length });
    }
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/tracking/packing-ledger  (v44K #8 — per-day per-batch packing scans, READ-ONLY)
// Aggregates tracking_scans at dept='packing' into per (date, batch) scan-in / scan-out counts.
// Optional from/to date range (parameterized). Customer/Colour/PC code joined client-side from
// state.batches. Aggregation in JS so the pgPool and SQLite paths are identical. ('packing' is a
// fixed literal, not user input.)
app.get('/api/tracking/packing-ledger', async (req, res) => {
  try {
    const from = (req.query.from || '').trim();
    const to   = (req.query.to   || '').trim();
    const { fromTs, toTs } = _istRangeBounds(from, to);
    let rows;
    if (pgPool) {
      const cond=["dept='packing'"]; const params=[]; let i=1;
      if(fromTs){ cond.push(`ts >= $${i++}`); params.push(fromTs); }
      if(toTs){ cond.push(`ts < $${i++}`); params.push(toTs); }
      const r = await pgPool.query(`SELECT ts,batch_number,type FROM tracking_scans WHERE ${cond.join(' AND ')}`, params);
      rows = r.rows;
    } else {
      const cond=["dept='packing'"]; const params=[];
      if(fromTs){ cond.push('ts >= ?'); params.push(fromTs); }
      if(toTs){ cond.push('ts < ?'); params.push(toTs); }
      rows = db.prepare(`SELECT ts,batch_number,type FROM tracking_scans WHERE ${cond.join(' AND ')}`).all(...params);
    }
    const agg = {}; // 'date|batch' -> {date,batch,in,out}
    for (const r of rows) {
      const d = _istDate(r.ts); const b = r.batch_number || '';
      if (!d || !b) continue;
      const k = d+'|'+b;
      if (!agg[k]) agg[k] = { date:d, batch:b, in:0, out:0 };
      if (r.type === 'in') agg[k].in++; else if (r.type === 'out') agg[k].out++;
    }
    const ledger = Object.values(agg).sort((a,b)=> a.date<b.date?1 : a.date>b.date?-1 : (a.batch<b.batch?-1:1));
    res.json({ ok:true, ledger, count:ledger.length });
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
    // v41ZH #2: authoritative per-batch NON-VOIDED label total. `labels` was loaded above with
    // `WHERE COALESCE(voided,0)=0`, and labelsByBatch groups it — so this count matches the client's
    // getLabelsByBatch(bn).length definition exactly. It is the source-of-truth fallback the tracking
    // client uses for the "Labels" header when its own label cache hasn't loaded yet (kills the
    // transient "Labels: 0 / Packed: N" race without ever changing a count that the cache would show).
    const labelCountByBatch = {};
    for (const bn in labelsByBatch) labelCountByBatch[bn] = labelsByBatch[bn].length;
    res.json({
      ok: true,
      stages,
      boxCounts,
      labelCountByBatch,
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

    // v43A #4: ORANGE-LABEL GATE at PI scan-OUT — a box cannot leave PI unless its orange label has
    // been scanned (PI printed-matter inspection). Server-authoritative so incomplete local scan state
    // can't bypass it. Orange scans live on the separate 'orange' channel (no WIP impact). Fail-OPEN
    // when the box has NO orange label at all (e.g. generation skipped) so the line is never halted —
    // we only block when an orange label exists but was not scanned. Admin may override for data fixes.
    if (scan.type === 'out' && scan.dept === 'pi' && !adminOverride) {
      try {
        let orangeRow;
        if (pgPool) {
          const ro = await pgPool.query(
            `SELECT ol.id, EXISTS(SELECT 1 FROM tracking_scans os WHERE os.label_id=ol.id AND os.dept='orange') AS scanned
               FROM tracking_labels ol
              WHERE ol.parent_label_id=$1 AND COALESCE(ol.is_orange,0)=1 AND COALESCE(ol.voided,0)=0
              LIMIT 1`, [labelId]);
          orangeRow = ro.rows[0];
        } else {
          orangeRow = db.prepare(
            `SELECT ol.id AS id, EXISTS(SELECT 1 FROM tracking_scans os WHERE os.label_id=ol.id AND os.dept='orange') AS scanned
               FROM tracking_labels ol
              WHERE ol.parent_label_id=? AND COALESCE(ol.is_orange,0)=1 AND COALESCE(ol.voided,0)=0
              LIMIT 1`).get(labelId);
        }
        if (orangeRow && !orangeRow.scanned) {
          return res.json({ ok:false, blocked:true, orange_gate:true,
            error:`🟠 Orange label not scanned for this box. PI must scan its orange label (printed-matter verification) before the box can leave PI.` });
        }
        // No orange label found → fail-open (allow). Orange scanned → allow.
      } catch (e) { console.warn('[v43A #4] orange-gate check failed (fail-open):', e.message); }
    }

    // v43 #5: numeric box number for box-identity dedup (orange 'OL-*' labels → null; matched by id).
    const _lnNum = (scan.labelNumber!=null && /^-?\d+$/.test(String(scan.labelNumber))) ? parseInt(scan.labelNumber,10) : null;

    if (pgPool) {
      // v43 #5: dedup by BOX IDENTITY (batch + box number) as well as label_id. The same physical box
      // can reach the server with different label_id values — manual batch-box entry vs QR SystemID, or
      // two synced label records sharing a box number — which let duplicate IN/OUT slip past the old
      // label_id-only check. Match label_id OR label_number, but EXCLUDE scans whose label has been
      // voided so a reprinted box (regeneratePartialLabel reuses the box number on a fresh, non-voided
      // label) is never wrongly blocked. Scoped to batch_number + dept → tiny result set, no perf cost.
      const existing = await pgPool.query(
        `SELECT s.type FROM tracking_scans s
           LEFT JOIN tracking_labels l ON l.id = s.label_id
          WHERE s.dept=$2 AND s.batch_number=$3
            AND (s.label_id=$1 OR ($4::integer IS NOT NULL AND s.label_number IS NOT NULL AND s.label_number=$4::integer))
            AND COALESCE(l.voided,0)=0
            AND NOT EXISTS (SELECT 1 FROM tracking_scan_reversals rv WHERE rv.reversed_scan_id = s.id)`,
        [labelId, scan.dept, batchNumber, _lnNum]
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
      // SQLite path: same box-identity dedup + IN-before-OUT check (v43 #5)
      const existing = db.prepare(
        `SELECT s.type FROM tracking_scans s
           LEFT JOIN tracking_labels l ON l.id = s.label_id
          WHERE s.dept=? AND s.batch_number=?
            AND (s.label_id=? OR (? IS NOT NULL AND s.label_number IS NOT NULL AND s.label_number=?))
            AND COALESCE(l.voided, 0)=0
            AND NOT EXISTS (SELECT 1 FROM tracking_scan_reversals rv WHERE rv.reversed_scan_id = s.id)`
      ).all(scan.dept, batchNumber, labelId, _lnNum, _lnNum);
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
    // v41ZG #3: optional ?since=YYYY-MM-DD window. The three aggregations below scan the FULL
    // tracking_scans / tracking_wastage / production_actuals tables, which have grown unbounded
    // since April. On production Postgres that exceeds the planning client's fetch timeout, so the
    // live A-Grade feed (window._liveAGrade) never loads and the Daily Printing Log's Scan Out /
    // Reconciliation columns all show "—". Windowing to recent activity keeps every per-batch value
    // identical for the batches the planning view cares about, while cutting the rows scanned.
    const sinceRaw = (req.query.since || '').trim();
    const since = /^\d{4}-\d{2}-\d{2}$/.test(sinceRaw) ? sinceRaw : null;
    let scans, wastage, prodActuals;
    if (pgPool) {
      const scanSql = since
        ? 'SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans WHERE ts >= $1 GROUP BY batch_number, dept, type'
        : 'SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans GROUP BY batch_number, dept, type';
      const wasteSql = since
        ? 'SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage WHERE ts >= $1 GROUP BY batch_number, dept, type'
        : 'SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type';
      const prodSql = since
        ? 'SELECT batch_number, SUM(qty_lakhs) as gross_prod FROM production_actuals WHERE date >= $1 GROUP BY batch_number'
        : 'SELECT batch_number, SUM(qty_lakhs) as gross_prod FROM production_actuals GROUP BY batch_number';
      const args = since ? [since] : [];
      const [r1, r2, r3] = await Promise.all([
        pgPool.query(scanSql, args),
        pgPool.query(wasteSql, args),
        pgPool.query(prodSql, args),
      ]);
      scans = r1.rows; wastage = r2.rows; prodActuals = r3.rows;
    } else {
      const scanSql = since
        ? 'SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans WHERE ts >= ? GROUP BY batch_number, dept, type'
        : 'SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans GROUP BY batch_number, dept, type';
      const wasteSql = since
        ? 'SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage WHERE ts >= ? GROUP BY batch_number, dept, type'
        : 'SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type';
      const prodSql = since
        ? 'SELECT batch_number, SUM(qty_lakhs) as gross_prod FROM production_actuals WHERE date >= ? GROUP BY batch_number'
        : 'SELECT batch_number, SUM(qty_lakhs) as gross_prod FROM production_actuals GROUP BY batch_number';
      scans = since ? db.prepare(scanSql).all(since) : db.prepare(scanSql).all();
      wastage = since ? db.prepare(wasteSql).all(since) : db.prepare(wasteSql).all();
      prodActuals = since ? db.prepare(prodSql).all(since) : db.prepare(prodSql).all();
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
      const wipLakhs = _retiredBatchSet.has((batchNo||'').toUpperCase()) ? 0 : Math.max(0, grossProd - totalWastageForWIP - packInQty);
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
  let totalBoxes = 0;
  // v40 P18.14d: dispatched_qty = sum of Phase 18 records + sum of legacy dispatch.out scan qty.
  // Both flows can co-exist on a straddle batch (started under v37, finished under v40 truck flow);
  // each represents distinct physical shipments. Planning consumes this value so it must match
  // Tracking's combined-source helpers.
  // v44R Phase 2: also aggregate dispatched_boxes (record boxes + one box per legacy dispatch-out
  // scan), so the truck binner can compute remaining = planned - dispatched per lot.
  if (pgPool) {
    const r1 = await pgPool.query(
      `SELECT COALESCE(SUM(qty),0) AS total, COALESCE(SUM(boxes),0) AS boxes FROM tracking_dispatch_records WHERE batch_number=$1`,
      [batchNumber]
    );
    const r2 = await pgPool.query(
      `SELECT COALESCE(SUM(qty),0) AS total, COUNT(*) AS boxes FROM tracking_scans WHERE batch_number=$1 AND dept='dispatch' AND type='out'`,
      [batchNumber]
    );
    const phase18Qty = parseFloat(r1.rows[0]?.total || 0);
    const legacyQty = parseFloat(r2.rows[0]?.total || 0);
    totalQty = phase18Qty + legacyQty;
    totalBoxes = (parseFloat(r1.rows[0]?.boxes || 0)) + (parseFloat(r2.rows[0]?.boxes || 0));
    await pgPool.query(`
      INSERT INTO tracking_dispatch_actuals (batch_number,dispatched_qty,dispatched_boxes,vehicle_no,invoice_no,updated_at)
      VALUES ($1,$2,$3,$4,$5,NOW())
      ON CONFLICT(batch_number) DO UPDATE SET
        dispatched_qty=EXCLUDED.dispatched_qty,
        dispatched_boxes=EXCLUDED.dispatched_boxes,
        vehicle_no=COALESCE(EXCLUDED.vehicle_no, tracking_dispatch_actuals.vehicle_no),
        invoice_no=COALESCE(EXCLUDED.invoice_no, tracking_dispatch_actuals.invoice_no),
        updated_at=NOW()
    `, [batchNumber, totalQty, totalBoxes, vehicleNo||null, invoiceNo||null]);
  } else {
    const r1 = db.prepare(`SELECT COALESCE(SUM(qty),0) AS total, COALESCE(SUM(boxes),0) AS boxes FROM tracking_dispatch_records WHERE batch_number=?`).get(batchNumber);
    const r2 = db.prepare(`SELECT COALESCE(SUM(qty),0) AS total, COUNT(*) AS boxes FROM tracking_scans WHERE batch_number=? AND dept='dispatch' AND type='out'`).get(batchNumber);
    const phase18Qty = parseFloat(r1?.total || 0);
    const legacyQty = parseFloat(r2?.total || 0);
    totalQty = phase18Qty + legacyQty;
    totalBoxes = (parseFloat(r1?.boxes || 0)) + (parseFloat(r2?.boxes || 0));
    db.prepare(`
      INSERT INTO tracking_dispatch_actuals (batch_number,dispatched_qty,dispatched_boxes,vehicle_no,invoice_no,updated_at)
      VALUES (?,?,?,?,?,datetime('now'))
      ON CONFLICT(batch_number) DO UPDATE SET
        dispatched_qty=excluded.dispatched_qty,
        dispatched_boxes=excluded.dispatched_boxes,
        vehicle_no=COALESCE(excluded.vehicle_no, tracking_dispatch_actuals.vehicle_no),
        invoice_no=COALESCE(excluded.invoice_no, tracking_dispatch_actuals.invoice_no),
        updated_at=excluded.updated_at
    `).run(batchNumber, totalQty, totalBoxes, vehicleNo||null, invoiceNo||null);
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

// ════════════════════════════════════════════════════════════════════════
// v44R Phase 2/3 — Stable truck identity (lock-on-activation)
// ────────────────────────────────────────────────────────────────────────
// A truck is ephemeral (recomputed each render) until acted on. The client LOCKS a truck the
// moment a truck-scan-session starts, or a partial dispatch/regularise hits one of its lots: the
// truck's number + manifest freeze here. The binner then renders locked trucks from their frozen
// manifest and lays remaining unlocked boxes AROUND them (no renumbering of a truck in progress).
// Short remainders roll forward automatically via the remaining-boxes re-bin — no server math here.

// GET all truck locks (optionally by zone). Planning merges these into buildTruckPlans.
app.get('/api/dispatch/truck-locks', async (req, res) => {
  try {
    const zone = req.query.zone ? String(req.query.zone) : null;
    let rows;
    if (pgPool) {
      rows = zone
        ? (await pgPool.query(`SELECT * FROM dispatch_truck_locks WHERE zone=$1`, [zone])).rows
        : (await pgPool.query(`SELECT * FROM dispatch_truck_locks`)).rows;
    } else {
      rows = zone
        ? db.prepare(`SELECT * FROM dispatch_truck_locks WHERE zone=?`).all(zone)
        : db.prepare(`SELECT * FROM dispatch_truck_locks`).all();
    }
    res.json({ ok: true, locks: rows });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST lock/upsert a truck. Body: { truckId, zone, truckNumber, manifest:[{planId,batchNumber,
// allocatedBoxes,allocatedQty}], vehicleNo, lrNo, lockedBy, remarks }. Idempotent on truckId:
// re-locking refreshes the manifest/vehicle but never silently flips a finalized truck back to active.
app.post('/api/dispatch/truck-lock', async (req, res) => {
  try {
    const b = req.body || {};
    const truckId = (b.truckId && String(b.truckId).trim());
    const zone = String(b.zone || '');
    if (!truckId) return res.status(400).json({ ok: false, error: 'truckId required' });
    if (!zone)    return res.status(400).json({ ok: false, error: 'zone required' });
    const truckNumber = (b.truckNumber != null) ? parseInt(b.truckNumber, 10) : null;
    const manifestJson = JSON.stringify(Array.isArray(b.manifest) ? b.manifest : []);
    const vehicleNo = b.vehicleNo != null ? String(b.vehicleNo) : null;
    const lrNo = b.lrNo != null ? String(b.lrNo) : null;
    const lockedBy = String(b.lockedBy || 'unknown');
    const remarks = b.remarks != null ? String(b.remarks) : null;
    const now = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(`
        INSERT INTO dispatch_truck_locks (truck_id,zone,truck_number,manifest_json,status,vehicle_no,lr_no,locked_by,locked_at,remarks)
        VALUES ($1,$2,$3,$4,'active',$5,$6,$7,$8,$9)
        ON CONFLICT(truck_id) DO UPDATE SET
          zone=EXCLUDED.zone,
          truck_number=EXCLUDED.truck_number,
          manifest_json=EXCLUDED.manifest_json,
          vehicle_no=COALESCE(EXCLUDED.vehicle_no, dispatch_truck_locks.vehicle_no),
          lr_no=COALESCE(EXCLUDED.lr_no, dispatch_truck_locks.lr_no),
          remarks=COALESCE(EXCLUDED.remarks, dispatch_truck_locks.remarks)
      `, [truckId, zone, truckNumber, manifestJson, vehicleNo, lrNo, lockedBy, now, remarks]);
    } else {
      db.prepare(`
        INSERT INTO dispatch_truck_locks (truck_id,zone,truck_number,manifest_json,status,vehicle_no,lr_no,locked_by,locked_at,remarks)
        VALUES (?,?,?,?,'active',?,?,?,?,?)
        ON CONFLICT(truck_id) DO UPDATE SET
          zone=excluded.zone,
          truck_number=excluded.truck_number,
          manifest_json=excluded.manifest_json,
          vehicle_no=COALESCE(excluded.vehicle_no, dispatch_truck_locks.vehicle_no),
          lr_no=COALESCE(excluded.lr_no, dispatch_truck_locks.lr_no),
          remarks=COALESCE(excluded.remarks, dispatch_truck_locks.remarks)
      `).run(truckId, zone, truckNumber, manifestJson, vehicleNo, lrNo, lockedBy, now, remarks);
    }
    res.json({ ok: true, truckId });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST finalize a locked truck. Body: { finalizedBy, remarks }. Marks it finalized; the physical
// dispatch records were already written by the scan-out, and any short remainder rolls forward via
// the remaining-boxes re-bin on the next render. We KEEP the row (status='finalized') so the truck
// retains its number and manifest in history rather than vanishing.
app.post('/api/dispatch/truck-lock/:truckId/finalize', async (req, res) => {
  try {
    const truckId = String(req.params.truckId);
    const finalizedBy = String((req.body && req.body.finalizedBy) || 'unknown');
    const remarks = (req.body && req.body.remarks != null) ? String(req.body.remarks) : null;
    const now = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(`UPDATE dispatch_truck_locks SET status='finalized', finalized_by=$1, finalized_at=$2, remarks=COALESCE($3,remarks) WHERE truck_id=$4`, [finalizedBy, now, remarks, truckId]);
    } else {
      db.prepare(`UPDATE dispatch_truck_locks SET status='finalized', finalized_by=?, finalized_at=?, remarks=COALESCE(?,remarks) WHERE truck_id=?`).run(finalizedBy, now, remarks, truckId);
    }
    res.json({ ok: true, truckId, status: 'finalized' });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// DELETE unlock a truck (re-plan on edits / abandon a not-yet-dispatched lock). Only removes the
// lock row — it does NOT touch dispatch records, so anything already dispatched stays immutable.
app.delete('/api/dispatch/truck-lock/:truckId', async (req, res) => {
  try {
    const truckId = String(req.params.truckId);
    if (pgPool) { await pgPool.query(`DELETE FROM dispatch_truck_locks WHERE truck_id=$1`, [truckId]); }
    else        { db.prepare(`DELETE FROM dispatch_truck_locks WHERE truck_id=?`).run(truckId); }
    res.json({ ok: true, truckId, deleted: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
// ════════════════════════════════════════════════════════════════════════


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
    let { wastage, batchNumber, dept, salvage, remelt, backdateTs } = req.body;
    // v41ZR Issue 3: accept EITHER an explicit wastage[] array (each {batch_number,dept,type,qty,ts[,id]}),
    // OR the admin Settings "Wastage Backfill" form shape {batchNumber,dept,salvage,remelt,backdateTs} and
    // build the salvage/remelt rows here. IDs are generated server-side when absent (the form sends none).
    if (!Array.isArray(wastage)) {
      const ts = backdateTs || new Date().toISOString();
      const sv = parseFloat(salvage) || 0;
      const rm = parseFloat(remelt) || 0;
      wastage = [];
      if (sv > 0) wastage.push({ batch_number: batchNumber, dept, type: 'salvage', qty: sv, ts });
      if (rm > 0) wastage.push({ batch_number: batchNumber, dept, type: 'remelt',  qty: rm, ts });
    }
    if (!Array.isArray(wastage) || wastage.length === 0) {
      return res.status(400).json({ ok: false, error: 'Provide a wastage array, or batch with salvage/remelt' });
    }
    let count = 0;
    for (const w of wastage) {
      const bn = w.batchNumber || w.batch_number || batchNumber || null;
      if (!bn) continue;
      const id  = w.id || ('bfw_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8));
      const wd  = w.dept || dept || null;
      const wt  = w.type || null;
      const wq  = (w.qty != null ? w.qty : null);
      const wts = w.ts || backdateTs || new Date().toISOString();
      if (pgPool) {
        await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`,
          [id, bn, wd, wt, wq, wts]);
      } else {
        db.prepare(`INSERT OR IGNORE INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES (?,?,?,?,?,?)`).run(id, bn, wd, wt, wq, wts);
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
    const { batchNumber, dept, closedBy, short, shortReason, shortBoxes } = req.body;
    if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'Missing batchNumber or dept' });
    const id = `${batchNumber}-${dept}`;
    const ts = new Date().toISOString();
    const isShort = short ? 1 : 0;
    const sReason = isShort ? (shortReason || '') : null;
    const sBoxes  = isShort ? (parseInt(shortBoxes,10) || 0) : 0;
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO tracking_stage_closure (id, batch_number, dept, closed, closed_at, closed_by, short_close, short_reason, short_boxes)
         VALUES ($1,$2,$3,1,$4,$5,$6,$7,$8)
         ON CONFLICT(batch_number, dept) DO UPDATE SET closed=1, closed_at=EXCLUDED.closed_at, closed_by=EXCLUDED.closed_by, short_close=EXCLUDED.short_close, short_reason=EXCLUDED.short_reason, short_boxes=EXCLUDED.short_boxes`,
        [id, batchNumber, dept, ts, closedBy||null, isShort, sReason, sBoxes]
      );
    } else {
      db.prepare(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at,closed_by,short_close,short_reason,short_boxes)
        VALUES (?,?,?,1,?,?,?,?,?) ON CONFLICT(batch_number,dept) DO UPDATE SET closed=1,closed_at=excluded.closed_at,closed_by=excluded.closed_by,short_close=excluded.short_close,short_reason=excluded.short_reason,short_boxes=excluded.short_boxes`)
        .run(id, batchNumber, dept, ts, closedBy||null, isShort, sReason, sBoxes);
    }
    // v44 #2(ii): audit short closes (printing manager accepting fewer boxes than scanned in).
    if (isShort) {
      try {
        const details = JSON.stringify({ batch_number:batchNumber, dept, short_reason:sReason, short_boxes:sBoxes });
        if (pgPool) await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,'tracking','tracking','STAGE_CLOSE_SHORT',$2)`, [closedBy||'tracking', details]);
        else db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,'tracking','tracking','STAGE_CLOSE_SHORT',?)`).run(closedBy||'tracking', details);
      } catch(e) { console.warn('[v44 #2(ii)] short-close audit failed:', e.message); }
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// v44C #6: ADMIN RE-CUSTOMER RELABEL — re-assign a packed batch to a different customer.
//   • FULL: in-place customer/address/PO update + forced reprint (printed=0, QR regen). Box identity,
//     every stage scan, and each invoice's selected_labels are preserved (label ids unchanged).
//   • SPLIT (Addition 1, modeled on W/O split-approve): carve the last N boxes into a new suffixed child
//     batch (<batch>A/B/…) re-customered to B; the original keeps the rest with customer A. Labels + their
//     scans are re-batched IN PLACE; parent qty/boxes reduced proportionally; child gets proportional actualProd.
//   • CONVERT (Addition 2): an unprinted target → printed. Sets isPrinted, creates a print_orders row for
//     machine assignment, and (D5) POSTS A PACKING-SCAN REVERSAL (ledger debit) rather than deleting the
//     packing-in — original scans stay; reversed scans are netted from summary counts + dedup so the box
//     shows pending-printing and re-flows printing→PI(+orange)→packing→dispatch. Blocked if any target box
//     is already dispatched.
//   • LOG (Addition 3): full before/after snapshot row in recustomer_log.
// BLOCKED if a SAP invoice DOCUMENT already exists for the batch. Admin-gated (verifyToken→role) + audited.
app.post('/api/tracking/recustomer', async (req, res) => {
  try {
    const session = verifyToken(req.headers['x-session-token'] || req.body?.token);
    if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
    if (session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin required' });
    const { batchNumber, newCustomer, newCardCode, newPoNumber, shipTo, billTo, reason,
            splitBoxes, convertToPrinted, printMatter, printType } = req.body;
    if (!batchNumber || !newCustomer) return res.status(400).json({ ok:false, error:'batchNumber and newCustomer required' });
    const ts = new Date().toISOString();
    const _PACK = {'0':1.5,'00':1.5,'000':1.5,'1':1.25,'2':1.0,'3':0.75,'4':0.5,'5':0.333};
    const boxesToLakhsServer = (boxes,size)=>{ const ps=_PACK[String(size)]||1; return (parseInt(boxes,10)||0)*ps; };

    // SAP-finalized guard — a real SAP invoice document for this batch cannot be silently re-pointed.
    let sapBlock = false, sapWhat = '';
    if (pgPool) {
      const ir = (await pgPool.query(`SELECT 1 FROM invoices_received WHERE batch_number=$1 LIMIT 1`,[batchNumber])).rows[0];
      const rq = (await pgPool.query(`SELECT 1 FROM invoice_requests WHERE batch_number=$1 AND (sap_doc_entry IS NOT NULL OR sap_response_doc_entry IS NOT NULL OR status NOT IN ('pending')) LIMIT 1`,[batchNumber])).rows[0];
      if (ir) { sapBlock=true; sapWhat='a SAP invoice has already been received'; }
      else if (rq) { sapBlock=true; sapWhat='an invoice request has already been pushed to SAP'; }
    } else {
      const ir = db.prepare(`SELECT 1 FROM invoices_received WHERE batch_number=? LIMIT 1`).get(batchNumber);
      const rq = db.prepare(`SELECT 1 FROM invoice_requests WHERE batch_number=? AND (sap_doc_entry IS NOT NULL OR sap_response_doc_entry IS NOT NULL OR status NOT IN ('pending')) LIMIT 1`).get(batchNumber);
      if (ir) { sapBlock=true; sapWhat='a SAP invoice has already been received'; }
      else if (rq) { sapBlock=true; sapWhat='an invoice request has already been pushed to SAP'; }
    }
    if (sapBlock) return res.json({ ok:false, sap_blocked:true, error:`Cannot re-customer ${batchNumber}: ${sapWhat}. Cancel that SAP document first, then retry.` });

    const planState = getPlanningState();
    const ord = (planState.orders||[]).find(o => o.batchNumber===batchNumber && !o.deleted);

    // Non-voided, non-orange labels (the customer boxes), ascending box number.
    const labelSel = pgPool
      ? (await pgPool.query(`SELECT id, label_number, customer, size, colour FROM tracking_labels WHERE batch_number=$1 AND COALESCE(voided,0)=0 AND COALESCE(is_orange,0)=0 ORDER BY ABS(label_number) ASC`,[batchNumber])).rows
      : db.prepare(`SELECT id, label_number, customer, size, colour FROM tracking_labels WHERE batch_number=? AND COALESCE(voided,0)=0 AND COALESCE(is_orange,0)=0 ORDER BY ABS(label_number) ASC`).all(batchNumber);
    const totalBoxes = labelSel.length;
    let oldCustomer = labelSel.find(l=>l.customer)?.customer || ord?.customer || '';
    const size = ord?.size || labelSel[0]?.size || '2';
    const wasPrinted = !!(ord?.isPrinted);

    const nSplit = Math.max(0, parseInt(splitBoxes,10)||0);
    const doSplit = nSplit > 0 && nSplit < totalBoxes;     // full switch if 0 or ≥ total
    const doConvert = !!convertToPrinted && !wasPrinted;

    // Split safety: a pending invoice request references specific labels; moving boxes to a child batch
    // would leave its selected_labels stale. Block split until that pending request is resolved.
    if (doSplit) {
      const pend = pgPool
        ? (await pgPool.query(`SELECT 1 FROM invoice_requests WHERE batch_number=$1 AND status='pending' LIMIT 1`,[batchNumber])).rows[0]
        : db.prepare(`SELECT 1 FROM invoice_requests WHERE batch_number=? AND status='pending' LIMIT 1`).get(batchNumber);
      if (pend) return res.json({ ok:false, split_blocked:true, error:`Cannot split ${batchNumber}: a pending invoice request references this batch. Resolve it first, then split.` });
    }
    const before = { batchNumber, customer: oldCustomer, isPrinted: wasPrinted, totalBoxes, poNumber: ord?.poNumber||'' };

    // ── Resolve the TARGET (the batch being re-customered): a new child on split, else the original.
    let targetBatch = batchNumber, childBatch = null, targetLabelIds = labelSel.map(l=>l.id);

    if (doSplit) {
      // Next free single-letter child suffix (A, B, C …) — like W/O split child batches.
      const used = new Set();
      (planState.orders||[]).forEach(o=>{ if(!o.deleted && o.batchNumber && o.batchNumber.length===batchNumber.length+1 && o.batchNumber.startsWith(batchNumber)){ const s=o.batchNumber.slice(batchNumber.length); if(/^[A-Z]$/.test(s)) used.add(s); }});
      let suffix='Z'; for(let i=65;i<=90;i++){ const c=String.fromCharCode(i); if(!used.has(c)){ suffix=c; break; } }
      childBatch = `${batchNumber}${suffix}`;
      const moveLabels = labelSel.slice(totalBoxes - nSplit);   // last N boxes → child
      targetLabelIds = moveLabels.map(l=>l.id);
      targetBatch = childBatch;
      // Re-batch the moved labels + ALL their scans to the child (in place — no void/mint, ids preserved).
      for (const l of moveLabels) {
        if (pgPool) {
          await pgPool.query(`UPDATE tracking_labels SET batch_number=$1, customer=$2, po_number=COALESCE($3,po_number), ship_to=COALESCE($4,ship_to), bill_to=COALESCE($5,bill_to), printed=0, printed_at=NULL, qr_data=NULL WHERE id=$6`, [childBatch, newCustomer, newPoNumber||null, shipTo||null, billTo||null, l.id]);
          await pgPool.query(`UPDATE tracking_scans SET batch_number=$1 WHERE label_id=$2`, [childBatch, l.id]);
        } else {
          db.prepare(`UPDATE tracking_labels SET batch_number=?, customer=?, po_number=COALESCE(?,po_number), ship_to=COALESCE(?,ship_to), bill_to=COALESCE(?,bill_to), printed=0, printed_at=NULL, qr_data=NULL WHERE id=?`).run(childBatch, newCustomer, newPoNumber||null, shipTo||null, billTo||null, l.id);
          db.prepare(`UPDATE tracking_scans SET batch_number=? WHERE label_id=?`).run(childBatch, l.id);
        }
      }
      // Create the child order (clone parent; customer B; proportional actualProd; active).
      const parentActual = parseFloat(ord?.actualProd || ord?.actualQty || 0);
      const child = {
        ...(ord||{}), id: childBatch, batchNumber: childBatch, customer: newCustomer,
        shipTo: shipTo||newCustomer, billTo: billTo||'', poNumber: newPoNumber||'',
        qty: +boxesToLakhsServer(nSplit, size).toFixed(4), totalBoxes: nSplit,
        actualProd: +(totalBoxes>0 ? parentActual*nSplit/totalBoxes : 0).toFixed(3),
        actualQty:  +(totalBoxes>0 ? parentActual*nSplit/totalBoxes : 0).toFixed(3),
        isPrinted: doConvert ? true : !!(ord?.isPrinted),
        status: (ord?.status==='closed' ? 'running' : (ord?.status||'running')),
        deleted: false, recustomeredFrom: oldCustomer, recustomerSplitFrom: batchNumber,
        recustomeredAt: ts, recustomeredBy: session.username,
        sapDocEntry: null, sapDocNum: '', _localEditedAt: Date.now()
      };
      delete child.woStatus;
      planState.orders.push(child);
      // Reduce the parent proportionally (residual stays with customer A).
      if (ord) {
        const residual = totalBoxes - nSplit;
        const cap = parseInt(ord.totalBoxes) || totalBoxes;
        if (ord.qty != null) ord.qty = +(parseFloat(ord.qty) * (residual/ (cap||totalBoxes))).toFixed(4);
        ord.totalBoxes = residual;
        ord._localEditedAt = Date.now();
      }
    } else {
      // ── FULL switch: in-place customer/address/PO update + forced reprint on the original batch.
      if (pgPool) {
        await pgPool.query(`UPDATE tracking_labels SET customer=$2, po_number=COALESCE($3,po_number), ship_to=COALESCE($4,ship_to), bill_to=COALESCE($5,bill_to), printed=0, printed_at=NULL, qr_data=NULL WHERE batch_number=$1 AND COALESCE(voided,0)=0 AND COALESCE(is_orange,0)=0`, [batchNumber, newCustomer, newPoNumber||null, shipTo||null, billTo||null]);
        await pgPool.query(`UPDATE tracking_dispatch_records SET customer=$2 WHERE batch_number=$1`, [batchNumber, newCustomer]);
        await pgPool.query(`UPDATE invoice_requests SET customer=$2, card_code=COALESCE($3,card_code), po_number=COALESCE($4,po_number), updated_at=NOW()::TEXT WHERE batch_number=$1 AND status='pending' AND sap_doc_entry IS NULL`, [batchNumber, newCustomer, newCardCode||null, newPoNumber||null]);
      } else {
        db.prepare(`UPDATE tracking_labels SET customer=?, po_number=COALESCE(?,po_number), ship_to=COALESCE(?,ship_to), bill_to=COALESCE(?,bill_to), printed=0, printed_at=NULL, qr_data=NULL WHERE batch_number=? AND COALESCE(voided,0)=0 AND COALESCE(is_orange,0)=0`).run(newCustomer, newPoNumber||null, shipTo||null, billTo||null, batchNumber);
        db.prepare(`UPDATE tracking_dispatch_records SET customer=? WHERE batch_number=?`).run(newCustomer, batchNumber);
        db.prepare(`UPDATE invoice_requests SET customer=?, card_code=COALESCE(?,card_code), po_number=COALESCE(?,po_number), updated_at=datetime('now') WHERE batch_number=? AND status='pending' AND sap_doc_entry IS NULL`).run(newCustomer, newCardCode||null, newPoNumber||null, batchNumber);
      }
      if (ord) {
        ord.recustomeredFrom = ord.customer || oldCustomer;
        ord.customer = newCustomer;
        if (newPoNumber) ord.poNumber = newPoNumber;
        if (doConvert) { ord.isPrinted = true; if (ord.status==='closed') { ord.status='running'; ord.reopenedForConvert=true; } } // re-enter printing chain
        ord.recustomeredAt = ts; ord.recustomeredBy = session.username; ord._localEditedAt = Date.now();
        (planState.dispatchPlans||[]).forEach(d => { if (d.batchNumber===batchNumber || d.productionOrderId===ord.id) { d.customer=newCustomer; if(newPoNumber) d.poNumber=newPoNumber; } });
      }
    }

    // ── CONVERT to printed (Addition 2): block if any target box already dispatched, else create the
    //    print order + post packing-scan reversals (ledger debit; originals untouched).
    let reversedScans = 0;
    if (doConvert) {
      const dispatched = pgPool
        ? (await pgPool.query(`SELECT 1 FROM tracking_scans WHERE batch_number=$1 AND dept='dispatch' LIMIT 1`,[targetBatch])).rows[0]
        : db.prepare(`SELECT 1 FROM tracking_scans WHERE batch_number=? AND dept='dispatch' LIMIT 1`).get(targetBatch);
      if (dispatched) return res.json({ ok:false, convert_blocked:true, error:`Cannot convert ${targetBatch} to printed — box(es) already dispatched. Re-customer without conversion, or handle dispatch first.` });
      // print order (status pending, machine unassigned → surfaces in planning/printing for assignment)
      const poId = `${targetBatch}-PRINT`;
      const colour = ord?.colour || labelSel[0]?.colour || '';
      const qtyLakhs = boxesToLakhsServer((doSplit?nSplit:totalBoxes), size);
      if (pgPool) {
        await pgPool.query(`INSERT INTO print_orders (id, machine_id, customer, batch_number, pc_code, size, colour, print_matter, print_type, qty_to_print, order_qty, status, zone, production_order_id, updated_at) VALUES ($1,NULL,$2,$3,$4,$5,$6,$7,$8,$9,$9,'pending',$10,$11,NOW()::TEXT) ON CONFLICT(id) DO UPDATE SET customer=$2, batch_number=$3, print_matter=$7, print_type=$8, qty_to_print=$9, order_qty=$9, status='pending', updated_at=NOW()::TEXT`,
          [poId, newCustomer, targetBatch, ord?.pcCode||null, size, colour, printMatter||null, printType||null, qtyLakhs, ord?.zone||null, (doSplit?childBatch:(ord?.id||targetBatch))]);
      } else {
        db.prepare(`INSERT INTO print_orders (id, machine_id, customer, batch_number, pc_code, size, colour, print_matter, print_type, qty_to_print, order_qty, status, zone, production_order_id, updated_at) VALUES (?,NULL,?,?,?,?,?,?,?,?,?,'pending',?,?,datetime('now')) ON CONFLICT(id) DO UPDATE SET customer=excluded.customer, batch_number=excluded.batch_number, print_matter=excluded.print_matter, print_type=excluded.print_type, qty_to_print=excluded.qty_to_print, order_qty=excluded.order_qty, status='pending', updated_at=datetime('now')`)
          .run(poId, newCustomer, targetBatch, ord?.pcCode||null, size, colour, printMatter||null, printType||null, qtyLakhs, qtyLakhs, ord?.zone||null, (doSplit?childBatch:(ord?.id||targetBatch)));
      }
      // Reverse the target's packing scans (ledger debit) — originals preserved; idempotent by id.
      const rvReason = `Re-customer→printed (${batchNumber}→${targetBatch})`;
      if (pgPool) {
        const r = await pgPool.query(`INSERT INTO tracking_scan_reversals (id, reversed_scan_id, batch_number, label_id, dept, type, reason, by_user, ts) SELECT 'rev-'||s.id, s.id, s.batch_number, s.label_id, s.dept, s.type, $2, $3, $4 FROM tracking_scans s WHERE s.batch_number=$1 AND s.dept='packing' AND NOT EXISTS (SELECT 1 FROM tracking_scan_reversals r WHERE r.reversed_scan_id=s.id)`, [targetBatch, rvReason, session.username, ts]);
        reversedScans = r.rowCount||0;
      } else {
        const r = db.prepare(`INSERT OR IGNORE INTO tracking_scan_reversals (id, reversed_scan_id, batch_number, label_id, dept, type, reason, by_user, ts) SELECT 'rev-'||s.id, s.id, s.batch_number, s.label_id, s.dept, s.type, ?, ?, ? FROM tracking_scans s WHERE s.batch_number=? AND s.dept='packing'`).run(rvReason, session.username, ts, targetBatch);
        reversedScans = r.changes||0;
      }
    }

    // ── Persist planning state + production_orders (parent and/or child).
    try {
      if (pgPool) await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json,saved_at=NOW()::TEXT`, [JSON.stringify(planState)]);
      else db.prepare(`INSERT INTO planning_state (id, state_json) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json, updated_at = datetime('now')`).run(JSON.stringify(planState));
      _planningStateCache = planState; _planningStateCacheTime = Date.now();
      const writeOrd = async (o) => {
        if (!o) return; const oj = JSON.stringify(o);
        if (pgPool) await pgPool.query(`INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at) VALUES ($1,$2,$3,$4,$5,false,NOW()::TEXT) ON CONFLICT(id) DO UPDATE SET data_json=$2, machine_id=$3, batch_number=$4, status=$5, deleted=false, updated_at=NOW()::TEXT`, [o.id, oj, o.machineId||null, o.batchNumber, o.status||'running']);
        else db.prepare(`INSERT INTO production_orders (id,data_json,machine_id,batch_number,status,deleted,updated_at) VALUES (?,?,?,?,?,0,datetime('now')) ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json, machine_id=excluded.machine_id, batch_number=excluded.batch_number, status=excluded.status, deleted=0, updated_at=datetime('now')`).run(o.id, oj, o.machineId||null, o.batchNumber, o.status||'running');
      };
      await writeOrd(ord);
      if (doSplit) { const child = planState.orders.find(o=>o.batchNumber===childBatch); await writeOrd(child); }
    } catch(e) { console.warn('[v44C #6] planning persist:', e.message); }

    // ── Before/after log (Addition 3) + audit.
    const labelCount = targetLabelIds.length;
    const after = { batchNumber: targetBatch, childBatch, customer: newCustomer, isPrinted: doConvert?true:wasPrinted, boxes: doSplit?nSplit:totalBoxes, poNumber: newPoNumber||before.poNumber, convertedToPrinted: doConvert, reversedPackingScans: reversedScans };
    const actionType = doSplit ? (doConvert?'split+convert':'split') : (doConvert?'full+convert':'full');
    try {
      const logId = `rc-${Date.now()}-${Math.random().toString(36).slice(2,7)}`;
      const row = [logId, batchNumber, childBatch, actionType, oldCustomer, newCustomer, before.poNumber, newPoNumber||'', newCardCode||'', shipTo||'', billTo||'', (doSplit?nSplit:0), totalBoxes, doConvert?1:0, labelCount, JSON.stringify(before), JSON.stringify(after), reason||'', session.username, ts];
      if (pgPool) await pgPool.query(`INSERT INTO recustomer_log (id,batch_number,child_batch_number,action_type,from_customer,to_customer,from_po,to_po,card_code,ship_to,bill_to,split_boxes,total_boxes,converted_to_printed,labels_affected,before_json,after_json,reason,by_user,ts) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`, row);
      else db.prepare(`INSERT INTO recustomer_log (id,batch_number,child_batch_number,action_type,from_customer,to_customer,from_po,to_po,card_code,ship_to,bill_to,split_boxes,total_boxes,converted_to_printed,labels_affected,before_json,after_json,reason,by_user,ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(...row);
    } catch(e) { console.warn('[v44C #6] recustomer_log:', e.message); }
    try { logAudit(session.username, session.role, 'tracking', 'RECUSTOMER', JSON.stringify({ batch_number:batchNumber, target:targetBatch, action:actionType, from:oldCustomer, to:newCustomer, labels:labelCount, converted:doConvert, reversed:reversedScans, reason:reason||'' }), req.ip); } catch(e) {}

    res.json({ ok:true, batchNumber, targetBatch, childBatch, action:actionType, from:oldCustomer, to:newCustomer, labels:labelCount, convertedToPrinted:doConvert, reversedPackingScans:reversedScans });
  } catch(err) { console.error('[v44C #6] recustomer:', err); res.status(500).json({ ok:false, error:err.message }); }
});

// v44C #6 (Addition 3): re-customer log — admin-only, recent first.
app.get('/api/tracking/recustomer-log', async (req, res) => {
  try {
    const session = verifyToken(req.headers['x-session-token'] || req.query?.token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin required' });
    const rows = pgPool
      ? (await pgPool.query(`SELECT * FROM recustomer_log ORDER BY ts DESC LIMIT 500`)).rows
      : db.prepare(`SELECT * FROM recustomer_log ORDER BY ts DESC LIMIT 500`).all();
    res.json({ ok:true, log: rows });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// ── Wastage — save salvage/remelt records ─────────────────────
// Lets admin resolve residual WIP on a batch at month changeover with an explicit
// A-Grade impact choice:
//   mode='writeoff' → inserts salvage/remelt into tracking_wastage → A-Grade % DROPS
//                     (the unaccounted material is declared scrapped/remelted)
//   mode='output'   → inserts 'out' scans into tracking_scans → A-Grade % HOLDS
//                     (material was good, just never scanned out)
// The entry ts is placed INSIDE the target month's production window (just before the
// 6 AM cutoff) so the A-Grade/WIP impact is attributed to the correct production month,
// even if the admin performs the reconciliation in a later month. Fully audit-logged.
app.post('/api/tracking/reconcile-wip', async (req, res) => {
  try {
    const { batchNumber, dept, mode, salvage, remelt, outQty, month, reason, reconciledBy } = req.body;
    if (!batchNumber || !dept) return res.status(400).json({ ok:false, error:'batchNumber and dept required' });
    if (mode !== 'writeoff' && mode !== 'output') return res.status(400).json({ ok:false, error:"mode must be 'writeoff' or 'output'" });
    // Timestamp the reconciliation entry inside the target month's window (1s before cutoff).
    // If no month given, use current time (impact lands in current month).
    let ts;
    if (/^\d{4}-\d{2}$/.test(String(month||''))) {
      const { end } = _v41_monthWindow(month);
      // end is 'YYYY-MM-DD 06:00:00' (next month) — subtract 1 second to land inside target month
      const endDate = new Date(end.replace(' ','T'));
      endDate.setSeconds(endDate.getSeconds() - 1);
      const pad = n => String(n).padStart(2,'0');
      ts = `${endDate.getFullYear()}-${pad(endDate.getMonth()+1)}-${pad(endDate.getDate())} ${pad(endDate.getHours())}:${pad(endDate.getMinutes())}:${pad(endDate.getSeconds())}`;
    } else {
      ts = new Date().toISOString();
    }
    const genId = () => Math.random().toString(36).slice(2,10) + Date.now().toString(36);
    const who = reconciledBy || 'admin';
    const auditDetails = JSON.stringify({ batchNumber, dept, mode, salvage:salvage||0, remelt:remelt||0, outQty:outQty||0, month:month||null, reason:reason||'', ts });

    if (mode === 'writeoff') {
      const sv = parseFloat(salvage||0), rm = parseFloat(remelt||0);
      if (sv <= 0 && rm <= 0) return res.status(400).json({ ok:false, error:'writeoff needs salvage and/or remelt > 0' });
      if (pgPool) {
        if (sv > 0) await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,"by") VALUES ($1,$2,$3,'salvage',$4,$5,$6) ON CONFLICT(id) DO NOTHING`, [genId(),batchNumber,dept,sv,ts,`recon:${who}`]);
        if (rm > 0) await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,"by") VALUES ($1,$2,$3,'remelt',$4,$5,$6) ON CONFLICT(id) DO NOTHING`, [genId(),batchNumber,dept,rm,ts,`recon:${who}`]);
        await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,'admin','tracking','WIP_RECONCILE_WRITEOFF',$2)`, [who, auditDetails]);
      } else {
        const insW = db.prepare(`INSERT OR IGNORE INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES (?,?,?,?,?,?,?)`);
        if (sv > 0) insW.run(genId(),batchNumber,dept,'salvage',sv,ts,`recon:${who}`);
        if (rm > 0) insW.run(genId(),batchNumber,dept,'remelt',rm,ts,`recon:${who}`);
        db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,'admin','tracking','WIP_RECONCILE_WRITEOFF',?)`).run(who, auditDetails);
      }
    } else { // mode === 'output'
      const oq = parseFloat(outQty||0);
      if (oq <= 0) return res.status(400).json({ ok:false, error:'output needs outQty > 0' });
      // Insert a single synthetic 'out' scan carrying the qty. label_id is a synthetic recon id.
      const sid = 'recon-' + genId();
      if (pgPool) {
        await pgPool.query(`INSERT INTO tracking_scans (id,label_id,batch_number,dept,type,ts,operator,qty) VALUES ($1,$2,$3,$4,'out',$5,$6,$7) ON CONFLICT(id) DO NOTHING`, [genId(),sid,batchNumber,dept,ts,`recon:${who}`,oq]);
        await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,'admin','tracking','WIP_RECONCILE_OUTPUT',$2)`, [who, auditDetails]);
      } else {
        db.prepare(`INSERT OR IGNORE INTO tracking_scans (id,label_id,batch_number,dept,type,ts,operator,qty) VALUES (?,?,?,?,'out',?,?,?)`).run(genId(),sid,batchNumber,dept,ts,`recon:${who}`,oq);
        db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,'admin','tracking','WIP_RECONCILE_OUTPUT',?)`).run(who, auditDetails);
      }
    }
    res.json({ ok:true, ts, mode });
  } catch(err) {
    console.error('[reconcile-wip]', err.message);
    res.status(500).json({ ok:false, error: err.message });
  }
});

app.post('/api/tracking/wastage', async (req, res) => {
  try {
    const { batchNumber, dept, salvage, remelt, note } = req.body;
    if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'batchNumber and dept required' });
    const ts = new Date().toISOString();
    const noteVal = (typeof note === 'string' && note.trim()) ? note.trim().slice(0,200) : null;
    const genId = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
    if (pgPool) {
      if (parseFloat(salvage) > 0) await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,note) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO NOTHING`, [genId(),batchNumber,dept,'salvage',parseFloat(salvage),ts,noteVal]);
      if (parseFloat(remelt)  > 0) await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,note) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO NOTHING`, [genId(),batchNumber,dept,'remelt',parseFloat(remelt),ts,noteVal]);
    } else {
      const insert = db.prepare(`INSERT OR IGNORE INTO tracking_wastage (id,batch_number,dept,type,qty,ts,note) VALUES (?,?,?,?,?,?,?)`);
      if (parseFloat(salvage) > 0) insert.run(genId(),batchNumber,dept,'salvage',parseFloat(salvage),ts,noteVal);
      if (parseFloat(remelt)  > 0) insert.run(genId(),batchNumber,dept,'remelt',parseFloat(remelt),ts,noteVal);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Wastage edit — admin/planning correction ──────────────────
app.post('/api/tracking/wastage-edit', async (req, res) => {
  try {
    const { id, qty, editedBy } = req.body;
    if (!id || qty === undefined) return res.status(400).json({ ok: false, error: 'id and qty required' });
    const newQty = parseFloat(qty);
    if (pgPool) {
      const cur = await pgPool.query('SELECT * FROM tracking_wastage WHERE id=$1', [id]);
      if (!cur.rows[0]) return res.status(404).json({ ok: false, error: 'Wastage entry not found' });
      const old = cur.rows[0];
      await pgPool.query(`UPDATE tracking_wastage SET qty=$1, "by"=COALESCE("by",'') || ' [edited by ' || $2 || ']' WHERE id=$3`, [newQty, editedBy||'admin', id]);
      // v43 #7: audit the salvage/remelt correction (old → new) so wastage edits are traceable, at
      // parity with WASTAGE_DELETE. Downstream A-Grade / WIP recompute live from tracking_wastage on
      // the next fetch, so the corrected value re-syncs automatically — no cache to bust.
      await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,'admin','tracking','WASTAGE_EDIT',$2)`,
        [editedBy||'admin', JSON.stringify({id, batch_number:old.batch_number, dept:old.dept, type:old.type, old_qty:old.qty, new_qty:newQty})]);
    } else {
      const old = db.prepare('SELECT * FROM tracking_wastage WHERE id=?').get(id);
      if (!old) return res.status(404).json({ ok: false, error: 'Wastage entry not found' });
      db.prepare(`UPDATE tracking_wastage SET qty=?, by=COALESCE(by,'')||' [edited by '||?||']' WHERE id=?`).run(newQty, editedBy||'admin', id);
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,'admin','tracking','WASTAGE_EDIT',?)`).run(editedBy||'admin', JSON.stringify({id, batch_number:old.batch_number, dept:old.dept, type:old.type, old_qty:old.qty, new_qty:newQty}));
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
    if (!pgPool) return res.json({ ok: true, month, note: 'archive store unavailable (SQLite dev)' });
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
    if (!pgPool) return res.json({ ok: true, archives: [] }); // SQLite dev — no archive store
    const result = await pgPool.query(
      `SELECT month, archived_at, archived_by, is_auto FROM month_archives ORDER BY month DESC`
    );
    res.json({ ok: true, archives: result.rows });
  } catch (err) {
    console.error('[Archives] list error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/archives/:month — fetch full snapshot for a specific month
app.get('/api/archives/:month', async (req, res) => {
  try {
    const { month } = req.params;
    if (!pgPool) return res.json({ ok: false, error: 'archive store unavailable' });
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
    if (!pgPool) return res.json({ ok: true, status: 'unarchived', editable: true });
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
// v41ZG #1: ensurePostgresTables() creates ~30 tables inside a SINGLE try block, so if any one
// CREATE/ALTER throws (transient error, type conflict on a pre-existing table, etc.) the whole
// function aborts and tables defined later — notably month_archives — never get created. A missing
// month_archives makes /api/archives/save return 500, which silently fails the auto-archive and
// leaves the active planning month stuck on the prior month (the symptom reported on June 5).
// This helper creates the SAME critical tables idempotently, each in its OWN try, so one failure
// can't block the others. CREATE TABLE IF NOT EXISTS is safe to run on every boot.
async function ensureCriticalPostgresTables() {
  if (!pgPool) return;
  const stmts = [
    ['month_archives', `CREATE TABLE IF NOT EXISTS month_archives (
        id SERIAL PRIMARY KEY,
        month TEXT NOT NULL UNIQUE,
        archived_at TIMESTAMPTZ DEFAULT NOW(),
        archived_by TEXT,
        snapshot_json JSONB,
        is_auto BOOLEAN DEFAULT TRUE
      )`],
    ['idx_month_archives_month', `CREATE INDEX IF NOT EXISTS idx_month_archives_month ON month_archives(month)`],
    ['retired_batches', `CREATE TABLE IF NOT EXISTS retired_batches (
        batch_number TEXT PRIMARY KEY,
        order_id TEXT,
        retired_at TEXT,
        retired_by TEXT,
        reason TEXT,
        prod_month TEXT,
        residual_wip REAL DEFAULT 0,
        prev_order_status TEXT,
        prev_dpr_closed INTEGER DEFAULT 0
      )`],
  ];
  for (const [label, sql] of stmts) {
    try { await pgPool.query(sql); }
    catch (e) { console.warn(`[ensureCriticalPostgresTables] ${label} failed:`, e.message); }
  }
}

// v41ZJ: terminal error-handling middleware. Registered LAST (after all routes) so it catches errors
// from any handler or from body-parser. Its main job is to absorb client-disconnect errors quietly:
// when a browser closes the connection mid-request (navigation, its own fetch AbortSignal timeout, or
// simply because the pool was momentarily busy during the hourly integrity scan), body-parser raises
// BadRequestError "request aborted". That is a CLIENT event, not a server fault — without this handler
// Express's default handler prints the full stack as an [err], which looked alarming in the logs and
// was mistaken for a server overload. We swallow those, return a clean status for real errors, and
// never re-throw (which could otherwise crash the process on an unhandled error).
app.use((err, req, res, next) => {
  const msg = err && err.message;
  const isClientAbort = err && (err.type === 'request.aborted' || err.code === 'ECONNABORTED' || msg === 'request aborted');
  if (isClientAbort) {
    // Benign: the client went away. Don't log a stack, don't try to write a body to a dead socket.
    if (!res.headersSent) { try { res.status(400).end(); } catch(_e) {} }
    return;
  }
  if (err && err.type === 'entity.too.large') {
    if (!res.headersSent) { try { res.status(413).json({ ok:false, error:'payload too large' }); } catch(_e) {} }
    return;
  }
  console.error('[error]', req && req.method, req && req.originalUrl, '-', msg);
  if (!res.headersSent) { try { res.status(500).json({ ok:false, error:'server error' }); } catch(_e) {} }
});

app.listen(PORT, () => {
  console.log(`[Sunloc] Server running on port ${PORT}`);
  console.log(`[Sunloc] DB: ${DB_PATH}`);
  // Ensure PostgreSQL tables exist (handles cases where PgDatabase migrations didn't create them)
  // v41ZG #1: run the isolated critical-table creator FIRST so month_archives is guaranteed even if
  // the big ensurePostgresTables() aborts partway through.
  ensureCriticalPostgresTables().then(() => ensurePostgresTables()).then(()=>{
    warmPlanningCache();
    warmActualsCache();
    loadRetiredBatches(); // v41ZZ: populate retired-batch set for WIP exclusion
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
