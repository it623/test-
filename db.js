/**
 * Database abstraction layer
 * Supports both SQLite (local/Docker) and PostgreSQL (Railway)
 */

const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

let db = null;
let dbType = null;
let dbPath = null;

// Detect database type and initialize
function initializeDatabase() {
  // Check for DATABASE_URL (Railway PostgreSQL)
  if (process.env.DATABASE_URL) {
    console.log('🐘 Detected PostgreSQL (DATABASE_URL set)');
    dbType = 'postgres';
    // For now, we'll handle async PostgreSQL connection separately
    // This returns a promise
    const { Client } = require('pg');
    const client = new Client({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    });
    client.connect()
      .then(() => {
        console.log('✅ PostgreSQL connected');
        db = client;
      })
      .catch((err) => {
        console.error('❌ PostgreSQL connection failed:', err.message);
        console.log('⚠️  Falling back to in-memory SQLite');
        initializeSQLite(':memory:');
      });
    return db;
  }

  // Otherwise use SQLite
  const sqlitePath = process.env.DB_PATH || (process.env.NODE_ENV === 'production' ? '/tmp/sunloc.db' : path.join(__dirname, 'sunloc.db'));
  initializeSQLite(sqlitePath);
  return db;
}

function initializeSQLite(filePath) {
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
      console.log(`📁 Created database directory: ${dir}`);
    }
    db = new Database(filePath);
    dbType = 'sqlite';
    dbPath = filePath;
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
    console.log(`💾 SQLite database: ${filePath}`);
  } catch (err) {
    console.warn(`⚠️  Cannot open ${filePath}: ${err.message}`);
    console.log('💾 Falling back to in-memory database (data will not persist)');
    db = new Database(':memory:');
    dbType = 'sqlite';
    dbPath = ':memory:';
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
  }
}

// Schema initialization for SQLite only (we're not using PostgreSQL yet)
function createSchema() {
  if (dbType === 'sqlite' && db) {
    db.exec(getSQLiteSchema());
  }
}

function getSQLiteSchema() {
  return `
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
      qty REAL,
      FOREIGN KEY(label_id) REFERENCES tracking_labels(id)
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
      UNIQUE(label_id, dept)
    );

    CREATE INDEX IF NOT EXISTS idx_scans_label ON tracking_scans(label_id);
    CREATE INDEX IF NOT EXISTS idx_scans_batch ON tracking_scans(batch_number);
    CREATE INDEX IF NOT EXISTS idx_scans_dept ON tracking_scans(dept, type);
    CREATE INDEX IF NOT EXISTS idx_labels_batch ON tracking_labels(batch_number);
    CREATE INDEX IF NOT EXISTS idx_closure_batch ON tracking_stage_closure(batch_number);
    CREATE INDEX IF NOT EXISTS idx_wastage_batch ON tracking_wastage(batch_number, dept);
  `;
}

// Export database interface
module.exports = {
  init: initializeDatabase,
  createSchema: createSchema,
  get: () => db,
  getType: () => dbType,
  getPath: () => dbPath,
};
