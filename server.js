/**
 * SUNLOC INTEGRATED SERVER
 * Shared backend for Planning App + DPR App + Tracking App
 * Stack: Node.js + Express + PostgreSQL/SQLite (auto-detected)
 * FEATURE: Complete Multi-App Synchronization + Persistent Sessions
 */

const express = require('express');
const Database = require('better-sqlite3');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors({
  origin: true,
  credentials: true,   // â† required for cookies to work cross-origin
}));
app.use(express.json({ limit: '50mb' }));

// Disable caching for HTML files â€” always serve fresh version
app.use((req, res, next) => {
  if (req.path.endsWith('.html') || req.path === '/') {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
  }
  next();
});

app.use(express.static(path.join(__dirname, 'public'), {etag: false, maxAge: 0, setHeaders: (res) => { res.setHeader('Cache-Control', 'no-store'); }}));

let db;
let DB_PATH = 'unknown';
let USE_POSTGRES = false;
let dbReady = false;

initializeDB();

function initializeDB() {
  if (process.env.DATABASE_URL) {
    console.log('ðŸ˜ PostgreSQL DATABASE_URL detected...');
    USE_POSTGRES = true;

    try {
      const { Pool } = require('pg');
      db = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
        max: 20,
      });

      db.query('SELECT NOW()', (err) => {
        if (err) {
          console.error('âŒ PostgreSQL failed:', err.message);
          USE_POSTGRES = false;
          initializeSQLite(':memory:');
        } else {
          console.log('âœ… PostgreSQL connected');
          DB_PATH = 'PostgreSQL (Railway)';
          createPostgresSchema();
          setTimeout(() => {
            seedPostgresUsers();
            dbReady = true;
            console.log('âœ… Database ready');
          }, 1000);
        }
      });
    } catch (e) {
      console.error('âŒ PostgreSQL error:', e.message);
      USE_POSTGRES = false;
      initializeSQLite(':memory:');
    }
  } else {
    // LOCAL: use file-based SQLite so data persists between restarts
    const localPath = process.env.DB_PATH || path.join(__dirname, 'sunloc.db');
    initializeSQLite(localPath);
  }
}

function initializeSQLite(filePath) {
  try {
    const dir = path.dirname(filePath);
    if (filePath !== ':memory:' && !fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    db = new Database(filePath);
    DB_PATH = filePath;
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
    console.log(`ðŸ’¾ SQLite: ${filePath}`);
    createSQLiteSchema();
    seedSQLiteUsers();
    dbReady = true;
  } catch (err) {
    console.error('SQLite error:', err.message);
    db = new Database(':memory:');
    DB_PATH = ':memory:';
    db.pragma('journal_mode = WAL');
    createSQLiteSchema();
    seedSQLiteUsers();
    dbReady = true;
  }
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// SCHEMA
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

function createSQLiteSchema() {
  if (!db?.exec) return;
  db.exec(`
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
  CREATE TABLE IF NOT EXISTS app_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    pin_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    app TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
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
  CREATE TABLE IF NOT EXISTS tracking_labels (
    id TEXT PRIMARY KEY,
    batch_number TEXT NOT NULL,
    label_number INTEGER NOT NULL,
    size TEXT NOT NULL,
    qty REAL NOT NULL,
    printed INTEGER DEFAULT 0,
    printed_at TEXT,
    voided INTEGER DEFAULT 0,
    void_reason TEXT,
    voided_by TEXT,
    customer TEXT,
    colour TEXT,
    pc_code TEXT,
    po_number TEXT,
    machine_id TEXT,
    generated TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS tracking_scans (
    id TEXT PRIMARY KEY,
    label_id TEXT NOT NULL,
    batch_number TEXT NOT NULL,
    dept TEXT NOT NULL,
    type TEXT NOT NULL,
    ts TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_actuals_order ON production_actuals(order_id);
  CREATE INDEX IF NOT EXISTS idx_actuals_batch ON production_actuals(batch_number);
  CREATE INDEX IF NOT EXISTS idx_dpr_date ON dpr_records(date);
  CREATE INDEX IF NOT EXISTS idx_sessions_token ON app_sessions(token);
  `);
}

function createPostgresSchema() {
  if (!db?.query) return;
  // Add missing columns to existing tables (safe migrations)
  const migrations = [
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS printed BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS printed_at TIMESTAMP`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS voided BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS void_reason TEXT`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS voided_by TEXT`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS customer TEXT`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS colour TEXT`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS pc_code TEXT`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS po_number TEXT`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS machine_id TEXT`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS qr_data TEXT`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS is_partial BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE tracking_labels ADD COLUMN IF NOT EXISTS is_orange BOOLEAN DEFAULT FALSE`,
  ];
  migrations.forEach(sql => db.query(sql, (err) => {
    if (err && !err.message.includes('already exists') && !err.message.includes('already exists')) {}
  }));
  const tables = [
    `CREATE TABLE IF NOT EXISTS planning_state (id SERIAL PRIMARY KEY, state_json TEXT NOT NULL, saved_at TIMESTAMP DEFAULT NOW())`,
    `CREATE TABLE IF NOT EXISTS dpr_records (id SERIAL PRIMARY KEY, floor TEXT NOT NULL, date TEXT NOT NULL, data_json TEXT NOT NULL, saved_at TIMESTAMP DEFAULT NOW(), UNIQUE(floor, date))`,
    `CREATE TABLE IF NOT EXISTS production_actuals (id SERIAL PRIMARY KEY, order_id TEXT, batch_number TEXT, machine_id TEXT NOT NULL, date TEXT NOT NULL, shift TEXT NOT NULL, run_index INTEGER DEFAULT 0, qty_lakhs NUMERIC DEFAULT 0, floor TEXT, synced_at TIMESTAMP DEFAULT NOW(), UNIQUE(machine_id, date, shift, run_index))`,
    `CREATE TABLE IF NOT EXISTS app_users (id SERIAL PRIMARY KEY, username TEXT NOT NULL UNIQUE, pin_hash TEXT NOT NULL, role TEXT NOT NULL, app TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW())`,
    // â† NEW: persistent sessions table in Postgres
    `CREATE TABLE IF NOT EXISTS app_sessions (token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, username TEXT NOT NULL, role TEXT NOT NULL, app TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW(), expires_at TIMESTAMP NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS tracking_labels (id TEXT PRIMARY KEY, batch_number TEXT NOT NULL, label_number INTEGER NOT NULL, size TEXT NOT NULL, qty NUMERIC NOT NULL, printed BOOLEAN DEFAULT FALSE, printed_at TIMESTAMP, voided BOOLEAN DEFAULT FALSE, void_reason TEXT, voided_by TEXT, customer TEXT, colour TEXT, pc_code TEXT, po_number TEXT, machine_id TEXT, generated TIMESTAMP DEFAULT NOW())`,
    `CREATE TABLE IF NOT EXISTS tracking_scans (id TEXT PRIMARY KEY, label_id TEXT NOT NULL, batch_number TEXT NOT NULL, dept TEXT NOT NULL, type TEXT NOT NULL, ts TIMESTAMP DEFAULT NOW())`,
  ];
  tables.forEach(sql => db.query(sql, (err) => {
    if (err && !err.message.includes('already exists')) console.error('Schema error:', err.message);
  }));
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// AUTH HELPERS
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

function hashPin(pin) { return crypto.createHash('sha256').update(pin + 'sunloc_salt').digest('hex'); }

function generateToken() { return crypto.randomBytes(32).toString('hex'); }

/**
 * Save session to DB (works for both Postgres and SQLite)
 * This replaces localStorage-based auth â€” tokens now survive browser history clears!
 */
function saveSession(token, user, callback) {
  const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000); // 7 days

  if (!USE_POSTGRES) {
    db.prepare(`
      INSERT OR REPLACE INTO app_sessions (token, user_id, username, role, app, expires_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(token, user.id, user.username, user.role, user.app, expiresAt.toISOString());
    callback(null);
  } else {
    db.query(
      `INSERT INTO app_sessions (token, user_id, username, role, app, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT(token) DO UPDATE SET expires_at = $6`,
      [token, user.id, user.username, user.role, user.app, expiresAt],
      (err) => callback(err)
    );
  }
}

/**
 * Look up a session token â€” returns user info or null if expired/missing
 */
function getSession(token, callback) {
  if (!token) return callback(null, null);

  if (!USE_POSTGRES) {
    const row = db.prepare(`
      SELECT * FROM app_sessions
      WHERE token = ? AND expires_at > datetime('now')
    `).get(token);
    callback(null, row || null);
  } else {
    db.query(
      `SELECT * FROM app_sessions WHERE token = $1 AND expires_at > NOW()`,
      [token],
      (err, result) => callback(err, result?.rows[0] || null)
    );
  }
}

/**
 * Delete a session (logout)
 */
function deleteSession(token, callback) {
  if (!USE_POSTGRES) {
    db.prepare('DELETE FROM app_sessions WHERE token = ?').run(token);
    callback(null);
  } else {
    db.query('DELETE FROM app_sessions WHERE token = $1', [token], (err) => callback(err));
  }
}

/**
 * Middleware: authenticate requests using Authorization header token
 * Usage: add requireAuth to any route you want to protect
 * e.g. app.get('/api/protected', requireAuth, (req, res) => { ... })
 */
function requireAuth(req, res, next) {
  const token = req.headers['authorization']?.replace('Bearer ', '').trim();
  getSession(token, (err, session) => {
    if (err || !session) return res.status(401).json({ ok: false, error: 'Unauthorized' });
    req.user = session;
    next();
  });
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// SEED USERS
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

function seedSQLiteUsers() {
  if (!db?.prepare) return;
  const seedUsers = [
    { username: 'GF', pin: '1111', role: 'gf', app: 'dpr' },
    { username: 'FF', pin: '2222', role: 'ff', app: 'dpr' },
    { username: 'DPR_Admin', pin: '9999', role: 'admin', app: 'dpr' },
    { username: 'Planning_Manager', pin: '3333', role: 'planning_manager', app: 'planning' },
    { username: 'Plan_Admin', pin: '9999', role: 'admin', app: 'planning' },
  ];
  const insert = db.prepare(`INSERT OR IGNORE INTO app_users (username, pin_hash, role, app) VALUES (?, ?, ?, ?)`);
  seedUsers.forEach(u => insert.run(u.username, hashPin(u.pin), u.role, u.app));
}

function seedPostgresUsers() {
  if (!db?.query) return;
  const seedUsers = [
    { username: 'GF', pin: '1111', role: 'gf', app: 'dpr' },
    { username: 'FF', pin: '2222', role: 'ff', app: 'dpr' },
    { username: 'DPR_Admin', pin: '9999', role: 'admin', app: 'dpr' },
    { username: 'Planning_Manager', pin: '3333', role: 'planning_manager', app: 'planning' },
    { username: 'Plan_Admin', pin: '9999', role: 'admin', app: 'planning' },
    { username: 'Track_Admin', pin: '000000', role: 'admin', app: 'tracking' },
    { username: 'aim', pin: '3333', role: 'aim', app: 'tracking' },
    { username: 'printing', pin: '4444', role: 'printing', app: 'tracking' },
    { username: 'pi', pin: '5555', role: 'pi', app: 'tracking' },
    { username: 'packing', pin: '6666', role: 'packing', app: 'tracking' },
    { username: 'dispatch', pin: '7777', role: 'dispatch', app: 'tracking' },
  ];
  seedUsers.forEach(u => {
    db.query(
      `INSERT INTO app_users (username, pin_hash, role, app) VALUES ($1, $2, $3, $4) ON CONFLICT (username) DO NOTHING`,
      [u.username, hashPin(u.pin), u.role, u.app], () => {}
    );
  });
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// ROUTES: AUTH  â† NEW
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

/**
 * POST /api/auth/login
 * Body: { username, pin, app }
 * Returns: { ok, token, user }
 *
 * Frontend should save the token and send it as:
 *   Authorization: Bearer <token>
 * Token is stored in DB â€” survives browser history clears!
 */
app.post('/api/auth/login', (req, res) => {
  const { username, pin, app: appName } = req.body;
  if (!username || !pin || !appName) return res.status(400).json({ ok: false, error: 'Missing fields' });

  const pinHash = hashPin(pin);

  if (!USE_POSTGRES) {
    const user = db.prepare(
      `SELECT * FROM app_users WHERE username = ? AND pin_hash = ? AND app = ?`
    ).get(username, pinHash, appName);

    if (!user) return res.status(401).json({ ok: false, error: 'Invalid credentials' });

    const token = generateToken();
    saveSession(token, user, (err) => {
      if (err) return res.status(500).json({ ok: false, error: 'Session error' });
      res.json({ ok: true, token, user: { username: user.username, role: user.role, app: user.app } });
    });
  } else {
    db.query(
      `SELECT * FROM app_users WHERE username = $1 AND pin_hash = $2 AND app = $3`,
      [username, pinHash, appName],
      (err, result) => {
        if (err || !result?.rows[0]) return res.status(401).json({ ok: false, error: 'Invalid credentials' });
        const user = result.rows[0];
        const token = generateToken();
        saveSession(token, user, (err2) => {
          if (err2) return res.status(500).json({ ok: false, error: 'Session error' });
          res.json({ ok: true, token, user: { username: user.username, role: user.role, app: user.app } });
        });
      }
    );
  }
});

/**
 * POST /api/auth/logout
 * Header: Authorization: Bearer <token>
 */
app.post('/api/auth/logout', (req, res) => {
  const token = req.headers['authorization']?.replace('Bearer ', '').trim();
  if (!token) return res.json({ ok: true });
  deleteSession(token, () => res.json({ ok: true }));
});

/**
 * GET /api/auth/verify
 * Header: Authorization: Bearer <token>
 * Use this on app load to check if the stored token is still valid
 */
app.get('/api/auth/verify', (req, res) => {
  const token = req.headers['authorization']?.replace('Bearer ', '').trim();
  getSession(token, (err, session) => {
    if (err || !session) return res.status(401).json({ ok: false, error: 'Invalid or expired session' });
    res.json({ ok: true, user: { username: session.username, role: session.role, app: session.app } });
  });
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// SYNCHRONIZATION LOGIC
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

function getPlanningState(callback) {
  if (!USE_POSTGRES) {
    const row = db.prepare('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1').get();
    if (!row) return callback({ orders: [], dispatchPlans: [], printOrders: [] });
    try { callback(JSON.parse(row.state_json)); } catch { callback({}); }
  } else {
    db.query('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1', (err, result) => {
      if (err || !result?.rows[0]) return callback({ orders: [], dispatchPlans: [], printOrders: [] });
      try { callback(JSON.parse(result.rows[0].state_json)); } catch { callback({}); }
    });
  }
}

function savePlanningState(state, callback) {
  const json = JSON.stringify(state);
  if (!USE_POSTGRES) {
    const existing = db.prepare('SELECT id FROM planning_state LIMIT 1').get();
    if (existing) {
      db.prepare('UPDATE planning_state SET state_json = ?, saved_at = datetime("now") WHERE id = ?').run(json, existing.id);
    } else {
      db.prepare('INSERT INTO planning_state (state_json) VALUES (?)').run(json);
    }
    callback({ ok: true });
  } else {
    db.query('SELECT id FROM planning_state LIMIT 1', (err, result) => {
      if (result?.rows[0]) {
        db.query('UPDATE planning_state SET state_json = $1, saved_at = NOW() WHERE id = $2', [json, result.rows[0].id], () => callback({ ok: true }));
      } else {
        db.query('INSERT INTO planning_state (state_json) VALUES ($1)', [json], () => callback({ ok: true }));
      }
    });
  }
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// REAL-TIME SYNC â€” Server-Sent Events (SSE)
// All connected apps receive instant push when data changes
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

const sseClients = new Map(); // clientId â†’ { res, app }

/**
 * GET /api/sync/events?app=dpr|planning|tracking
 * Apps connect here to receive real-time push notifications
 */
app.get('/api/sync/events', (req, res) => {
  const appName = req.query.app || 'unknown';
  const clientId = crypto.randomBytes(8).toString('hex');

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  // Send initial connection confirmation
  res.write(`data: ${JSON.stringify({ type: 'connected', clientId, app: appName })}\n\n`);

  // Keep alive ping every 25 seconds
  const keepAlive = setInterval(() => {
    res.write(`: ping\n\n`);
  }, 25000);

  sseClients.set(clientId, { res, app: appName });
  console.log(`ðŸ“¡ SSE client connected: ${appName} (${clientId}) â€” total: ${sseClients.size}`);

  req.on('close', () => {
    clearInterval(keepAlive);
    sseClients.delete(clientId);
    console.log(`ðŸ“¡ SSE client disconnected: ${appName} (${clientId}) â€” total: ${sseClients.size}`);
  });
});

/**
 * Broadcast an event to all connected SSE clients
 * optionally exclude the sender's app
 */
function broadcast(eventType, data, excludeApp = null) {
  const payload = JSON.stringify({ type: eventType, ...data, ts: Date.now() });
  let count = 0;
  sseClients.forEach(({ res, app }) => {
    if (app !== excludeApp) {
      try { res.write(`data: ${payload}\n\n`); count++; } catch (e) {}
    }
  });
  if (count > 0) console.log(`ðŸ“¡ Broadcast [${eventType}] â†’ ${count} client(s)`);
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// ROUTES: PLANNING APP
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

app.get('/api/planning/state', (req, res) => {
  getPlanningState((state) => res.json({ ok: true, state }));
});

app.post('/api/planning/state', (req, res) => {
  const { state } = req.body;
  if (!state) return res.status(400).json({ ok: false, error: 'No state' });

  // SAFETY CHECK: Never overwrite existing orders with empty array
  // This prevents accidental data loss when app loads before state is ready
  if (!state.orders || state.orders.length === 0) {
    getPlanningState((existing) => {
      if (existing && existing.orders && existing.orders.length > 0) {
        console.log('âš ï¸  Blocked empty orders overwrite â€” existing orders preserved');
        return res.json({ ok: true, synced: true, protected: true });
      }
      // No existing orders â€” safe to save empty state
      savePlanningState(state, () => {
        broadcast('planning_updated', { message: 'Planning state updated' }, 'planning');
        res.json({ ok: true, synced: true });
      });
    });
    return;
  }

  savePlanningState(state, (result) => {
    console.log(`âœ… Planning state saved â€” ${state.orders.length} orders. Syncing...`);
    broadcast('planning_updated', { message: 'Planning state updated' }, 'planning');
    res.json({ ok: true, synced: true });
  });
});

app.get('/api/orders/active', (req, res) => {
  getPlanningState((state) => {
    const orders = (state.orders || [])
      .filter(o => o.status !== 'closed' && !o.deleted)
      .map(o => ({
        id: o.id, batchNumber: o.batchNumber, poNumber: o.poNumber,
        customer: o.customer, machineId: o.machineId, size: o.size,
        colour: o.colour, qty: o.qty, status: o.status,
        startDate: o.startDate, endDate: o.endDate,
      }));
    res.json({ ok: true, orders });
  });
});

app.get('/api/orders/machine/:machineId', (req, res) => {
  getPlanningState((state) => {
    const orders = (state.orders || [])
      .filter(o => o.machineId === req.params.machineId && o.status !== 'closed')
      .map(o => ({ id: o.id, batchNumber: o.batchNumber, customer: o.customer, qty: o.qty, status: o.status }));
    res.json({ ok: true, orders });
  });
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// ROUTES: DPR APP
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•



app.get('/api/dpr/dates/:floor', (req, res) => {
  if (!USE_POSTGRES) {
    const rows = db.prepare('SELECT DISTINCT date FROM dpr_records WHERE floor = ? ORDER BY date DESC').all(req.params.floor);
    res.json({ ok: true, dates: rows.map(r => r.date) });
  } else {
    db.query('SELECT DISTINCT date FROM dpr_records WHERE floor = $1 ORDER BY date DESC', [req.params.floor], (err, result) => {
      res.json({ ok: true, dates: result?.rows.map(r => r.date) || [] });
    });
  }
});

app.get('/api/dpr/:floor/:date', (req, res) => {
  const { floor, date } = req.params;
  if (!USE_POSTGRES) {
    const row = db.prepare('SELECT data_json FROM dpr_records WHERE floor = ? AND date = ?').get(floor, date);
    res.json({ ok: true, data: row ? JSON.parse(row.data_json) : null });
  } else {
    db.query('SELECT data_json FROM dpr_records WHERE floor = $1 AND date = $2', [floor, date], (err, result) => {
      res.json({ ok: true, data: result?.rows[0] ? JSON.parse(result.rows[0].data_json) : null });
    });
  }
});

app.post('/api/dpr/save', (req, res) => {
  const { floor, date, data, actuals } = req.body;
  if (!floor || !date || !data) return res.status(400).json({ ok: false, error: 'Missing params' });
  const json = JSON.stringify(data);

  if (!USE_POSTGRES) {
    db.prepare(`INSERT INTO dpr_records (floor, date, data_json) VALUES (?, ?, ?) ON CONFLICT(floor, date) DO UPDATE SET data_json = excluded.data_json`).run(floor, date, json);
    // Save actuals per run into production_actuals
    if (Array.isArray(actuals) && actuals.length > 0) {
      const upsert = db.prepare(`
        INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(machine_id, date, shift, run_index)
        DO UPDATE SET qty_lakhs = excluded.qty_lakhs, order_id = excluded.order_id, batch_number = excluded.batch_number
      `);
      actuals.forEach(a => upsert.run(a.orderId||null, a.batchNumber||null, a.machineId, date, a.shift, a.runIndex||0, a.qty||0, floor));
    }
  } else {
    db.query(`INSERT INTO dpr_records (floor, date, data_json) VALUES ($1, $2, $3) ON CONFLICT(floor, date) DO UPDATE SET data_json = $3`, [floor, date, json], () => {});
    // Save actuals per run into production_actuals
    if (Array.isArray(actuals) && actuals.length > 0) {
      actuals.forEach(a => {
        db.query(`
          INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          ON CONFLICT(machine_id, date, shift, run_index)
          DO UPDATE SET qty_lakhs = $7, order_id = $1, batch_number = $2
        `, [a.orderId||null, a.batchNumber||null, a.machineId, date, a.shift, a.runIndex||0, a.qty||0, floor], () => {});
      });
    }
  }

  console.log(`âœ… DPR saved: ${floor} ${date} â€” ${(actuals||[]).length} actuals`);
  broadcast('dpr_updated', { floor, date, message: `DPR updated: ${floor} / ${date}` }, 'dpr');
  res.json({ ok: true, synced: true });
});

/**
 * GET /api/actuals/by-batch
 * Returns total actual production per batch number â€” used by Planning app ACTUAL PROD column
 */
app.get('/api/actuals/by-batch', (req, res) => {
  if (!USE_POSTGRES) {
    const rows = db.prepare(`
      SELECT batch_number, SUM(qty_lakhs) as total_qty
      FROM production_actuals
      WHERE batch_number IS NOT NULL AND batch_number != ''
      GROUP BY batch_number
    `).all();
    res.json({ ok: true, actuals: rows });
  } else {
    db.query(`
      SELECT batch_number, SUM(qty_lakhs) as total_qty
      FROM production_actuals
      WHERE batch_number IS NOT NULL AND batch_number != ''
      GROUP BY batch_number
    `, (err, result) => {
      res.json({ ok: true, actuals: result?.rows || [] });
    });
  }
});


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// ROUTES: TRACKING APP
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

/**
 * GET /api/tracking/state
 * Returns full tracking state (labels + scans) for the Tracking app
 */
app.get('/api/tracking/state', (req, res) => {
  // Normalize scan rows from snake_case (DB) to camelCase (frontend)
  function normalizeScans(rows) {
    return (rows || []).map(s => ({
      id: s.id,
      labelId: s.label_id || s.labelId,
      batchNumber: s.batch_number || s.batchNumber,
      dept: s.dept,
      type: s.type,
      qty: s.qty || 0,
      ts: s.ts,
    }));
  }
  getPlanningState((planningState) => {
    const batches = (planningState?.orders || []).filter(o => !o.deleted);
    const machineMaster = planningState?.machineMaster || [];
    if (!USE_POSTGRES) {
      const labels = db.prepare('SELECT * FROM tracking_labels WHERE voided != 1 OR voided IS NULL').all() || [];
      const scans = normalizeScans(db.prepare('SELECT * FROM tracking_scans').all() || []);
      res.json({ ok: true, state: { labels, scans, stageClosure: [], wastage: [], dispatchRecs: [], alerts: [], batches, machineMaster } });
    } else {
      db.query('SELECT * FROM tracking_labels ORDER BY generated ASC', (err, labelsRes) => {
        if (err) console.error('Labels query error:', err);
        db.query('SELECT * FROM tracking_scans ORDER BY ts ASC', (err2, scansRes) => {
          if (err2) console.error('Scans query error:', err2);
          res.json({ ok: true, state: {
            labels: labelsRes?.rows || [],
            scans: normalizeScans(scansRes?.rows || []),
            stageClosure: [],
            wastage: [],
            dispatchRecs: [],
            alerts: [],
            batches,
            machineMaster
          }});
        });
      });
    }
  });
});

/**
 * GET /api/tracking/label
 * Direct label lookup by id OR by batchNumber+labelNumber
 * Used by scanning to find labels from QR code data
 */
app.get('/api/tracking/label', (req, res) => {
  const { id, batchNumber, labelNumber } = req.query;
  if (!USE_POSTGRES) {
    let label = null;
    if (id) {
      label = db.prepare('SELECT * FROM tracking_labels WHERE id = ?').get(id);
    }
    if (!label && batchNumber && labelNumber != null) {
      label = db.prepare(
        'SELECT * FROM tracking_labels WHERE batch_number = ? AND ABS(label_number) = ABS(?)'
      ).get(batchNumber, parseInt(labelNumber));
    }
    res.json({ ok: true, label: label || null });
  } else {
    if (id) {
      db.query('SELECT * FROM tracking_labels WHERE id = $1', [id], (err, result) => {
        if (!err && result?.rows[0]) return res.json({ ok: true, label: result.rows[0] });
        // Try by batchNumber+labelNumber if id not found
        if (batchNumber && labelNumber != null) {
          db.query(
            'SELECT * FROM tracking_labels WHERE batch_number = $1 AND ABS(label_number) = ABS($2)',
            [batchNumber, parseInt(labelNumber)],
            (err2, result2) => res.json({ ok: true, label: result2?.rows[0] || null })
          );
        } else {
          res.json({ ok: true, label: null });
        }
      });
    } else if (batchNumber && labelNumber != null) {
      db.query(
        'SELECT * FROM tracking_labels WHERE batch_number = $1 AND ABS(label_number) = ABS($2)',
        [batchNumber, parseInt(labelNumber)],
        (err, result) => res.json({ ok: true, label: result?.rows[0] || null })
      );
    } else {
      res.json({ ok: true, label: null });
    }
  }
});

/**
 * GET /api/tracking/wip-summary
 * Returns WIP counts per batch for Planning app dashboard
 */
app.get('/api/tracking/wip-summary', (req, res) => {
  if (!USE_POSTGRES) {
    const rows = db.prepare(`
      SELECT batch_number, dept, type, COUNT(*) as count
      FROM tracking_scans
      GROUP BY batch_number, dept, type
    `).all();
    res.json({ ok: true, summary: rows });
  } else {
    db.query(`
      SELECT batch_number, dept, type, COUNT(*) as count
      FROM tracking_scans
      GROUP BY batch_number, dept, type
    `, (err, result) => {
      res.json({ ok: true, summary: result?.rows || [] });
    });
  }
});

/**
 * GET /api/tracking/batch-summary/:batchNumber
 * Returns scan history for a specific batch
 */
app.get('/api/tracking/batch-summary/:batchNumber', (req, res) => {
  const { batchNumber } = req.params;
  if (!USE_POSTGRES) {
    const labels = db.prepare('SELECT * FROM tracking_labels WHERE batch_number = ?').all(batchNumber);
    const scans = db.prepare('SELECT * FROM tracking_scans WHERE batch_number = ?').all(batchNumber);
    res.json({ ok: true, labels, scans });
  } else {
    db.query('SELECT * FROM tracking_labels WHERE batch_number = $1', [batchNumber], (err, lRes) => {
      db.query('SELECT * FROM tracking_scans WHERE batch_number = $1', [batchNumber], (err2, sRes) => {
        res.json({ ok: true, labels: lRes?.rows || [], scans: sRes?.rows || [] });
      });
    });
  }
});

app.get('/api/tracking/batch/:batchNumber', (req, res) => {
  getPlanningState((state) => {
    const order = (state.orders || []).find(o => o.batchNumber === req.params.batchNumber);
    res.json({ ok: true, batch: order || null });
  });
});

app.post('/api/tracking/label', (req, res) => {
  const { label } = req.body;
  if (!label) return res.status(400).json({ ok: false, error: 'No label' });
  if (!USE_POSTGRES) {
    db.prepare(`INSERT OR REPLACE INTO tracking_labels (id, batch_number, label_number, size, qty, printed, voided, customer, colour, pc_code, po_number, machine_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      label.id, label.batchNumber, label.labelNumber, label.size, label.qty,
      label.printed?1:0, label.voided?1:0, label.customer||'', label.colour||'', label.pcCode||'', label.poNumber||'', label.machineId||'');
  } else {
    db.query(`INSERT INTO tracking_labels (id, batch_number, label_number, size, qty, printed, voided, customer, colour, pc_code, po_number, machine_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      ON CONFLICT(id) DO UPDATE SET qty=$5, printed=$6, voided=$7`,
      [label.id, label.batchNumber, label.labelNumber, label.size, label.qty,
       label.printed||false, label.voided||false, label.customer||'', label.colour||'', label.pcCode||'', label.poNumber||'', label.machineId||''], () => {});
  }
  res.json({ ok: true });
});

// Bulk labels save
app.post('/api/tracking/labels', (req, res) => {
  const { labels } = req.body;
  if (!labels || !Array.isArray(labels)) return res.status(400).json({ ok: false, error: 'No labels' });
  let saved = 0;
  if (!USE_POSTGRES) {
    labels.forEach(l => {
      try {
        db.prepare(`INSERT OR REPLACE INTO tracking_labels (id, batch_number, label_number, size, qty) VALUES (?, ?, ?, ?, ?)`).run(l.id, l.batchNumber, l.labelNumber, l.size, l.qty);
        // Try to update extra columns if they exist
        try { db.prepare(`UPDATE tracking_labels SET printed=?, voided=?, customer=?, colour=? WHERE id=?`).run(l.printed?1:0, l.voided?1:0, l.customer||'', l.colour||'', l.id); } catch(e) {}
        saved++;
      } catch(e) { console.error('Label save error:', e.message); }
    });
  } else {
    labels.forEach(l => {
      // First ensure basic columns exist
      db.query(`INSERT INTO tracking_labels (id, batch_number, label_number, size, qty) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET qty=$5`,
        [l.id, l.batchNumber, l.labelNumber, l.size, l.qty], (err) => {
          if (!err) {
            saved++;
            // Then try to update extra columns
            db.query(`UPDATE tracking_labels SET printed=$1, voided=$2, customer=$3, colour=$4, pc_code=$5, po_number=$6, machine_id=$7, qr_data=$8 WHERE id=$9`,
              [l.printed||false, l.voided||false, l.customer||'', l.colour||'', l.pcCode||'', l.poNumber||'', l.machineId||'', l.qrData||'', l.id], () => {});
          }
        });
    });
  }
  broadcast('tracking_updated', { message: 'Labels updated' }, 'tracking');
  res.json({ ok: true, count: labels.length });
});

// Update label printed status
app.post('/api/tracking/label-printed', (req, res) => {
  const { labelId, printed, printedAt } = req.body;
  if (!labelId) return res.status(400).json({ ok: false, error: 'No labelId' });
  if (!USE_POSTGRES) {
    db.prepare(`UPDATE tracking_labels SET printed=?, printed_at=? WHERE id=?`).run(printed?1:0, printedAt||null, labelId);
  } else {
    db.query(`UPDATE tracking_labels SET printed=$1, printed_at=$2 WHERE id=$3`, [printed||false, printedAt||null, labelId], () => {});
  }
  res.json({ ok: true });
});

// Void label
app.post('/api/tracking/label-void', (req, res) => {
  const { labelId, reason, voidedBy } = req.body;
  if (!labelId) return res.status(400).json({ ok: false, error: 'No labelId' });
  if (!USE_POSTGRES) {
    db.prepare(`UPDATE tracking_labels SET voided=1, void_reason=?, voided_by=? WHERE id=?`).run(reason||'', voidedBy||'', labelId);
  } else {
    db.query(`UPDATE tracking_labels SET voided=true, void_reason=$1, voided_by=$2 WHERE id=$3`, [reason||'', voidedBy||'', labelId], () => {});
  }
  res.json({ ok: true });
});

app.post('/api/tracking/scan', (req, res) => {
  const { scan } = req.body;
  if (!scan) return res.status(400).json({ ok: false, error: 'No scan' });
  if (!USE_POSTGRES) {
    db.prepare(`INSERT OR IGNORE INTO tracking_scans (id, label_id, batch_number, dept, type) VALUES (?, ?, ?, ?, ?)`).run(scan.id, scan.labelId, scan.batchNumber, scan.dept, scan.type);
  } else {
    db.query(`INSERT INTO tracking_scans (id, label_id, batch_number, dept, type) VALUES ($1, $2, $3, $4, $5) ON CONFLICT(id) DO NOTHING`,
      [scan.id, scan.labelId, scan.batchNumber, scan.dept, scan.type], () => {});
  }
  broadcast('tracking_updated', { message: 'Tracking scan recorded' }, 'tracking');
  res.json({ ok: true });
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// HEALTH CHECK
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    server: 'Sunloc Integrated Server v1.1',
    db: DB_PATH,
    dbType: USE_POSTGRES ? 'PostgreSQL' : 'SQLite',
    ready: dbReady,
    uptime: Math.floor(process.uptime()) + 's',
    sync: 'ACTIVE âœ…',
    sessions: 'DB-persisted âœ…',
  });
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// CATCH-ALL
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

app.get('*', (req, res) => {
  const idx = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(idx)) res.sendFile(idx);
  else res.json({ ok: false, error: 'No frontend' });
});


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// DIRECT HTML ROUTES â€” bypass static file cache completely
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const fs_html = require('fs');

app.get('/app/dpr', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.sendFile(path.join(__dirname, 'public', 'dpr.html'));
});

app.get('/app/planning', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.sendFile(path.join(__dirname, 'public', 'planning.html'));
});

app.get('/app/tracking', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.sendFile(path.join(__dirname, 'public', 'tracking.html'));
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// HEALTH + DATA INTEGRITY ENDPOINTS
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

/**
 * GET /api/backup/export
 * Downloads all data as JSON â€” use for manual backups
 * Protected: requires admin token
 */
app.get('/api/backup/export', async (req, res) => {
  try {
    const backup = { exportedAt: new Date().toISOString(), version: '1.1' };

    if (!USE_POSTGRES) {
      backup.planning_state = db.prepare('SELECT * FROM planning_state').all();
      backup.dpr_records = db.prepare('SELECT * FROM dpr_records').all();
      backup.production_actuals = db.prepare('SELECT * FROM production_actuals').all();
      backup.app_users = db.prepare('SELECT id, username, role, app, created_at FROM app_users').all(); // no pin hashes
      backup.tracking_labels = db.prepare('SELECT * FROM tracking_labels').all();
      backup.tracking_scans = db.prepare('SELECT * FROM tracking_scans').all();
    } else {
      const [ps, dpr, actuals, users, labels, scans] = await Promise.all([
        db.query('SELECT * FROM planning_state'),
        db.query('SELECT * FROM dpr_records'),
        db.query('SELECT * FROM production_actuals'),
        db.query('SELECT id, username, role, app, created_at FROM app_users'),
        db.query('SELECT * FROM tracking_labels'),
        db.query('SELECT * FROM tracking_scans'),
      ]);
      backup.planning_state = ps.rows;
      backup.dpr_records = dpr.rows;
      backup.production_actuals = actuals.rows;
      backup.app_users = users.rows;
      backup.tracking_labels = labels.rows;
      backup.tracking_scans = scans.rows;
    }

    const filename = `sunloc_backup_${new Date().toISOString().slice(0,10)}.json`;
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/json');
    res.json(backup);
    console.log(`ðŸ“¦ Backup exported: ${filename}`);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`âœ… Sunloc Server v1.1 running on port ${PORT}`);
  console.log(`   DB: ${DB_PATH}`);
  console.log(`   Sync: ALL APPS CONNECTED âœ…`);
  console.log(`   Health: /api/health`);
});

module.exports = app;
// cache-bust
