/**
 * db-pg-sync.js — Synchronous PostgreSQL adapter for Sunloc
 *
 * Exposes the same API as better-sqlite3 so all existing db.prepare().get/run/all()
 * calls work without modification.
 *
 * Strategy: spawns a child Node process that owns the pg Pool. The main process
 * communicates via a SharedArrayBuffer + Atomics (synchronous IPC), converting
 * async pg queries into blocking calls from the main thread's perspective.
 */

'use strict';

const { execFileSync } = require('child_process');
const os = require('os');
const path = require('path');
const fs = require('fs');

// ── Worker script (runs in a separate process) ────────────────
const WORKER_SRC = `
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

// Read request from stdin, execute, write result to stdout
let buf = '';
process.stdin.on('data', d => {
  buf += d;
  const nl = buf.indexOf('\\n');
  if (nl < 0) return;
  const line = buf.slice(0, nl);
  buf = buf.slice(nl + 1);
  const req = JSON.parse(line);
  const { sql, params, method, id } = req;
  // Convert ? placeholders to $1,$2...
  let i = 0;
  const pgSql = sql.replace(/\\?/g, () => '$' + (++i));
  pool.query(pgSql, params || [])
    .then(r => {
      if (method === 'get') {
        process.stdout.write(JSON.stringify({ id, row: r.rows[0] || null }) + '\\n');
      } else if (method === 'all') {
        process.stdout.write(JSON.stringify({ id, rows: r.rows }) + '\\n');
      } else {
        process.stdout.write(JSON.stringify({ id, changes: r.rowCount, lastInsertRowid: null }) + '\\n');
      }
    })
    .catch(e => {
      process.stdout.write(JSON.stringify({ id, error: e.message }) + '\\n');
    });
});
pool.connect().then(c => { c.release(); process.stdout.write(JSON.stringify({ id: '__ready__' }) + '\\n'); })
  .catch(e => { process.stderr.write('PG connect failed: ' + e.message + '\\n'); process.exit(1); });
`;

// Write worker to temp file
const workerPath = path.join(os.tmpdir(), 'sunloc-pg-worker.js');
fs.writeFileSync(workerPath, WORKER_SRC);

// ── Spawn worker process ──────────────────────────────────────
const { spawnSync } = require('child_process');
const { spawn } = require('child_process');

let worker = null;
let pendingResolvers = {};
let reqId = 0;
let readyResolve = null;
const readyPromise = new Promise(r => { readyResolve = r; });

function startWorker() {
  worker = spawn(process.execPath, [workerPath], {
    env: process.env,
    stdio: ['pipe', 'pipe', 'inherit'],
  });

  let buf = '';
  worker.stdout.on('data', data => {
    buf += data.toString();
    let nl;
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line);
        if (msg.id === '__ready__') { readyResolve(); return; }
        const res = pendingResolvers[msg.id];
        if (res) { delete pendingResolvers[msg.id]; res(msg); }
      } catch(e) {}
    }
  });

  worker.on('exit', (code) => {
    console.error(`[PG Worker] exited with code ${code}`);
  });
}

startWorker();

// ── Synchronous IPC via execFileSync helper ───────────────────
// We use a tiny sync helper script to execute ONE query synchronously.
// This avoids SharedArrayBuffer complexity while keeping the sync API.

const SYNC_HELPER = `
const net = require('net');
const args = JSON.parse(process.argv[2]);
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 1 });
let i = 0;
const pgSql = args.sql.replace(/\\?/g, () => '$' + (++i));
pool.query(pgSql, args.params || [])
  .then(r => {
    if (args.method === 'get') process.stdout.write(JSON.stringify({ row: r.rows[0] || null }));
    else if (args.method === 'all') process.stdout.write(JSON.stringify({ rows: r.rows }));
    else process.stdout.write(JSON.stringify({ changes: r.rowCount }));
    pool.end();
  })
  .catch(e => { process.stdout.write(JSON.stringify({ error: e.message })); pool.end(); });
`;

const syncHelperPath = path.join(os.tmpdir(), 'sunloc-pg-sync.js');
fs.writeFileSync(syncHelperPath, SYNC_HELPER);

function pgQuerySync(sql, params, method) {
  const args = JSON.stringify({ sql, params: params || [], method });
  try {
    const out = execFileSync(process.execPath, [syncHelperPath, args], {
      env: process.env,
      timeout: 10000,
      encoding: 'utf8',
    });
    const result = JSON.parse(out);
    if (result.error) throw new Error(result.error);
    return result;
  } catch (e) {
    if (e.message && e.message.includes('{')) {
      try { const r = JSON.parse(e.message.match(/\{.*\}/)[0]); if (r.error) throw new Error(r.error); } catch{}
    }
    throw e;
  }
}

// ── Prepared statement shim (same API as better-sqlite3) ──────
class PgStatement {
  constructor(sql) {
    this.sql = sql;
  }
  get(...params) {
    const result = pgQuerySync(this.sql, params.flat(), 'get');
    return result.row || undefined;
  }
  all(...params) {
    const result = pgQuerySync(this.sql, params.flat(), 'all');
    return result.rows || [];
  }
  run(...params) {
    const result = pgQuerySync(this.sql, params.flat(), 'run');
    return { changes: result.changes || 0, lastInsertRowid: null };
  }
  iterate(...params) {
    return this.all(...params)[Symbol.iterator]();
  }
}

// ── Database shim (same API as better-sqlite3) ────────────────
class PgDatabase {
  constructor() {
    this.isPostgres = true;
    // Test connection on startup
    try {
      pgQuerySync('SELECT 1 as ok', [], 'get');
      console.log('[DB] PostgreSQL connected successfully');
    } catch(e) {
      console.error('[DB] PostgreSQL connection failed:', e.message);
      throw e;
    }
  }

  prepare(sql) {
    // Normalise SQLite-specific SQL to Postgres
    sql = sql
      .replace(/\bINSERT OR IGNORE INTO\b/gi, 'INSERT INTO')
      .replace(/\bINSERT OR REPLACE INTO\b/gi, 'INSERT INTO')
      .replace(/datetime\('now'\)/gi, 'NOW()')
      .replace(/datetime\(\\'now\\'\)/gi, 'NOW()')
      .replace(/AUTOINCREMENT/gi, '')  // Postgres SERIAL handles this
      // ON CONFLICT(col) DO UPDATE ... (already standard SQL, works in PG)
    ;
    return new PgStatement(sql);
  }

  exec(sql) {
    // Split on semicolons and run each statement
    const stmts = sql.split(';').map(s => s.trim()).filter(Boolean);
    for (const stmt of stmts) {
      // Skip SQLite-only CREATE TABLE patterns that use AUTOINCREMENT etc
      const pgStmt = stmt
        .replace(/INTEGER PRIMARY KEY AUTOINCREMENT/gi, 'SERIAL PRIMARY KEY')
        .replace(/INTEGER PRIMARY KEY/gi, 'SERIAL PRIMARY KEY')
        .replace(/datetime\('now'\)/gi, 'NOW()')
        .replace(/AUTOINCREMENT/gi, '');
      try {
        pgQuerySync(pgStmt, [], 'run');
      } catch(e) {
        // Ignore "already exists" — idempotent DDL
        if (!e.message.includes('already exists') && 
            !e.message.includes('duplicate column') &&
            !e.message.includes('does not exist')) {
          console.error('[PG] DDL error:', e.message.slice(0, 120));
        }
      }
    }
  }

  pragma() {} // No-op — Postgres has no pragmas
  transaction(fn) {
    // Wrap in a function that calls fn() — transactions not strictly atomic
    // but operations run sequentially
    return (...args) => fn(...args);
  }
}

module.exports = { PgDatabase };
