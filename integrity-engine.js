// ════════════════════════════════════════════════════════════════
// v40 Phase 18.17: Data Integrity Engine
//
// Runs cross-app consistency checks. Each check returns an array of findings.
// The orchestrator dedups findings by finding_key, upserts into integrity_findings,
// marks resolved findings whose underlying condition no longer applies.
//
// Severities:
//   critical — likely data corruption / blocks dispatch / invoicing
//   warning  — measurable inconsistency, needs investigation
//   info     — gap in expected data flow, possibly normal
//
// Tolerances (configurable defaults):
//   QTY_TOL_GENERIC = 5%   (planning vs DPR vs packed)
//   QTY_TOL_TIGHT   = 1%   (same physical material counted twice — DPR vs AIM)
//
// First-deploy behavior: scans current data, surfaces all existing inconsistencies.
// Findings persist across scans (deduped); resolved findings stay in history.
// ════════════════════════════════════════════════════════════════

const crypto = require('crypto');

const QTY_TOL_GENERIC = 0.05;  // 5%
const QTY_TOL_TIGHT   = 0.01;  // 1%
const LOOKBACK_DAYS_DEFAULT = 30;

function _hashKey(...parts) {
  return crypto.createHash('sha256').update(parts.join('|')).digest('hex').slice(0, 24);
}

// Fix-path catalog. Each check_type maps to a default suggested fix.
// Used when a finding has no more specific override.
const FIX_CATALOG = {
  qty_mismatch_plan_dpr: {
    app: 'dpr',
    page: 'entry',
    role: 'role:gf,role:ff,role:admin',
    action: 'Review DPR entries for this batch — production qty should match planned ±5%. Correct any wrong shift totals.',
  },
  qty_mismatch_dpr_aim: {
    app: 'tracking',
    page: 'aim',
    role: 'role:tracking_aim,role:admin',
    action: 'AIM scan-in total differs from DPR production. Either AIM missed scanning some boxes (re-scan) or DPR was over-reported. Compare and correct.',
  },
  qty_mismatch_aim_packed: {
    app: 'tracking',
    page: 'packing',
    role: 'role:tracking_packing,role:admin',
    action: 'Packed quantity differs from AIM scan-in beyond expected wastage. Investigate missing boxes between AIM and Packing.',
  },
  upstream_missing_scan: {
    app: 'tracking',
    page: 'aim',
    role: 'role:tracking_aim,role:tracking_printing,role:tracking_pi,role:admin',
    action: 'Box has downstream scan but missing upstream scan. Either re-scan in the missing stage or void the orphan downstream scan.',
  },
  upstream_timestamp_inversion: {
    app: 'tracking',
    page: 'batch-tracker',
    role: 'role:admin',
    action: 'Box was scanned OUT of a stage before being scanned IN. Likely data corruption — investigate via Batch Tracker.',
  },
  wastage_exceeds_input: {
    app: 'tracking',
    page: 'aim',
    role: 'role:admin',
    action: 'Wastage at this stage exceeds the quantity scanned IN. Reduce wastage entry or scan more boxes IN.',
  },
  dpr_missing_day: {
    app: 'dpr',
    page: 'entry',
    role: 'role:gf,role:ff,role:admin',
    action: 'Machine had running orders this day but no DPR entry was made. Add the missing entry.',
  },
  tracking_idle_running_batch: {
    app: 'tracking',
    page: 'batch-tracker',
    role: 'role:tracking_aim,role:admin',
    action: 'Running batch has had no tracking scans in over 24h. Resume scanning or close the order if production has stopped.',
  },
  tracking_stage_stalled: {
    app: 'tracking',
    page: 'aim',
    role: 'role:tracking_aim,role:tracking_pi,role:admin',
    action: 'Boxes are sitting in this stage with no progression. Scan them OUT to the next stage if completed.',
  },
  closed_batch_unreconciled: {
    app: 'planning',
    page: 'production',
    role: 'role:planning_manager,role:admin',
    action: 'Order is closed but planned and actual quantities do not reconcile within tolerance. Reopen and investigate, or accept variance.',
  },
  over_production: {
    app: 'planning',
    page: 'production',
    role: 'role:planning_manager,role:admin',
    action: 'Actual production exceeds gross planned + 10%. Verify the batch is not double-counted in DPR.',
  },
  wo_unsplit_overdue: {
    app: 'planning',
    page: 'reconciliation',
    role: 'role:planning_manager,role:admin',
    action: 'W/O order is past endDate but not yet split or reconciled to a customer. Use Split & Assign or move to a known customer.',
  },
  temp_batch_overdue: {
    app: 'planning',
    page: 'reconciliation',
    role: 'role:planning_manager,role:admin',
    action: 'TEMP batch is older than 7 days. Reconcile it to a real order via the Reconciliation page.',
  },
  orphan_tracking_scan: {
    app: 'tracking',
    page: 'batch-tracker',
    role: 'role:admin',
    action: 'Scan references a label_id that does not exist. Admin-only data cleanup required.',
  },
  invoice_no_dispatch: {
    app: 'tracking',
    page: 'dispatch',
    role: 'role:tracking_dispatch,role:admin',
    action: 'SAP invoice received over 48h ago but no scan-out recorded. Complete truck dispatch.',
  },
  labels_not_printed: {
    app: 'tracking',
    page: 'labels',
    role: 'role:tracking_labels,role:admin',
    action: 'Labels were generated but never marked printed for a batch that has dispatched. Print or mark as printed.',
  },
  overlimit_running: {
    app: 'planning',
    page: 'production',
    role: 'role:planning_manager,role:admin',
    action: 'Machine has more than 2 orders In Production. Close or revert one to Pending.',
  },
  wo_split_child_missing: {
    app: 'planning',
    page: 'reconciliation',
    role: 'role:admin',
    action: 'WO split line refers to a child_order_id that no longer exists in planning state. Admin-only data cleanup.',
  },
};

function _enrichFinding(f) {
  const fix = FIX_CATALOG[f.check_type] || {};
  return {
    ...f,
    suggested_app:    f.suggested_app    || fix.app    || null,
    suggested_page:   f.suggested_page   || fix.page   || null,
    suggested_role:   f.suggested_role   || fix.role   || null,
    suggested_action: f.suggested_action || fix.action || null,
  };
}

// ────────────────────────────────────────────────────────────────
// Data access — works with both pgPool and SQLite db
// ────────────────────────────────────────────────────────────────
async function _query(ctx, sql, params = []) {
  if (ctx.pgPool) {
    // Convert ? placeholders to $1, $2, ...
    let i = 0;
    const pgSql = sql.replace(/\?/g, () => `$${++i}`);
    const r = await ctx.pgPool.query(pgSql, params);
    return r.rows;
  }
  return ctx.db.prepare(sql).all(...params);
}

// ────────────────────────────────────────────────────────────────
// Check 1: Quantity reconciliation per batch (4 stages)
//   Plan grossQty vs DPR sum vs AIM scan-in sum vs Packed scan-in sum
// ────────────────────────────────────────────────────────────────
async function check_qty_reconciliation(ctx) {
  const findings = [];
  const lookback = ctx.lookbackDays || LOOKBACK_DAYS_DEFAULT;
  const cutoff = new Date(Date.now() - lookback * 86400000).toISOString().slice(0, 10);
  const orders = (ctx.planningState.orders || []).filter(o =>
    !o.deleted &&
    o.batchNumber &&
    !String(o.batchNumber).startsWith('TEMP-') &&
    (o.status === 'running' || o.status === 'closed') &&
    (!o.endDate || String(o.endDate).slice(0,10) >= cutoff)
  );

  for (const ord of orders) {
    const batchNumber = ord.batchNumber;
    const planQty  = parseFloat(ord.qty || 0);
    const grossQty = parseFloat(ord.grossQty || (planQty * 1.07 * 1.01 * 1.01));

    // DPR sum for this batch
    const dprRows = await _query(ctx,
      `SELECT COALESCE(SUM(qty_lakhs),0) AS total FROM production_actuals WHERE batch_number=? OR order_id=?`,
      [batchNumber, ord.id]
    );
    const dprQty = parseFloat(dprRows[0]?.total || 0);

    // AIM scan-in sum (qty per label, no double-count)
    const aimRows = await _query(ctx,
      `SELECT COALESCE(SUM(l.qty),0) AS total
       FROM tracking_scans s
       JOIN tracking_labels l ON l.id = s.label_id
       WHERE s.batch_number=? AND s.dept='aim' AND s.type='in'
         AND (l.voided IS NULL OR l.voided=0)`,
      [batchNumber]
    );
    const aimInQty = parseFloat(aimRows[0]?.total || 0);

    // Packed scan-in sum
    const packRows = await _query(ctx,
      `SELECT COALESCE(SUM(l.qty),0) AS total
       FROM tracking_scans s
       JOIN tracking_labels l ON l.id = s.label_id
       WHERE s.batch_number=? AND s.dept='packing' AND s.type='in'
         AND (l.voided IS NULL OR l.voided=0)`,
      [batchNumber]
    );
    const packedQty = parseFloat(packRows[0]?.total || 0);

    // Skip batches with no activity at all yet
    if (dprQty === 0 && aimInQty === 0 && packedQty === 0) continue;

    const isClosed = ord.status === 'closed';
    const sev = (gapPct) => {
      if (isClosed && gapPct > QTY_TOL_GENERIC) return 'critical';
      if (gapPct > 0.15) return 'critical';
      if (gapPct > QTY_TOL_GENERIC) return 'warning';
      if (gapPct > 0.01) return 'info';
      return null;
    };

    // Pair 1: planned vs DPR
    if (dprQty > 0 && grossQty > 0) {
      const gap = Math.abs(dprQty - grossQty);
      const pct = gap / Math.max(grossQty, dprQty);
      const s = sev(pct);
      if (s) {
        findings.push(_enrichFinding({
          finding_key: _hashKey('qty_mismatch_plan_dpr', batchNumber),
          check_type: 'qty_mismatch_plan_dpr',
          severity: s,
          batch_number: batchNumber,
          order_id: ord.id,
          machine_id: ord.machineId,
          day: null,
          description: `Batch ${batchNumber}: Plan ${grossQty.toFixed(2)}L vs DPR ${dprQty.toFixed(2)}L (gap ${(pct*100).toFixed(1)}%, ${(gap).toFixed(2)}L)`,
          raw_data: { planQty, grossQty, dprQty, gap, pct, status: ord.status, customer: ord.customer },
        }));
      }
    }

    // Pair 2: DPR vs AIM — same physical material, tight tolerance
    if (dprQty > 0 && aimInQty > 0) {
      const gap = Math.abs(dprQty - aimInQty);
      const pct = gap / Math.max(dprQty, aimInQty);
      let s = null;
      if (isClosed && pct > QTY_TOL_TIGHT) s = 'critical';
      else if (pct > 0.10) s = 'critical';
      else if (pct > QTY_TOL_TIGHT) s = 'warning';
      else if (pct > 0.005) s = 'info';
      if (s) {
        findings.push(_enrichFinding({
          finding_key: _hashKey('qty_mismatch_dpr_aim', batchNumber),
          check_type: 'qty_mismatch_dpr_aim',
          severity: s,
          batch_number: batchNumber,
          order_id: ord.id,
          machine_id: ord.machineId,
          day: null,
          description: `Batch ${batchNumber}: DPR ${dprQty.toFixed(2)}L vs AIM scan-in ${aimInQty.toFixed(2)}L (gap ${(pct*100).toFixed(1)}%, ${(gap).toFixed(2)}L)`,
          raw_data: { dprQty, aimInQty, gap, pct, status: ord.status, customer: ord.customer },
        }));
      }
    }

    // Pair 3: AIM in vs Packed — wastage between them is OK, but only up to ~10% combined
    if (aimInQty > 0 && packedQty > 0 && ord.status === 'closed') {
      const expectedWastage = aimInQty * 0.10;  // generous; real wastage is smaller
      const actualLoss = aimInQty - packedQty;
      if (actualLoss > expectedWastage) {
        const lossPct = actualLoss / aimInQty;
        findings.push(_enrichFinding({
          finding_key: _hashKey('qty_mismatch_aim_packed', batchNumber),
          check_type: 'qty_mismatch_aim_packed',
          severity: lossPct > 0.20 ? 'critical' : 'warning',
          batch_number: batchNumber,
          order_id: ord.id,
          machine_id: ord.machineId,
          day: null,
          description: `Batch ${batchNumber}: AIM-in ${aimInQty.toFixed(2)}L but Packed only ${packedQty.toFixed(2)}L — loss ${(actualLoss).toFixed(2)}L (${(lossPct*100).toFixed(1)}%) exceeds expected wastage`,
          raw_data: { aimInQty, packedQty, actualLoss, lossPct, customer: ord.customer },
        }));
      }
    }
  }

  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 2: Upstream progression violations (batch-level summary)
// ────────────────────────────────────────────────────────────────
async function check_upstream_progression(ctx) {
  const findings = [];
  // Find labels that have scans in a downstream dept but no upstream scan.
  // Use the same dept order P18.14b enforces.
  const DEPT_ORDER = ['aim', 'printing', 'pi', 'packing'];

  for (let i = 1; i < DEPT_ORDER.length; i++) {
    const upstream = DEPT_ORDER[i - 1];
    const downstream = DEPT_ORDER[i];
    // Find labels with downstream IN scan but no upstream OUT scan
    const rows = await _query(ctx, `
      SELECT s.batch_number, COUNT(DISTINCT s.label_id) AS gap_count
      FROM tracking_scans s
      WHERE s.dept = ? AND s.type = 'in'
        AND NOT EXISTS (
          SELECT 1 FROM tracking_scans u
          WHERE u.label_id = s.label_id AND u.dept = ? AND u.type = 'out'
        )
      GROUP BY s.batch_number
      HAVING COUNT(DISTINCT s.label_id) > 0
    `, [downstream, upstream]);

    for (const r of rows) {
      // Skip if batch's product type indicates this stage is legitimately skipped
      const ord = (ctx.planningState.orders || []).find(o => o.batchNumber === r.batch_number);
      if (ord && upstream === 'printing' && (!ord.isPrinted || ord.isPrinted === false)) continue;  // unprinted batches skip Printing

      findings.push(_enrichFinding({
        finding_key: _hashKey('upstream_missing_scan', r.batch_number, upstream, downstream),
        check_type: 'upstream_missing_scan',
        severity: ord && ord.status === 'closed' ? 'critical' : 'warning',
        batch_number: r.batch_number,
        order_id: ord?.id,
        machine_id: ord?.machineId,
        day: null,
        description: `Batch ${r.batch_number}: ${r.gap_count} box(es) scanned IN to ${downstream.toUpperCase()} without OUT scan from ${upstream.toUpperCase()}`,
        raw_data: { upstream, downstream, gapCount: r.gap_count, batchStatus: ord?.status },
      }));
    }
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 3: Wastage > input at a stage
// ────────────────────────────────────────────────────────────────
async function check_wastage_exceeds_input(ctx) {
  const findings = [];
  const rows = await _query(ctx, `
    SELECT w.batch_number, w.dept, SUM(w.qty) AS wastage_qty
    FROM tracking_wastage w
    GROUP BY w.batch_number, w.dept
  `);
  for (const w of rows) {
    const inRows = await _query(ctx, `
      SELECT COALESCE(SUM(l.qty),0) AS in_qty
      FROM tracking_scans s
      JOIN tracking_labels l ON l.id = s.label_id
      WHERE s.batch_number=? AND s.dept=? AND s.type='in'
        AND (l.voided IS NULL OR l.voided=0)
    `, [w.batch_number, w.dept]);
    const inQty = parseFloat(inRows[0]?.in_qty || 0);
    const wasteQty = parseFloat(w.wastage_qty || 0);
    if (wasteQty > inQty * 1.001 && inQty > 0) {  // 0.1% tolerance for floats
      const ord = (ctx.planningState.orders || []).find(o => o.batchNumber === w.batch_number);
      findings.push(_enrichFinding({
        finding_key: _hashKey('wastage_exceeds_input', w.batch_number, w.dept),
        check_type: 'wastage_exceeds_input',
        severity: 'critical',
        batch_number: w.batch_number,
        order_id: ord?.id,
        machine_id: ord?.machineId,
        day: null,
        description: `Batch ${w.batch_number}: Wastage at ${w.dept.toUpperCase()} (${wasteQty.toFixed(2)}L) exceeds scan-IN (${inQty.toFixed(2)}L)`,
        raw_data: { dept: w.dept, wasteQty, inQty },
      }));
    }
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 4: DPR entry missing for a day where machine had running orders
// ────────────────────────────────────────────────────────────────
async function check_dpr_missing_day(ctx) {
  const findings = [];
  // For each day in lookback window, find machines with running orders on that day
  // that have no production_actuals entries.
  const lookback = ctx.lookbackDays || LOOKBACK_DAYS_DEFAULT;
  const today = new Date();
  const cutoff = new Date(today.getTime() - lookback * 86400000);

  // Find machines with running orders touching the lookback window
  const runningOrders = (ctx.planningState.orders || []).filter(o =>
    !o.deleted &&
    (o.status === 'running' || o.status === 'closed') &&
    o.machineId &&
    o.startDate
  );

  // Build (machine, day) pairs where production was expected
  const expectedPairs = new Set();
  for (const ord of runningOrders) {
    const start = new Date(ord.startDate);
    const end = ord.endDate ? new Date(ord.endDate) : (ord.closedDate ? new Date(ord.closedDate) : today);
    let day = new Date(Math.max(start.getTime(), cutoff.getTime()));
    const stopAt = new Date(Math.min(end.getTime(), today.getTime() - 86400000));  // up to yesterday
    while (day <= stopAt) {
      expectedPairs.add(`${ord.machineId}|${day.toISOString().slice(0,10)}`);
      day = new Date(day.getTime() + 86400000);
    }
  }

  // Find pairs that have NO actuals
  const reportedRows = await _query(ctx, `
    SELECT DISTINCT machine_id, date FROM production_actuals
    WHERE date >= ?
  `, [cutoff.toISOString().slice(0,10)]);
  const reportedPairs = new Set(reportedRows.map(r => `${r.machine_id}|${r.date}`));

  for (const pair of expectedPairs) {
    if (reportedPairs.has(pair)) continue;
    const [machineId, day] = pair.split('|');
    findings.push(_enrichFinding({
      finding_key: _hashKey('dpr_missing_day', machineId, day),
      check_type: 'dpr_missing_day',
      severity: 'info',
      batch_number: null,
      order_id: null,
      machine_id: machineId,
      day,
      description: `Machine ${machineId} had running order(s) on ${day} but no DPR entry was made`,
      raw_data: { machineId, day },
    }));
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 5: Running batch idle (no scans in >24h)
// ────────────────────────────────────────────────────────────────
async function check_tracking_idle_running_batch(ctx) {
  const findings = [];
  const cutoffISO = new Date(Date.now() - 86400000).toISOString();
  const runningOrders = (ctx.planningState.orders || []).filter(o =>
    !o.deleted && o.status === 'running' && o.batchNumber && !String(o.batchNumber).startsWith('TEMP-')
  );
  for (const ord of runningOrders) {
    const rows = await _query(ctx, `
      SELECT MAX(ts) AS last_scan FROM tracking_scans WHERE batch_number=?
    `, [ord.batchNumber]);
    const lastScan = rows[0]?.last_scan;
    if (!lastScan) continue;  // Never scanned; covered by other checks
    if (lastScan < cutoffISO) {
      const hoursIdle = Math.round((Date.now() - new Date(lastScan).getTime()) / 3600000);
      findings.push(_enrichFinding({
        finding_key: _hashKey('tracking_idle_running_batch', ord.batchNumber),
        check_type: 'tracking_idle_running_batch',
        severity: hoursIdle > 72 ? 'warning' : 'info',
        batch_number: ord.batchNumber,
        order_id: ord.id,
        machine_id: ord.machineId,
        day: null,
        description: `Batch ${ord.batchNumber} is In Production but has had no tracking scans for ${hoursIdle}h`,
        raw_data: { batchNumber: ord.batchNumber, lastScan, hoursIdle, customer: ord.customer },
      }));
    }
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 6: Closed batch unreconciled (closed but planned vs actual gap)
// ────────────────────────────────────────────────────────────────
async function check_closed_batch_unreconciled(ctx) {
  const findings = [];
  const closedOrders = (ctx.planningState.orders || []).filter(o =>
    !o.deleted && o.status === 'closed' && o.batchNumber && !String(o.batchNumber).startsWith('TEMP-')
  );
  const lookback = ctx.lookbackDays || LOOKBACK_DAYS_DEFAULT;
  const cutoff = new Date(Date.now() - lookback * 86400000).toISOString().slice(0,10);
  for (const ord of closedOrders) {
    if (ord.closedDate && String(ord.closedDate).slice(0,10) < cutoff) continue;
    const grossQty = parseFloat(ord.grossQty || ord.qty || 0);
    const actualQty = parseFloat(ord.actualProd || ord.actualQty || 0);
    if (grossQty === 0) continue;
    const pct = Math.abs(actualQty - grossQty) / grossQty;
    if (pct > QTY_TOL_GENERIC && !ord.mismatchFlag) {  // mismatchFlag means admin already acknowledged
      findings.push(_enrichFinding({
        finding_key: _hashKey('closed_batch_unreconciled', ord.id),
        check_type: 'closed_batch_unreconciled',
        severity: pct > 0.15 ? 'critical' : 'warning',
        batch_number: ord.batchNumber,
        order_id: ord.id,
        machine_id: ord.machineId,
        day: null,
        description: `Closed batch ${ord.batchNumber}: planned ${grossQty.toFixed(2)}L vs actual ${actualQty.toFixed(2)}L (${(pct*100).toFixed(1)}% gap)`,
        raw_data: { grossQty, actualQty, gap: Math.abs(actualQty-grossQty), pct, customer: ord.customer, closedDate: ord.closedDate },
      }));
    }
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 7: Over-production (actual > gross * 1.10)
// ────────────────────────────────────────────────────────────────
async function check_over_production(ctx) {
  const findings = [];
  for (const ord of (ctx.planningState.orders || [])) {
    if (ord.deleted) continue;
    const grossQty = parseFloat(ord.grossQty || ord.qty || 0);
    const actualQty = parseFloat(ord.actualProd || ord.actualQty || 0);
    if (grossQty === 0 || actualQty <= grossQty * 1.10) continue;
    const pct = (actualQty - grossQty) / grossQty;
    findings.push(_enrichFinding({
      finding_key: _hashKey('over_production', ord.id),
      check_type: 'over_production',
      severity: pct > 0.20 ? 'critical' : 'warning',
      batch_number: ord.batchNumber,
      order_id: ord.id,
      machine_id: ord.machineId,
      day: null,
      description: `Batch ${ord.batchNumber}: actual ${actualQty.toFixed(2)}L exceeds gross ${grossQty.toFixed(2)}L by ${(pct*100).toFixed(1)}%`,
      raw_data: { grossQty, actualQty, overByPct: pct, customer: ord.customer },
    }));
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 8: WO orders past endDate without split or reconciliation
// ────────────────────────────────────────────────────────────────
async function check_wo_unsplit_overdue(ctx) {
  const findings = [];
  const today = new Date().toISOString().slice(0,10);
  for (const ord of (ctx.planningState.orders || [])) {
    if (ord.deleted) continue;
    if (ord.woStatus !== 'wo' && ord.woStatus !== 'wo-split-partial') continue;
    if (!ord.endDate) continue;
    const endStr = String(ord.endDate).slice(0,10);
    if (endStr >= today) continue;
    const daysOverdue = Math.floor((Date.now() - new Date(endStr).getTime()) / 86400000);
    if (daysOverdue < 1) continue;
    findings.push(_enrichFinding({
      finding_key: _hashKey('wo_unsplit_overdue', ord.id),
      check_type: 'wo_unsplit_overdue',
      severity: daysOverdue > 14 ? 'warning' : 'info',
      batch_number: ord.batchNumber,
      order_id: ord.id,
      machine_id: ord.machineId,
      day: endStr,
      description: `W/O order ${ord.batchNumber} is ${daysOverdue} day(s) past endDate without customer split`,
      raw_data: { daysOverdue, endDate: endStr, woStatus: ord.woStatus },
    }));
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 9: TEMP batch >7 days unreconciled
// ────────────────────────────────────────────────────────────────
async function check_temp_batch_overdue(ctx) {
  const findings = [];
  let tempRows = [];
  try {
    tempRows = await _query(ctx, `SELECT id, machine_id, date FROM temp_batches WHERE reconciled = 0 OR reconciled IS NULL`);
  } catch (e) { /* tolerate older deployments without the column */ }
  const today = new Date();
  for (const t of tempRows) {
    if (!t.date) continue;
    const daysOld = Math.floor((today.getTime() - new Date(t.date).getTime()) / 86400000);
    if (daysOld < 7) continue;
    findings.push(_enrichFinding({
      finding_key: _hashKey('temp_batch_overdue', t.id),
      check_type: 'temp_batch_overdue',
      severity: daysOld > 21 ? 'warning' : 'info',
      batch_number: t.id,
      order_id: null,
      machine_id: t.machine_id,
      day: t.date,
      description: `TEMP batch ${t.id} on ${t.machine_id} is ${daysOld} day(s) old, awaiting reconciliation`,
      raw_data: { daysOld, machineId: t.machine_id, date: t.date },
    }));
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 10: Orphan tracking_scans (label_id has no matching label row)
// ────────────────────────────────────────────────────────────────
async function check_orphan_tracking_scan(ctx) {
  const findings = [];
  const rows = await _query(ctx, `
    SELECT s.batch_number, COUNT(*) AS orphan_count
    FROM tracking_scans s
    WHERE NOT EXISTS (SELECT 1 FROM tracking_labels l WHERE l.id = s.label_id)
    GROUP BY s.batch_number
  `);
  for (const r of rows) {
    findings.push(_enrichFinding({
      finding_key: _hashKey('orphan_tracking_scan', r.batch_number),
      check_type: 'orphan_tracking_scan',
      severity: 'warning',
      batch_number: r.batch_number,
      description: `Batch ${r.batch_number}: ${r.orphan_count} scan(s) reference label_ids that no longer exist`,
      raw_data: { batchNumber: r.batch_number, orphanCount: r.orphan_count },
    }));
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 11: Invoice received >48h ago but no dispatch scan-out
// ────────────────────────────────────────────────────────────────
async function check_invoice_no_dispatch(ctx) {
  const findings = [];
  let rows = [];
  try {
    rows = await _query(ctx, `
      SELECT i.id, i.doc_num, i.customer, i.batch_number, i.received_at, i.status
      FROM invoices_received i
      WHERE i.status NOT IN ('dispatched','cancelled') AND i.received_at < ?
    `, [new Date(Date.now() - 48 * 3600000).toISOString()]);
  } catch (e) { /* invoices_received absent on older deployments */ }
  for (const inv of rows) {
    const hoursPending = Math.round((Date.now() - new Date(inv.received_at).getTime()) / 3600000);
    findings.push(_enrichFinding({
      finding_key: _hashKey('invoice_no_dispatch', inv.id),
      check_type: 'invoice_no_dispatch',
      severity: hoursPending > 168 ? 'critical' : 'warning',
      batch_number: inv.batch_number,
      order_id: null,
      machine_id: null,
      day: null,
      description: `Invoice ${inv.doc_num} (${inv.customer || '?'}) received ${hoursPending}h ago, status="${inv.status}", not yet dispatched`,
      raw_data: { invoiceId: inv.id, docNum: inv.doc_num, customer: inv.customer, batchNumber: inv.batch_number, hoursPending, status: inv.status },
    }));
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Check 12: >2 In Production per machine (duplicate of P18.14i banner)
// ────────────────────────────────────────────────────────────────
async function check_overlimit_running(ctx) {
  const findings = [];
  const byMc = {};
  for (const o of (ctx.planningState.orders || [])) {
    if (o.status !== 'running' || o.deleted) continue;
    const mc = o.machineId || '(unassigned)';
    if (!byMc[mc]) byMc[mc] = [];
    byMc[mc].push(o);
  }
  for (const [mcId, orders] of Object.entries(byMc)) {
    if (orders.length <= 2) continue;
    findings.push(_enrichFinding({
      finding_key: _hashKey('overlimit_running', mcId),
      check_type: 'overlimit_running',
      severity: 'warning',
      batch_number: null,
      order_id: null,
      machine_id: mcId,
      day: null,
      description: `Machine ${mcId} has ${orders.length} orders In Production (limit is 2)`,
      raw_data: { machineId: mcId, runningCount: orders.length, batches: orders.map(o=>o.batchNumber) },
    }));
  }
  return findings;
}

// ────────────────────────────────────────────────────────────────
// Orchestrator
// ────────────────────────────────────────────────────────────────
const ALL_CHECKS = [
  { key: 'qty_reconciliation',     fn: check_qty_reconciliation },
  { key: 'upstream_progression',   fn: check_upstream_progression },
  { key: 'wastage_exceeds_input',  fn: check_wastage_exceeds_input },
  { key: 'dpr_missing_day',        fn: check_dpr_missing_day },
  { key: 'tracking_idle',          fn: check_tracking_idle_running_batch },
  { key: 'closed_batch_unrec',     fn: check_closed_batch_unreconciled },
  { key: 'over_production',        fn: check_over_production },
  { key: 'wo_unsplit_overdue',     fn: check_wo_unsplit_overdue },
  { key: 'temp_batch_overdue',     fn: check_temp_batch_overdue },
  { key: 'orphan_tracking_scan',   fn: check_orphan_tracking_scan },
  { key: 'invoice_no_dispatch',    fn: check_invoice_no_dispatch },
  { key: 'overlimit_running',      fn: check_overlimit_running },
];

async function runAllChecks(ctx) {
  const startMs = Date.now();
  const allFindings = [];
  const errors = [];

  // Load muted check_types
  let mutedCheckTypes = new Set();
  try {
    const muteRows = await _query(ctx, `SELECT check_type FROM integrity_mutes`);
    mutedCheckTypes = new Set(muteRows.map(r => r.check_type));
  } catch (e) { /* tolerate */ }

  for (const { key, fn } of ALL_CHECKS) {
    try {
      const found = await fn(ctx);
      // Filter out findings whose check_type is muted
      const kept = found.filter(f => !mutedCheckTypes.has(f.check_type));
      allFindings.push(...kept);
    } catch (e) {
      console.error(`[integrity] check ${key} failed:`, e.message);
      errors.push({ check: key, error: e.message });
    }
  }

  // Upsert findings. Dedup by finding_key.
  let upserted = 0, resolved = 0;
  const allKeysThisRun = new Set(allFindings.map(f => f.finding_key));

  for (const f of allFindings) {
    const id = `IF-${f.finding_key}`;
    const rawJson = JSON.stringify(f.raw_data || {});
    if (ctx.pgPool) {
      await ctx.pgPool.query(`
        INSERT INTO integrity_findings
          (id, finding_key, check_type, severity, batch_number, order_id, machine_id, day,
           description, suggested_app, suggested_page, suggested_role, suggested_action,
           raw_data_json, first_seen, last_seen, resolved)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW()::TEXT,NOW()::TEXT,0)
        ON CONFLICT(finding_key) DO UPDATE SET
          severity = EXCLUDED.severity,
          description = EXCLUDED.description,
          raw_data_json = EXCLUDED.raw_data_json,
          last_seen = NOW()::TEXT,
          resolved = 0,
          resolved_at = NULL
      `, [id, f.finding_key, f.check_type, f.severity, f.batch_number, f.order_id, f.machine_id,
          f.day, f.description, f.suggested_app, f.suggested_page, f.suggested_role, f.suggested_action,
          rawJson]);
    } else {
      ctx.db.prepare(`
        INSERT INTO integrity_findings
          (id, finding_key, check_type, severity, batch_number, order_id, machine_id, day,
           description, suggested_app, suggested_page, suggested_role, suggested_action,
           raw_data_json, first_seen, last_seen, resolved)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), 0)
        ON CONFLICT(finding_key) DO UPDATE SET
          severity = excluded.severity,
          description = excluded.description,
          raw_data_json = excluded.raw_data_json,
          last_seen = datetime('now'),
          resolved = 0,
          resolved_at = NULL
      `).run(id, f.finding_key, f.check_type, f.severity, f.batch_number, f.order_id, f.machine_id,
            f.day, f.description, f.suggested_app, f.suggested_page, f.suggested_role, f.suggested_action,
            rawJson);
    }
    upserted++;
  }

  // Mark findings as resolved if they were in DB but not in this scan
  // (only for non-resolved, last-seen within last 7 days)
  const cutoff = new Date(Date.now() - 7 * 86400000).toISOString();
  const existingRows = await _query(ctx,
    `SELECT id, finding_key FROM integrity_findings WHERE resolved = 0 AND last_seen > ?`, [cutoff]);
  for (const row of existingRows) {
    if (!allKeysThisRun.has(row.finding_key)) {
      if (ctx.pgPool) {
        await ctx.pgPool.query(`UPDATE integrity_findings SET resolved=1, resolved_at=NOW()::TEXT WHERE id=$1`, [row.id]);
      } else {
        ctx.db.prepare(`UPDATE integrity_findings SET resolved=1, resolved_at=datetime('now') WHERE id=?`).run(row.id);
      }
      resolved++;
    }
  }

  return {
    durationMs: Date.now() - startMs,
    findingsFound: allFindings.length,
    upserted,
    resolved,
    errors,
    mutedCheckTypes: Array.from(mutedCheckTypes),
  };
}

module.exports = {
  runAllChecks,
  ALL_CHECKS,
  FIX_CATALOG,
  QTY_TOL_GENERIC,
  QTY_TOL_TIGHT,
};
