# 🔧 DPR App - Server Configuration & Sync Setup

## DPR Server Configuration

### **1. Server URL Configuration**

The DPR app needs to know where the Sunloc server is located to sync with Planning and Tracking apps.

**In DPR app (⚙ Settings tab):**
- Click the **⚙ Server** badge (top right) or go to **Settings** tab
- Enter your server URL: `https://sunloc.up.railway.app`
- Click **Save & Test**
- Status should show: **🟢 Synced**

### **2. DPR Server API Endpoints**

Your DPR app communicates with the Sunloc server via these endpoints:

```
GET  /api/health                          → Check server status
GET  /api/orders/active                   → Fetch active orders from Planning
GET  /api/orders/machine/:machineId       → Fetch machine-specific orders
POST /api/dpr/save                        → Save DPR records
GET  /api/dpr/:floor/:date                → Load DPR data for a date
GET  /api/dpr/dates/:floor                → Get all saved dates for a floor
POST /api/auth/login                      → DPR user login
POST /api/auth/logout                     → DPR user logout
POST /api/audit/log                       → Log audit trail
```

### **3. DPR Authentication**

**Default DPR Users (Offline Mode):**
- **GF** (Ground Floor) - PIN: `1111`
- **FF** (First Floor) - PIN: `2222`
- **DPR_Admin** (Admin) - PIN: `9999`

When connected to Sunloc server, these credentials are validated server-side.

### **4. Data Syncing Flow**

```
DPR App (Browser) 
    ↓
    ├─ [1] Login → /api/auth/login
    │
    ├─ [2] Load Active Orders → /api/orders/active
    │         ↓ (from Planning App)
    │         Shows all active production orders
    │
    ├─ [3] Load Previous DPR → /api/dpr/GF/2026-03-20
    │         ↓ (from PostgreSQL)
    │         Shows past records
    │
    ├─ [4] Enter Data Locally (in browser)
    │         ├─ Shift A: Production entries
    │         ├─ Shift B: Production entries
    │         ├─ Shift C: Production entries
    │         ├─ Downtime by category
    │         ├─ Re-sort entries
    │         └─ Remarks
    │
    ├─ [5] Save DPR → POST /api/dpr/save
    │         ↓ (to PostgreSQL)
    │         Data persisted on server
    │         Badge changes to 🟢 Synced
    │
    └─ [6] Auto-sync to Tracking App
           Tracking app can query same DPR records
```

### **5. Machines per Floor**

**Ground Floor (GF):**
- MC20, MC21, MC22, MC29, MC30, MC31, MC32, MC33, MC34 (Size #00)

**First Floor (FF):**
- MC14, MC15, MC16

**Second Floor (SF):**
- MC23, MC24, MC25, MC26, MC27, MC28

### **6. DPR Fields**

Each machine per shift records:

**Production Data:**
- ✏️ Operator name
- 📦 Batch runs (Order ID / Batch Number / Qty / Notes)
  - Can add multiple runs for colour/batch changes
- 🎯 AIM (target units)
- 🏭 Total production
- 📊 Efficiency % (vs target)

**Downtime (minutes per shift):**
- Process DT
- Mechanical DT
- Electrical DT
- GPR DT
- HVAC DT
- Dish Wash DT

**Quality:**
- ↩️ Re-sort entries (machine, shift, reason, qty)
- 💬 Remarks (issues/notes)

### **7. Role-Based Access Control**

| Role | Allowed Floors | Can Edit Past Records | Features |
|------|---|---|---|
| **GF** | Ground Floor only | ❌ Today only | Basic data entry |
| **FF** | All floors | ❌ Today only | Basic data entry |
| **DPR_Admin** | All floors | ✅ Yes | Audit log, override |

### **8. Data Persistence**

**Local Storage:**
- Data saved automatically in browser
- Survives browser cache clear (stored in localStorage, not cookies)
- Syncs to server when Save button clicked

**Server Storage:**
- POST `/api/dpr/save` → Stored in PostgreSQL
- GET `/api/dpr/:floor/:date` → Loaded from PostgreSQL
- Accessible to all connected users
- Persists across restarts

### **9. Order Integration**

When you save DPR:
1. **Production Actuals** are extracted from batch runs
2. Sent to server as: `{machineId, date, shift, orderId, batchNumber, qty, runIndex}`
3. Stored in `production_actuals` table
4. Available for Tracking app to query
5. Planning app can see what was produced vs planned

### **10. Analytics Pages**

| Page | Data Source | Description |
|------|---|---|
| **Data Entry** | LocalStorage + Server | Real-time input, shift data |
| **Analytics** | LocalStorage | Charts: P vs T, DT by category, Efficiency, Re-sort |
| **Trends** | LocalStorage | 7/14/30/60-day trend lines |
| **AIM Status** | LocalStorage | AIM vs produced, backlog tracking |
| **Shift Performance** | LocalStorage | Incharge scorecard, efficiency trends |
| **History** | LocalStorage | All saved records with quick load |
| **Search** | Server cache | Search active orders by batch/customer |
| **Audit Log** | Server | Admin-only: All user actions (login/logout/save) |

### **11. Connection Status Badge**

- 🟢 **Synced** - Connected to server, orders loaded, ready to sync
- 🟡 **Offline** - Server not reachable, using cached data
- ⟳ **Syncing** - Sending data to server

Click badge to reconfigure server URL.

### **12. Offline Mode**

DPR works fully offline:
- ✅ Enter data locally
- ✅ Save to browser localStorage
- ✅ View analytics, trends, history
- ❌ Cannot load active orders (use cached if available)
- ❌ Cannot sync to server

**To re-sync offline data later:**
1. Reconnect server when available
2. Click ⚙ Server to reconfigure
3. All offline data remains in browser
4. Click Save to send to server

### **13. Data Export/Import**

In Settings tab:
- **Export All (JSON)** - Download backup of all DPR data
- **Import JSON** - Restore from backup file
- **Clear All Data** - Wipe local data (cannot undo)

## DPR to Server Sync Checklist

✅ Server URL configured: `https://sunloc.up.railway.app`
✅ Health check passed: `/api/health` returns 200
✅ Orders synced from Planning: `/api/orders/active` shows orders
✅ DPR data persisting: `/api/dpr/save` successful
✅ Data retrievable: `/api/dpr/:floor/:date` works
✅ Multi-user access: Different users see same data
✅ Offline fallback: Works without server
✅ Audit logging: Admin can see all saves

## Common Issues & Fixes

| Issue | Cause | Fix |
|---|---|---|
| Server badge shows 🟡 Offline | URL incorrect or server down | Click badge, re-enter URL, test |
| Orders not showing in dropdown | Server not connected | Click ⚙ Server, save URL |
| Cannot save to server | Network error | Check internet, try again |
| Data not synced to Tracking | DPR not saved | Click Save button in DPR |
| Back-date save blocked | Non-admin user | Only DPR_Admin can edit past dates |

## DPR + Planning Sync Example

**Scenario:** You enter production data in DPR

1. **Planning App** created order:
   - Batch: BATCH001
   - Machine: MC34
   - Customer: ALKEM
   - Qty: 1000L

2. **DPR App** user (GF) enters:
   - Shift A, MC34
   - Batch: BATCH001
   - Qty: 500L

3. **Save DPR** → Posts to server

4. **Tracking App** can now:
   - See BATCH001 in production (500L of 1000L)
   - Create labels for the batch
   - Track output through scanning

5. **Planning App** can see:
   - Actual production vs planned
   - Calculate backlog (1000 - 500 = 500L remaining)

## Production Deployment Checklist

Before going live:
- [ ] Server URL configured in all 3 apps (Planning, DPR, Tracking)
- [ ] Test order sync: Create order in Planning → appears in DPR dropdown
- [ ] Test DPR save: Enter data → appears in Tracking app
- [ ] Test persistence: Close browser → data still there
- [ ] Test role access: Login as GF vs Admin → verify restrictions
- [ ] Test offline: Disconnect server → DPR still works locally
- [ ] Backup export: Export all DPR data as JSON
- [ ] User training: Train staff on DPR entry workflow

---

**Your Sunloc DPR app is now fully configured and synced with the server!** 🚀
