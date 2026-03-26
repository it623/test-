# SUNLOC SUITE — DEPLOYMENT GUIDE

## Quick Deploy
```bash
railway up
```

---

## CRITICAL: Persistent Database Setup (Do Once)

**Your database MUST be on a persistent volume or data will be wiped on every deploy.**

### Step 1 — Create a Railway Volume
1. Open your project in Railway dashboard
2. Click **+ New** → **Volume**
3. Set mount path: `/data`
4. Click **Create**

That's it. The server automatically detects `/data` and stores `sunloc.db` there.

### Step 2 — Verify it's working
After deploying, visit:
```
https://your-app.railway.app/api/admin/db-status
```
You should see `"db_path": "/data/sunloc.db"` — confirming the persistent volume is in use.

If you see `__dirname/sunloc.db` — the volume is NOT mounted. Go back to Step 1.

---

## GitHub Auto-Deploy Setup (Recommended)

Eliminates manual `railway up` — every push to GitHub triggers automatic deployment.

### Step 1 — Create GitHub Repository
```bash
cd sunloc-server
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/sunloc-server.git
git push -u origin main
```

### Step 2 — Connect Railway to GitHub
1. Railway dashboard → your project → **Settings**
2. **Source** → **Connect Repo**
3. Select your GitHub repository
4. Set **Branch**: `main`
5. Railway will now auto-deploy on every push

### Step 3 — Future deployments
```bash
git add .
git commit -m "Describe your change"
git push
# Railway deploys automatically — no railway up needed
```

---

## Data Migration (Moving data between databases)

### Export current data
```bash
curl "https://your-app.railway.app/api/admin/export?key=sunloc-export-2024" \
  -o sunloc-backup-$(date +%Y%m%d).json
```

### Import data to new database
```bash
curl -X POST "https://your-app.railway.app/api/admin/import?key=sunloc-export-2024" \
  -H "Content-Type: application/json" \
  -d "{\"confirm\": \"IMPORT_CONFIRMED\", \"tables\": $(cat sunloc-backup-YYYYMMDD.json | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)[\"tables\"]))')}"
```

**Change the EXPORT_KEY** — set it as a Railway environment variable:
```
EXPORT_KEY = your-secret-key-here
```

---

## Database Schema Changes (Safe Updates)

**Never edit existing migrations.** Always add a new one in `server.js`:

```javascript
// In MIGRATIONS array, add at the end:
{
  version: 4,
  name: 'add_invoice_table',
  sql: `
    CREATE TABLE IF NOT EXISTS invoices (
      id TEXT PRIMARY KEY,
      ...
    );
  `
}
```

On next deploy, the new migration runs automatically. Existing data is untouched.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3000 | Server port (Railway sets this) |
| `DB_PATH` | `/data/sunloc.db` | Database file path |
| `EXPORT_KEY` | `sunloc-export-2024` | Secret key for data export/import |

**Change EXPORT_KEY in Railway environment variables before go-live.**

---

## Default PINs (Change after first deploy)

| User | PIN | App |
|------|-----|-----|
| GF | 1111 | DPR |
| FF | 2222 | DPR |
| DPR_Admin | 9999 | DPR |
| Planning_Manager | 3333 | Planning |
| Printing_Manager | 4444 | Planning |
| Dispatch_Manager | 5555 | Planning |
| Plan_Admin | 9999 | Planning |

Change PINs via Admin → User Menu → Change PIN (requires server connection).
