# Sunloc Server — Deployment Guide
## Railway.app (Free, ~5 minutes, no GitHub needed)

---

### What You'll Have After This

```
https://sunloc-server.up.railway.app   ← your server URL
```

Both the Planning App and DPR App will connect to this URL.
All data lives in one SQLite database on the server.

---

## Step 1 — Create a Railway Account

1. Go to **https://railway.app**
2. Click **"Start a New Project"**
3. Sign up with Google or email (free, no credit card needed for Hobby tier)

---

## Step 2 — Deploy the Server

Railway supports direct folder upload (no GitHub needed):

1. Download and unzip the `sunloc-server.zip` file
2. On Railway dashboard, click **"New Project"**
3. Select **"Deploy from local directory"** (or use the Railway CLI — see below)
4. Select the `sunloc-server` folder
5. Railway auto-detects Node.js and deploys

### Alternative: Railway CLI (recommended)

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# From inside the sunloc-server folder:
cd sunloc-server
railway init        # creates a new project
railway up          # deploys the folder
railway open        # opens your live URL
```

---

## Step 3 — Get Your Server URL

After deployment, Railway gives you a URL like:
```
https://sunloc-server-production-abc123.up.railway.app
```

Copy this URL — you'll paste it into both apps.

---

## Step 4 — Configure Both Apps

When you open either the Planning App or the DPR App in a browser for the first time, a prompt will appear:

```
🔗 Enter your Sunloc Server URL
(e.g. https://sunloc.up.railway.app)
```

Paste your Railway URL and click OK. The app will:
- Test the connection immediately
- Show a 🟢 Synced badge in the top bar if successful
- Fall back to local storage automatically if the server is unreachable

You only need to enter this once per browser — it's saved in localStorage.

---

## Step 5 — Verify It's Working

Open your server URL + `/api/health` in a browser:
```
https://your-url.railway.app/api/health
```

You should see:
```json
{
  "ok": true,
  "server": "Sunloc Integrated Server v1.0",
  "dprRecords": 0,
  "actualsEntries": 0,
  "uptime": "12s"
}
```

---

## Day-to-Day Operation

| Action | What Happens |
|--------|-------------|
| Planning person saves an order | Pushed to server immediately |
| Production team saves a shift in DPR | Actuals extracted and synced to server |
| Planning App opens | Fetches latest state from server with live actuals already merged |
| Server is unreachable (internet down) | Both apps fall back to localStorage automatically. Data syncs when connection restores. |

---

## Offline Safety

Both apps always write to localStorage AND the server simultaneously.
If the server is down, data is safe in the browser.
When the server comes back online, the next save operation pushes all local data to the server.

---

## Railway Free Tier Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| Execution hours | 500 hrs/month | ~16 hrs/day — enough for 24/7 factory use |
| Memory | 512 MB | More than sufficient for SQLite |
| Storage | 1 GB | Years of planning + DPR data |
| Bandwidth | 100 GB/month | Way more than needed |

For a factory with light API usage (saves every few minutes), this is effectively unlimited.

---

## Upgrading / Updating the Server

To push a code update:
```bash
cd sunloc-server
railway up
```

That's it. Zero downtime deployment.

---

## Database Backup

The SQLite database file (`sunloc.db`) is stored on Railway's persistent volume.
To download a backup at any time:

```bash
railway run -- cat sunloc.db > backup_$(date +%Y%m%d).db
```

Or add a daily backup export endpoint to the server (available on request).

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| 🟡 Offline badge in app | Check Railway dashboard — server may have gone to sleep. Open `/api/health` to wake it. |
| Server URL prompt keeps appearing | Paste URL and click OK — don't click Cancel |
| Data from DPR not showing in Planning | Check that Batch Number matches exactly in both apps |
| Railway deployment fails | Make sure `package.json` and `server.js` are in the root of the uploaded folder |

---

## Support

If anything goes wrong during deployment, share the Railway error log and we can fix it.

---

## Tracking Suite v1.0 — Added

The tracking app is now included at `/tracking.html`.

### Department PINs (change in production)
| Department | PIN |
|---|---|
| Admin | 000000 |
| Planning | 1111 |
| Label Generation | 2222 |
| AIM | 3333 |
| Printing | 4444 |
| Print Inspection | 5555 |
| Packing | 6666 |
| Dispatch | 7777 |

**Important:** Change all PINs before going live. PINs are currently in the frontend JS — for production, move auth to the server.

### Zebra Label Printer Setup
1. Install **Zebra Browser Print** on the label printer PC
2. In Tracking App → Settings → enter `http://localhost:9100` as printer URL
3. Labels will print directly. Without setup, ZPL files are downloaded instead.

### Camera Scanning
- Works on any device with a camera via the browser
- Uses `BarcodeDetector` API (Chrome/Edge/Android)
- Dedicated USB scanners work in HID mode — just focus the scan input field and scan

