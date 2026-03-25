# 🚀 Quick Railway PostgreSQL Setup

## TL;DR - 3 Steps

### 1. Add PostgreSQL
Go to: https://railway.com/project/b2fb6d3d-44c9-4d17-827d-2e2384bb347e
Click: **+ Create** → **Database** → **PostgreSQL**

### 2. Redeploy
```bash
railway up --service 5faf9761-02d1-4d91-8e7c-040866b67539
```

### 3. Done! ✅
Your server now uses persistent PostgreSQL.

---

## Verification

Check logs after deploy:
```bash
railway logs --service 5faf9761-02d1-4d91-8e7c-040866b67539
```

Look for:
```
🐘 Using PostgreSQL (DATABASE_URL detected)
✅ PostgreSQL connected
```

---

## What Changed

- ✅ Added `pg` driver to package.json
- ✅ Created `db.js` abstraction layer
- ✅ Server auto-detects `DATABASE_URL` and uses PostgreSQL
- ✅ SQLite still works locally

---

## Cost

PostgreSQL on Railway:
- **First 10GB storage**: Included free tier
- **Beyond 10GB**: $0.25/GB/month
- **CPU/RAM**: Scales with your app plan

No additional cost if you stay under 10GB.

---

## Data Persistence

- ✅ Data survives app restarts
- ✅ Data survives deploys
- ✅ Railway includes daily backups
- ✅ All databases are encrypted at rest

---

## Commands Reference

```bash
# See database connection info
railway status

# SSH into Railway container
railway shell

# View environment variables
railway run env | grep DATABASE

# Check PostgreSQL tables
railway run psql "$DATABASE_URL" -c "\dt"

# View server logs (live)
railway logs -f --service 5faf9761-02d1-4d91-8e7c-040866b67539
```

---

## Still Have Questions?

Refer to: **RAILWAY_POSTGRES_SETUP.md** for detailed troubleshooting.

Ready to deploy? Run: `railway up`
