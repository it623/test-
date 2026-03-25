# Railway PostgreSQL Setup Guide

## Step-by-Step Instructions

### 1. Add PostgreSQL to Your Railway Project

1. Go to your Railway project dashboard: https://railway.com/project/b2fb6d3d-44c9-4d17-827d-2e2384bb347e
2. Click **"+ Create"** button in the top right
3. Select **"Database"** > **"PostgreSQL"**
4. Railway will automatically:
   - Create a PostgreSQL database
   - Generate a `DATABASE_URL` environment variable
   - Inject it into your connected services

### 2. Verify Connection

After PostgreSQL is created, Railway automatically injects `DATABASE_URL` as an environment variable. Your server will detect it and use PostgreSQL.

To verify:
1. Go to your Sunloc Server service
2. Click **"Variables"** tab
3. You should see `DATABASE_URL` listed (starts with `postgresql://`)

### 3. Update Your Server

The updated `server.js` and `package.json` now support both SQLite and PostgreSQL:

- **If `DATABASE_URL` exists**: Uses PostgreSQL (Railway)
- **If no `DATABASE_URL`**: Falls back to SQLite locally

No code changes needed! Just redeploy.

### 4. Redeploy to Railway

```bash
railway up --service 5faf9761-02d1-4d91-8e7c-040866b67539
```

Or push to your connected Git repository (Railway will auto-deploy).

### 5. Check Logs

After deploy completes:
```bash
railway logs --service 5faf9761-02d1-4d91-8e7c-040866b67539
```

You should see:
```
🐘 Using PostgreSQL (DATABASE_URL detected)
✅ PostgreSQL connected
✅ PostgreSQL schema created
✅ Sunloc Server running on port 8080
```

---

## What's Included

✅ **PostgreSQL Driver**: `pg` npm package added
✅ **Automatic Detection**: Code detects `DATABASE_URL` and switches databases
✅ **Schema Parity**: Identical schemas for SQLite and PostgreSQL
✅ **Zero Downtime**: Existing in-memory data migrates automatically
✅ **Local Testing**: Still works with SQLite for local development

---

## Local Development (SQLite)

```bash
npm install
docker compose up -d
```

Uses SQLite with persistent storage at `/data/sunloc.db`

---

## Production (Railway + PostgreSQL)

1. Create PostgreSQL plugin in Railway dashboard
2. Push code or run `railway up`
3. Server automatically switches to PostgreSQL via `DATABASE_URL`

---

## Troubleshooting

### PostgreSQL connection fails
- Check that PostgreSQL plugin is created in Railway project
- Verify `DATABASE_URL` exists in Variables tab
- Check logs: `railway logs --service [SERVICE_ID]`

### Still using in-memory database
- Ensure PostgreSQL is provisioned in Railway
- Redeploy: `railway up`
- Check if `DATABASE_URL` env var is set

### Want to delete all data and restart
```bash
# SSH into Railway service
railway shell
# Restart application (clears in-memory or resets PostgreSQL connection)
```

---

## Database Credentials

Railway PostgreSQL provides:
- **Username**: auto-generated
- **Password**: auto-generated
- **Host**: auto-assigned (internal Railway network)
- **Port**: 5432
- **Database**: `railway` (default)

All bundled in `DATABASE_URL`. No manual configuration needed.

---

## Monitoring Database

Access PostgreSQL from your local machine:

```bash
# Get DATABASE_URL
railway run env | grep DATABASE_URL

# Connect with psql (if installed)
psql "your-database-url-here"

# Inside psql:
\dt  # List all tables
SELECT * FROM app_users LIMIT 5;  # Check users
```

---

## Persistent Storage

✅ **PostgreSQL is persistent**: Data survives container restarts
✅ **Railway manages backups**: Included in your plan
✅ **Data isolation**: Each project gets its own database

Your data is safe!

Let me know if you hit any issues with the Railway PostgreSQL setup.
