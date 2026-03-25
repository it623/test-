# Sunloc Server - Docker Setup

This project is containerized with Docker using best practices for production deployments.

## Files

- **Dockerfile** - Multi-stage build optimized for Node.js + Express with PostgreSQL or SQLite
- **docker-compose.yml** - Orchestration for the server and (optionally) PostgreSQL database
- **.dockerignore** - Excludes unnecessary files from Docker build context
- **.env.example** - Template for environment variables

## Quick Start

### Development

```bash
# Build and start the containers
docker compose up -d

# View logs
docker logs -f sunloc-server

# Stop containers
docker compose down
```

### Production Deployment

```bash
# Set environment variables for production
export NODE_ENV=production
export DB_PASSWORD=your_secure_password_here

# Build and push to your registry
docker build -t your-registry/sunloc-server:latest .
docker push your-registry/sunloc-server:latest

# Deploy with docker compose
docker compose up -d
```

## Configuration

Copy `.env.example` to `.env` and update values:

```bash
cp .env.example .env
```

Edit `.env`:
- `NODE_ENV` - Set to `production` for production deployments
- `DB_USER` - PostgreSQL username (if using PostgreSQL version)
- `DB_PASSWORD` - PostgreSQL password (change from default!)
- `DB_NAME` - Database name

## Database Support

The docker-compose includes PostgreSQL, but your server.js determines which DB is used:

- **PostgreSQL** - If your code uses `pool.query()` (older production versions)
- **SQLite** - If your code uses `better-sqlite3` (newer development versions)

### Using PostgreSQL
1. Uncomment the `postgres` service in docker-compose.yml
2. Uncomment `depends_on` in the sunloc-server service
3. Set `DB_PASSWORD` in `.env`

### Using SQLite
1. Comment out or remove the `postgres` service
2. Remove or comment out `depends_on`
3. The database will be stored in the `sunloc-data` volume at `/data/sunloc.db`

## Docker Best Practices Applied

✅ **Multi-stage builds** - Reduces final image size by excluding build dependencies
✅ **Non-root user** - Container runs as `appuser` (uid: 1001) for security
✅ **Health checks** - Automatic detection of container health status
✅ **Signal handling** - Uses dumb-init for proper PID 1 signal propagation
✅ **Security scanning** - Alpine base image reduces attack surface
✅ **Volume management** - Persistent data with named volumes
✅ **Resource limits** - Optional CPU/memory constraints (commented out, uncomment for prod)

## Troubleshooting

### Container exits immediately
```bash
docker logs sunloc-server
```

### Database connection fails
- Verify PostgreSQL is healthy: `docker ps` (look for "healthy" status)
- Check credentials in `.env` match docker-compose.yml
- Ensure DB_HOST is set to `postgres` (service name)

### Port already in use
Change port mapping in docker-compose.yml:
```yaml
ports:
  - "3001:3000"  # Map to 3001 instead of 3000
```

### View running containers
```bash
docker compose ps
```

## Performance Notes

- First build takes ~30-40s (dependencies compiled)
- Subsequent builds use cache layers (~3-5s)
- Health checks start after 15s to allow database initialization
- The app listens on port 3000 by default

## Production Checklist

- [ ] Set `NODE_ENV=production` in `.env`
- [ ] Change `DB_PASSWORD` to a strong, random value
- [ ] Uncomment resource limits in docker-compose.yml
- [ ] Use a production-grade database backup solution for PostgreSQL data
- [ ] Set up log aggregation (Docker logs to external service)
- [ ] Enable Docker's restart policies: `restart: unless-stopped`
- [ ] Use a reverse proxy (nginx/traefik) in front of the container
