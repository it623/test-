# Multi-stage Dockerfile for Sunloc Server (Node.js + Express + SQLite/PostgreSQL)
# Stage 1: Dependencies builder
FROM node:20-alpine AS builder

WORKDIR /tmp/build

# Install build dependencies for native modules
RUN apk add --no-cache python3 make g++

# Copy package files
COPY package.json ./

# Install production dependencies
RUN npm install --omit=dev

# Clean up build tools
RUN apk del python3 make g++

# Stage 2: Runtime
FROM node:20-alpine

# Install dumb-init for proper signal handling
RUN apk add --no-cache dumb-init

# Create app user for non-root execution
RUN addgroup -g 1001 nodejs && \
    adduser -u 1001 -G nodejs -s /bin/sh -D appuser

WORKDIR /app

# Copy production dependencies from builder
COPY --from=builder --chown=appuser:nodejs /tmp/build/node_modules ./node_modules

# Copy application code
COPY --chown=appuser:nodejs server.js ./
COPY --chown=appuser:nodejs sunloc-api-client.js ./
COPY --chown=appuser:nodejs public ./public

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD node -e "require('http').get('http://localhost:3000/api/health', (r) => { if (r.statusCode !== 200) throw new Error(r.statusCode); }).on('error', () => { process.exit(1); })"

# Use dumb-init to handle signals properly
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Start the application
# Railway will override PORT via environment variable
CMD ["node", "server.js"]
