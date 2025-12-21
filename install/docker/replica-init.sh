#!/bin/bash

set -euo pipefail
LOGFILE="/data/db/debug_replica_init.log"

function log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOGFILE"
}

log "[DEBUG] Checking running mongod processes before starting..."
ps aux | grep mongod >> "$LOGFILE"

log "[DEBUG] Killing existing mongod processes (if any)..."
pkill -f mongod || true

log "[DEBUG] Checking running mongod processes after pkill:"
ps aux | grep mongod >> "$LOGFILE"

log "[DEBUG] Ensuring all MongoDB processes are stopped..."
while pgrep -f mongod >/dev/null; do
  log "[DEBUG] Waiting for MongoDB processes to terminate..."
  sleep 1
done

log "[DEBUG] Starting MongoDB in noauth mode for user creation..."
mongod --noauth --dbpath /data/db --bind_ip_all --logpath /data/db/mongod.log &

log "[DEBUG] Waiting for MongoDB to become available..."
while ! nc -z 127.0.0.1 27017; do
  log "[DEBUG] Still waiting for MongoDB..."
  sleep 1
done
log "[DEBUG] MongoDB is available. Proceeding to user creation."

log "[DEBUG] Creating user with mongo_setup.js..."
mongosh < /app/install/mongo_setup.js >> "$LOGFILE" 2>&1

log "[DEBUG] Stopping MongoDB after user creation..."
pkill -f mongod || true

log "[DEBUG] Checking running mongod processes after pkill:"
ps aux | grep mongod >> "$LOGFILE"

log "[DEBUG] Ensuring all MongoDB processes are stopped..."
while pgrep -f mongod >/dev/null; do
  log "[DEBUG] Waiting for MongoDB processes to terminate..."
  sleep 1
done

log "[DEBUG] Enabling authorization in MongoDB configuration."
cp /app/install/docker/mongod.conf /etc/mongod.conf

log "[DEBUG] Restarting MongoDB with authorization enabled..."
mongod --bind_ip_all --config /etc/mongod.conf &

log "[DEBUG] Waiting for MongoDB to restart..."
while ! nc -z 127.0.0.1 27017; do
  log "[DEBUG] MongoDB still not available..."
  sleep 1
done
log "[DEBUG] MongoDB restart complete."