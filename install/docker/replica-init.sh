#!/bin/bash

set -euo pipefail
LOGFILE="/data/db/debug_replica_init.log"

function log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOGFILE"
}

# Check for the existence of keyfile and move it to /etc/mongodb/
if [[ -f "/app/install/docker/keyfile" ]]; then
    log "[DEBUG] Found keyfile in /app/install/docker/. Moving to /etc/mongodb/..."
    mkdir -p /etc/mongodb/
    cp /app/install/docker/keyfile /etc/mongodb/keyfile
    chown mongod:mongod /etc/mongodb/keyfile
    chmod 600 /etc/mongodb/keyfile
    log "[DEBUG] Keyfile moved and permissions set successfully."
fi

log "[DEBUG] Checking running mongod processes before starting..."
ps aux | grep mongod >> "$LOGFILE"

log "[DEBUG] Killing existing mongod processes (if any)..."
pkill -f mongod || true

log "[DEBUG] Ensuring all MongoDB processes are stopped..."
while pgrep -f mongod; do
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

log "[DEBUG] Creating users via mongo_setup.js..."
mongosh < /app/install/mongo_setup.js >> "$LOGFILE" 2>&1

log "[DEBUG] Stopping MongoDB after user creation..."
pkill -f mongod || true

log "[DEBUG] Ensuring all MongoDB processes are stopped..."
while pgrep -f mongod; do
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
log "[DEBUG] MongoDB restart complete and available."

# Replication: check/init
if [[ "$MONGO_ROLE" == "PRIMARY" ]]; then
    log "[INFO] Configuring this node as PRIMARY for replica set '${REPLICA_SET_NAME}'..."
    mongosh --eval "
        try {
            rs.initiate();
            print('Replica set initiated successfully.');
        } catch (err) {
            if (err.codeName === 'AlreadyInitialized') {
                print('Replica set already initialized.');
            } else {
                print('Error during replica set initiation: ' + err);
            }
        }
    " >> "$LOGFILE" 2>&1
elif [[ "$MONGO_ROLE" == "SECONDARY" ]]; then
    log "[INFO] Configuring this node as SECONDARY. Connecting to PRIMARY..."
    PRIMARY_HOST=$(echo "$MONGO_HOSTS" | cut -d ',' -f1)

    mongosh --host "$PRIMARY_HOST" --eval "
        try {
            rs.add('${HOSTNAME}:27017');
            print('Node added to replica set as SECONDARY.');
        } catch (err) {
            print('Error during rs.add(): ' + err);
        }
    " >> "$LOGFILE" 2>&1
fi