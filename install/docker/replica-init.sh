#!/bin/bash


set -euo pipefail

echo "[INFO] Starting replica initialization script..."

# check on available replica params
if [[ -z "${MONGO_ROLE:-}" ]]; then
  echo "[ERROR] MONGO_ROLE is not set. Skipping replica setup."
  exit 0
fi

if [[ -z "${REPLICA_SET_NAME:-}" ]]; then
  echo "[ERROR] REPLICA_SET_NAME is not set. Skipping replica setup."
  exit 0
fi

if [[ -z "${MONGO_HOSTS:-}" ]]; then
  echo "[ERROR] MONGO_HOSTS is not set. Skipping replica setup."
  exit 0
fi

IFS=',' read -ra HOSTS <<< "$MONGO_HOSTS"
if [[ ${#HOSTS[@]} -lt 2 ]]; then
  echo "[ERROR] MONGO_HOSTS should contain at least two members."
  exit 1
fi

echo "[INFO] Replica Set Name: $REPLICA_SET_NAME"
echo "[INFO] Number of hosts in the replica set: ${#HOSTS[@]}"

# Skip initialization if replica is already set up
if mongo --eval "rs.status()" --quiet; then
  echo "[INFO] Replica set already initialized. Skipping initialization."
  exit 0
fi

# Generate node list for `rs.initiate`
MEMBERS=""
for i in "${!HOSTS[@]}"; do
  MEMBERS+="{ _id: $i, host: '${HOSTS[$i]}' },"
done
MEMBERS="${MEMBERS%,}"

if [[ "$MONGO_ROLE" == "PRIMARY" ]]; then
  echo "[INFO] Configuring as PRIMARY..."
  if ! mongo --eval "
    rs.initiate({
      _id: '$REPLICA_SET_NAME',
      members: [ $MEMBERS ]
    })
  "; then
    echo "[ERROR] Failed to initiate replica set as PRIMARY."
    exit 1
  fi
elif [[ "$MONGO_ROLE" == "SECONDARY" ]]; then
  echo "[INFO] Configuring as SECONDARY. Waiting for PRIMARY to initialize..."
  while ! mongo --eval "db.isMaster().ismaster" --quiet | grep -q 'true'; do
    echo "[INFO] Waiting for PRIMARY to become available..."
    sleep 2
  done
  if ! mongo --eval "
    rs.add('${HOSTS[0]}')
  "; then
    echo "[ERROR] Failed to join the replica set as SECONDARY."
    exit 1
  fi
else
  echo "[ERROR] Invalid MONGO_ROLE: $MONGO_ROLE. Use PRIMARY or SECONDARY."
  exit 1
fi

echo "[INFO] MongoDB initialized with role: $MONGO_ROLE"
echo "[INFO] Replica initialization script completed."