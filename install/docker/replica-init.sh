#!/bin/bash

set -euo pipefail

echo -e "[INFO] Starting MongoDB initialization process...\n"

while ! nc -z 127.0.0.1 27017; do
  echo -e "[INFO] Waiting for MongoDB to become available...\n"
  sleep 1
done

if mongosh --quiet --eval "db.getSiblingDB('$MONGO_DB_NAME').getUser('$MONGO_USER')" | grep -q '"user"'; then
  echo -e "[INFO] MongoDB user '$MONGO_USER' already exists. Skipping user creation.\n"
else
  echo -e "[INFO] Executing user creation script from 'mongo_setup.js'...\n"
  mongosh < /app/install/mongo_setup.js
  echo -e "[INFO] MongoDB user '$MONGO_USER' created successfully!\n"
fi

echo -e "[INFO] MongoDB initialization process completed.\n"