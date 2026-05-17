#!/bin/bash
set -e

# Create per-service application databases on the shared warehouse_db instance,
# mirroring 03-extra-dbs.sh. Runs only on first boot of an empty data volume.
for db in openwebui litellm; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "SELECT 1 FROM pg_database WHERE datname = '$db'" | grep -q 1 || \
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "CREATE DATABASE $db;"
done

# OpenWebUI uses pgvector for RAG inside its own database.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname openwebui \
  -c "CREATE EXTENSION IF NOT EXISTS vector;"
