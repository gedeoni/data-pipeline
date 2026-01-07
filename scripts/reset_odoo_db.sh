#!/usr/bin/env bash
set -euo pipefail

echo "Stopping Odoo..."
docker compose stop odoo

echo "Dropping and recreating Odoo DB..."
psql -U postgres -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'odoo' AND pid <> pg_backend_pid();"
psql -U postgres -d postgres -c "DROP DATABASE odoo;"
psql -U postgres -d postgres -c "CREATE DATABASE odoo;"

echo "Starting Odoo..."
docker compose start odoo

echo "Initializing base module..."
docker compose exec -T odoo odoo \
  --stop-after-init \
  -d odoo \
  -i base \
  --db_host=host.docker.internal \
  --db_user=odoo \
  --db_password=odoo

echo "Restarting Odoo..."
docker compose restart odoo

echo "Done."
