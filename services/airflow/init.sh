#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

if airflow users list --output json | python -c \
  'import json, sys; username = sys.argv[1]; sys.exit(0 if any(user["username"] == username for user in json.load(sys.stdin)) else 1)' \
  "$AIRFLOW_ADMIN_USERNAME"; then
  echo "Airflow admin user already exists."
else
  airflow users create \
    --username "$AIRFLOW_ADMIN_USERNAME" \
    --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email "$AIRFLOW_ADMIN_EMAIL"
fi
