#!/usr/bin/env bash
set -euo pipefail

docker compose exec postgres \
  psql -U airflow -d airflow -c \
  'SELECT record_id, feature, prediction FROM beam_predictions ORDER BY record_id;'
