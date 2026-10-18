#!/bin/sh
set -eu

echo "Running Beam container startup unit tests."
python -m unittest discover -s /app/tests -v
echo "Beam startup unit tests passed; starting external worker pool."
exec python -m apache_beam.runners.worker.worker_pool_main --service_port=50000
