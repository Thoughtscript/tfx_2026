#!/bin/bash
set -euo pipefail

echo "Starting TensorFlow Serving: model=${MODEL_NAME} path=${MODEL_BASE_PATH} grpc_port=8500 rest_port=8501"
exec /usr/bin/tensorflow_model_server \
  --port=8500 \
  --rest_api_port=8501 \
  --model_name="${MODEL_NAME}" \
  --model_base_path="${MODEL_BASE_PATH}" \
  --enable_model_warmup=true \
  --alsologtostderr
