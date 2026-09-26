#!/usr/bin/env bash
set -euo pipefail

echo "[jupyter] Spark master: ${SPARK_MASTER_URL:-spark://spark-master:7077}"
echo "[jupyter] HDFS endpoint: ${HDFS_URL:-hdfs://namenode:8020}"

exec python3 -m jupyter lab \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --ServerApp.token="${JUPYTER_TOKEN:-fraud-demo}" \
  --ServerApp.password='' \
  --ServerApp.root_dir=/workspace/notebooks

