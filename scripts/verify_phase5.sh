#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

docker compose exec -T jupyter /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 2g \
  --conf spark.driver.host=jupyter \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.shuffle.partitions=16 \
  /workspace/src/verify_phase5.py
