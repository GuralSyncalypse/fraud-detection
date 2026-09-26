#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

echo "[verify] Container status"
docker compose ps

echo "[verify] HDFS live DataNodes"
report="$(docker compose exec -T namenode hdfs dfsadmin -report)"
printf '%s\n' "${report}"
printf '%s\n' "${report}" | grep -q "Live datanodes (2)"

echo "[verify] Running a distributed Spark -> HDFS smoke test"
docker compose exec -T jupyter /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=jupyter \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /workspace/scripts/smoke_test.py

echo "[verify] Phase 1 verification passed"

