#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

test -f data/sample/new_transactions.csv
docker compose exec -T namenode hdfs dfs -mkdir -p /financial/raw
docker compose exec -T namenode hdfs dfs -put -f \
  /workspace/data/sample/new_transactions.csv \
  /financial/raw/new_transactions.csv

docker compose exec -T jupyter /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 2g \
  --conf spark.driver.host=jupyter \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.shuffle.partitions=16 \
  /workspace/src/predict.py "$@"
