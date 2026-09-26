#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

echo "[hdfs-init] Creating project directories..."
docker compose exec -T namenode hdfs dfs -mkdir -p \
  /financial/raw \
  /financial/processed \
  /financial/predictions \
  /financial/models \
  /financial/_health

docker compose exec -T namenode hdfs dfs -chmod -R 775 /financial
docker compose exec -T namenode hdfs dfs -ls /financial

