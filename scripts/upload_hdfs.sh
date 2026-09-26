#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

echo "[phase2] Normalizing the one-line fraud label JSON..."
docker compose exec -T jupyter python3 /workspace/scripts/normalize_labels.py \
  --input /workspace/data/raw/train_fraud_labels.json \
  --output /workspace/data/staging/fraud_labels.csv

echo "[phase2] Uploading selected raw files to HDFS..."
docker compose exec -T namenode /opt/project/upload-raw.sh

