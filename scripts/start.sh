#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

docker compose up -d --build --wait
"${project_root}/scripts/init_hdfs.sh"
"${project_root}/scripts/verify_cluster.sh"

echo "NameNode UI: http://localhost:9870"
echo "Spark UI:    http://localhost:8080"
echo "Jupyter:     http://localhost:8888 (default token: fraud-demo)"

