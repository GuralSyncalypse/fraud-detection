#!/usr/bin/env bash
set -euo pipefail

name_dir="/data/name"

if [[ ! -d "${name_dir}/current" ]]; then
  echo "[namenode] Formatting a new HDFS namespace..."
  hdfs namenode -format -force -nonInteractive -clusterId financial-fraud-cluster
fi

echo "[namenode] Starting NameNode on namenode:8020"
exec hdfs namenode

