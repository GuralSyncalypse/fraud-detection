#!/usr/bin/env bash
set -euo pipefail

echo "[datanode] Starting DataNode and registering with namenode:8020"
exec hdfs datanode

