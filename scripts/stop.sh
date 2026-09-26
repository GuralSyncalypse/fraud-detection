#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

# Named volumes are intentionally preserved. Use `docker compose down -v` only
# when you explicitly want to erase the HDFS namespace and all stored blocks.
docker compose down

