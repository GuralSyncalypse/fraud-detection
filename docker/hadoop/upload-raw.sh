#!/usr/bin/env bash
set -euo pipefail

raw_dir="/workspace/data/raw"
staging_dir="/workspace/data/staging"
hdfs_dir="/financial/raw"

upload_if_changed() {
  local source_path="$1"
  local destination_path="$2"
  local local_size
  local remote_size

  if [[ ! -f "${source_path}" ]]; then
    echo "[upload] Missing required file: ${source_path}" >&2
    return 1
  fi

  local_size="$(stat -c '%s' "${source_path}")"
  remote_size=""
  if hdfs dfs -test -e "${destination_path}"; then
    remote_size="$(hdfs dfs -stat '%b' "${destination_path}")"
  fi

  if [[ "${local_size}" == "${remote_size}" ]]; then
    echo "[upload] Unchanged, skipping: ${destination_path} (${local_size} bytes)"
    return 0
  fi

  echo "[upload] ${source_path} -> ${destination_path} (${local_size} bytes)"
  hdfs dfs -put -f "${source_path}" "${destination_path}"
}

hdfs dfs -mkdir -p "${hdfs_dir}"

# Only transaction data, labels and the non-sensitive MCC lookup are uploaded.
# Card numbers/CVV and user-address files are deliberately excluded.
upload_if_changed "${raw_dir}/transactions_data.csv" "${hdfs_dir}/transactions_data.csv"
upload_if_changed "${raw_dir}/train_fraud_labels.json" "${hdfs_dir}/train_fraud_labels.json"
upload_if_changed "${staging_dir}/fraud_labels.csv" "${hdfs_dir}/fraud_labels.csv"
upload_if_changed "${raw_dir}/mcc_codes.json" "${hdfs_dir}/mcc_codes.json"

echo "[upload] HDFS raw files:"
hdfs dfs -ls -h "${hdfs_dir}"

