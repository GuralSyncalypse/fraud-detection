$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host "[phase2] Normalizing the one-line fraud label JSON..."
docker compose exec -T jupyter python3 /workspace/scripts/normalize_labels.py `
    --input /workspace/data/raw/train_fraud_labels.json `
    --output /workspace/data/staging/fraud_labels.csv
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "[phase2] Uploading selected raw files to HDFS..."
docker compose exec -T namenode /opt/project/upload-raw.sh
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

