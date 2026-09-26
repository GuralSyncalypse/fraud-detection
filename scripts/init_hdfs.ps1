$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host "[hdfs-init] Creating project directories..."
docker compose exec -T namenode hdfs dfs -mkdir -p `
    /financial/raw `
    /financial/processed `
    /financial/predictions `
    /financial/models `
    /financial/_health
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose exec -T namenode hdfs dfs -chmod -R 775 /financial
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose exec -T namenode hdfs dfs -ls /financial
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

