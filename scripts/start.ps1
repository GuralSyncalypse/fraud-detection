$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

docker compose up -d --build --wait
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& "$PSScriptRoot/init_hdfs.ps1"
& "$PSScriptRoot/verify_cluster.ps1"

Write-Host "NameNode UI: http://localhost:9870"
Write-Host "Spark UI:    http://localhost:8080"
Write-Host "Jupyter:     http://localhost:8888 (default token: fraud-demo)"

