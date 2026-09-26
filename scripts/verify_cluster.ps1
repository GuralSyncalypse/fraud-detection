$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host "[verify] Container status"
docker compose ps
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "[verify] HDFS live DataNodes"
$Report = docker compose exec -T namenode hdfs dfsadmin -report
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$Report
if (($Report | Select-String -SimpleMatch "Live datanodes (2)").Count -eq 0) {
    throw "Expected 2 live HDFS DataNodes."
}

Write-Host "[verify] Running a distributed Spark -> HDFS smoke test"
docker compose exec -T jupyter /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.driver.host=jupyter `
    --conf spark.driver.bindAddress=0.0.0.0 `
    /workspace/scripts/smoke_test.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "[verify] Phase 1 verification passed"

