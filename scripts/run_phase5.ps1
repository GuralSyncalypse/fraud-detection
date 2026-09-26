$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

& "$PSScriptRoot\predict_new_transactions.ps1"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose exec -T jupyter /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --driver-memory 2g `
    --conf spark.driver.host=jupyter `
    --conf spark.driver.bindAddress=0.0.0.0 `
    --conf spark.sql.shuffle.partitions=16 `
    /workspace/src/generate_visualizations.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
