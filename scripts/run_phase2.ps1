$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

docker compose exec -T jupyter /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --driver-memory 2g `
    --conf spark.driver.host=jupyter `
    --conf spark.driver.bindAddress=0.0.0.0 `
    --conf spark.sql.shuffle.partitions=16 `
    /workspace/src/preprocessing.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

