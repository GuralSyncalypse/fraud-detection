$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

if (-not (Test-Path -LiteralPath ".\data\sample\new_transactions.csv")) {
    throw "Missing data/sample/new_transactions.csv"
}

docker compose exec -T namenode hdfs dfs -mkdir -p /financial/raw
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose exec -T namenode hdfs dfs -put -f /workspace/data/sample/new_transactions.csv /financial/raw/new_transactions.csv
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose exec -T jupyter /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --driver-memory 2g `
    --conf spark.driver.host=jupyter `
    --conf spark.driver.bindAddress=0.0.0.0 `
    --conf spark.sql.shuffle.partitions=16 `
    /workspace/src/predict.py @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
