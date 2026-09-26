$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

# Named volumes are intentionally preserved. Add -v only when intentionally
# resetting every HDFS file and the NameNode namespace.
docker compose down
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

