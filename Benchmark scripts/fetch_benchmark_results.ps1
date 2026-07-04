$ErrorActionPreference = "Stop"

$soloRoot = "C:\Users\BAKU\Desktop\University\Projects\Cloud Computing Solo"
$localResults = Join-Path $soloRoot "results\benchmark"
New-Item -ItemType Directory -Force -Path $localResults | Out-Null

Push-Location $localResults
try {
    pscp -pw ubuntu -r root@10.1.1.123:/home/hadoop/single_project/results/benchmark/* .
}
finally {
    Pop-Location
}

Write-Host "Benchmark results copied to: $localResults"
