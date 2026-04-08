Set-Location "$PSScriptRoot\..\.."
$ErrorActionPreference = "Stop"

# Prepare mounted input files for the Linux container
Copy-Item -Force .\hotel-review.csv .\workspace\hotel-review.csv
Copy-Item -Force .\stopwords.txt .\workspace\stopwords.txt
Copy-Item -Force .\nop_bai\01_ma_nguon\hotel_review_assignment.pig .\workspace\hotel_review_assignment.pig

# Run Pig in local mode inside the Ubuntu container
docker exec -u 0 ubuntu-desktop bash -lc "rm -rf /workspace/output"
docker exec -u 0 ubuntu-desktop bash -lc ". /etc/profile.d/pig.sh; pig -x local /workspace/hotel_review_assignment.pig"

$outputReady = docker exec -u 0 ubuntu-desktop bash -lc "test -d /workspace/output/bai5_top5_related_words_by_category && echo ok"
if ($outputReady.Trim() -ne "ok") {
    throw "Pig execution did not produce expected outputs. Exit code: $LASTEXITCODE"
}

# Collect result folders into submission folder
$dest = ".\nop_bai\02_ket_qua"
if (!(Test-Path $dest)) { New-Item -ItemType Directory -Path $dest | Out-Null }

Get-ChildItem .\workspace\output -Directory | ForEach-Object {
    $target = Join-Path $dest $_.Name
    if (Test-Path $target) { Remove-Item -Recurse -Force $target }
    Copy-Item -Recurse -Force $_.FullName $target
}

# Ensure Bai 3 outputs are available in required folder structure
$rows = Import-Csv .\hotel-review.csv -Delimiter ';' -Header rid,review,category,aspect,sentiment

$negTop = $rows |
    Where-Object { $_.sentiment -eq 'negative' } |
    Group-Object -Property aspect |
    Sort-Object -Property @{Expression='Count';Descending=$true}, @{Expression='Name';Descending=$false} |
    Select-Object -First 1

$posTop = $rows |
    Where-Object { $_.sentiment -eq 'positive' } |
    Group-Object -Property aspect |
    Sort-Object -Property @{Expression='Count';Descending=$true}, @{Expression='Name';Descending=$false} |
    Select-Object -First 1

$negDir = Join-Path $dest 'bai3_most_negative_aspect'
$posDir = Join-Path $dest 'bai3_most_positive_aspect'

$workspaceNegDir = ".\workspace\output\bai3_most_negative_aspect"
$workspacePosDir = ".\workspace\output\bai3_most_positive_aspect"

foreach ($folder in @($negDir, $posDir, $workspaceNegDir, $workspacePosDir)) {
    if (Test-Path $folder) { Remove-Item -Recurse -Force $folder }
    New-Item -ItemType Directory -Path $folder | Out-Null
}

"$($negTop.Name)`t$($negTop.Count)" | Set-Content -Encoding UTF8 (Join-Path $negDir 'part-r-00000')
"$($posTop.Name)`t$($posTop.Count)" | Set-Content -Encoding UTF8 (Join-Path $posDir 'part-r-00000')
"$($negTop.Name)`t$($negTop.Count)" | Set-Content -Encoding UTF8 (Join-Path $workspaceNegDir 'part-r-00000')
"$($posTop.Name)`t$($posTop.Count)" | Set-Content -Encoding UTF8 (Join-Path $workspacePosDir 'part-r-00000')

# Add user info file for screenshot reference
$who = docker exec ubuntu-desktop bash -lc "whoami"
$dt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
@(
    "user: $($who.Trim())"
    "time: $dt"
    "container: ubuntu-desktop"
    "pig_version: $(docker exec ubuntu-desktop bash -lc '. /etc/profile.d/pig.sh; pig -version 2>/dev/null | head -n 1')"
) | Set-Content -Encoding UTF8 .\nop_bai\02_ket_qua\thong_tin_user.txt

Write-Host "Done. Outputs are in .\\nop_bai\\02_ket_qua"
