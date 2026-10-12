# $ErrorActionPreference = "Stop"

# $bucket = "ad-fraud-data"
# $prefix = "predictions/year=2026/month=10/day=11/"
# $folder = ".\s3-duplicate-check"

# New-Item -ItemType Directory -Force -Path $folder | Out-Null

# Write-Host "Fetching objects from LocalStack S3..." -ForegroundColor Cyan
# $objects = aws --endpoint-url http://localhost:4566 s3api list-objects-v2 `
#     --bucket $bucket `
#     --prefix $prefix `
#     --output json | ConvertFrom-Json

# if (-not $objects.Contents) {
#     throw "No S3 prediction objects found for the selected date."
# }

# $latest =$objects.Contents |
#     Sort-Object LastModified -Descending |
#     Select-Object -First 100

# Write-Host "Found $($latest.Count) objects. Downloading and processing..." -ForegroundColor Green

# $counter = 0
# $allRecords = foreach ($obj in $latest) {
#     $counter++
#     Write-Host "[$counter / $($latest.Count)] Downloading: $($obj.Key)" -ForegroundColor Yellow
    
#     $localFile = Join-Path $folder ([IO.Path]::GetFileName($obj.Key))

#     aws --endpoint-url http://localhost:4566 s3 cp `
#         "s3://$bucket/$($obj.Key)" $localFile --only-show-errors

#     if ($LASTEXITCODE -ne 0) {
#         Write-Warning "Failed to download $($obj.Key)"
#         continue
#     }

#     Write-Host "  -> Processing JSON..." -ForegroundColor DarkGray
#     Get-Content $localFile | ForEach-Object {
#         if ($_ -and $_.Trim()) {
#             try {
#                 $record = $_ | ConvertFrom-Json -ErrorAction Stop
#                 [PSCustomObject]@{
#                     event_id = $record.event_id
#                     timestamp = $record.timestamp
#                     prediction = $record.prediction
#                     probability = $record.fraud_probability
#                     file = $obj.Key
#                 }
#             } catch {
#                 Write-Warning "Skipping malformed JSON in $($obj.Key)"
#             }
#         }
#     }
# }

# # Rest of your reporting code...
# $validRecords = @($allRecords | Where-Object { $_.event_id })

# "Objects checked: $($latest.Count)"
# "Records checked: $($validRecords.Count)"
# "Unique event IDs: $(($validRecords.event_id | Sort-Object -Unique).Count)"

# $duplicates = @(
#     $validRecords |
#         Group-Object event_id |
#         Where-Object Count -gt 1
# )

# "Duplicate event IDs: $($duplicates.Count)"

# $duplicates |
#     Select-Object Count, Name |
#     Format-Table -AutoSize

# if ($duplicates.Count -gt 0) {
#     "`nExample duplicate records:"
#     $duplicateIds = @($duplicates | Select-Object -First 5 -ExpandProperty Name)
#     $validRecords |
#         Where-Object { $_.event_id -in $duplicateIds } |
#         Select-Object event_id, timestamp, prediction, probability, file |
#         Format-Table -AutoSize
# }


$records = @(
    Get-ChildItem .\s3-duplicate-check -Filter *.jsonl |
        ForEach-Object {
            Get-Content $_.FullName |
                Where-Object { $_.Trim() } |
                ForEach-Object {
                    try {
                        $_ | ConvertFrom-Json -ErrorAction Stop
                    } catch {}
                }
        }
)

$valid = @(
    $records | Where-Object {
        $null -ne $_.fraud_probability -and
        $null -ne $_.prediction
    }
)

"Valid prediction records: $($valid.Count)"
"Fraud predictions: $( @($valid | Where-Object { [int]$_.prediction -eq 1 }).Count )"
"Not-fraud predictions: $( @($valid | Where-Object { [int]$_.prediction -eq 0 }).Count )"

if ($valid.Count -gt 0) {
    $probabilities = @(
        $valid | ForEach-Object { [double]$_.fraud_probability }
    )

    "Average fraud probability: $([math]::Round(($probabilities | Measure-Object -Average).Average, 6))"
    "Maximum fraud probability: $([math]::Round(($probabilities | Measure-Object -Maximum).Maximum, 6))"

    "`nHighest-risk predictions:"
    $valid |
        Sort-Object { [double]$_.fraud_probability } -Descending |
        Select-Object -First 10 event_id, timestamp, fraud_probability, prediction |
        Format-Table -AutoSize
}
