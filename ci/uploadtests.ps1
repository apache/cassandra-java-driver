$testResults=Get-ChildItem TEST-TestSuite.xml -Recurse

Write-Host "Uploading test results."

$url = "https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)"
$wc = New-Object 'System.Net.WebClient'

foreach ($testResult in $testResults) {
  try {
    Write-Host -ForegroundColor Green "Uploading $testResult -> $url."
    $wc.UploadFile($url, $testResult)
  } catch [Net.WebException] {
    Write-Host -ForegroundColor Red "Failed Uploading $testResult -> $url.  $_"
  }
}

Write-Host "Done uploading test results."
