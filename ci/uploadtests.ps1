<#
 # Licensed to the Apache Software Foundation (ASF) under one
 # or more contributor license agreements.  See the NOTICE file
 # distributed with this work for additional information
 # regarding copyright ownership.  The ASF licenses this file
 # to you under the Apache License, Version 2.0 (the
 # "License"); you may not use this file except in compliance
 # with the License.  You may obtain a copy of the License at
 # 
 #   http://www.apache.org/licenses/LICENSE-2.0
 # 
 # Unless required by applicable law or agreed to in writing,
 # software distributed under the License is distributed on an
 # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 # KIND, either express or implied.  See the License for the
 # specific language governing permissions and limitations
 # under the License.
 #>

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
