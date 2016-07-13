Add-Type -AssemblyName System.IO.Compression.FileSystem

$dep_dir="C:\Users\appveyor\deps"
If (!(Test-Path $dep_dir)) {
  Write-Host "Creating $($dep_dir)"
  New-Item -Path $dep_dir -ItemType Directory -Force
}

$apr_platform = "Win32"
$openssl_platform = "Win32"
$vc_platform = "x86"
$env:PYTHON="C:\Python27"
$env:OPENSSL_PATH="C:\OpenSSL-Win32"
If ($env:PLATFORM -eq "X64") {
  $apr_platform = "x64"
  $vc_platform = "x64"
  $env:PYTHON="C:\Python27-x64"
  $env:OPENSSL_PATH="C:\OpenSSL-Win64"
}

$env:JAVA_HOME="C:\Program Files\Java\jdk$($env:java_version)"
$env:JAVA_8_HOME="C:\Program Files\Java\jdk1.8.0"
$env:PATH="$($env:PYTHON);$($env:PYTHON)\Scripts;$($env:JAVA_HOME)\bin;$($env:OPENSSL_PATH)\bin;$($env:PATH)"
$env:CCM_PATH="$($dep_dir)\ccm"

$apr_dist_path = "$($dep_dir)\apr"
# Build APR if it hasn't been previously built.
If (!(Test-Path $apr_dist_path)) {
  Write-Host "Cloning APR"
  $apr_path = "C:\Users\appveyor\apr"
  Start-Process git -ArgumentList "clone --branch=1.5.2 --depth=1 https://github.com/apache/apr.git $($apr_path)" -Wait -nnw
  Write-Host "Setting Visual Studio Environment to VS 2015"
  Push-Location "$($env:VS140COMNTOOLS)\..\..\VC"
  cmd /c "vcvarsall.bat $vc_platform & set" |
  foreach {
    if ($_ -match "=") {
      $v = $_.split("="); Set-Item -force -path "ENV:\$($v[0])"  -value "$($v[1])"
    }
  }
  Pop-Location
  Write-Host "Building APR (an error may be printed, but it will still build)"
  Push-Location $($apr_path)
  cmd /c nmake -f Makefile.win ARCH="$apr_platform Release" PREFIX=$($apr_dist_path) buildall install
  Pop-Location
  Write-Host "Done Building APR"
}
$env:PATH="$($apr_dist_path)\bin;$($env:PATH)"

# Install Ant and Maven
$ant_base = "$($dep_dir)\ant"
$ant_path = "$($ant_base)\apache-ant-1.9.7"
If (!(Test-Path $ant_path)) {
  Write-Host "Installing Ant"
  $ant_url = "https://www.dropbox.com/s/lgx95x1jr6s787l/apache-ant-1.9.7-bin.zip?dl=1"
  $ant_zip = "C:\Users\appveyor\apache-ant-1.9.7-bin.zip"
  (new-object System.Net.WebClient).DownloadFile($ant_url, $ant_zip)
  [System.IO.Compression.ZipFile]::ExtractToDirectory($ant_zip, $ant_base)
}
$env:PATH="$($ant_path)\bin;$($env:PATH)"

$maven_base = "$($dep_dir)\maven"
$maven_path = "$($maven_base)\apache-maven-3.2.5"
If (!(Test-Path $maven_path)) {
  Write-Host "Installing Maven"
  $maven_url = "https://www.dropbox.com/s/fh9kffmexprsmha/apache-maven-3.2.5-bin.zip?dl=1"
  $maven_zip = "C:\Users\appveyor\apache-maven-3.2.5-bin.zip"
  (new-object System.Net.WebClient).DownloadFile($maven_url, $maven_zip)
  [System.IO.Compression.ZipFile]::ExtractToDirectory($maven_zip, $maven_base)
}
$env:PATH="$($maven_path)\bin;$($env:PATH)"

$jdks = @("1.6.0", "1.7.0", "1.8.0")
foreach ($jdk in $jdks) {
  $java_dir = "C:\Program Files\Java\jdk$jdk"
  $jce_target = "$java_dir\jre\lib\security"
  $jce_indicator = "$jce_target\README.txt"
  # Install Java Cryptographic Extensions, needed for SSL.
  # If this file doesn't exist we know JCE hasn't been installed.
  If (!(Test-Path $jce_indicator)) {
    Write-Host "Installing JCE for $jdk"
    $zip = "$dep_dir\jce_policy-$jdk.zip"
    $url = "https://www.dropbox.com/s/po4308hlwulpvep/UnlimitedJCEPolicyJDK7.zip?dl=1"
    $extract_folder = "UnlimitedJCEPolicy"
    If ($jdk -eq "1.8.0") {
      $url = "https://www.dropbox.com/s/al1e6e92cjdv7m7/jce_policy-8.zip?dl=1"
      $extract_folder = "UnlimitedJCEPolicyJDK8"
    }
    ElseIf ($jdk -eq "1.6.0") {
      $url = "https://www.dropbox.com/s/dhrtucxcif4n11k/jce_policy-6.zip?dl=1"
      $extract_folder = "jce"
    }
    # Download zip to staging area if it doesn't exist, we do this because
    # we extract it to the directory based on the platform and we want to cache
    # this file so it can apply to all platforms.
    if(!(Test-Path $zip)) {
      (new-object System.Net.WebClient).DownloadFile($url, $zip)
    }

    [System.IO.Compression.ZipFile]::ExtractToDirectory($zip, $jce_target)

    $jcePolicyDir = "$jce_target\$extract_folder"
    Move-Item $jcePolicyDir\* $jce_target\ -force
    Remove-Item $jcePolicyDir
  }
}

# Install Python Dependencies for CCM.
Write-Host "Installing Python Dependencies for CCM"
Start-Process python -ArgumentList "-m pip install psutil pyYaml six" -Wait -nnw

# Clone ccm from git and use master.
If (!(Test-Path $env:CCM_PATH)) {
  Write-Host "Cloning CCM"
  Start-Process git -ArgumentList "clone https://github.com/pcmanus/ccm.git $($env:CCM_PATH)" -Wait -nnw
}

# Copy ccm -> ccm.py so windows knows to run it.
If (!(Test-Path $env:CCM_PATH\ccm.py)) {
  Copy-Item "$env:CCM_PATH\ccm" "$env:CCM_PATH\ccm.py"
}
$env:PYTHONPATH="$($env:CCM_PATH);$($env:PYTHONPATH)"
$env:PATH="$($env:CCM_PATH);$($env:PATH)"

# Predownload cassandra version for CCM if it isn't already downloaded.
If (!(Test-Path C:\Users\appveyor\.ccm\repository\$env:cassandra_version)) {
  Write-Host "Preinstalling C* $($env:cassandra_version)"
  Start-Process python -ArgumentList "$($env:CCM_PATH)\ccm.py create -v $($env:cassandra_version) -n 1 predownload" -Wait -nnw
  Start-Process python -ArgumentList "$($env:CCM_PATH)\ccm.py remove predownload" -Wait -nnw
}
