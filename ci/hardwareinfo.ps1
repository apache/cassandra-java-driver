# Local System Information v3
# Shows details of currently running PC
# Thom McKiernan 11/09/2014
# Taken from: https://community.spiceworks.com/scripts/show/1831-get-computer-system-and-hardware-information
$computerSystem = Get-CimInstance CIM_ComputerSystem
$computerOS = Get-CimInstance CIM_OperatingSystem
$computerCPU = Get-CimInstance CIM_Processor
$computerHDD = Get-CimInstance Win32_LogicalDisk -Filter "DeviceID = 'C:'"
$javaVersion =  & java.exe -version 2>&1
$java8Version = & 'C:\Program Files\Java\jdk1.8.0\bin\java.exe' -version 2>&1

Write-Host "System Information for: " $computerSystem.Name -BackgroundColor DarkCyan
"CPU: " + $computerCPU.Name
"HDD Capacity: "  + "{0:N2}" -f ($computerHDD.Size/1GB) + "GB"
"HDD Space: " + "{0:P2}" -f ($computerHDD.FreeSpace/$computerHDD.Size) + " Free (" + "{0:N2}" -f ($computerHDD.FreeSpace/1GB) + "GB)"
"RAM: " + "{0:N2}" -f ($computerSystem.TotalPhysicalMemory/1GB) + "GB"
"Operating System: " + $computerOS.caption + ", Service Pack: " + $computerOS.ServicePackMajorVersion

Write-Host "Java Version (for build): " $javaVersion
Write-Host "Java Version (for cassandra): " $java8Version
