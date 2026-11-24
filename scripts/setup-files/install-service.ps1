#Requires -RunAsAdministrator
<#
.SYNOPSIS
  Installs NexusMQ as a Windows service: starts with Windows, shows in services.msc, restarted automatically if it fails.
.EXAMPLE
  .\install-service.ps1
  .\install-service.ps1 -Urls http://0.0.0.0:5680 -DataDirectory D:\NexusMQData
#>
param(
    [string]$Urls = "http://localhost:5680",
    [string]$DataDirectory = (Join-Path $env:ProgramData "NexusMQ\data"),
    [string]$ServiceName = "NexusMQ"
)
$ErrorActionPreference = "Stop"
$server = Join-Path $PSScriptRoot "server"
$exe = Join-Path $server "NexusMQ.Server.exe"
$arguments = "--windows-service --urls $Urls --NexusMQ:DataDirectory=`"$DataDirectory`""
if (Test-Path $exe) {
    $binPath = "`"$exe`" $arguments"
} else {
    $dotnet = (Get-Command dotnet).Source   # portable setup: needs the .NET 10 runtime
    $binPath = "`"$dotnet`" `"$(Join-Path $server 'NexusMQ.Server.dll')`" $arguments"
}
if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    throw "A service named '$ServiceName' already exists. Remove it first with .\uninstall-service.ps1"
}
New-Item -ItemType Directory -Force -Path $DataDirectory | Out-Null
New-Service -Name $ServiceName -BinaryPathName $binPath -DisplayName "NexusMQ message broker" `
    -Description "NexusMQ: queues, topics and exchanges. Dashboard: $Urls" -StartupType Automatic | Out-Null
# Restart after 5 s on the first two failures and after 1 min on later ones; reset the count after a day.
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/60000 | Out-Null
Start-Service -Name $ServiceName
Write-Host "NexusMQ is installed as the Windows service '$ServiceName' and is running."
Write-Host "  Dashboard: $Urls    Data: $DataDirectory"
Write-Host "  Manage it in services.msc, or: Stop-Service $ServiceName / Start-Service $ServiceName"
Write-Host "  Remove it with .\uninstall-service.ps1"
