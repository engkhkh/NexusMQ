#Requires -RunAsAdministrator
<# Stops and removes the NexusMQ Windows service. The data folder is kept. #>
param([string]$ServiceName = "NexusMQ")
$ErrorActionPreference = "Stop"
$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if (-not $service) { Write-Host "There is no service '$ServiceName'."; return }
if ($service.Status -ne "Stopped") { Stop-Service -Name $ServiceName -Force }
sc.exe delete $ServiceName | Out-Null
Write-Host "Removed the service '$ServiceName' (its data folder is kept)."
