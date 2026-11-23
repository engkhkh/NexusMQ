#Requires -RunAsAdministrator
<#
.SYNOPSIS
  Makes NexusMQ start with Windows (a scheduled task that runs at boot as SYSTEM and restarts it if it stops).
.EXAMPLE
  .\install-startup-task.ps1
  .\install-startup-task.ps1 -Urls http://0.0.0.0:5680 -DataDirectory D:\NexusMQData
#>
param(
    [string]$Urls = "http://localhost:5680",
    [string]$DataDirectory = (Join-Path $env:ProgramData "NexusMQ\data"),
    [string]$TaskName = "NexusMQ"
)
$ErrorActionPreference = "Stop"
$server = Join-Path $PSScriptRoot "server"
$arguments = "--urls $Urls --NexusMQ:DataDirectory=`"$DataDirectory`""
$exe = Join-Path $server "NexusMQ.Server.exe"
if (Test-Path $exe) {
    $action = New-ScheduledTaskAction -Execute $exe -Argument $arguments -WorkingDirectory $server
} else {
    $dotnet = (Get-Command dotnet).Source   # this setup needs the .NET 10 runtime
    $action = New-ScheduledTaskAction -Execute $dotnet -Argument "`"$(Join-Path $server 'NexusMQ.Server.dll')`" $arguments" -WorkingDirectory $server
}
New-Item -ItemType Directory -Force -Path $DataDirectory | Out-Null
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
Write-Host "NexusMQ now starts with Windows (task '$TaskName') and is starting now."
Write-Host "  Dashboard: $Urls    Data: $DataDirectory"
Write-Host "  Remove it with .\uninstall-startup-task.ps1"
