#Requires -RunAsAdministrator
<# Stops NexusMQ and removes the start-with-Windows task. The data folder is kept. #>
param([string]$TaskName = "NexusMQ")
$ErrorActionPreference = "Stop"
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $TaskName
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed task '$TaskName'. NexusMQ no longer starts with Windows (its data folder is kept)."
} else {
    Write-Host "There is no task '$TaskName'."
}
