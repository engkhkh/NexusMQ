# Builds the NexusMQ NuGet packages into .\nupkg
# Usage (PowerShell, from this folder):  .\pack.ps1
# Bump <Version> in Directory.Build.props first when you change the libraries.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
foreach ($p in "src/NexusMQ.Core", "src/NexusMQ.Client", "src/NexusMQ.AspNetCore", "src/NexusMQ.Cli") {
    dotnet pack $p -c Release -o nupkg
    if ($LASTEXITCODE -ne 0) { throw "pack failed for $p" }
}
Write-Host "`nPackages written to $PSScriptRoot\nupkg" -ForegroundColor Green
Get-ChildItem nupkg\*.nupkg | ForEach-Object { Write-Host "  $($_.Name)" }
