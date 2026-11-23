<#
.SYNOPSIS
  Builds a ready-to-run NexusMQ server setup in the project's setup\ folder.
.EXAMPLE
  .\scripts\make-setup.ps1                     # portable: needs the .NET 10 runtime on the machine
  .\scripts\make-setup.ps1 -Runtime win-x64    # self-contained Windows build (no .NET needed), NexusMQ.Server.exe
#>
param(
    [string]$Runtime = "",
    [string]$Output = (Join-Path (Split-Path $PSScriptRoot -Parent) "setup")
)
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$extra = @("-p:UseAppHost=false")   # portable: run with dotnet, no platform launcher
if ($Runtime) { $extra = @("-r", $Runtime, "--self-contained", "true", "-p:PublishSingleFile=true") }
if (Test-Path $Output) { Remove-Item -Recurse -Force (Join-Path $Output "server"), (Join-Path $Output "cli") -ErrorAction SilentlyContinue }
dotnet publish (Join-Path $root "src\NexusMQ.Server") -c Release -o (Join-Path $Output "server") @extra
if ($LASTEXITCODE) { throw "publishing the server failed" }
dotnet publish (Join-Path $root "src\NexusMQ.Cli") -c Release -o (Join-Path $Output "cli") @extra
if ($LASTEXITCODE) { throw "publishing the CLI failed" }
Copy-Item (Join-Path $PSScriptRoot "setup-files\*") $Output -Force
Write-Host "NexusMQ setup ready in $Output  (start-server.cmd, then http://localhost:5680)"
