@echo off
rem Starts NexusMQ in this window. Dashboard: http://localhost:5680  (Ctrl+C stops it)
rem Another address:  set NEXUSMQ_URLS=http://0.0.0.0:5680  (all network cards) before running this.
rem Settings: server\appsettings.json (access keys, data folder, cluster). Data is kept in server\data.
setlocal
set URLS=%NEXUSMQ_URLS%
if "%URLS%"=="" set URLS=http://localhost:5680
cd /d "%~dp0server"
if exist NexusMQ.Server.exe (
  NexusMQ.Server.exe --urls %URLS% %*
) else (
  dotnet NexusMQ.Server.dll --urls %URLS% %*
)
