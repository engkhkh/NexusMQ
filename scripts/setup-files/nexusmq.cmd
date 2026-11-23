@echo off
rem The NexusMQ command-line tool, e.g.:  nexusmq --url http://localhost:5680 stats
if exist "%~dp0cli\nexusmq.exe" (
  "%~dp0cli\nexusmq.exe" %*
) else (
  dotnet "%~dp0cli\nexusmq.dll" %*
)
