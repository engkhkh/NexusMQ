# NexusMQ server setup

Everything needed to run a NexusMQ broker on this machine.

| | |
|---|---|
| `start-server.cmd` / `start-server.sh` | Start the broker in a window. Dashboard: http://localhost:5680 |
| `nexusmq.cmd` / `nexusmq.sh` | The command-line tool: `nexusmq --url http://localhost:5680 stats` |
| `install-service.ps1` | Windows: install NexusMQ as a **Windows service** (run PowerShell **as administrator**) — recommended |
| `uninstall-service.ps1` | Windows: stop and remove the service (data is kept) |
| `install-startup-task.ps1` | Windows: alternative to the service, a task that starts NexusMQ at boot |
| `uninstall-startup-task.ps1` | Windows: remove that task (data is kept) |
| `server\appsettings.json` | Settings: access keys, data folder, cluster, storage format |

**دليل عربي خطوة بخطوة مع صور:** `docs/GUIDE-AR.md` في مجلد المشروع، أو على GitHub.

## Quick start
1. Double-click `start-server.cmd` (Linux/macOS: `./start-server.sh`).
2. Open http://localhost:5680 for the dashboard.
3. See it working: run `nexusmq demo` in a second window and watch the dashboard (orders, events, dead letters, errors).
4. Try it yourself: `nexusmq create queue orders`, then `nexusmq send orders "hello"`, then `nexusmq receive orders`.

**Other machines.** By default only this machine can connect. To accept connections from the network, start it with
`set NEXUSMQ_URLS=http://0.0.0.0:5680` (or `-Urls http://0.0.0.0:5680` for the startup task). **Set an access key first**
(`NexusMQ:ApiKey` in `server\appsettings.json`), and allow port 5680 in the firewall.

**Starting with Windows.** `install-service.ps1` installs the Windows service "NexusMQ": it starts automatically, appears in
`services.msc` (Start/Stop/Restart there or with `Stop-Service NexusMQ`), is restarted by Windows if it fails, and keeps data in
`C:\ProgramData\NexusMQ\data` (change with `-DataDirectory`). `install-startup-task.ps1` is a simpler alternative (a scheduled
task at boot); use one or the other, not both.

**Which .NET.** The `*-win-x64` and `*-linux-x64` downloads from the GitHub release include .NET (no install needed). The
"portable" setup needs the .NET 10 runtime (`dotnet --list-runtimes` shows `Microsoft.AspNetCore.App 10.x`).

More: docs/USING-IN-YOUR-PROJECT.md (connecting your apps), docs/CLUSTER.md (3 servers), docs/TLS.md (HTTPS).
